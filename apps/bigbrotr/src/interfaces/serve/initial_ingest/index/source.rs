//! The two explicitly selected native lifetimes, never a reopened source store.
use super::super::owner::NativeOwner;
use super::*;
use bigbrotr::storage::{
    postgres::control_owner::{LiveInitialSource, LiveInitialWriter},
    rocks::store::{PendingBatch, SourceHead},
};

pub(in crate::interfaces::serve::initial_ingest) enum Source<'a> {
    Initial(&'a IngestStore),
    Live {
        writer: &'a LiveInitialWriter,
        view: LiveInitialSource<'a>,
    },
}

/// Fixed-size inert observation: one current native stream and, only on the
/// borrowed live path, the actual handoff's immutable closed predecessor.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(in crate::interfaces::serve::initial_ingest) struct Observed {
    head: SourceHead,
    predecessor: Option<StreamBound>,
    history_within_limit: bool,
}
impl Observed {
    pub(in crate::interfaces::serve::initial_ingest) fn initial(head: SourceHead) -> Self {
        Self {
            head,
            predecessor: None,
            history_within_limit: true,
        }
    }
    pub(in crate::interfaces::serve::initial_ingest) fn manifest(&self) -> &IngestManifest {
        self.head.manifest()
    }
    pub(in crate::interfaces::serve::initial_ingest) fn identity(&self) -> &ManifestIdentity {
        self.head.identity()
    }
    pub(in crate::interfaces::serve::initial_ingest) fn through(&self) -> CompleteThrough {
        self.head.through()
    }
    pub(in crate::interfaces::serve::initial_ingest) fn bound(&self) -> StreamBound {
        self.head.bound()
    }
    pub(in crate::interfaces::serve::initial_ingest) fn cut(self) -> Result<SourceCut> {
        let mut bounds = Vec::with_capacity(if self.predecessor.is_some() { 2 } else { 1 });
        bounds.extend(self.predecessor);
        bounds.push(self.head.bound());
        SourceCut::new(
            self.identity().dataset_id,
            self.identity().topology_generation,
            bounds,
        )
        .map_err(|_| failed("source vector invalid"))
    }
    pub(in crate::interfaces::serve::initial_ingest) fn matches(self, cut: &SourceCut) -> bool {
        self.history_within_limit
            && source_matches(self.manifest(), self.through(), self.predecessor, cut)
    }
    pub(in crate::interfaces::serve::initial_ingest) fn within_history_limit(self) -> bool {
        self.history_within_limit
    }
}

pub(in crate::interfaces::serve::initial_ingest) fn source_matches(
    manifest: &IngestManifest,
    through: CompleteThrough,
    predecessor: Option<StreamBound>,
    cut: &SourceCut,
) -> bool {
    let id = manifest.identity();
    let current = StreamBound {
        stream: StreamId {
            shard_id: id.shard_id,
            owner_epoch: id.owner_epoch,
        },
        last_complete_sequence: through,
    };
    cut.dataset_id() == id.dataset_id
        && cut.topology_generation() == id.topology_generation
        && match predecessor {
            None => {
                manifest.key_schema() == 1 && id.owner_epoch.get() == 1 && cut.bounds() == [current]
            }
            Some(old) => {
                manifest.key_schema() == 2
                    && id.owner_epoch.get() == 2
                    && id.topology_generation.get() == 1
                    && old.stream.shard_id == id.shard_id
                    && old.stream.owner_epoch.get() == 1
                    && cut.bounds() == [old, current]
            }
        }
}

impl<'a> Source<'a> {
    pub(super) async fn require_current(
        &self,
        control: &mut ControlSession,
        lease: &mut PolicyLeaseTracker,
        session: &IndexSession,
        state: &IndexState,
        cancelled: &AtomicBool,
    ) -> Result<()> {
        if let Self::Live { writer, .. } = self {
            control
                .require_live_index_source(session, state.custody(), writer, lease, cancelled)
                .await
                .map_err(|_| failed("current live source authority unavailable"))?;
        }
        Ok(())
    }
    pub(super) async fn prepare(
        &self,
        control: &mut ControlSession,
        lease: &mut PolicyLeaseTracker,
        session: &IndexSession,
        state: &IndexState,
        cancelled: &AtomicBool,
        planned: &PlannedBatch,
    ) -> Result<PreparedIndexPlan> {
        let commit = Cell::new(false);
        match self {
            Self::Initial(_) => {
                control
                    .prepare_index_plan(
                        session,
                        state.custody(),
                        planned.plan(),
                        planned.source(),
                        &commit,
                    )
                    .await
            }
            Self::Live { writer, .. } => {
                control
                    .prepare_live_index_plan(
                        session,
                        state.custody(),
                        planned.plan(),
                        planned.source(),
                        &commit,
                        writer,
                        lease,
                        cancelled,
                    )
                    .await
            }
        }
        .map_err(|_| failed("PG plan preparation failed"))
    }
    #[allow(clippy::too_many_arguments)]
    pub(super) async fn publish(
        &self,
        control: &mut ControlSession,
        lease: &mut PolicyLeaseTracker,
        session: &IndexSession,
        state: &IndexState,
        cancelled: &AtomicBool,
        prepared: &PreparedIndexPlan,
        proof: &VerifiedIndexPlan,
    ) -> Result<bigbrotr::storage::postgres::IndexPublicationReceipt> {
        let commit = Cell::new(false);
        match self {
            Self::Initial(_) => {
                control
                    .publish_index_plan(session, state.custody(), prepared, proof, &commit)
                    .await
            }
            Self::Live { writer, .. } => {
                control
                    .publish_live_index_plan(
                        session,
                        state.custody(),
                        prepared,
                        proof,
                        &commit,
                        writer,
                        lease,
                        cancelled,
                    )
                    .await
            }
        }
        .map_err(|_| failed("PG publication failed"))
    }
    pub(in crate::interfaces::serve::initial_ingest) fn from_owner(
        owner: &'a NativeOwner,
    ) -> Result<Self> {
        match owner {
            NativeOwner::Initial(store) => Ok(Self::Initial(store)),
            NativeOwner::Live(writer) => Ok(Self::Live {
                writer,
                view: writer
                    .index_source()
                    .map_err(|_| failed("live source unavailable"))?,
            }),
            _ => Err(failed("no selected initial Index source capability")),
        }
    }
    pub(in crate::interfaces::serve::initial_ingest) fn manifest(&self) -> &IngestManifest {
        match self {
            Self::Initial(store) => store.manifest(),
            Self::Live { view, .. } => view.manifest(),
        }
    }
    pub(in crate::interfaces::serve::initial_ingest) fn observe(
        &self,
        cancelled: &AtomicBool,
    ) -> Result<Observed> {
        match self {
            Self::Initial(store) => store
                .observe_source_head(store.manifest(), cancelled)
                .map(Observed::initial)
                .map_err(|_| failed("source observation unavailable")),
            Self::Live { view, .. } => Ok(Observed {
                head: view
                    .observe(cancelled)
                    .map_err(|_| failed("live source observation unavailable"))?,
                predecessor: Some(StreamBound {
                    stream: StreamId {
                        shard_id: view.manifest().identity().shard_id,
                        owner_epoch: OwnerEpoch::new(1).unwrap(),
                    },
                    last_complete_sequence: CompleteThrough::new(
                        view.handoff().last_complete_sequence(),
                    ),
                }),
                history_within_limit: view
                    .retained_requests()
                    .map_err(|_| failed("live request census unavailable"))?
                    <= view.history_request_limit(),
            }),
        }
    }
    pub(in crate::interfaces::serve::initial_ingest) fn within_history_limit(
        &self,
    ) -> Result<bool> {
        match self {
            Self::Initial(_) => Ok(true),
            Self::Live { view, .. } => Ok(view
                .retained_requests()
                .map_err(|_| failed("live request census unavailable"))?
                <= view.history_request_limit()),
        }
    }
    pub(in crate::interfaces::serve::initial_ingest) fn next(
        &self,
        cut: &SourceCut,
    ) -> Result<SourceRecordId> {
        match self {
            Self::Initial(store) => next_source(*store.manifest().identity(), cut),
            Self::Live { view, .. } => {
                let id = *view.manifest().identity();
                let through = cut
                    .bounds()
                    .last()
                    .ok_or_else(|| failed("live cut missing"))?
                    .last_complete_sequence;
                let predecessor = StreamBound {
                    stream: StreamId {
                        shard_id: id.shard_id,
                        owner_epoch: OwnerEpoch::new(1).unwrap(),
                    },
                    last_complete_sequence: CompleteThrough::new(
                        view.handoff().last_complete_sequence(),
                    ),
                };
                if !source_matches(view.manifest(), through, Some(predecessor), cut) {
                    return Err(failed("live cut changed predecessor"));
                }
                Ok(SourceRecordId {
                    dataset_id: id.dataset_id,
                    shard_id: id.shard_id,
                    owner_epoch: id.owner_epoch,
                    sequence: Sequence::new(
                        through
                            .get()
                            .checked_add(1)
                            .ok_or_else(|| failed("source exhausted"))?,
                    )
                    .map_err(|_| failed("source exhausted"))?,
                })
            }
        }
    }
    pub(in crate::interfaces::serve::initial_ingest) fn read(
        &self,
        first: SourceRecordId,
        limits: PendingReadLimits,
        permit: OwnedPermit,
        cancelled: &AtomicBool,
    ) -> Result<Option<PendingBatch>> {
        match self {
            Self::Initial(store) => store.read_pending_batch(first, limits, permit, cancelled),
            Self::Live { view, .. } => view.read_pending_batch(first, limits, permit, cancelled),
        }
        .map_err(|_| failed("retained source batch unavailable"))
    }
}
