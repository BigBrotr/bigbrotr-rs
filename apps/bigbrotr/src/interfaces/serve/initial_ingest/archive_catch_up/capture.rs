//! Join the current native cut while its actual catalog barrier remains live.
use super::super::archive::native;
use super::*;
use bigbrotr::storage::{
    archive::{RetainedFrontierIntent, RetainedInputs},
    postgres::{
        catalog_guard::{self, CatalogCapture},
        control_owner::ArchiveFrontierReceipt,
    },
};
use bigbrotr_protocol::archive_cut::CutRead;

pub(super) struct Context<'a, 'lineage> {
    pub(super) root: &'a Resolved,
    pub(super) budget: &'a ResourceBudget,
    pub(super) native: &'a ResourceBudget,
    pub(super) reader: &'a Arc<native::Reader>,
    pub(super) proof: &'a VerifiedSuccessorHistory<'lineage>,
    pub(super) coordinator: &'a LocalCoordinator,
}
pub(super) struct Current<'a> {
    pub(super) receipt: ArchiveFrontierReceipt,
    pub(super) cut: &'a CutRead,
}
pub(super) enum Action<'a> {
    Observe,
    Publish {
        inputs: &'a [RetainedInputs],
        current: Option<Current<'a>>,
    },
}
pub(super) struct Outcome {
    pub(super) snapshot: u64,
    pub(super) family_counts: [u64; 5],
    pub(super) receipt: Option<ArchiveFrontierReceipt>,
    pub(super) created: bool,
}

impl Context<'_, '_> {
    pub(super) async fn prepare_manager(
        &self,
        authority: &mut Authority<'_>,
        owner: &mut ControlSession,
        deadline: Instant,
    ) -> Result<(PreparedConnection, OwnedPermit)> {
        let root = self.root.clone();
        let app = self.budget.clone();
        authority
            .blocking(owner, deadline, move |_| {
                postgres_command::prepare_catalog_as(
                    &root,
                    required(&root, "postgres.catalog.user")?,
                    required(&root, "postgres.catalog.password_ref")?,
                    &app,
                )
            })
            .await
    }

    pub(super) async fn run(
        &self,
        authority: &mut Authority<'_>,
        owner: &mut ControlSession,
        mut read: CutRead,
        action: Action<'_>,
    ) -> Result<Outcome> {
        authority.refresh(owner).await?;
        authority.local(|cancel| self.proof.validate(cancel).map_err(error))?;
        let deadline = Instant::now() + OPERATION;
        let prepared = self.prepare_manager(authority, owner, deadline).await?;
        let (mut manager, connection) = authority
            .external(
                owner,
                deadline,
                native::connect_manager(prepared.0, self.reader),
            )
            .await?;
        let connection = Box::pin(
            self.coordinator
                .retain_connection(connection)
                .map_err(error)?,
        );
        let result = native::drive(connection, async {
            let result = async {
                authority
                    .external(owner, deadline, async {
                        catalog_guard::inspect_reader(
                            &mut manager,
                            self.coordinator.identity(),
                            required(self.root, "postgres.catalog.read_user")?,
                        )
                        .await
                        .map_err(error)
                    })
                    .await?;
                let capture = authority
                    .external(owner, deadline, async {
                        CatalogCapture::open(
                            &mut manager,
                            self.coordinator,
                            deadline.saturating_duration_since(Instant::now()),
                        )
                        .await
                        .map_err(error)
                    })
                    .await?;
                read.snapshot_id = Some(capture.snapshot_id());
                let custody = self.coordinator.reader_custody().map_err(error)?;
                let (root, app, native, reader) = (
                    self.root.clone(),
                    self.budget.clone(),
                    self.native.clone(),
                    self.reader.clone(),
                );
                let observed = authority
                    .blocking(owner, deadline, move |cancel| {
                        native::cut_with_scan_limit(
                            root,
                            app,
                            native,
                            reader,
                            custody,
                            read,
                            MAX_ATTEMPTS,
                            deadline,
                            cancel,
                        )
                    })
                    .await?;
                let captured = authority
                    .external(owner, deadline, async {
                        capture.accept(observed).await.map_err(error)
                    })
                    .await?;
                let observed = captured.observed();
                let mut outcome = Outcome {
                    snapshot: observed.observation().snapshot_id,
                    family_counts: observed.observation().family_counts,
                    receipt: None,
                    created: false,
                };
                if let Action::Publish { inputs, current } = action {
                    let identical = current.as_ref().filter(|current| {
                        current.cut == observed.read()
                            && current.receipt.snapshot_id == observed.observation().snapshot_id
                            && current.receipt.request_digest
                                == observed.observation().request_digest
                            && current.receipt.family_counts == observed.observation().family_counts
                    });
                    if let Some(current) = identical {
                        // No fresh operation, Archive role or revision exists on
                        // this path. Actual native membership was still joined.
                        outcome.receipt = Some(current.receipt);
                    } else {
                        authority.refresh(owner).await?;
                        let operation =
                            super::super::super::fresh_process_incarnation().map_err(error)?;
                        let revision = current.as_ref().map_or(0, |c| c.receipt.revision);
                        let intent = authority.local(|cancel| {
                            self.proof.validate(cancel).map_err(error)?;
                            self.coordinator
                                .retain_frontier_intent(
                                    *operation.as_bytes(),
                                    revision,
                                    observed,
                                    grant(
                                        self.budget,
                                        RetainedFrontierIntent::required_data_bytes(),
                                        0,
                                    )?,
                                    cancel,
                                )
                                .map_err(error)
                        })?;
                        // The synchronized original survives every subsequent
                        // failure, including an unknown Control COMMIT delivery.
                        authority.refresh(owner).await?;
                        authority.local(|cancel| self.proof.validate(cancel).map_err(error))?;
                        let receipt = authority
                            .operation(owner.publish_archive_frontier(
                                &captured,
                                inputs,
                                intent.operation_id(),
                                revision,
                                &Cell::new(false),
                            ))
                            .await?;
                        if !intent.matches_receipt(&receipt) {
                            return Err(error(
                                "frontier delivery differs from synchronized original intent",
                            ));
                        }
                        outcome.created = receipt.created;
                        outcome.receipt = Some(receipt);
                    }
                }
                authority.refresh(owner).await?;
                authority.local(|cancel| self.proof.validate(cancel).map_err(error))?;
                authority
                    .external(owner, deadline, async {
                        captured.verify_barrier().await.map_err(error)
                    })
                    .await?;
                drop(captured);
                authority
                    .external(owner, deadline, async {
                        capture.finish().await.map_err(error)
                    })
                    .await?;
                Ok(outcome)
            }
            .await;
            drop(manager);
            result
        })
        .await;
        drop(prepared.1);
        result
    }
}
