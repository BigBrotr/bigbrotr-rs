//! Prefunded request obligations and confirmed per-quantum native dependencies.
use super::*;
use crate::{
    acquisition::{
        job::AcquisitionJob,
        quantum::{LinearWindow, QuantumError, RequestTerminal, TerminalKind},
        relay_source::RelaySourceBinding,
        session::RetainedRelayEvent,
    },
    runtime::OwnedPermit,
};
use bigbrotr_model::{AcquisitionScopeId, FactRef, JobId};
use std::sync::{Arc, Mutex};
use tokio_postgres::Transaction;

#[path = "linear_continuation.rs"]
mod continuation;
pub(in crate::storage::postgres::control_owner) use continuation::next_window;

pub struct QuantumGrant {
    pub(in crate::storage::postgres::control_owner) permit: Arc<OwnedPermit>,
}
impl QuantumGrant {
    pub fn required_data_bytes() -> u64 {
        use bigbrotr_model::acquisition::{
            Filter, MAX_FILTERS, MAX_PREIMAGE_BYTES, MAX_TOTAL_VALUES,
        };
        ProgressGrant::REQUIRED_DATA_BYTES
            + bigbrotr_protocol::EventLimits::default().wire_bytes as u64
            // A prior terminal is decoded one row at a time before retaining
            // the next plan. Fund the same bounded wire parser used at receive.
            + bigbrotr_protocol::EventLimits::default().parse_bytes as u64
            + (MAX_PREIMAGE_BYTES
                + MAX_TOTAL_VALUES * size_of::<String>()
                + MAX_FILTERS * size_of::<Filter>()
                + 2 * 3 * 256 * (size_of::<FactRef>() + 72)
                + 128 * 1024) as u64
    }
    pub fn terminal_bytes() -> u64 {
        (bigbrotr_protocol::EventLimits::default().wire_bytes + size_of::<RequestTerminal>()) as u64
    }
    pub fn new(permit: OwnedPermit) -> Result<Self, QuantumError> {
        if permit.request().units().data_bytes() < Self::required_data_bytes()
            || permit.request().units().items() == 0
        {
            return Err(QuantumError::Capacity);
        }
        Ok(Self {
            permit: Arc::new(permit),
        })
    }
}

pub struct LinearQuantum {
    pub(in crate::storage::postgres::control_owner) id: [u8; 16],
    pub(in crate::storage::postgres::control_owner) context: Context,
    pub(in crate::storage::postgres::control_owner) job: JobId,
    pub(in crate::storage::postgres::control_owner) scope: AcquisitionScopeId,
    pub(in crate::storage::postgres::control_owner) before: u64,
    pub(in crate::storage::postgres::control_owner) classified_before: u64,
    pub(in crate::storage::postgres::control_owner) window: LinearWindow,
    pub(in crate::storage::postgres::control_owner) permit: Arc<OwnedPermit>,
    pub(in crate::storage::postgres::control_owner) effects: Arc<Mutex<QuantumEffects>>,
}
impl LinearQuantum {
    pub fn filters(&self) -> &[bigbrotr_model::acquisition::Filter] {
        self.window.filters()
    }
    pub fn capture(
        &self,
        terminal: RequestTerminal,
        events: &[&RetainedRelayEvent<'_>],
        teardown_complete: bool,
    ) -> Result<QuantumCapture, QuantumError> {
        if matches!(terminal.kind, TerminalKind::Eose | TerminalKind::Closed)
            && terminal.predicate != Some(self.window.predicate_digest())
        {
            return Err(QuantumError::Context);
        }
        let mut frames = Vec::with_capacity(events.len());
        if events.len() > 3 {
            return Err(QuantumError::Context);
        }
        for event in events {
            if terminal.request != Some(event.request().id())
                || event.request().limit() != Some(3)
                || event.request().filters() != self.filters()
            {
                return Err(QuantumError::Context);
            }
            frames.push((event.sequence(), Sha256::digest(event.json()).into()));
        }
        if terminal
            .sequence
            .is_some_and(|sequence| frames.last().is_some_and(|(last, _)| sequence <= *last))
        {
            return Err(QuantumError::Context);
        }
        Ok(QuantumCapture {
            terminal,
            frames,
            teardown_complete,
        })
    }
}
pub struct QuantumCapture {
    terminal: RequestTerminal,
    frames: Vec<(u64, [u8; 32])>,
    teardown_complete: bool,
}

/// Shared terminal interpretation for capture and retained continuation reads.
/// This preserves the precedence of sticky loss over remote hints.
struct TerminalState {
    kind: TerminalKind,
    teardown: bool,
    prefix: usize,
    retained: usize,
    ordering: bool,
    finish: bool,
    more: bool,
    auth: bool,
}
impl TerminalState {
    fn gap(&self) -> Option<&'static str> {
        if !self.teardown {
            Some("teardown")
        } else if self.prefix < self.retained {
            Some("unclassified_suffix")
        } else if self.ordering {
            Some("ordering")
        } else if self.more && self.finish {
            Some("contradictory_hints")
        } else if self.auth {
            Some("auth")
        } else {
            match self.kind {
                TerminalKind::Eose => None,
                kind => Some(kind.code()),
            }
        }
    }
    fn class(&self) -> &'static str {
        if self.gap().is_some() {
            "interrupted"
        } else if self.finish {
            "relay_reported_exhaustion"
        } else {
            "linear_observed"
        }
    }
}

pub(in crate::storage::postgres::control_owner) struct QuantumEffect {
    position: Position,
    kind: u16,
    mutation: u64,
    signed_time: u64,
    event_id: bigbrotr_model::EventId,
    prefix: bool,
    confirmed: bool,
}
pub(in crate::storage::postgres::control_owner) struct QuantumEffects {
    job: JobId,
    scope: AcquisitionScopeId,
    context: Context,
    maximum_dependencies: usize,
    effects: Vec<QuantumEffect>,
    dependencies: Vec<FactRef>,
    _grant: QuantumGrant,
}
impl QuantumEffects {
    pub(in crate::storage::postgres::control_owner) fn stage(
        &mut self,
        event: BoundEvent<'_>,
        basis: &crate::storage::rocks::store::NativeCheckpointBasis<'_>,
    ) -> Result<(), ProducerProgressError> {
        let captured = event.captured;
        if captured.job != self.job
            || captured.scope != self.scope
            || basis.manifest_sha256() != self.context.manifest
            || basis.captured() != Some(captured)
            || basis.request().is_some_and(|request| {
                request.producer_slot_id != self.context.slot
                    || request.producer_epoch.get() != self.context.epoch
            })
            || self.effects.len() == 3
            || self
                .effects
                .iter()
                .any(|old| old.position.ordinal == event.position().ordinal)
        {
            return Err(ProducerProgressError::InvalidContext);
        }
        for reference in basis.dependencies() {
            if reference.source_record_id.dataset_id != self.context.dataset {
                return Err(ProducerProgressError::InvalidContext);
            }
            match self
                .dependencies
                .binary_search_by_key(&reference.source_record_id, |value| value.source_record_id)
            {
                Ok(index) if self.dependencies[index] != *reference => {
                    return Err(ProducerProgressError::InvalidContext);
                }
                Ok(_) => {}
                Err(index) if self.dependencies.len() < self.maximum_dependencies => {
                    self.dependencies.insert(index, *reference)
                }
                Err(_) => return Err(ProducerProgressError::Limit),
            }
        }
        self.effects.push(QuantumEffect {
            position: event.position(),
            kind: basis.kind() as u16,
            mutation: basis
                .request()
                .map_or(0, |request| request.request_sequence.get()),
            signed_time: captured.signed_time,
            event_id: captured.event_id,
            prefix: event.classifies_prefix(),
            confirmed: false,
        });
        Ok(())
    }
    pub(in crate::storage::postgres::control_owner) fn confirm(
        &mut self,
        position: Position,
    ) -> Result<(), ProducerProgressError> {
        if let Some(effect) = self
            .effects
            .iter_mut()
            .find(|effect| effect.position == position)
        {
            if effect.confirmed {
                return Err(ProducerProgressError::Conflict);
            }
            effect.confirmed = true;
        }
        Ok(())
    }
}

fn failure(_: tokio_postgres::Error) -> QuantumError {
    QuantumError::Backend
}
pub(in crate::storage::postgres::control_owner) async fn require_history(
    tx: &Transaction<'_>,
    native: &crate::storage::rocks::store::ProgressRead<'_>,
    context: Context,
    mut authority: impl FnMut() -> Result<(), crate::storage::rocks::store::AdmissionError>,
) -> Result<(), ProducerProgressError> {
    use futures_util::TryStreamExt;
    let enabled: bool = tx
        .query_one(
            "SELECT EXISTS(SELECT 1 FROM control.schema_versions WHERE migration=43)",
            &[],
        )
        .await
        .map_err(|_| ProducerProgressError::Backend)?
        .try_get(0)
        .map_err(|_| ProducerProgressError::Backend)?;
    if !enabled {
        return Ok(());
    }
    let valid:bool=tx.query_one("SELECT coalesce(bool_and(quantum_profile=1 AND dataset_id=$3 AND manifest_sha256=$4
        AND charged_bytes=CASE WHEN terminal_kind IS NULL THEN
            1024+octet_length(request_predicate)+2097152+3*1024+256::bigint*maximum_dependencies
            ELSE 1024+octet_length(request_predicate)+octet_length(terminal_frame)
                +octet_length(effect_records)+256::bigint*(octet_length(dependencies)/72) END),true)
        FROM control.acquisition_quanta WHERE producer_slot_id=$1 AND producer_epoch=$2::text::numeric",
        &[&&context.slot.as_bytes()[..],&context.epoch.to_string(),&&context.dataset.as_bytes()[..],&&context.manifest[..]])
        .await.map_err(|_|ProducerProgressError::Backend)?.try_get(0).map_err(|_|ProducerProgressError::Backend)?;
    if !valid {
        return Err(ProducerProgressError::InvalidContext);
    }
    let params: &[&(dyn tokio_postgres::types::ToSql + Sync)] =
        &[&&context.slot.as_bytes()[..], &context.epoch.to_string()];
    let stream=tx.query_raw("SELECT substring(q.dependencies FROM 1+72*g.i FOR 72)
        FROM control.acquisition_quanta q CROSS JOIN LATERAL generate_series(0,octet_length(q.dependencies)/72-1) g(i)
        WHERE q.producer_slot_id=$1 AND q.producer_epoch=$2::text::numeric AND q.terminal_kind IS NOT NULL
        ORDER BY q.quantum_id,g.i",params.iter().copied()).await.map_err(|_|ProducerProgressError::Backend)?;
    futures_util::pin_mut!(stream);
    let mut count = 0usize;
    while let Some(row) = stream
        .try_next()
        .await
        .map_err(|_| ProducerProgressError::Backend)?
    {
        authority().map_err(ProducerProgressError::Native)?;
        count += 1;
        if count > RETAINED_BYTES as usize / DEPENDENCY_CHARGE as usize {
            return Err(ProducerProgressError::Limit);
        }
        let bytes: &[u8] = row.try_get(0).map_err(|_| ProducerProgressError::Backend)?;
        let reference = rows::decode_dependency(bytes)?;
        if reference.source_record_id.dataset_id != context.dataset {
            return Err(ProducerProgressError::InvalidContext);
        }
        native
            .require_fact(reference, &mut authority)
            .map_err(ProducerProgressError::Native)?;
    }
    Ok(())
}
#[allow(
    clippy::too_many_arguments,
    reason = "locked job/source/native identity and separately retained quantum grant"
)]
pub(in crate::storage::postgres::control_owner) async fn prepare(
    tx: &Transaction<'_>,
    context: Context,
    job: &AcquisitionJob,
    scope: AcquisitionScopeId,
    source: RelaySourceBinding,
    window: LinearWindow,
    control: ControlOwner,
    grant: QuantumGrant,
) -> Result<LinearQuantum, QuantumError> {
    let snapshot = load(tx, context).await.map_err(|_| QuantumError::Context)?;
    if snapshot.complete != snapshot.reserved {
        return Err(QuantumError::Context);
    }
    let classified_before = snapshot
        .jobs
        .iter()
        .find(|head| head.job == job.id)
        .map_or(0, |head| head.classified);
    let maximum_dependencies = 3 * crate::storage::rocks::store::AdmissionLimits::default()
        .max_dependencies
        .max(crate::storage::rocks::store::CertificationLimits::default().max_dependencies);
    let mut id = [0; 16];
    getrandom::fill(&mut id).map_err(|_| QuantumError::Context)?;
    let charge = 1024
        + window.predicate().len() as i64
        + 2097152
        + 3 * 1024
        + 256 * maximum_dependencies as i64;
    tx.execute("INSERT INTO control.acquisition_quanta(quantum_id,quantum_number,dataset_id,job_id,scope_id,
        enrollment_generation,route_id,producer_slot_id,producer_epoch,manifest_sha256,
        issuer_resource,issuer_epoch,issuer_incarnation,interval_lower,interval_upper,horizon,
        request_predicate,predicate_sha256,maximum_dependencies,checkpoint_before,classified_before,charged_bytes,created_at_us)
        VALUES($1::bytea,(SELECT coalesce(max(q.quantum_number),0)+1 FROM control.acquisition_quanta q WHERE q.job_id=$3::bytea),$2::bytea,$3::bytea,$4::bytea,$5::text::numeric,$6::bytea,$7::bytea,$8::text::numeric,$9::bytea,$10::bytea,$11::text::numeric,$12::bytea,
        $13::text::numeric,$14::text::numeric,$15::text::numeric,$16::bytea,$17::bytea,$18,$19::text::numeric,$20::text::numeric,$21,
        (EXTRACT(EPOCH FROM clock_timestamp())*1000000)::bigint)",
        &[&&id[..],&&context.dataset.as_bytes()[..],&&job.id.as_bytes()[..],&&scope.as_bytes()[..],
            &source.enrollment_generation.get().to_string(),&&source.route_id.as_bytes()[..],&&context.slot.as_bytes()[..],
            &context.epoch.to_string(),&&context.manifest[..],&&control.resource_id.as_bytes()[..],&control.epoch.get().to_string(),
            &&control.process_incarnation.as_bytes()[..],&window.lower().to_string(),&window.upper().to_string(),&window.horizon().to_string(),
            &window.predicate(),&&window.predicate_digest()[..],&(maximum_dependencies as i32),&snapshot.revision.to_string(),
            &classified_before.to_string(),&charge]).await.map_err(failure)?;
    let mut effects = Vec::new();
    effects
        .try_reserve_exact(3)
        .map_err(|_| QuantumError::Capacity)?;
    let mut dependencies = Vec::new();
    dependencies
        .try_reserve_exact(maximum_dependencies)
        .map_err(|_| QuantumError::Capacity)?;
    Ok(LinearQuantum {
        id,
        context,
        job: job.id,
        scope,
        before: snapshot.revision,
        classified_before,
        window,
        permit: grant.permit.clone(),
        effects: Arc::new(Mutex::new(QuantumEffects {
            job: job.id,
            scope,
            context,
            maximum_dependencies,
            effects,
            dependencies,
            _grant: grant,
        })),
    })
}

pub(in crate::storage::postgres::control_owner) async fn finish(
    tx: &Transaction<'_>,
    plan: &LinearQuantum,
    capture: QuantumCapture,
) -> Result<(), QuantumError> {
    let snapshot = load(tx, plan.context)
        .await
        .map_err(|_| QuantumError::Context)?;
    if snapshot.complete != snapshot.reserved || snapshot.revision < plan.before {
        return Err(QuantumError::Context);
    }
    let (request, prefix, records, dependencies, continuation, class, gap, digest, charge) = {
        let effects = plan.effects.lock().map_err(|_| QuantumError::Context)?;
        if effects.effects.iter().any(|effect| !effect.confirmed) {
            return Err(QuantumError::Context);
        }
        let request = capture
            .terminal
            .request
            .map(|id| id.as_str().as_bytes().to_vec());
        for effect in &effects.effects {
            let index = usize::from(effect.position.ordinal)
                .checked_sub(1)
                .ok_or(QuantumError::Context)?;
            if capture.frames.get(index)
                != Some(&(effect.position.wire_sequence, effect.position.input_digest))
                || request.as_deref() != Some(&effect.position.request[..])
            {
                return Err(QuantumError::Context);
            }
        }
        let mut prefix = 0;
        for ordinal in 1..=capture.frames.len() {
            if effects
                .effects
                .iter()
                .any(|effect| usize::from(effect.position.ordinal) == ordinal && effect.prefix)
            {
                prefix += 1;
            } else {
                break;
            }
        }
        let head = snapshot.jobs.iter().find(|head| head.job == plan.job);
        if head.map_or(0, |head| head.classified) != plan.classified_before + prefix as u64
            || (prefix > 0
                && head.is_none_or(|head| {
                    head.scope != plan.scope
                        || head.position
                            != effects
                                .effects
                                .iter()
                                .find(|effect| usize::from(effect.position.ordinal) == prefix)
                                .expect("prefix")
                                .position
                }))
        {
            return Err(QuantumError::Context);
        }
        let mut dependencies = Vec::with_capacity(effects.dependencies.len() * 72);
        for reference in &effects.dependencies {
            dependencies.extend_from_slice(&encode_dependency(*reference));
        }
        let mut records = Vec::with_capacity(effects.effects.len() * 128);
        for effect in &effects.effects {
            records.extend_from_slice(&effect.position.ordinal.to_be_bytes());
            records.extend_from_slice(&effect.position.wire_sequence.to_be_bytes());
            records.extend_from_slice(&effect.position.input_digest);
            records.extend_from_slice(&effect.kind.to_be_bytes());
            records.extend_from_slice(&effect.mutation.to_be_bytes());
            records.extend_from_slice(&effect.signed_time.to_be_bytes());
            records.push(u8::from(effect.prefix));
            records.extend_from_slice(effect.event_id.as_bytes());
        }
        let oldest = effects
            .effects
            .iter()
            .filter(|effect| matches!(effect.kind, 2 | 3 | 5))
            .map(|effect| effect.signed_time)
            .min();
        let ordering_gap = effects
            .effects
            .iter()
            .filter(|effect| matches!(effect.kind, 2 | 3 | 5))
            .map(|effect| effect.signed_time)
            .collect::<Vec<_>>()
            .windows(2)
            .any(|pair| pair[1] > pair[0]);
        let terminal = TerminalState {
            kind: capture.terminal.kind,
            teardown: capture.teardown_complete,
            prefix,
            retained: capture.frames.len(),
            ordering: ordering_gap,
            finish: capture.terminal.finish,
            more: capture.terminal.more,
            auth: capture.terminal.auth,
        };
        let gap = terminal.gap();
        // A lost/unclassified/ordering suffix keeps the original pending upper;
        // successful later mutations cannot move a cursor past that obligation.
        let continuation = if gap.is_some() {
            plan.window.upper()
        } else {
            plan.window.continuation(oldest)
        };
        let class = terminal.class();
        let digest = (snapshot.revision > 0).then(|| super::digest(plan.context, &snapshot));
        let charge = 1024
            + plan.window.predicate().len() as i64
            + capture.terminal.raw.len() as i64
            + records.len() as i64
            + 256 * effects.dependencies.len() as i64;
        (
            request,
            prefix,
            records,
            dependencies,
            continuation,
            class,
            gap,
            digest,
            charge,
        )
    };
    let kind = capture.terminal.kind.code();
    let changed=tx.execute("UPDATE control.acquisition_quanta SET terminal_kind=$2,terminal_frame=$3,request_id=$4,
        terminal_sequence=$5::text::numeric,terminal_received_at_us=$6,teardown_complete=$7,retained_events=$8,
        classified_prefix=$9,effect_records=$10,dependencies=$11,checkpoint_after=$12::text::numeric,checkpoint_sha256=$13::bytea,
        result_class=$14,gap_reason=$15,continuation_upper=$16::text::numeric,charged_bytes=$17,
        completed_at_us=(EXTRACT(EPOCH FROM clock_timestamp())*1000000)::bigint
        WHERE quantum_id=$1 AND terminal_kind IS NULL",
        &[&&plan.id[..],&kind,&capture.terminal.raw,&request,&capture.terminal.sequence.map(|n|n.to_string()),
            &capture.terminal.received_at_us,&capture.teardown_complete,&(capture.frames.len() as i16),&(prefix as i16),
            &records,&dependencies,&snapshot.revision.to_string(),&digest.as_ref().map(|value|value.as_slice()),
            &class,&gap,&continuation.to_string(),&charge]).await.map_err(failure)?;
    if changed != 1 {
        return Err(QuantumError::Context);
    }
    Ok(())
}
