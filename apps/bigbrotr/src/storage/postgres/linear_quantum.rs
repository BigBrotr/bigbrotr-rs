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
use continuation::page_result;
pub(in crate::storage::postgres::control_owner) use continuation::{
    Traversal, next_window, observe_status,
};

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
            // Application terminal decode uses the same bounded wire parser.
            // Driver chunk/backing retention belongs to the held Control grant,
            // including its explicit 128 MiB buffered SQL receive allowance.
            + bigbrotr_protocol::EventLimits::default().parse_bytes as u64
            // Streaming rows retain bounded predicate bytes beside terminal bytes.
            + bigbrotr_protocol::EventLimits::default().wire_bytes as u64
            + (MAX_PREIMAGE_BYTES
                + MAX_TOTAL_VALUES * size_of::<String>()
                + MAX_FILTERS * size_of::<Filter>()
                + 2 * 3 * 256 * (size_of::<FactRef>() + 72)
                + 128 * 1024
                + continuation::MAX_BOUNDARY_IDS * size_of::<bigbrotr_model::EventId>()
                + size_of::<Traversal>()) as u64
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
    traversal: Mutex<Traversal>,
    pub(in crate::storage::postgres::control_owner) permit: Arc<OwnedPermit>,
    pub(in crate::storage::postgres::control_owner) effects: Arc<Mutex<QuantumEffects>>,
}
impl LinearQuantum {
    pub fn filters(&self) -> &[bigbrotr_model::acquisition::Filter] {
        self.window.filters()
    }
    pub(crate) fn profile(&self) -> Result<(u64, u16), QuantumError> {
        let traversal = self.traversal.lock().map_err(|_| QuantumError::Context)?;
        Ok((traversal.geometry.width, traversal.geometry.budget))
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
    let valid:bool=tx.query_one("SELECT coalesce(bool_and(quantum_profile=2 AND dataset_id=$3 AND manifest_sha256=$4
        AND charged_bytes=CASE WHEN entry_kind='selector_exclusion' THEN 1024 WHEN terminal_kind IS NULL THEN
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
        WHERE q.producer_slot_id=$1 AND q.producer_epoch=$2::text::numeric AND q.entry_kind='request' AND q.terminal_kind IS NOT NULL
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
    let stream = tx
        .query_raw(
            "SELECT interval_lower::text,interval_upper::text,
        retained_events,classified_prefix,substring(effect_records FROM 1 FOR 280),
        substring(dependencies FROM 1 FOR 55297) FROM control.acquisition_quanta
        WHERE producer_slot_id=$1 AND producer_epoch=$2::text::numeric
        AND entry_kind='request' AND terminal_kind IS NOT NULL ORDER BY job_id,quantum_number",
            params.iter().copied(),
        )
        .await
        .map_err(|_| ProducerProgressError::Backend)?;
    futures_util::pin_mut!(stream);
    while let Some(row) = stream
        .try_next()
        .await
        .map_err(|_| ProducerProgressError::Backend)?
    {
        authority().map_err(ProducerProgressError::Native)?;
        continuation::authenticate_saved_effects(
            native,
            row.try_get(4).map_err(|_| ProducerProgressError::Backend)?,
            row.try_get(2).map_err(|_| ProducerProgressError::Backend)?,
            row.try_get(3).map_err(|_| ProducerProgressError::Backend)?,
            rows::number(&row, 0)?,
            rows::number(&row, 1)?,
            row.try_get(5).map_err(|_| ProducerProgressError::Backend)?,
            &mut authority,
        )
        .map_err(|error| match error {
            QuantumError::Native(error) => ProducerProgressError::Native(error),
            _ => ProducerProgressError::InvalidContext,
        })?;
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
    mut traversal: Traversal,
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
    traversal.geometry.request(
        window.lower(),
        window.upper(),
        traversal.geometry.window_end,
        traversal.geometry.spent + 1,
    )?;
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
        issuer_resource,issuer_epoch,issuer_incarnation,interval_lower,interval_upper,horizon,window_end,window_width,query_budget,query_number,
        request_predicate,predicate_sha256,maximum_dependencies,checkpoint_before,classified_before,charged_bytes,created_at_us)
        VALUES($1::bytea,(SELECT coalesce(max(q.quantum_number),0)+1 FROM control.acquisition_quanta q WHERE q.job_id=$3::bytea),$2::bytea,$3::bytea,$4::bytea,$5::text::numeric,$6::bytea,$7::bytea,$8::text::numeric,$9::bytea,$10::bytea,$11::text::numeric,$12::bytea,
        $13::text::numeric,$14::text::numeric,$15::text::numeric,$22::text::numeric,$23::text::numeric,$24,$25,$16::bytea,$17::bytea,$18,$19::text::numeric,$20::text::numeric,$21,
        (EXTRACT(EPOCH FROM clock_timestamp())*1000000)::bigint)",
        &[&&id[..],&&context.dataset.as_bytes()[..],&&job.id.as_bytes()[..],&&scope.as_bytes()[..],
            &source.enrollment_generation.get().to_string(),&&source.route_id.as_bytes()[..],&&context.slot.as_bytes()[..],
            &context.epoch.to_string(),&&context.manifest[..],&&control.resource_id.as_bytes()[..],&control.epoch.get().to_string(),
            &&control.process_incarnation.as_bytes()[..],&window.lower().to_string(),&window.upper().to_string(),&window.horizon().to_string(),
            &window.predicate(),&&window.predicate_digest()[..],&(maximum_dependencies as i32),&snapshot.revision.to_string(),
            &classified_before.to_string(),&charge,&traversal.geometry.window_end.to_string(),
            &traversal.geometry.width.to_string(),&(traversal.geometry.budget as i16),&(traversal.geometry.spent as i16)]).await.map_err(failure)?;
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
        traversal: Mutex::new(traversal),
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

#[allow(
    clippy::too_many_arguments,
    reason = "same locked authority and native history as request planning"
)]
pub(in crate::storage::postgres::control_owner) async fn exclude(
    tx: &Transaction<'_>,
    context: Context,
    job: &AcquisitionJob,
    scope: AcquisitionScopeId,
    source: RelaySourceBinding,
    control: ControlOwner,
    traversal: &mut Traversal,
    span: crate::acquisition::quantum::Exclusion,
) -> Result<(), QuantumError> {
    let snapshot = load(tx, context).await.map_err(|_| QuantumError::Context)?;
    if snapshot.complete != snapshot.reserved
        || traversal.entries as usize >= 3 * usize::from(traversal.geometry.budget) + 1
    {
        return Err(QuantumError::Context);
    }
    let classified = snapshot
        .jobs
        .iter()
        .find(|head| head.job == job.id)
        .map_or(0, |head| head.classified);
    let digest = (snapshot.revision > 0).then(|| super::digest(context, &snapshot));
    let mut id = [0; 16];
    getrandom::fill(&mut id).map_err(|_| QuantumError::Context)?;
    let disposition = if span.empty_target {
        "empty_target"
    } else {
        "selector_excluded"
    };
    tx.execute("INSERT INTO control.acquisition_quanta(quantum_id,quantum_number,entry_kind,dataset_id,
        job_id,scope_id,enrollment_generation,route_id,producer_slot_id,producer_epoch,manifest_sha256,
        issuer_resource,issuer_epoch,issuer_incarnation,interval_lower,interval_upper,horizon,window_end,
        window_width,query_budget,query_number,maximum_dependencies,checkpoint_before,classified_before,
        charged_bytes,created_at_us,checkpoint_after,checkpoint_sha256,disposition,result_class,completed_at_us)
        VALUES($1,$2::text::numeric,'selector_exclusion',$3,$4,$5,$6::text::numeric,$7,$8,$9::text::numeric,$10,
        $11,$12::text::numeric,$13,$14::text::numeric,$15::text::numeric,$16::text::numeric,$17::text::numeric,
        $18::text::numeric,$19,$20,0,$21::text::numeric,$22::text::numeric,1024,
        (EXTRACT(EPOCH FROM clock_timestamp())*1000000)::bigint,$21::text::numeric,$23,$24,$24,
        (EXTRACT(EPOCH FROM clock_timestamp())*1000000)::bigint)",
        &[&&id[..],&(traversal.entries+1).to_string(),&&context.dataset.as_bytes()[..],&&job.id.as_bytes()[..],
        &&scope.as_bytes()[..],&source.enrollment_generation.get().to_string(),&&source.route_id.as_bytes()[..],
        &&context.slot.as_bytes()[..],&context.epoch.to_string(),&&context.manifest[..],&&control.resource_id.as_bytes()[..],
        &control.epoch.get().to_string(),&&control.process_incarnation.as_bytes()[..],&span.lower.to_string(),
        &span.upper.to_string(),&traversal.geometry.horizon.to_string(),&span.window_end.to_string(),
        &traversal.geometry.width.to_string(),&(traversal.geometry.budget as i16),&(traversal.geometry.spent as i16),
        &snapshot.revision.to_string(),&classified.to_string(),&digest.as_ref().map(|v|v.as_slice()),&disposition])
        .await.map_err(failure)?;
    traversal.geometry.exclude(span)?;
    traversal.entries += 1;
    Ok(())
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
    let (request, prefix, records, dependencies, settlement, class, gap, digest, charge) = {
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
        let mut traversal = plan.traversal.lock().map_err(|_| QuantumError::Context)?;
        let mut targets = [None; 3];
        for effect in &effects.effects {
            if matches!(effect.kind, 2 | 3 | 5) {
                targets[usize::from(effect.position.ordinal) - 1] =
                    Some((effect.event_id, effect.signed_time));
            }
        }
        let geometry = traversal.geometry;
        let settlement = traversal.boundary.settle(&geometry, &terminal, &targets)?;
        let (class, gap) = page_result(&terminal, &targets, &settlement);
        traversal.geometry.finish(settlement)?;
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
            settlement,
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
        disposition=$18,gap_lower=$19::text::numeric,gap_upper=$20::text::numeric,
        completed_at_us=(EXTRACT(EPOCH FROM clock_timestamp())*1000000)::bigint
        WHERE quantum_id=$1 AND entry_kind='request' AND terminal_kind IS NULL",
        &[&&plan.id[..],&kind,&capture.terminal.raw,&request,&capture.terminal.sequence.map(|n|n.to_string()),
            &capture.terminal.received_at_us,&capture.teardown_complete,&(capture.frames.len() as i16),&(prefix as i16),
            &records,&dependencies,&snapshot.revision.to_string(),&digest.as_ref().map(|value|value.as_slice()),
            &class,&gap,&settlement.continuation.to_string(),&charge,&settlement.disposition,
            &settlement.gap.map(|(a,_)|a.to_string()),&settlement.gap.map(|(_,b)|b.to_string())]).await.map_err(failure)?;
    if changed != 1 {
        return Err(QuantumError::Context);
    }
    Ok(())
}
