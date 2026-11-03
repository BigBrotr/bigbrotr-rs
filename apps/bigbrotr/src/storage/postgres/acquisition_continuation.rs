//! Sealed finite-job segments. A seal is retained evidence, never a writer.
use super::producer_progress::{
    Context,
    quantum::{self, QuantumGrant},
};
use super::*;
use crate::{
    acquisition::{job::AcquisitionJob, quantum::QuantumError, relay_source::RelaySourceBinding},
    storage::rocks::{
        manifest::IngestManifest,
        store::{IngestRecovery, ProgressRead, ResolvedActivatedSource, ResolvedSource},
    },
};
use bigbrotr_model::{JobId, ProducerSlotId};
use std::sync::atomic::AtomicBool;

/// Only complete positively resolved source capabilities select a seal. No
/// serialized closure or SQL row can construct any of these witnesses.
pub enum AcquisitionContinuationSource<'a, 'native> {
    Initial(&'a ResolvedSource<'native>),
    Activated(&'a ResolvedActivatedSource<'native>),
    Successor(&'a ResolvedActivatedSuccessorSource<'native>),
}
struct Source<'a> {
    recovery: &'a IngestRecovery,
    manifest: &'a IngestManifest,
    slot: ProducerSlotId,
    sequence: u64,
    digest: [u8; 32],
    requests: u64,
}
impl AcquisitionContinuationSource<'_, '_> {
    fn checked(&self, cancelled: &AtomicBool) -> Result<Source<'_>, ProducerCloseError> {
        let source = match self {
            Self::Initial(s) => {
                s.recovery_acknowledgement()
                    .map_err(ProducerCloseError::Native)?;
                Source {
                    recovery: s.recovery(),
                    manifest: s.producer().manifest(),
                    slot: s.producer().producer_slot_id(),
                    sequence: s.last_complete_sequence(),
                    digest: s.digest(),
                    requests: s.resolved_requests(),
                }
            }
            Self::Activated(s) => {
                // This increment retains R=0 and every original input.
                if s.has_compacted_initial() {
                    return Err(OwnerError::InvalidInput.into());
                }
                s.recovery_acknowledgement()
                    .map_err(ProducerCloseError::Native)?;
                Source {
                    recovery: s.recovery(),
                    manifest: s.manifest(),
                    slot: s.producer_slot_id(),
                    sequence: s.last_complete_sequence(),
                    digest: s.digest(),
                    requests: s.resolved_requests(),
                }
            }
            Self::Successor(s) => {
                s.validate(cancelled).map_err(ProducerCloseError::Native)?;
                let last = s.last_epoch();
                let recovery = s.continuation_recovery();
                Source {
                    recovery,
                    manifest: recovery.manifest(),
                    slot: s.continuation_slot(),
                    sequence: last.last_complete_sequence,
                    digest: last.digest,
                    requests: last.resolved_requests,
                }
            }
        };
        if source.requests > 64 {
            return Err(OwnerError::InvalidInput.into());
        }
        Ok(source)
    }
}
fn context(manifest: &IngestManifest, slot: ProducerSlotId) -> Context {
    let id = manifest.identity();
    Context {
        dataset: id.dataset_id,
        slot,
        epoch: id.owner_epoch.get(),
        shard: id.shard_id,
        manifest: Sha256::digest(manifest.encode().as_bytes()).into(),
    }
}
fn qerror(_: impl fmt::Debug) -> QuantumError {
    QuantumError::Context
}

/// One complete global history fold under the actual retained native view.
/// The current job tuple supplies historical attribution, never fresh authority.
pub(super) async fn authenticate(
    tx: &Transaction<'_>,
    read: &ProgressRead<'_>,
    context: Context,
    job: &AcquisitionJob,
    grant: &QuantumGrant,
    authority: &mut impl FnMut() -> Result<(), crate::storage::rocks::store::AdmissionError>,
) -> Result<(RelaySourceBinding, u64), QuantumError> {
    let scope = acquisition_read::load_shared(tx, context.dataset, job.scope, grant.permit.clone())
        .await
        .map_err(qerror)?;
    let row=tx.query_one("SELECT e.generation::text,e.route_id, \
        coalesce((SELECT window_width FROM control.acquisition_quanta WHERE job_id=$2 ORDER BY quantum_number LIMIT 1), \
            (SELECT (document->'settings'->>'sync.window_width')::numeric FROM control.config_revisions WHERE dataset_id=$1 AND revision=$3::text::numeric))::text, \
        coalesce((SELECT query_budget::numeric FROM control.acquisition_quanta WHERE job_id=$2 ORDER BY quantum_number LIMIT 1), \
            (SELECT (document->'settings'->>'sync.partition_budget')::numeric FROM control.config_revisions WHERE dataset_id=$1 AND revision=$3::text::numeric))::text, \
        EXISTS(SELECT 1 FROM control.acquisition_quanta WHERE job_id=$2) \
        FROM control.relay_enrollments e WHERE e.dataset_id=$1 AND e.endpoint_id=$4 AND e.access_scope_id=$5 AND e.enabled",
        &[&&context.dataset.as_bytes()[..],&&job.id.as_bytes()[..],&job.config_revision.to_string(),
            &&scope.scope().endpoint().as_bytes()[..],&&scope.scope().access().as_bytes()[..]])
        .await.map_err(qerror)?;
    let source = RelaySourceBinding {
        endpoint_id: scope.scope().endpoint(),
        access_scope_id: scope.scope().access(),
        enrollment_generation: std::num::NonZeroU64::new(number(&row, 0).map_err(qerror)?)
            .ok_or(QuantumError::Context)?,
        route_id: fixed::<16>(&row, 1)
            .map_err(qerror)?
            .try_into()
            .map_err(qerror)?,
    };
    let owner = job.owner.ok_or(QuantumError::Context)?;
    let historical = ingest_registry::ControlOwner {
        resource_id: owner.resource,
        epoch: owner.epoch,
        process_incarnation: owner.incarnation,
        role_instance_id: [0; 16],
    };
    let traversal = quantum::next_window(
        tx,
        read,
        context,
        job,
        &scope,
        source,
        historical,
        profile_width(
            row.try_get::<_, &str>(2).map_err(qerror)?,
            row.try_get(4).map_err(qerror)?,
        )?,
        u16::try_from(number(&row, 3).map_err(qerror)?).map_err(qerror)?,
        authority,
    )
    .await?;
    if traversal.geometry.pending {
        return Err(QuantumError::Context);
    }
    Ok((source, u64::from(traversal.entries)))
}
impl ControlSession {
    /// Seal all original queued claims of this exact excluded native lifetime.
    /// The operation holds one prefunded replay state, streaming at most64 jobs;
    /// each job has at most254 retained handoffs through the existing epoch cap.
    pub async fn seal_acquisition_continuations(
        &mut self,
        source: AcquisitionContinuationSource<'_, '_>,
        input: InitialHistoryRequest<'_>,
        grant: QuantumGrant,
        validate: impl FnMut(&str, [u8; 32]) -> Result<(), OwnerError>,
    ) -> Result<u64, ProducerCloseError> {
        let source = source.checked(input.cancelled)?;
        if !source.recovery.matches_configured_root(input.ingest_root)
            || source.manifest != source.recovery.manifest()
            || source.manifest.identity().dataset_id != self.request.dataset
        {
            return Err(OwnerError::InvalidInput.into());
        }
        let context = context(source.manifest, source.slot);
        let current = self.identity;
        let (tx, mut lease, policy) = self
            .begin_history_authority(source.recovery, &input, 45, validate)
            .await?;
        let read = source
            .recovery
            .progress_read(&grant.permit)
            .map_err(|_| OwnerError::InvalidMetadata)?;
        let original=tx.query_one("SELECT resource,epoch::text,incarnation FROM ( \
            SELECT registered_by_resource_id AS resource,registered_by_epoch AS epoch,registered_by_incarnation AS incarnation FROM control.producer_slots WHERE producer_slot_id=$1 AND $2::text::numeric=1 \
            UNION ALL SELECT resource,epoch,incarnation FROM control.ingest_lineage_activation($3,$2::text::numeric) WHERE $2::text::numeric>1) original",
            &[&&context.slot.as_bytes()[..],&context.epoch.to_string(),&&source.manifest.identity().owner_resource_id.as_bytes()[..]])
            .await.map_err(backend)?;
        let old_resource = fixed::<16>(&original, 0)?;
        let old_epoch = number(&original, 1)?;
        let old_incarnation = fixed::<16>(&original, 2)?;
        // Fix the bounded claim set before job/grant/source locks. Dataset UPDATE
        // already excludes competing job transitions and fresh native effects.
        let ids=tx.query("SELECT substring(job_id FROM 1 FOR 17) FROM control.jobs WHERE dataset_id=$1 AND kind='acquisition' \
            AND owner_resource_id=$2 AND owner_epoch=$3::text::numeric AND owner_incarnation=$4 ORDER BY job_id LIMIT 65",
            &[&&context.dataset.as_bytes()[..],&&old_resource[..],&old_epoch.to_string(),&&old_incarnation[..]])
            .await.map_err(backend)?;
        if ids.len() > 64 {
            return Err(OwnerError::InvalidMetadata.into());
        }
        let mut sealed = 0;
        for id in ids {
            let job: JobId = fixed::<16>(&id, 0)?
                .try_into()
                .map_err(|_| OwnerError::InvalidMetadata)?;
            tx.query_one(
                "SELECT control.require_acquisition_continuation_authority($1,$2)",
                &[&&context.dataset.as_bytes()[..], &&job.as_bytes()[..]],
            )
            .await
            .map_err(backend)?;
            let job = acquisition_claims::read(&tx, context.dataset, job)
                .await
                .map_err(|_| OwnerError::InvalidMetadata)?
                .ok_or(OwnerError::InvalidMetadata)?;
            let (binding, last) = authenticate(&tx, &read, context, &job, &grant, &mut || {
                producer_progress::recovery::authority(&mut lease, policy, input.cancelled)
            })
            .await
            .map_err(|_| OwnerError::InvalidMetadata)?;
            // Every dependency is authenticated again through complete pre-COMMIT
            // closure below. A pending request never reaches this insertion.
            let mut id = [0; 16];
            getrandom::fill(&mut id).map_err(|_| OwnerError::Unavailable)?;
            // Bind Rust's base wire types before target-domain coercion, including
            // parameters reused by the SELECT's predicates and checkpoint join.
            let changed=tx.execute("INSERT INTO control.acquisition_continuations(continuation_id,dataset_id,job_id,previous_job_revision,previous_continuation, \
                previous_owner_resource,previous_owner_epoch,previous_owner_incarnation,scope_id,enrollment_generation,route_id,producer_slot_id,producer_epoch,manifest_sha256,shard_id, \
                source_sequence,source_digest,checkpoint_revision,complete_through_request,checkpoint_sha256,last_quantum_number,sealing_resource,sealing_epoch,sealing_incarnation,sealed_at_us) \
                SELECT $1::bytea,$2::bytea,$3::bytea,$4::text::numeric,(SELECT continuation_id FROM control.acquisition_continuations WHERE job_id=$3::bytea AND adopted_job_revision=$4::text::numeric), \
                $5::bytea,$6::text::numeric,$7::bytea,$8::bytea,$9::text::numeric,$10::bytea,$11::bytea,$12::text::numeric,$13::bytea,$14::bigint,$15::text::numeric,$16::bytea,coalesce(h.checkpoint_revision,0),$17::text::numeric,h.checkpoint_digest, \
                $18::text::numeric,$19::bytea,$20::text::numeric,$21::bytea,(extract(epoch FROM clock_timestamp())*1000000)::bigint \
                FROM (SELECT 1) anchor LEFT JOIN control.producer_checkpoints h ON h.producer_slot_id=$11::bytea AND h.producer_epoch=$12::text::numeric \
                WHERE NOT EXISTS(SELECT 1 FROM control.acquisition_continuations WHERE job_id=$3::bytea AND previous_job_revision=$4::text::numeric)",
                &[&&id[..],&&context.dataset.as_bytes()[..],&&job.id.as_bytes()[..],&job.revision.to_string(),&&old_resource[..],&old_epoch.to_string(),&&old_incarnation[..],
                &&job.scope.as_bytes()[..],&binding.enrollment_generation.get().to_string(),&&binding.route_id.as_bytes()[..],&&context.slot.as_bytes()[..],&context.epoch.to_string(),&&context.manifest[..],
                &i64::from(context.shard),&source.sequence.to_string(),&&source.digest[..],&source.requests.to_string(),&last.to_string(),&&current.resource_id.as_bytes()[..],&current.epoch.get().to_string(),&&current.process_incarnation.as_bytes()[..]])
                .await.map_err(backend)?;
            // Exact retries preserve the original sealer/time/xmin. Changed cuts,
            // source bindings or checkpoint identities are refused, not replaced.
            let exact:bool=tx.query_one("SELECT control.acquisition_continuation_covers($1,$2,$3::text::numeric) AND EXISTS(SELECT 1 FROM control.acquisition_continuations \
                WHERE job_id=$1 AND previous_job_revision=$4::text::numeric AND manifest_sha256=$5 AND last_quantum_number=$6::text::numeric \
                AND enrollment_generation=$7::text::numeric AND route_id=$8 AND source_sequence=$9::text::numeric AND source_digest=$10)",
                &[&&job.id.as_bytes()[..],&&context.slot.as_bytes()[..],&context.epoch.to_string(),&job.revision.to_string(),&&context.manifest[..],&last.to_string(),
                    &binding.enrollment_generation.get().to_string(),&&binding.route_id.as_bytes()[..],&source.sequence.to_string(),&&source.digest[..]])
                .await.map_err(backend)?.try_get(0).map_err(backend)?;
            if !exact || changed > 1 {
                return Err(OwnerError::InvalidMetadata.into());
            }
            sealed += 1;
        }
        producer_progress::recovery::require(
            &tx,
            source.recovery,
            source.manifest,
            context.slot,
            &grant.permit,
            || producer_progress::recovery::authority(&mut lease, policy, input.cancelled),
        )
        .await?;
        read.validate().map_err(|_| OwnerError::InvalidMetadata)?;
        initial_history::check_delivery(&mut lease, policy, input.cancelled)?;
        tx.commit()
            .await
            .map_err(|_| ProducerCloseError::CommitUnknown)?;
        initial_history::check_delivery(&mut lease, policy, input.cancelled)?;
        if self.client.is_closed() || source.recovery.native_unavailable() {
            return Err(ProducerCloseError::DeliveryUnknown);
        }
        self.available = true;
        Ok(sealed)
    }
}

/// Historical retirement adapters still support their original schema. Only a
/// catalog containing the new capability can name its continuation helper.
pub(super) async fn workflow_clause(tx: &Transaction<'_>) -> Result<&'static str, OwnerError> {
    let enabled: bool = tx
        .query_one(
            "SELECT EXISTS(SELECT 1 FROM control.schema_versions WHERE migration=45)",
            &[],
        )
        .await
        .map_err(backend)?
        .try_get(0)
        .map_err(backend)?;
    Ok(if enabled {
        " OR control.acquisition_continuation_covers(r.acquisition_job_id,r.producer_slot_id,r.producer_epoch)"
    } else {
        ""
    })
}

/// Source coordinates allow zero; positive identity/revision parsing remains
/// unchanged. The wire representation must still be canonical and bounded.
pub(super) fn cut_number(row: &Row, index: usize) -> Result<u64, OwnerError> {
    parse_cut(
        row.try_get::<_, &str>(index)
            .map_err(|_| OwnerError::InvalidMetadata)?,
    )
}
fn parse_cut(text: &str) -> Result<u64, OwnerError> {
    text.parse::<u64>()
        .ok()
        .filter(|n| *n <= 385 && n.to_string() == text)
        .ok_or(OwnerError::InvalidMetadata)
}
fn profile_width(text: &str, retained_seconds: bool) -> Result<u64, QuantumError> {
    let value = text
        .parse::<u64>()
        .ok()
        .filter(|n| *n > 0 && n.to_string() == text)
        .ok_or(QuantumError::Context)?;
    if retained_seconds {
        return Ok(value);
    }
    if value < 1_000_000 || !value.is_multiple_of(1_000_000) {
        return Err(QuantumError::Context);
    }
    Ok(value / 1_000_000)
}
#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn empty_and_maximal_retained_cuts_are_canonical_coordinates() {
        for (text, wanted) in [("0", 0), ("1", 1), ("385", 385)] {
            assert_eq!(parse_cut(text).unwrap(), wanted);
        }
        for text in [
            "",
            "01",
            "+1",
            "-1",
            "386",
            "18446744073709551615",
            "18446744073709551616",
        ] {
            assert!(parse_cut(text).is_err(), "{text}");
        }
    }
    #[test]
    fn zero_query_profiles_convert_only_integral_accepted_seconds() {
        assert_eq!(profile_width("86400000000", false).unwrap(), 86400);
        assert_eq!(profile_width("1000000", false).unwrap(), 1);
        assert_eq!(profile_width("86400", true).unwrap(), 86400);
        for text in [
            "0",
            "1",
            "1500000",
            "86400000000.000000",
            "086400000000",
            "18446744073709551616",
        ] {
            assert!(profile_width(text, false).is_err(), "{text}");
        }
    }
}
