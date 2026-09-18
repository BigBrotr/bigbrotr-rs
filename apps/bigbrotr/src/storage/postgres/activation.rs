//! Explicit first security snapshot. A receipt is never a runtime lease.
use super::initialization::application_transaction;
use bigbrotr_model::{DatasetId, PrincipalId};
use sha2::{Digest, Sha256};
use std::{cell::Cell, fmt};
use tokio_postgres::{Client, Row};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ActivationError {
    InvalidInput,
    Prerequisite,
    Backend,
    WrongDeployment,
    InvalidMetadata,
    RevisionConflict,
    PolicyInvalid,
    ExistingActivity,
    RequestConflict,
    CommitUnknown,
}
impl fmt::Display for ActivationError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "initial policy activation refused: {self:?}")
    }
}
impl std::error::Error for ActivationError {}
fn backend(_: tokio_postgres::Error) -> ActivationError {
    ActivationError::Backend
}

pub struct ActivationRequest<'a> {
    pub dataset: DatasetId,
    pub instance: DatasetId,
    pub request_id: [u8; 16],
    pub expected_revision: u64,
    pub authority_mode: &'a str,
}
pub struct ActivationReceipt {
    pub created: bool,
    pub actor: PrincipalId,
    pub audit_id: [u8; 16],
    pub document_digest: [u8; 32],
    pub request_digest: [u8; 32],
}

/// A protected local deployment caller must restore the entire closed policy
/// against current roots in `validate`. It owns the bounded parse reservation,
/// deadline and connection driver through completion. No arbitrary remote route
/// may invoke this bootstrap operation. Validation runs even on receipt replay.
pub async fn activate_initial(
    client: &mut Client,
    input: ActivationRequest<'_>,
    validate: impl FnOnce(&str, [u8; 32]) -> Result<(), ActivationError>,
    commit_started: &Cell<bool>,
) -> Result<ActivationReceipt, ActivationError> {
    if input.expected_revision == 0
        || input.request_id == [0; 16]
        || !matches!(input.authority_mode, "managed" | "declarative")
    {
        return Err(ActivationError::InvalidInput);
    }
    let tx = application_transaction(client, 802)
        .await
        .map_err(|error| match error {
            super::initialization::InitializeError::Backend => ActivationError::Backend,
            _ => ActivationError::Prerequisite,
        })?;
    // FOR UPDATE (not NO KEY UPDATE) also serializes new FK references to this
    // deployment. A later statement sees children committed before lock grant.
    let rows = tx.query("SELECT substring(dataset_id FROM 1 FOR 17), substring(instance_id FROM 1 FOR 17), left(config_revision::text,21), policy_revision IS NOT NULL, topology_generation IS NOT NULL FROM control.datasets LIMIT 2 FOR UPDATE", &[]).await.map_err(backend)?;
    let [row] = rows.as_slice() else {
        return Err(ActivationError::Prerequisite);
    };
    if fixed::<16>(row, 0)? != *input.dataset.as_bytes()
        || fixed::<16>(row, 1)? != *input.instance.as_bytes()
    {
        return Err(ActivationError::WrongDeployment);
    }
    let current = positive(row, 2)?;
    let prior_policy: bool = row.try_get(3).map_err(backend)?;
    let topology: bool = row.try_get(4).map_err(backend)?;
    let dataset = &input.dataset.as_bytes()[..];
    let revision = input.expected_revision.to_string();
    // Read the requested immutable revision so same-ID recovery still works
    // after later configuration revisions, without reseeding from the file.
    let rows = tx.query("SELECT c.schema_version, left(c.authority_mode,12), CASE WHEN octet_length(c.document::text) <= 10485760 THEN c.document::text ELSE NULL END, substring(c.document_digest FROM 1 FOR 33), substring(p.principal_id FROM 1 FOR 17) FROM control.config_revisions c CROSS JOIN control.principals p WHERE c.dataset_id=$1::bytea AND c.revision=$2::text::numeric AND p.dataset_id=c.dataset_id AND p.kind='installation' LIMIT 2", &[&dataset, &revision]).await.map_err(backend)?;
    let [row] = rows.as_slice() else {
        return Err(ActivationError::RevisionConflict);
    };
    if row.try_get::<_, i32>(0).map_err(backend)? != 1 {
        return Err(ActivationError::InvalidMetadata);
    }
    if row.try_get::<_, &str>(1).map_err(backend)? != input.authority_mode {
        return Err(ActivationError::PolicyInvalid);
    }
    let document: &str = row
        .try_get(2)
        .map_err(|_| ActivationError::InvalidMetadata)?;
    let document_digest = fixed::<32>(row, 3)?;
    let actor_bytes = fixed::<16>(row, 4)?;
    let actor = PrincipalId::try_from(actor_bytes).map_err(|_| ActivationError::InvalidMetadata)?;
    validate(document, document_digest)?;
    let request_digest: [u8; 32] = Sha256::new()
        .chain_update(b"bigbrotr/activate-initial/1\0")
        .chain_update(input.dataset.as_bytes())
        .chain_update(input.instance.as_bytes())
        .chain_update(input.expected_revision.to_be_bytes())
        .chain_update([u8::from(input.authority_mode == "declarative")])
        .chain_update(document_digest)
        .finalize()
        .into();
    let request_id = &input.request_id[..];
    let rows = tx.query("SELECT substring(m.request_digest FROM 1 FOR 33), substring(m.audit_id FROM 1 FOR 17), m.action='policy.activate-initial' AND m.expected_config_revision=$3::text::numeric AND m.authority_mode=$4 AND m.document_digest=$5::bytea AND m.policy_revision=1 AND p.config_revision=m.expected_config_revision AND p.document_digest=m.document_digest AND p.document=c.document AND p.actor_id=m.actor_id AND p.accepted_at_us=m.accepted_at_us AND a.created_at_us=m.accepted_at_us AND a.outcome='accepted' FROM control.management_commands m JOIN control.policy_revisions p ON p.dataset_id=m.dataset_id AND p.revision=m.policy_revision JOIN control.config_revisions c ON c.dataset_id=m.dataset_id AND c.revision=m.expected_config_revision JOIN control.management_audit a ON a.audit_id=m.audit_id WHERE m.dataset_id=$1::bytea AND m.actor_id=$6::bytea AND m.request_key=$2::bytea LIMIT 2", &[&dataset, &request_id, &revision, &input.authority_mode, &&document_digest[..], &&actor_bytes[..]]).await.map_err(backend)?;
    if let [row] = rows.as_slice() {
        if fixed::<32>(row, 0)? != request_digest {
            return Err(ActivationError::RequestConflict);
        }
        if !row
            .try_get::<_, bool>(2)
            .map_err(|_| ActivationError::InvalidMetadata)?
            || !prior_policy
        {
            return Err(ActivationError::InvalidMetadata);
        }
        let receipt = ActivationReceipt {
            created: false,
            actor,
            audit_id: fixed(row, 1)?,
            document_digest,
            request_digest,
        };
        tx.rollback().await.map_err(backend)?;
        return Ok(receipt);
    }
    if !rows.is_empty() {
        return Err(ActivationError::InvalidMetadata);
    }
    if current != input.expected_revision {
        return Err(ActivationError::RevisionConflict);
    }
    let activity: bool = tx.query_one("SELECT EXISTS(SELECT 1 FROM control.policy_revisions) OR EXISTS(SELECT 1 FROM control.topology_generations) OR EXISTS(SELECT 1 FROM control.role_instances) OR EXISTS(SELECT 1 FROM control.owner_epochs) OR EXISTS(SELECT 1 FROM control.jobs) OR EXISTS(SELECT 1 FROM control.management_commands)", &[]).await.map_err(backend)?.try_get(0).map_err(backend)?;
    if prior_policy || topology || activity {
        return Err(ActivationError::ExistingActivity);
    }
    let mut audit_id = [0; 16];
    getrandom::fill(&mut audit_id).map_err(|_| ActivationError::Backend)?;
    if audit_id == [0; 16] {
        return Err(ActivationError::Backend);
    }
    let now: i64 = tx
        .query_one(
            "SELECT (EXTRACT(EPOCH FROM pg_catalog.clock_timestamp()) * 1000000)::bigint",
            &[],
        )
        .await
        .map_err(backend)?
        .try_get(0)
        .map_err(backend)?;
    tx.execute("INSERT INTO control.policy_revisions(revision,dataset_id,config_revision,schema_version,document,document_digest,accepted_at_us,actor_id) SELECT 1,dataset_id,revision,schema_version,document,document_digest,$3::bigint,$4::bytea FROM control.config_revisions WHERE dataset_id=$1::bytea AND revision=$2::text::numeric", &[&dataset, &revision, &now, &&actor_bytes[..]]).await.map_err(backend)?;
    tx.execute("INSERT INTO control.management_audit(audit_id,dataset_id,actor_id,action,outcome,created_at_us,config_revision,subject_id,requester_database_role,requester_os_uid) VALUES($1::bytea,$2::bytea,$3::bytea,'policy.activate-initial','accepted',$4::bigint,$5::text::numeric,$6::bytea,SESSION_USER,$7::bigint)", &[&&audit_id[..], &dataset, &&actor_bytes[..], &now, &revision, &request_id, &i64::from(rustix::process::geteuid().as_raw())]).await.map_err(backend)?;
    tx.execute("INSERT INTO control.management_commands(actor_id,request_key,dataset_id,action,request_digest,expected_config_revision,authority_mode,document_digest,policy_revision,audit_id,accepted_at_us) VALUES($1::bytea,$2::bytea,$3::bytea,'policy.activate-initial',$4::bytea,$5::text::numeric,$6,$7::bytea,1,$8::bytea,$9::bigint)", &[&&actor_bytes[..], &request_id, &dataset, &&request_digest[..], &revision, &input.authority_mode, &&document_digest[..], &&audit_id[..], &now]).await.map_err(backend)?;
    if tx.execute("UPDATE control.datasets SET policy_revision=1 WHERE dataset_id=$1::bytea AND config_revision=$2::text::numeric AND policy_revision IS NULL AND topology_generation IS NULL", &[&dataset, &revision]).await.map_err(backend)? != 1 {
        return Err(ActivationError::RevisionConflict);
    }
    commit_started.set(true);
    tx.commit()
        .await
        .map_err(|_| ActivationError::CommitUnknown)?;
    Ok(ActivationReceipt {
        created: true,
        actor,
        audit_id,
        document_digest,
        request_digest,
    })
}

fn fixed<const N: usize>(row: &Row, index: usize) -> Result<[u8; N], ActivationError> {
    row.try_get::<_, &[u8]>(index)
        .map_err(|_| ActivationError::InvalidMetadata)?
        .try_into()
        .map_err(|_| ActivationError::InvalidMetadata)
}
fn positive(row: &Row, index: usize) -> Result<u64, ActivationError> {
    let text: &str = row
        .try_get(index)
        .map_err(|_| ActivationError::Prerequisite)?;
    text.parse::<u64>()
        .ok()
        .filter(|n| *n > 0 && n.to_string() == text)
        .ok_or(ActivationError::InvalidMetadata)
}
