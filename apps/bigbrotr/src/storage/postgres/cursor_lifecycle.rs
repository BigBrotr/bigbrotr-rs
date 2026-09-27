//! Protected maintenance transitions; historical receipts confer no cursor grant.
use super::initialization::{InitializeError, application_transaction};
use crate::read::keyring::{Keyring, PreparedKeyring};
use bigbrotr_model::{DatasetId, PrincipalId};
use sha2::{Digest, Sha256};
use std::{cell::Cell, fmt};
use tokio_postgres::{Client, Row, Transaction, error::SqlState};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LifecycleError {
    InvalidInput,
    Backend,
    Prerequisite,
    WrongDeployment,
    Conflict,
    Unavailable,
    Entropy,
    CommitUnknown,
}
impl fmt::Display for LifecycleError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "cursor key lifecycle refused: {self:?}")
    }
}
impl std::error::Error for LifecycleError {}

fn backend(error: tokio_postgres::Error) -> LifecycleError {
    match error.code() {
        Some(&SqlState::OBJECT_NOT_IN_PREREQUISITE_STATE) => LifecycleError::Unavailable,
        Some(
            &SqlState::CHECK_VIOLATION
            | &SqlState::UNIQUE_VIOLATION
            | &SqlState::FOREIGN_KEY_VIOLATION,
        ) => LifecycleError::Conflict,
        _ => LifecycleError::Backend,
    }
}
fn fixed<const N: usize>(row: &Row, name: &str) -> Result<[u8; N], LifecycleError> {
    row.try_get::<_, &[u8]>(name)
        .map_err(backend)?
        .try_into()
        .map_err(|_| LifecycleError::Conflict)
}
fn active_key(row: &Row) -> Result<Option<[u8; 16]>, LifecycleError> {
    row.try_get::<_, Option<&[u8]>>("active_key")
        .map_err(backend)?
        .map(|id| id.try_into().map_err(|_| LifecycleError::Conflict))
        .transpose()
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Action {
    Activate,
    Revoke,
}
impl Action {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Activate => "activate",
            Self::Revoke => "revoke",
        }
    }
}
#[derive(Debug, Clone, Copy)]
pub struct Request {
    pub request_id: [u8; 16],
    pub key_id: [u8; 16],
    pub expected_revision: u64,
    pub action: Action,
}
impl Request {
    fn digest(self, dataset: DatasetId, ring: [u8; 16], actor: PrincipalId) -> [u8; 32] {
        Sha256::new()
            .chain_update(b"bigbrotr/cursor-key-lifecycle/1\0")
            .chain_update(dataset.as_bytes())
            .chain_update(ring)
            .chain_update(actor.as_bytes())
            .chain_update(self.request_id)
            .chain_update([match self.action {
                Action::Activate => 1,
                Action::Revoke => 2,
            }])
            .chain_update(self.key_id)
            .chain_update(self.expected_revision.to_be_bytes())
            .finalize()
            .into()
    }
}
pub struct Receipt {
    pub created: bool,
    pub actor: PrincipalId,
    pub audit_id: [u8; 16],
    pub revision: u64,
    pub active_key: Option<[u8; 16]>,
    pub accepted_at_us: i64,
    pub request_digest: [u8; 32],
}

/// Retain `prepared` and its connection reservation through the actual driver
/// join. Cancellation after COMMIT starts is unknown, never inferred rollback.
pub async fn transition(
    client: &mut Client,
    instance: DatasetId,
    prepared: &PreparedKeyring,
    request: Request,
    commit_started: &Cell<bool>,
) -> Result<Receipt, LifecycleError> {
    if request.request_id == [0; 16]
        || request.key_id == [0; 16]
        || request.expected_revision == u64::MAX
    {
        return Err(LifecycleError::InvalidInput);
    }
    let keys = prepared.keys();
    let dataset_id = keys.dataset();
    let dataset = &dataset_id.as_bytes()[..];
    let ring = keys.id();
    let tx = application_transaction(client, 604)
        .await
        .map_err(|error| match error {
            InitializeError::Backend => LifecycleError::Backend,
            _ => LifecycleError::Prerequisite,
        })?;
    // No dataset lock before Control exclusion. These immutable identities can
    // authenticate a historical retry while a live Control holds the fence.
    let rows = tx
        .query(
            "SELECT substring(d.dataset_id FROM 1 FOR 17) AS dataset,
                substring(d.instance_id FROM 1 FOR 17) AS instance,
                substring(p.principal_id FROM 1 FOR 17) AS actor,
                substring(r.keyring_id FROM 1 FOR 17) AS ring
         FROM control.datasets d
         JOIN control.principals p ON p.dataset_id=d.dataset_id
             AND p.kind='installation' AND p.database_role=SESSION_USER
         JOIN control.cursor_keyrings r ON r.dataset_id=d.dataset_id
         WHERE d.config_revision IS NOT NULL LIMIT 2",
            &[],
        )
        .await
        .map_err(backend)?;
    let [row] = rows.as_slice() else {
        return Err(LifecycleError::Prerequisite);
    };
    if fixed::<16>(row, "dataset")? != *dataset_id.as_bytes()
        || fixed::<16>(row, "instance")? != *instance.as_bytes()
        || fixed::<16>(row, "ring")? != ring
    {
        return Err(LifecycleError::WrongDeployment);
    }
    let actor =
        PrincipalId::try_from(fixed::<16>(row, "actor")?).map_err(|_| LifecycleError::Conflict)?;
    let digest = request.digest(dataset_id, ring, actor);
    if let Some(receipt) = historical(&tx, keys, actor, request, digest).await? {
        tx.rollback().await.map_err(backend)?;
        return Ok(receipt);
    }
    tx.query_one(
        "SELECT control.lock_cursor_maintenance($1::bytea)",
        &[&dataset],
    )
    .await
    .map_err(backend)?;
    // A preceding transition may have committed between the first observation
    // and exclusion. Resolve it before checking its now-stale expected revision.
    if let Some(receipt) = historical(&tx, keys, actor, request, digest).await? {
        tx.rollback().await.map_err(backend)?;
        return Ok(receipt);
    }
    validate_materials(&tx, keys).await?;
    if request.action == Action::Activate && !keys.key_ids().any(|id| id == request.key_id) {
        return Err(LifecycleError::Prerequisite);
    }
    let mut audit = [0; 16];
    getrandom::fill(&mut audit).map_err(|_| LifecycleError::Entropy)?;
    if audit == [0; 16] {
        return Err(LifecycleError::Entropy);
    }
    let now: i64 = tx
        .query_one(
            "SELECT (EXTRACT(EPOCH FROM pg_catalog.clock_timestamp())*1000000)::bigint",
            &[],
        )
        .await
        .map_err(backend)?
        .try_get(0)
        .map_err(backend)?;
    let action = request.action.as_str();
    tx.execute(
        "INSERT INTO control.management_audit
            (audit_id,dataset_id,actor_id,action,outcome,created_at_us,subject_id,
             requester_database_role,requester_os_uid)
         VALUES($1::bytea,$2::bytea,$3::bytea,'cursor-key.' || $4::text,'accepted',
                $5::bigint,$6::bytea,SESSION_USER,$7::bigint)",
        &[
            &&audit[..],
            &dataset,
            &&actor.as_bytes()[..],
            &action,
            &now,
            &&request.key_id[..],
            &i64::from(rustix::process::geteuid().as_raw()),
        ],
    )
    .await
    .map_err(backend)?;
    tx.execute(
        "INSERT INTO control.cursor_key_transitions
            (dataset_id,request_id,keyring_id,actor_id,action,key_id,expected_revision,
             request_digest,accepted_at_us,audit_id)
         VALUES($1::bytea,$2::bytea,$3::bytea,$4::bytea,$5::text,$6::bytea,
                $7::text::numeric,$8::bytea,$9::bigint,$10::bytea)",
        &[
            &dataset,
            &&request.request_id[..],
            &&ring[..],
            &&actor.as_bytes()[..],
            &action,
            &&request.key_id[..],
            &request.expected_revision.to_string(),
            &&digest[..],
            &now,
            &&audit[..],
        ],
    )
    .await
    .map_err(backend)?;
    // Validate the resulting required set, including revocation's effect, before
    // COMMIT. No driver or SQL receipt can attest another worker's keyfile.
    validate_required_keys(&tx, keys).await?;
    let mut receipt = historical(&tx, keys, actor, request, digest)
        .await?
        .ok_or(LifecycleError::Conflict)?;
    commit_started.set(true);
    tx.commit()
        .await
        .map_err(|_| LifecycleError::CommitUnknown)?;
    receipt.created = true;
    Ok(receipt)
}

async fn historical(
    tx: &Transaction<'_>,
    keys: &Keyring,
    actor: PrincipalId,
    request: Request,
    digest: [u8; 32],
) -> Result<Option<Receipt>, LifecycleError> {
    let dataset = keys.dataset();
    let ring = keys.id();
    let row = tx
        .query_opt(
            "SELECT left(t.revision::text,21) AS revision,
                substring(t.resulting_active_key_id FROM 1 FOR 17) AS active_key,
                substring(t.audit_id FROM 1 FOR 17) AS audit,
                t.accepted_at_us,
                (t.keyring_id,t.actor_id,t.action,t.key_id,t.expected_revision,t.request_digest)
                    IS NOT DISTINCT FROM
                    ($3::bytea,$4::bytea,$5::text,$6::bytea,$7::text::numeric,$8::bytea)
                    AS matches_request,
                EXISTS(SELECT 1 FROM control.management_audit a WHERE a.audit_id=t.audit_id
                    AND (a.dataset_id,a.actor_id,a.action,a.outcome,a.subject_id,a.created_at_us)
                        IS NOT DISTINCT FROM (t.dataset_id,t.actor_id,'cursor-key.' || t.action,
                            'accepted'::text,t.key_id,t.accepted_at_us)
                    AND a.config_revision IS NULL AND a.policy_revision IS NULL
                    AND a.requester_database_role=SESSION_USER AND a.requester_os_uid IS NOT NULL)
                    AS matches_audit
         FROM control.cursor_key_transitions t
         WHERE t.dataset_id=$1::bytea AND t.request_id=$2::bytea",
            &[
                &&dataset.as_bytes()[..],
                &&request.request_id[..],
                &&ring[..],
                &&actor.as_bytes()[..],
                &request.action.as_str(),
                &&request.key_id[..],
                &request.expected_revision.to_string(),
                &&digest[..],
            ],
        )
        .await
        .map_err(backend)?;
    let Some(row) = row else {
        return Ok(None);
    };
    if !row.try_get::<_, bool>("matches_request").map_err(backend)?
        || !row.try_get::<_, bool>("matches_audit").map_err(backend)?
    {
        return Err(LifecycleError::Conflict);
    }
    let text: &str = row.try_get("revision").map_err(backend)?;
    let revision: u64 = text.parse().map_err(|_| LifecycleError::Conflict)?;
    if request.expected_revision.checked_add(1) != Some(revision) || revision.to_string() != text {
        return Err(LifecycleError::Conflict);
    }
    Ok(Some(Receipt {
        created: false,
        actor,
        audit_id: fixed(&row, "audit")?,
        revision,
        active_key: active_key(&row)?,
        accepted_at_us: row.try_get("accepted_at_us").map_err(backend)?,
        request_digest: digest,
    }))
}

pub(super) async fn validate_materials(
    tx: &Transaction<'_>,
    keys: &Keyring,
) -> Result<(), LifecycleError> {
    let dataset = keys.dataset();
    for (id, digest) in keys.bindings() {
        let row = tx
            .query_opt(
                "SELECT substring(keyring_id FROM 1 FOR 17) AS ring,
                    substring(material_digest FROM 1 FOR 33) AS digest
             FROM control.cursor_key_materials
             WHERE dataset_id=$1::bytea AND key_id=$2::bytea",
                &[&&dataset.as_bytes()[..], &&id[..]],
            )
            .await
            .map_err(backend)?
            .ok_or(LifecycleError::Prerequisite)?;
        if fixed::<16>(&row, "ring")? != keys.id()
            || fixed::<32>(&row, "digest")? != *digest.as_bytes()
        {
            return Err(LifecycleError::Conflict);
        }
    }
    Ok(())
}

/// Caller holds Control exclusion and the dataset lock. A17th row is a bounded
/// refusal, not permission to omit required old keys. No lifecycle grant escapes.
pub(super) async fn validate_required_keys(
    tx: &Transaction<'_>,
    keys: &Keyring,
) -> Result<(), LifecycleError> {
    let dataset = keys.dataset();
    let dataset = &dataset.as_bytes()[..];
    let row = tx
        .query_opt(
            "SELECT substring(resulting_active_key_id FROM 1 FOR 17) AS active_key
         FROM control.cursor_key_transitions WHERE dataset_id=$1::bytea
         ORDER BY revision DESC LIMIT 1",
            &[&dataset],
        )
        .await
        .map_err(backend)?;
    let active = row.as_ref().map(active_key).transpose()?.flatten();
    let now: i64 = tx
        .query_one(
            "SELECT (EXTRACT(EPOCH FROM pg_catalog.clock_timestamp())*1000000)::bigint",
            &[],
        )
        .await
        .map_err(backend)?
        .try_get(0)
        .map_err(backend)?;
    // Both sources are bounded before UNION. The partial retention index avoids
    // scanning unlimited revoked/expired history to find a small live set.
    let required = tx
        .query(
            "SELECT substring(key_id FROM 1 FOR 17) AS key_id FROM (
             SELECT $2::bytea AS key_id WHERE $2::bytea IS NOT NULL
             UNION
             (SELECT key_id FROM control.cursor_key_lifecycle
              WHERE dataset_id=$1::bytea AND revoked_revision IS NULL
                  AND decrypt_until_us IS NOT NULL AND decrypt_until_us >= $3::bigint
              ORDER BY decrypt_until_us,key_id LIMIT 17)
         ) required LIMIT 17",
            &[&dataset, &active.as_ref().map(|id| &id[..]), &now],
        )
        .await
        .map_err(backend)?;
    if required.len() > 16 {
        return Err(LifecycleError::Prerequisite);
    }
    for row in &required {
        let id = fixed::<16>(row, "key_id")?;
        if !keys.key_ids().any(|loaded| loaded == id) {
            return Err(LifecycleError::Prerequisite);
        }
    }
    Ok(())
}
