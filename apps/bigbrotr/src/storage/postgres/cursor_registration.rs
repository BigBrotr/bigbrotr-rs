//! Permanent cursor key registration through protected deployment access.
//! No operation activates issuance, acquires Control or grants a cursor authority.
use super::initialization::{InitializeError, application_transaction};
use crate::read::keyring::{Keyring, PreparedKeyring};
use bigbrotr_model::{DatasetId, PrincipalId};
use std::{cell::Cell, fmt};
use tokio_postgres::{Client, Row, Transaction};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RegistrationError {
    InvalidInput,
    Backend,
    Prerequisite,
    WrongDeployment,
    Conflict,
    Entropy,
    CommitUnknown,
}
impl fmt::Display for RegistrationError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "cursor key registration refused: {self:?}")
    }
}
impl std::error::Error for RegistrationError {}
fn backend(_: tokio_postgres::Error) -> RegistrationError {
    RegistrationError::Backend
}
fn fixed<const N: usize>(row: &Row, index: usize) -> Result<[u8; N], RegistrationError> {
    row.try_get::<_, &[u8]>(index)
        .map_err(backend)?
        .try_into()
        .map_err(|_| RegistrationError::Conflict)
}

pub struct RegistrationReceipt {
    pub created: bool,
    pub actor: PrincipalId,
    pub audit_id: [u8; 16],
    pub registered_at_us: i64,
}

/// The caller retains the prepared keys/combined reservation until its actual
/// driver has joined, including cancellation or an ambiguous COMMIT response.
pub async fn register(
    client: &mut Client,
    instance: DatasetId,
    prepared: &PreparedKeyring,
    commit_started: &Cell<bool>,
) -> Result<RegistrationReceipt, RegistrationError> {
    let keys = prepared.keys();
    let dataset_id = keys.dataset();
    let dataset = &dataset_id.as_bytes()[..];
    let (tx, actor) = deployment(client, instance, keys, 601).await?;
    let count = keys.key_ids().count() as i16;
    let ring = keys.id();
    let fingerprint = keys.fingerprint();
    let existing = tx.query_opt("SELECT substring(keyring_id FROM 1 FOR 17), file_format, substring(initial_file_digest FROM 1 FOR 33), key_count, substring(actor_id FROM 1 FOR 17), substring(audit_id FROM 1 FOR 17), registered_at_us FROM control.cursor_keyrings WHERE dataset_id=$1::bytea", &[&dataset]).await.map_err(backend)?;
    if let Some(row) = existing {
        if fixed::<16>(&row, 0)? != ring
            || row.try_get::<_, i32>(1).map_err(backend)? != 1
            || fixed::<32>(&row, 2)? != *fingerprint.as_bytes()
            || row.try_get::<_, i16>(3).map_err(backend)? != count
            || fixed::<16>(&row, 4)? != *actor.as_bytes()
        {
            return Err(RegistrationError::Conflict);
        }
        let members = tx.query("SELECT substring(key_id FROM 1 FOR 17), substring(material_digest FROM 1 FOR 33), registration_position FROM control.cursor_keys WHERE dataset_id=$1::bytea ORDER BY registration_position LIMIT 17", &[&dataset]).await.map_err(backend)?;
        if members.len() != count as usize {
            return Err(RegistrationError::Conflict);
        }
        for (index, (member, (id, digest))) in members.iter().zip(keys.bindings()).enumerate() {
            if fixed::<16>(member, 0)? != id
                || fixed::<32>(member, 1)? != *digest.as_bytes()
                || member.try_get::<_, i16>(2).map_err(backend)? != (index + 1) as i16
            {
                return Err(RegistrationError::Conflict);
            }
        }
        let receipt = RegistrationReceipt {
            created: false,
            actor,
            audit_id: fixed(&row, 5)?,
            registered_at_us: row.try_get(6).map_err(backend)?,
        };
        let exact: bool = tx.query_one("SELECT EXISTS(SELECT 1 FROM control.management_audit WHERE audit_id=$1::bytea AND dataset_id=$2::bytea AND actor_id=$3::bytea AND subject_id=$4::bytea AND action='cursor-keyring.register' AND outcome='accepted' AND created_at_us=$5::bigint AND config_revision IS NULL AND policy_revision IS NULL AND requester_database_role=SESSION_USER)", &[&&receipt.audit_id[..], &dataset, &&actor.as_bytes()[..], &&ring[..], &receipt.registered_at_us]).await.map_err(backend)?.try_get(0).map_err(backend)?;
        if !exact {
            return Err(RegistrationError::Conflict);
        }
        tx.rollback().await.map_err(backend)?;
        return Ok(receipt);
    }
    let mut audit = [0; 16];
    getrandom::fill(&mut audit).map_err(|_| RegistrationError::Entropy)?;
    if audit == [0; 16] {
        return Err(RegistrationError::Entropy);
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
    tx.execute("INSERT INTO control.management_audit(audit_id,dataset_id,actor_id,action,outcome,created_at_us,subject_id,requester_database_role,requester_os_uid) VALUES($1::bytea,$2::bytea,$3::bytea,'cursor-keyring.register','accepted',$4::bigint,$5::bytea,SESSION_USER,$6::bigint)", &[&&audit[..], &dataset, &&actor.as_bytes()[..], &now, &&ring[..], &i64::from(rustix::process::geteuid().as_raw())]).await.map_err(backend)?;
    tx.execute("INSERT INTO control.cursor_keyrings(dataset_id,keyring_id,file_format,initial_file_digest,key_count,registered_at_us,actor_id,audit_id) VALUES($1::bytea,$2::bytea,1,$3::bytea,$4::smallint,$5::bigint,$6::bytea,$7::bytea)", &[&dataset, &&ring[..], &&fingerprint.as_bytes()[..], &count, &now, &&actor.as_bytes()[..], &&audit[..]]).await.map_err(backend)?;
    for (index, (id, digest)) in keys.bindings().enumerate() {
        tx.execute("INSERT INTO control.cursor_keys(dataset_id,keyring_id,key_id,registration_position,material_digest) VALUES($1::bytea,$2::bytea,$3::bytea,$4::smallint,$5::bytea)", &[&dataset, &&ring[..], &&id[..], &((index + 1) as i16), &&digest.as_bytes()[..]]).await.map_err(backend)?;
    }
    commit_started.set(true);
    tx.commit()
        .await
        .map_err(|_| RegistrationError::CommitUnknown)?;
    Ok(RegistrationReceipt {
        created: true,
        actor,
        audit_id: audit,
        registered_at_us: now,
    })
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum KeyOrigin {
    Initial,
    Additional,
}
pub struct EnrollmentReceipt {
    pub receipt: RegistrationReceipt,
    pub origin: KeyOrigin,
}

/// Enroll only the selected prepared material. The original whole-file
/// registration remains immutable; this retry does not bind unrelated local keys.
pub async fn enroll(
    client: &mut Client,
    instance: DatasetId,
    prepared: &PreparedKeyring,
    key_id: [u8; 16],
    commit_started: &Cell<bool>,
) -> Result<EnrollmentReceipt, RegistrationError> {
    let keys = prepared.keys();
    let digest = keys
        .bindings()
        .find(|(id, _)| *id == key_id)
        .map(|(_, digest)| digest)
        .ok_or(RegistrationError::InvalidInput)?;
    let dataset_id = keys.dataset();
    let dataset = &dataset_id.as_bytes()[..];
    let ring = keys.id();
    let (tx, actor) = deployment(client, instance, keys, 602).await?;
    let registered = tx.query_opt("SELECT substring(keyring_id FROM 1 FOR 17) FROM control.cursor_keyrings WHERE dataset_id=$1::bytea", &[&dataset]).await.map_err(backend)?.ok_or(RegistrationError::Prerequisite)?;
    if fixed::<16>(&registered, 0)? != ring {
        return Err(RegistrationError::Conflict);
    }
    let existing = tx.query_opt("SELECT substring(keyring_id FROM 1 FOR 17), substring(material_digest FROM 1 FOR 33), left(origin,11), substring(actor_id FROM 1 FOR 17), substring(audit_id FROM 1 FOR 17), enrolled_at_us FROM control.cursor_key_materials WHERE dataset_id=$1::bytea AND key_id=$2::bytea", &[&dataset, &&key_id[..]]).await.map_err(backend)?;
    if let Some(row) = existing {
        if fixed::<16>(&row, 0)? != ring
            || fixed::<32>(&row, 1)? != *digest.as_bytes()
            || fixed::<16>(&row, 3)? != *actor.as_bytes()
        {
            return Err(RegistrationError::Conflict);
        }
        let origin = match row.try_get::<_, &str>(2).map_err(backend)? {
            "initial" => KeyOrigin::Initial,
            "additional" => KeyOrigin::Additional,
            _ => return Err(RegistrationError::Conflict),
        };
        let receipt = RegistrationReceipt {
            created: false,
            actor,
            audit_id: fixed(&row, 4)?,
            registered_at_us: row.try_get(5).map_err(backend)?,
        };
        let (action, subject) = match origin {
            KeyOrigin::Initial => ("cursor-keyring.register", ring),
            KeyOrigin::Additional => ("cursor-key.enroll", key_id),
        };
        let exact: bool = tx.query_one("SELECT EXISTS(SELECT 1 FROM control.management_audit WHERE audit_id=$1::bytea AND dataset_id=$2::bytea AND actor_id=$3::bytea AND subject_id=$4::bytea AND action=$5::text AND outcome='accepted' AND created_at_us=$6::bigint AND config_revision IS NULL AND policy_revision IS NULL AND requester_database_role=SESSION_USER)", &[&&receipt.audit_id[..], &dataset, &&actor.as_bytes()[..], &&subject[..], &action, &receipt.registered_at_us]).await.map_err(backend)?.try_get(0).map_err(backend)?;
        if !exact {
            return Err(RegistrationError::Conflict);
        }
        tx.rollback().await.map_err(backend)?;
        return Ok(EnrollmentReceipt { receipt, origin });
    }
    let mut audit = [0; 16];
    getrandom::fill(&mut audit).map_err(|_| RegistrationError::Entropy)?;
    if audit == [0; 16] {
        return Err(RegistrationError::Entropy);
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
    tx.execute("INSERT INTO control.management_audit(audit_id,dataset_id,actor_id,action,outcome,created_at_us,subject_id,requester_database_role,requester_os_uid) VALUES($1::bytea,$2::bytea,$3::bytea,'cursor-key.enroll','accepted',$4::bigint,$5::bytea,SESSION_USER,$6::bigint)", &[&&audit[..], &dataset, &&actor.as_bytes()[..], &now, &&key_id[..], &i64::from(rustix::process::geteuid().as_raw())]).await.map_err(backend)?;
    tx.execute("INSERT INTO control.cursor_key_materials(dataset_id,keyring_id,key_id,material_digest,origin,enrolled_at_us,actor_id,audit_id) VALUES($1::bytea,$2::bytea,$3::bytea,$4::bytea,'additional',$5::bigint,$6::bytea,$7::bytea)", &[&dataset, &&ring[..], &&key_id[..], &&digest.as_bytes()[..], &now, &&actor.as_bytes()[..], &&audit[..]]).await.map_err(backend)?;
    commit_started.set(true);
    tx.commit()
        .await
        .map_err(|_| RegistrationError::CommitUnknown)?;
    Ok(EnrollmentReceipt {
        receipt: RegistrationReceipt {
            created: true,
            actor,
            audit_id: audit,
            registered_at_us: now,
        },
        origin: KeyOrigin::Additional,
    })
}

async fn deployment<'a>(
    client: &'a mut Client,
    instance: DatasetId,
    keys: &Keyring,
    required: u32,
) -> Result<(Transaction<'a>, PrincipalId), RegistrationError> {
    let dataset_id = keys.dataset();
    let dataset = &dataset_id.as_bytes()[..];
    let tx = application_transaction(client, required)
        .await
        .map_err(|error| match error {
            InitializeError::Backend => RegistrationError::Backend,
            _ => RegistrationError::Prerequisite,
        })?;
    // Lock all candidates before inspecting deployment identity, at most two.
    // The next READ COMMITTED statement sees a preceding registrant's COMMIT.
    let rows = tx.query("SELECT substring(dataset_id FROM 1 FOR 17), substring(instance_id FROM 1 FOR 17), config_revision IS NOT NULL FROM control.datasets LIMIT 2 FOR UPDATE", &[]).await.map_err(backend)?;
    let [row] = rows.as_slice() else {
        return Err(RegistrationError::Prerequisite);
    };
    if fixed::<16>(row, 0)? != *dataset_id.as_bytes()
        || fixed::<16>(row, 1)? != *instance.as_bytes()
    {
        return Err(RegistrationError::WrongDeployment);
    }
    if !row.try_get::<_, bool>(2).map_err(backend)? {
        return Err(RegistrationError::Prerequisite);
    }
    let actor = tx.query_opt("SELECT substring(principal_id FROM 1 FOR 17) FROM control.principals WHERE dataset_id=$1::bytea AND kind='installation' AND database_role=SESSION_USER", &[&dataset]).await.map_err(backend)?.ok_or(RegistrationError::Prerequisite)?;
    let actor = PrincipalId::try_from(fixed::<16>(&actor, 0)?)
        .map_err(|_| RegistrationError::Prerequisite)?;
    Ok((tx, actor))
}
