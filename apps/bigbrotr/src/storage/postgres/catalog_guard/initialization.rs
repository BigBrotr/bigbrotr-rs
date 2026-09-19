//! Durable database reservation and fresh bootstrap handoff for explicit initialization.
use super::{GuardError, backend, credentials::Material, identifier, inspection, installation};
use crate::storage::archive::LocalInitialization;
use std::{cell::Cell, fmt, fmt::Write};
use tokio_postgres::{Client, Transaction};
use zeroize::Zeroizing;

/// Observation data, never authority to recreate or re-enable a bootstrap login.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Reservation {
    pub database_oid: u32,
    pub reservation_oid: u32,
    pub initialization_id: [u8; 16],
    pub bootstrap_login: String,
    pub bootstrap_oid: u32,
    pub manager_login: String,
    pub manager_oid: u32,
}

/// Bounded observation only; this is not backend exclusion or launch authority.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Observation {
    pub reservation: Reservation,
    pub bootstrap_retired: bool,
}

/// Observe only after actual local custody reacquisition. The actual fresh
/// transport must retain that custody before polling and through terminal join.
pub async fn observe_after_local_custody(
    client: &mut Client,
    custody: &LocalInitialization,
    manager_login: &str,
) -> Result<Option<Observation>, GuardError> {
    if !custody.is_recovered() {
        return Err(GuardError::InvalidInput);
    }
    identifier(manager_login)?;
    let tx = super::begin(client, true).await?;
    let observed = observe(&tx, custody, manager_login).await?;
    // Even a read result requires a healthy terminal transaction. The caller
    // must destroy/join the actual transport after any cancellation or failure.
    tx.rollback().await.map_err(backend)?;
    Ok(observed)
}

/// Retire exact known identities after reacquisition; NOLOGIN is not backend
/// exclusion. Cancellation/unknown COMMIT requires fresh serialized observation.
pub async fn retire_after_local_custody(
    client: &mut Client,
    custody: &LocalInitialization,
    expected: &Reservation,
    commit_started: &Cell<bool>,
) -> Result<bool, GuardError> {
    if !custody.is_recovered() {
        return Err(GuardError::InvalidInput);
    }
    identifier(&expected.manager_login)?;
    let tx = super::begin(client, true).await?;
    let observed = observe(&tx, custody, &expected.manager_login)
        .await?
        .ok_or(GuardError::Conflict)?;
    if observed.reservation != *expected {
        return Err(GuardError::Conflict);
    }
    if observed.bootstrap_retired {
        tx.rollback().await.map_err(backend)?;
        return Ok(false);
    }
    tx.batch_execute(&format!(
        "ALTER ROLE {} NOLOGIN PASSWORD NULL",
        identifier(&observed.reservation.bootstrap_login)?
    ))
    .await
    .map_err(backend)?;
    commit_started.set(true);
    tx.commit().await.map_err(|_| GuardError::CommitUnknown)?;
    Ok(true)
}

fn nonce(custody: &LocalInitialization) -> String {
    let mut value = String::with_capacity(32);
    for byte in custody.initialization_id() {
        write!(&mut value, "{byte:02x}").expect("String write");
    }
    value
}

async fn observe(
    tx: &Transaction<'_>,
    custody: &LocalInitialization,
    manager_login: &str,
) -> Result<Option<Observation>, GuardError> {
    let manager_oid = installation::manager(tx, manager_login).await?;
    let database_oid: u32 = tx
        .query_one(
            "SELECT oid FROM pg_database WHERE datname=current_database()",
            &[],
        )
        .await
        .map_err(backend)?
        .try_get(0)
        .map_err(backend)?;
    let reserved = format!("bb_init_database_{database_oid}");
    let nonce = nonce(custody);
    let bootstrap_login = format!("bb_init_{nonce}");
    let row = tx.query_one(
        "SELECT (SELECT oid FROM pg_roles WHERE rolname=$1::name),(SELECT oid FROM pg_roles WHERE rolname=$2::name)",
        &[&reserved, &bootstrap_login],
    ).await.map_err(backend)?;
    let reservation_oid: Option<u32> = row.try_get(0).map_err(backend)?;
    let bootstrap_oid: Option<u32> = row.try_get(1).map_err(backend)?;
    let (reservation_oid, bootstrap_oid) = match (reservation_oid, bootstrap_oid) {
        (None, None) => {
            inspection::require(
                tx,
                include_str!("../../../../../../contracts/archive/catalog-pristine-v1.sql"),
                &[],
                GuardError::ExistingData,
            )
            .await?;
            return Ok(None);
        }
        (Some(reservation), Some(bootstrap)) => (reservation, bootstrap),
        _ => return Err(GuardError::Conflict),
    };
    let comment =
        format!("bigbrotr-catalog-initialization/1:{nonce}:{bootstrap_oid}:{manager_oid}");
    // Return booleans only. No arbitrary comment or secret crosses this boundary;
    // CASE avoids comparing an oversized stored value as a valid identity.
    let row = tx.query_one(
        "SELECT COALESCE(octet_length(description)<=128,true), CASE WHEN octet_length(description)<=128 THEN description=$2::text ELSE false END IS TRUE FROM (SELECT shobj_description($1::oid,'pg_authid') AS description) d",
        &[&reservation_oid, &comment],
    ).await.map_err(backend)?;
    if !row.try_get::<_, bool>(0).map_err(backend)? {
        return Err(GuardError::ResourceLimit);
    }
    if !row.try_get::<_, bool>(1).map_err(backend)? {
        return Err(GuardError::Conflict);
    }
    inspection::require(tx,
        "SELECT NOT rolcanlogin AND rolpassword IS NULL AND NOT rolsuper AND NOT rolcreaterole AND NOT rolcreatedb AND NOT rolreplication AND NOT rolbypassrls AND NOT rolinherit AND NOT EXISTS(SELECT 1 FROM pg_auth_members WHERE roleid=r.oid OR member=r.oid) FROM pg_authid r WHERE oid=$1::oid",
        &[&reservation_oid], GuardError::UnsafeRole).await?;
    inspection::restricted(tx, &bootstrap_login).await?;
    // Guard installation transfers native tables to the manager. Do not impose
    // bootstrap ownership of public tables when observing that completed state.
    inspection::require(tx,
        "SELECT NOT rolinherit AND rolconnlimit=8 AND NOT EXISTS(SELECT 1 FROM pg_auth_members WHERE roleid=r.oid OR member=r.oid) AND NOT EXISTS(SELECT 1 FROM pg_namespace WHERE nspowner=r.oid) AND NOT EXISTS(SELECT 1 FROM pg_database WHERE datdba=r.oid) AND ((NOT rolcanlogin AND rolpassword IS NULL) OR (rolcanlogin AND rolpassword IS NOT NULL)) FROM pg_authid r WHERE oid=$1::oid",
        &[&bootstrap_oid], GuardError::UnsafeRole).await?;
    let bootstrap_retired = tx
        .query_one(
            "SELECT NOT rolcanlogin FROM pg_authid WHERE oid=$1::oid",
            &[&bootstrap_oid],
        )
        .await
        .map_err(backend)?
        .try_get(0)
        .map_err(backend)?;
    Ok(Some(Observation {
        reservation: Reservation {
            database_oid,
            reservation_oid,
            initialization_id: *custody.initialization_id(),
            bootstrap_login,
            bootstrap_oid,
            manager_login: manager_login.into(),
            manager_oid,
        },
        bootstrap_retired,
    }))
}

/// Newly committed bootstrap material. Observation cannot construct this type.
pub struct FreshBootstrap {
    reservation: Reservation,
    password: Zeroizing<String>,
}
impl fmt::Debug for FreshBootstrap {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("FreshBootstrap([redacted])")
    }
}
impl FreshBootstrap {
    pub fn reservation(&self) -> &Reservation {
        &self.reservation
    }
    /// Consume once into the same custodied native lifetime. No argv/env/logs.
    pub fn with_login<T>(self, handoff: impl FnOnce(&str, &str) -> T) -> T {
        handoff(&self.reservation.bootstrap_login, &self.password)
    }
}

/// One provisioning attempt under fresh local custody. The caller must wrap its
/// actual PostgreSQL driver before polling it and preserve custody/grants through
/// terminal join. No fresh secret is returned on cancellation or unknown COMMIT.
pub async fn reserve(
    client: &mut Client,
    custody: &LocalInitialization,
    manager_login: &str,
    commit_started: &Cell<bool>,
) -> Result<FreshBootstrap, GuardError> {
    identifier(manager_login)?;
    custody
        .take_provisioning()
        .map_err(|_| GuardError::InvalidInput)?;
    let tx = super::begin(client, true).await?;
    let manager_oid = installation::manager(&tx, manager_login).await?;
    inspection::require(
        &tx,
        include_str!("../../../../../../contracts/archive/catalog-pristine-v1.sql"),
        &[],
        GuardError::ExistingData,
    )
    .await?;
    let row = tx
        .query_one(
            "SELECT oid,datname::text FROM pg_database WHERE datname=current_database()",
            &[],
        )
        .await
        .map_err(backend)?;
    let database_oid: u32 = row.try_get(0).map_err(backend)?;
    let database: &str = row.try_get(1).map_err(backend)?;
    let reservation_login = format!("bb_init_database_{database_oid}");
    let nonce = nonce(custody);
    let bootstrap_login = format!("bb_init_{nonce}");
    inspection::require(
        &tx,
        "SELECT NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname IN ($1::name,$2::name))",
        &[&reservation_login, &bootstrap_login],
        GuardError::Conflict,
    )
    .await?;
    let material = Material::generate()?;
    let reserved = identifier(&reservation_login)?;
    let bootstrap = identifier(&bootstrap_login)?;
    // Both names are retained permanently. The unique role-name index also
    // excludes a colliding nonce created concurrently in a different database.
    tx.batch_execute(&format!(
        "CREATE ROLE {reserved} NOLOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE NOINHERIT NOREPLICATION NOBYPASSRLS PASSWORD NULL; \
         CREATE ROLE {bootstrap} LOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE NOINHERIT NOREPLICATION NOBYPASSRLS CONNECTION LIMIT 8 PASSWORD '{}'",
        material.verifier.as_str()
    )).await.map_err(backend)?;
    let roles = tx.query_one(
        "SELECT (SELECT oid FROM pg_roles WHERE rolname=$1::name),(SELECT oid FROM pg_roles WHERE rolname=$2::name)",
        &[&reservation_login, &bootstrap_login],
    ).await.map_err(backend)?;
    let reservation_oid = roles.try_get(0).map_err(backend)?;
    let bootstrap_oid: u32 = roles.try_get(1).map_err(backend)?;
    let comment =
        format!("bigbrotr-catalog-initialization/1:{nonce}:{bootstrap_oid}:{manager_oid}");
    tx.batch_execute(&format!(
        "COMMENT ON ROLE {reserved} IS '{comment}'; GRANT CONNECT ON DATABASE {} TO {bootstrap}; GRANT USAGE,CREATE ON SCHEMA public TO {bootstrap}",
        identifier(database)?
    )).await.map_err(backend)?;
    // Restricted attributes alone miss inherited PUBLIC database/parameter
    // privileges. Refuse those before the only plaintext handoff can happen.
    inspection::bootstrap(&tx, &bootstrap_login).await?;
    commit_started.set(true);
    tx.commit().await.map_err(|_| GuardError::CommitUnknown)?;
    Ok(FreshBootstrap {
        reservation: Reservation {
            database_oid,
            reservation_oid,
            initialization_id: *custody.initialization_id(),
            bootstrap_login,
            bootstrap_oid,
            manager_login: manager_login.into(),
            manager_oid,
        },
        password: material.password,
    })
}
