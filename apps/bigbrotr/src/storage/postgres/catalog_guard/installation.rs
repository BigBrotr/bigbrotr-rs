//! Explicit installer and runtime owner separation. This finite transaction
//! consumes already resolved role identities, never provisions/exposes secrets.
use super::{CatalogIdentity, GuardError, backend, identifier, inspection::require};
use std::cell::Cell;
use tokio_postgres::{Client, Transaction};

#[derive(Debug, Clone, Copy)]
pub struct InstallationRoles<'a> {
    pub bootstrap_login: &'a str,
    pub bootstrap_oid: u32,
    pub manager_login: &'a str,
    pub manager_oid: u32,
}

/// Install for the configured runtime manager without promoting it. The caller
/// must first positively exclude its sole native initializer and provisioning
/// driver, retire that exact bootstrap login/password, and resolve its backends.
/// These role/ACL observations cannot establish exclusion or authorize a retry
/// of an ambiguous initializer. Preserve the durable database/root reservations.
pub async fn install_for_manager(
    client: &mut Client,
    identity: &CatalogIdentity,
    roles: InstallationRoles<'_>,
    commit_started: &Cell<bool>,
) -> Result<bool, GuardError> {
    if !identity.scientific
        || roles.bootstrap_oid == 0
        || roles.manager_oid == 0
        || roles.bootstrap_oid == roles.manager_oid
        || roles.bootstrap_login == roles.manager_login
    {
        return Err(GuardError::InvalidInput);
    }
    identifier(roles.bootstrap_login)?;
    identifier(roles.manager_login)?;
    super::install_inner(
        client,
        identity,
        roles.bootstrap_login,
        Some(roles),
        commit_started,
    )
    .await
}

pub(super) async fn manager(tx: &Transaction<'_>, login: &str) -> Result<u32, GuardError> {
    // A separate initial superuser is the narrow supported provisioner. Runtime
    // manager CREATEROLE is sufficient later; no temporary superuser alteration.
    let row = tx.query_opt(
        r#"
SELECT m.oid, current_user=session_user AND (SELECT rolsuper FROM pg_roles WHERE rolname=current_user)
   AND m.rolname<>current_user AND m.rolcanlogin AND m.rolcreaterole
   AND NOT m.rolsuper AND NOT m.rolcreatedb AND NOT m.rolreplication
   AND NOT m.rolbypassrls AND NOT m.rolinherit
   AND NOT has_parameter_privilege(m.oid,'session_replication_role','SET')
   AND NOT EXISTS (SELECT 1 FROM pg_auth_members a WHERE a.roleid=m.oid
       OR (a.member=m.oid AND (NOT a.admin_option OR a.inherit_option OR a.set_option)))
FROM pg_roles m WHERE m.rolname=$1::name"#,
        &[&login],
    )
    .await.map_err(backend)?.ok_or(GuardError::UnsafeRole)?;
    if !row.try_get::<_, bool>(1).map_err(backend)? {
        return Err(GuardError::UnsafeRole);
    }
    row.try_get(0).map_err(backend)
}

pub(super) async fn validate(
    tx: &Transaction<'_>,
    roles: InstallationRoles<'_>,
) -> Result<String, GuardError> {
    if manager(tx, roles.manager_login).await? != roles.manager_oid {
        return Err(GuardError::UnsafeRole);
    }
    require(
        tx,
        r#"
SELECT oid=$2::oid AND rolname<>current_user AND NOT rolcanlogin AND rolpassword IS NULL
FROM pg_authid WHERE rolname=$1::name"#,
        &[&roles.bootstrap_login, &roles.bootstrap_oid],
        GuardError::UnsafeRole,
    )
    .await?;
    identifier(roles.manager_login)
}

pub(super) async fn transfer_guard(
    tx: &Transaction<'_>,
    roles: InstallationRoles<'_>,
) -> Result<(), GuardError> {
    let owner = identifier(roles.manager_login)?;
    let database: String = tx
        .query_one("SELECT current_database()", &[])
        .await
        .map_err(backend)?
        .try_get(0)
        .map_err(backend)?;
    let mut sql = format!(
        "GRANT CONNECT ON DATABASE {} TO {owner}; GRANT USAGE ON SCHEMA public TO {owner}; ALTER SCHEMA bigbrotr_archive OWNER TO {owner};",
        identifier(&database)?
    );
    for table in ["installation", "attempts", "owner", "scientific_schema"] {
        sql.push_str(&format!(
            "ALTER TABLE bigbrotr_archive.{table} OWNER TO {owner};"
        ));
    }
    for function in ["require_writer", "retain_history"] {
        sql.push_str(&format!(
            "ALTER FUNCTION bigbrotr_archive.{function}() OWNER TO {owner};"
        ));
    }
    tx.batch_execute(&sql).await.map_err(backend)
}

pub(super) async fn expected_owner(
    tx: &Transaction<'_>,
    roles: InstallationRoles<'_>,
) -> Result<(), GuardError> {
    require(
        tx,
        "SELECT nspowner=$1::oid FROM pg_namespace WHERE nspname='bigbrotr_archive'",
        &[&roles.manager_oid],
        GuardError::UnsafeRole,
    )
    .await
}
