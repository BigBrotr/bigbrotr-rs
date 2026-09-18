//! Finite, local credential-backed self-inspection. No reusable authority is returned.
use super::{enrollment::Deployment, initialization::application_transaction};
use crate::{
    management::credentials::{CredentialError, validate_name, verify_password},
    runtime::OwnedPermit,
};
use bigbrotr_model::PrincipalId;
use std::fmt;
use tokio_postgres::{Client, Row};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AuthenticationError {
    Denied,
    Backend,
    Prerequisite,
    WrongDeployment,
    InvalidMetadata,
    Capacity,
}
impl fmt::Display for AuthenticationError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "local authentication refused: {self:?}")
    }
}
impl std::error::Error for AuthenticationError {}
fn backend(_: tokio_postgres::Error) -> AuthenticationError {
    AuthenticationError::Backend
}
fn metadata(_: tokio_postgres::Error) -> AuthenticationError {
    AuthenticationError::InvalidMetadata
}

/// Point-in-time private account data, not a session, action grant or policy lease.
/// Never accept this observation as authentication for a later command.
pub struct AccountObservation {
    pub principal: PrincipalId,
    pub credential_revision: u64,
    pub grant_revision: u64,
    pub security_revision: Option<u64>,
    pub stored_actions: Vec<String>,
}

/// The caller owns the protected deployment connection and lends one exclusive
/// hash permit through completion. Only the finite local CLI calls this now;
/// remote login needs its own persistent attempt limits and session transaction.
pub async fn inspect_self(
    client: &mut Client,
    deployment: Deployment,
    username: &str,
    password: &[u8],
    grant: &mut OwnedPermit,
) -> Result<AccountObservation, AuthenticationError> {
    validate_name(username).map_err(|_| AuthenticationError::Denied)?;
    let tx = application_transaction(client, 805)
        .await
        .map_err(|error| match error {
            super::initialization::InitializeError::Backend => AuthenticationError::Backend,
            _ => AuthenticationError::Prerequisite,
        })?;
    // A single bounded snapshot binds all account, credential and grant fields.
    // No lock remains held while the deliberately expensive hash executes.
    let rows = tx.query(r#"
        SELECT substring(d.dataset_id FROM 1 FOR 17) AS dataset,
            substring(d.instance_id FROM 1 FOR 17) AS instance,
            d.config_revision IS NOT NULL AS initialized,
            left(d.policy_revision::text,21) AS security_revision,
            substring(a.principal_id FROM 1 FOR 17) AS principal,
            a.active, left(a.credential_revision::text,21) AS credential_revision,
            left(a.grant_revision::text,21) AS grant_revision,
            a.credential_revision = c.credential_revision AS same_credential,
            c.format = 'argon2id-v1' AND c.argon_version = 19 AND c.memory_kib = 65536
                AND c.passes = 3 AND c.lanes = 1 AS supported,
            substring(c.salt FROM 1 FOR 17) AS salt,
            substring(c.password_hash FROM 1 FOR 33) AS password_hash,
            ARRAY(SELECT CASE WHEN g.resource_kind = 'dataset' AND g.resource_id = d.dataset_id
                THEN left(g.action,65) ELSE 'invalid-resource' END
                FROM control.principal_grants g WHERE g.dataset_id = d.dataset_id
                    AND g.principal_id = a.principal_id ORDER BY g.action COLLATE "C" LIMIT 8) AS actions
        FROM control.datasets d
        LEFT JOIN control.local_accounts a ON a.dataset_id = d.dataset_id AND a.username = $1::text
        LEFT JOIN control.local_credentials c ON c.dataset_id = a.dataset_id AND c.principal_id = a.principal_id
        LIMIT 2
    "#, &[&username]).await.map_err(backend)?;
    tx.rollback().await.map_err(backend)?;
    let [row] = rows.as_slice() else {
        return Err(AuthenticationError::Prerequisite);
    };
    if fixed::<16>(row, "dataset")? != *deployment.dataset.as_bytes()
        || fixed::<16>(row, "instance")? != *deployment.instance.as_bytes()
    {
        return Err(AuthenticationError::WrongDeployment);
    }
    if !row.try_get::<_, bool>("initialized").map_err(metadata)? {
        return Err(AuthenticationError::Prerequisite);
    }
    let principal: Option<&[u8]> = row.try_get("principal").map_err(metadata)?;
    let Some(principal) = principal else {
        // Unknown accounts still pay the baseline hash cost, without inventing
        // a database identity or storing attacker-selected account names.
        check_password(password, &[0; 16], &[0; 32], grant)?;
        return Err(AuthenticationError::Denied);
    };
    let principal = PrincipalId::try_from(
        <[u8; 16]>::try_from(principal).map_err(|_| AuthenticationError::InvalidMetadata)?,
    )
    .map_err(|_| AuthenticationError::InvalidMetadata)?;
    if !row
        .try_get::<_, bool>("same_credential")
        .map_err(metadata)?
        || !row.try_get::<_, bool>("supported").map_err(metadata)?
    {
        return Err(AuthenticationError::InvalidMetadata);
    }
    let matched = check_password(
        password,
        &fixed(row, "salt")?,
        &fixed(row, "password_hash")?,
        grant,
    )?;
    if !matched || !row.try_get::<_, bool>("active").map_err(metadata)? {
        return Err(AuthenticationError::Denied);
    }
    let stored_actions: Vec<String> = row.try_get("actions").map_err(metadata)?;
    const ACTIONS: &[&str] = &[
        "audit.read",
        "budgets.change",
        "data.read",
        "policy.change",
        "principals.manage",
        "roles.control",
        "status.read",
    ];
    if stored_actions.len() > ACTIONS.len()
        || stored_actions
            .iter()
            .any(|action| !ACTIONS.contains(&action.as_str()))
        || stored_actions.windows(2).any(|pair| pair[0] >= pair[1])
    {
        return Err(AuthenticationError::InvalidMetadata);
    }
    let security_revision = row
        .try_get::<_, Option<&str>>("security_revision")
        .map_err(metadata)?
        .map(revision)
        .transpose()?;
    Ok(AccountObservation {
        principal,
        credential_revision: revision(row.try_get("credential_revision").map_err(metadata)?)?,
        grant_revision: revision(row.try_get("grant_revision").map_err(metadata)?)?,
        security_revision,
        stored_actions,
    })
}
fn fixed<const N: usize>(row: &Row, name: &str) -> Result<[u8; N], AuthenticationError> {
    row.try_get::<_, &[u8]>(name)
        .map_err(metadata)?
        .try_into()
        .map_err(|_| AuthenticationError::InvalidMetadata)
}
fn revision(text: &str) -> Result<u64, AuthenticationError> {
    text.parse::<u64>()
        .ok()
        .filter(|n| *n > 0 && n.to_string() == text)
        .ok_or(AuthenticationError::InvalidMetadata)
}
fn check_password(
    password: &[u8],
    salt: &[u8; 16],
    expected: &[u8; 32],
    grant: &mut OwnedPermit,
) -> Result<bool, AuthenticationError> {
    verify_password(password, salt, expected, grant).map_err(|error| match error {
        CredentialError::InvalidPassword => AuthenticationError::Denied,
        CredentialError::Capacity => AuthenticationError::Capacity,
        _ => AuthenticationError::Backend,
    })
}
