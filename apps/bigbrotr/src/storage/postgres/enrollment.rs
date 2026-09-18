//! Serialized first-owner enrollment through a protected deployment connection.
//! No function here issues a session, policy lease or runtime authority.
use super::initialization::application_transaction;
use crate::management::credentials::{OwnerToken, PasswordCredential, random, validate_name};
use bigbrotr_model::{DatasetId, PrincipalId};
use std::{cell::Cell, fmt};
use tokio_postgres::{Client, Row, Transaction};

const ADMIN_ACTIONS: &[&str] = &[
    "data.read",
    "status.read",
    "roles.control",
    "budgets.change",
    "policy.change",
    "principals.manage",
    "audit.read",
];

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EnrollmentError {
    InvalidInput,
    Backend,
    Prerequisite,
    WrongDeployment,
    OwnerExists,
    PendingChallenge,
    RequestConflict,
    MissingChallenge,
    Expired,
    Entropy,
    CommitUnknown,
}
impl fmt::Display for EnrollmentError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "owner enrollment refused: {self:?}")
    }
}
impl std::error::Error for EnrollmentError {}
fn backend(_: tokio_postgres::Error) -> EnrollmentError {
    EnrollmentError::Backend
}

#[derive(Clone, Copy)]
pub struct Deployment {
    pub dataset: DatasetId,
    pub instance: DatasetId,
}
pub enum Operation<'a> {
    Issue {
        request_id: [u8; 16],
        username: &'a str,
    },
    Status,
    Revoke {
        challenge_id: [u8; 16],
    },
    Enroll {
        challenge_id: [u8; 16],
        token: &'a OwnerToken,
        credential: &'a PasswordCredential,
    },
}
pub struct EnrollmentReceipt {
    pub status: &'static str,
    pub challenge: Option<[u8; 16]>,
    pub principal: Option<PrincipalId>,
    pub username: Option<String>,
    pub expires_at_us: Option<i64>,
    pub failed_attempts: Option<i32>,
    pub audit: Option<[u8; 16]>,
    pub token: Option<OwnerToken>,
}
impl EnrollmentReceipt {
    fn empty(status: &'static str) -> Self {
        Self {
            status,
            challenge: None,
            principal: None,
            username: None,
            expires_at_us: None,
            failed_attempts: None,
            audit: None,
            token: None,
        }
    }
}
fn fixed<const N: usize>(row: &Row, index: usize) -> Result<[u8; N], EnrollmentError> {
    row.try_get::<_, &[u8]>(index)
        .map_err(backend)?
        .try_into()
        .map_err(|_| EnrollmentError::Prerequisite)
}
fn fresh() -> Result<[u8; 16], EnrollmentError> {
    random().map_err(|_| EnrollmentError::Entropy)
}

pub async fn execute(
    client: &mut Client,
    deployment: Deployment,
    operation: Operation<'_>,
    commit_started: &Cell<bool>,
) -> Result<EnrollmentReceipt, EnrollmentError> {
    match &operation {
        Operation::Issue {
            request_id,
            username,
        } => {
            validate_name(username).map_err(|_| EnrollmentError::InvalidInput)?;
            if *request_id == [0; 16] {
                return Err(EnrollmentError::InvalidInput);
            }
        }
        Operation::Revoke { challenge_id } | Operation::Enroll { challenge_id, .. }
            if *challenge_id == [0; 16] =>
        {
            return Err(EnrollmentError::InvalidInput);
        }
        _ => {}
    }
    let tx = application_transaction(client, 805)
        .await
        .map_err(|_| EnrollmentError::Prerequisite)?;
    // A separate database-local lock serializes issue/consume/revoke/status,
    // including resolution after a caller lost its COMMIT response.
    tx.query_one(
        "SELECT pg_catalog.pg_advisory_xact_lock($1,$2)",
        &[&0x4242_7472_i32, &0x4f57_4e52_i32],
    )
    .await
    .map_err(backend)?;
    let rows=tx.query("SELECT substring(d.dataset_id FROM 1 FOR 17), substring(d.instance_id FROM 1 FOR 17), substring(p.principal_id FROM 1 FOR 17) FROM control.datasets d JOIN control.principals p ON p.dataset_id=d.dataset_id AND p.kind='installation' WHERE d.config_revision IS NOT NULL LIMIT 2",&[]).await.map_err(backend)?;
    let [row] = rows.as_slice() else {
        return Err(EnrollmentError::Prerequisite);
    };
    if fixed::<16>(row, 0)? != *deployment.dataset.as_bytes()
        || fixed::<16>(row, 1)? != *deployment.instance.as_bytes()
    {
        return Err(EnrollmentError::WrongDeployment);
    }
    let actor = fixed::<16>(row, 2)?;
    let dataset = &deployment.dataset.as_bytes()[..];
    let owner=tx.query_opt("SELECT substring(principal_id FROM 1 FOR 17), left(username,65) FROM control.local_accounts WHERE dataset_id=$1::bytea AND initial_owner", &[&dataset]).await.map_err(backend)?;
    if let Some(owner) = owner {
        if !matches!(operation, Operation::Status) {
            return Err(EnrollmentError::OwnerExists);
        }
        let mut report = EnrollmentReceipt::empty("enrolled");
        report.principal = Some(
            PrincipalId::try_from(fixed::<16>(&owner, 0)?)
                .map_err(|_| EnrollmentError::Prerequisite)?,
        );
        report.username = Some(owner.try_get(1).map_err(backend)?);
        // Unique consume is the durable link to the original audit and challenge.
        let row=tx.query_one("SELECT substring(c.challenge_id FROM 1 FOR 17), substring(a.audit_id FROM 1 FOR 17) FROM control.owner_challenges c JOIN control.management_audit a ON a.dataset_id=c.dataset_id AND a.subject_id=c.challenge_id AND a.action='owner.enroll' AND a.outcome='accepted' WHERE c.dataset_id=$1::bytea AND c.consumed_principal=$2::bytea", &[&dataset,&&report.principal.unwrap().as_bytes()[..]]).await.map_err(backend)?;
        report.challenge = Some(fixed(&row, 0)?);
        report.audit = Some(fixed(&row, 1)?);
        tx.rollback().await.map_err(backend)?;
        return Ok(report);
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
    let challenge_id = match operation {
        Operation::Issue { request_id, .. } => Some(request_id),
        Operation::Revoke { challenge_id } | Operation::Enroll { challenge_id, .. } => {
            Some(challenge_id)
        }
        Operation::Status => None,
    };
    let row = if let Some(id) = challenge_id {
        tx.query_opt("SELECT substring(challenge_id FROM 1 FOR 17), left(username,65), substring(intended_principal FROM 1 FOR 17), substring(token_digest FROM 1 FOR 33), expires_at_us, left(state,9), failed_attempts FROM control.owner_challenges WHERE dataset_id=$1::bytea AND challenge_id=$2::bytea", &[&dataset,&&id[..]]).await.map_err(backend)?
    } else {
        tx.query_opt("SELECT substring(challenge_id FROM 1 FOR 17), left(username,65), substring(intended_principal FROM 1 FOR 17), substring(token_digest FROM 1 FOR 33), expires_at_us, left(state,9), failed_attempts FROM control.owner_challenges WHERE dataset_id=$1::bytea AND state='pending'", &[&dataset]).await.map_err(backend)?
    };
    let mut report = EnrollmentReceipt::empty("unclaimed");
    if let Some(row) = &row {
        report.challenge = Some(fixed(row, 0)?);
        report.username = Some(row.try_get(1).map_err(backend)?);
        report.expires_at_us = Some(row.try_get(4).map_err(backend)?);
        report.failed_attempts = Some(row.try_get(6).map_err(backend)?);
    }
    match operation {
        Operation::Status => {
            if let Some(row) = row {
                report.status = if row.try_get::<_, &str>(5).map_err(backend)? != "pending" {
                    "terminal"
                } else if report.expires_at_us.unwrap() <= now {
                    "expired"
                } else if report.failed_attempts == Some(32) {
                    "exhausted"
                } else {
                    "pending"
                };
            }
            tx.rollback().await.map_err(backend)?;
            return Ok(report);
        }
        Operation::Issue {
            request_id,
            username,
        } => {
            if row.is_some() {
                if report.username.as_deref() != Some(username) {
                    return Err(EnrollmentError::RequestConflict);
                }
                report.status = "secret-unavailable";
                tx.rollback().await.map_err(backend)?;
                return Ok(report);
            }
            if tx.query_one("SELECT EXISTS(SELECT 1 FROM control.owner_challenges WHERE dataset_id=$1::bytea AND state='pending')",&[&dataset]).await.map_err(backend)?.try_get::<_,bool>(0).map_err(backend)? {return Err(EnrollmentError::PendingChallenge)}
            let token = OwnerToken::generate().map_err(|_| EnrollmentError::Entropy)?;
            let principal = fresh()?;
            let digest = token.digest();
            let expires = now
                .checked_add(600_000_000)
                .ok_or(EnrollmentError::Prerequisite)?;
            tx.execute("INSERT INTO control.owner_challenges(challenge_id,dataset_id,username,intended_principal,token_digest,issued_at_us,expires_at_us,state,failed_attempts) VALUES($1::bytea,$2::bytea,$3::text,$4::bytea,$5::bytea,$6::bigint,$7::bigint,'pending',0)", &[&&request_id[..],&dataset,&username,&&principal[..],&&digest[..],&now,&expires]).await.map_err(backend)?;
            report.status = "issued";
            report.challenge = Some(request_id);
            report.username = Some(username.into());
            report.expires_at_us = Some(expires);
            report.failed_attempts = Some(0);
            report.token = Some(token);
            report.audit = Some(
                audit(
                    &tx,
                    deployment,
                    &actor,
                    &request_id,
                    "owner-token.issue",
                    None,
                    now,
                )
                .await?,
            );
        }
        Operation::Revoke { challenge_id } => {
            let row = row.ok_or(EnrollmentError::MissingChallenge)?;
            if row.try_get::<_, &str>(5).map_err(backend)? == "revoked" {
                report.status = "already-revoked";
                tx.rollback().await.map_err(backend)?;
                return Ok(report);
            }
            if row.try_get::<_, &str>(5).map_err(backend)? != "pending" {
                return Err(EnrollmentError::RequestConflict);
            }
            tx.execute(
                "UPDATE control.owner_challenges SET state='revoked' WHERE challenge_id=$1::bytea",
                &[&&challenge_id[..]],
            )
            .await
            .map_err(backend)?;
            report.status = "revoked";
            report.audit = Some(
                audit(
                    &tx,
                    deployment,
                    &actor,
                    &challenge_id,
                    "owner-token.revoke",
                    None,
                    now,
                )
                .await?,
            );
        }
        Operation::Enroll {
            challenge_id,
            token,
            credential,
        } => {
            let row = row.ok_or(EnrollmentError::MissingChallenge)?;
            let reason = if row.try_get::<_, &str>(5).map_err(backend)? != "pending" {
                Some("terminal")
            } else if report.expires_at_us.unwrap() <= now {
                Some("expired")
            } else if report.failed_attempts == Some(32) {
                Some("exhausted")
            } else if !token.matches(&fixed(&row, 3)?) {
                Some("invalid-token")
            } else {
                None
            };
            if let Some(reason) = reason {
                if reason == "invalid-token" {
                    tx.execute("UPDATE control.owner_challenges SET failed_attempts=failed_attempts+1 WHERE challenge_id=$1::bytea",&[&&challenge_id[..]]).await.map_err(backend)?;
                    report.failed_attempts = report.failed_attempts.map(|n| n + 1);
                }
                report.status = reason;
                report.audit = Some(
                    audit(
                        &tx,
                        deployment,
                        &actor,
                        &challenge_id,
                        "owner.enroll",
                        Some(reason),
                        now,
                    )
                    .await?,
                );
            } else {
                let principal = fixed::<16>(&row, 2)?;
                tx.execute("INSERT INTO control.principals(principal_id,dataset_id,kind,created_at_us,database_role,local_os_uid) VALUES($1::bytea,$2::bytea,'local',$3::bigint,SESSION_USER,$4::bigint)", &[&&principal[..],&dataset,&now,&i64::from(rustix::process::geteuid().as_raw())]).await.map_err(backend)?;
                tx.execute("INSERT INTO control.local_accounts(principal_id,dataset_id,username,initial_owner,active,credential_revision,grant_revision) VALUES($1::bytea,$2::bytea,$3::text,true,true,1,1)", &[&&principal[..],&dataset,&report.username.as_deref().unwrap()]).await.map_err(backend)?;
                tx.execute("INSERT INTO control.local_credentials(principal_id,dataset_id,credential_revision,format,argon_version,memory_kib,passes,lanes,salt,password_hash) VALUES($1::bytea,$2::bytea,1,'argon2id-v1',19,65536,3,1,$3::bytea,$4::bytea)", &[&&principal[..],&dataset,&&credential.salt[..],&&credential.hash[..]]).await.map_err(backend)?;
                for action in ADMIN_ACTIONS {
                    tx.execute("INSERT INTO control.principal_grants(dataset_id,principal_id,action,resource_kind,resource_id) VALUES($1::bytea,$2::bytea,$3::text,'dataset',$1::bytea)", &[&dataset,&&principal[..],action]).await.map_err(backend)?;
                }
                // Check the database clock again at the actual consume, after
                // every preceding allocation/write/lock delay. A stale earlier
                // timestamp cannot authorize a newly expired challenge.
                let changed = tx.execute("UPDATE control.owner_challenges SET state='consumed',consumed_principal=$2::bytea,consumed_at_us=(EXTRACT(EPOCH FROM pg_catalog.clock_timestamp())*1000000)::bigint WHERE challenge_id=$1::bytea AND state='pending' AND expires_at_us > (EXTRACT(EPOCH FROM pg_catalog.clock_timestamp())*1000000)::bigint", &[&&challenge_id[..],&&principal[..]]).await.map_err(backend)?;
                if changed != 1 {
                    return Err(EnrollmentError::Expired);
                }
                report.status = "enrolled";
                report.principal = Some(
                    PrincipalId::try_from(principal).map_err(|_| EnrollmentError::Prerequisite)?,
                );
                report.audit = Some(
                    audit(
                        &tx,
                        deployment,
                        &principal,
                        &challenge_id,
                        "owner.enroll",
                        None,
                        now,
                    )
                    .await?,
                );
            }
        }
    }
    commit_started.set(true);
    tx.commit()
        .await
        .map_err(|_| EnrollmentError::CommitUnknown)?;
    Ok(report)
}

async fn audit(
    tx: &Transaction<'_>,
    deployment: Deployment,
    actor: &[u8; 16],
    subject: &[u8; 16],
    action: &str,
    reason: Option<&str>,
    now: i64,
) -> Result<[u8; 16], EnrollmentError> {
    let id = fresh()?;
    let outcome = if reason.is_some() {
        "rejected"
    } else {
        "accepted"
    };
    tx.execute("INSERT INTO control.management_audit(audit_id,dataset_id,actor_id,action,outcome,created_at_us,subject_id,reason,requester_database_role,requester_os_uid) VALUES($1::bytea,$2::bytea,$3::bytea,$4::text,$5::text,$6::bigint,$7::bytea,$8::text,SESSION_USER,$9::bigint)", &[&&id[..],&&deployment.dataset.as_bytes()[..],&&actor[..],&action,&outcome,&now,&&subject[..],&reason,&i64::from(rustix::process::geteuid().as_raw())]).await.map_err(backend)?;
    Ok(id)
}
