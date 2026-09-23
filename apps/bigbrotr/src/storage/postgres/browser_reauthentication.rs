//! Session-bound password work, outside short fenced Control transactions.
use super::{ControlSession, browser_sessions::*, require};
use crate::{
    management::{
        credentials::random,
        grants::GrantCommand,
        reauthentication::{
            Binding, ReauthenticationChallenge, ReauthenticationProof, VerifiedReauthentication,
        },
        sessions::{BrowserSecret, LoginChallenge, SessionError},
    },
    storage::postgres::initialization::application_transaction,
};
use bigbrotr_model::{CommandKey, DatasetId, PrincipalId, ReauthenticationId};
use std::net::IpAddr;
use tokio_postgres::{Row, Transaction};

impl ControlSession {
    pub async fn begin_browser_reauthentication(
        &mut self,
        token: &BrowserSecret,
        csrf: &BrowserSecret,
        key: CommandKey,
        command: &GrantCommand,
        peer: IpAddr,
    ) -> Result<ReauthenticationChallenge, SessionError> {
        command.validate(self.request.dataset)?;
        self.begin_operation()
            .map_err(|_| SessionError::Unavailable)?;
        let tx = application_transaction(&mut self.client, 809)
            .await
            .map_err(|_| SessionError::Prerequisite)?;
        require(&tx, self.request.dataset, self.identity)
            .await
            .map_err(|_| SessionError::Unavailable)?;
        lock_dataset(&tx, self.request.dataset, self.validated_policy).await?;
        let outcome = async {
            let current = command_session(
                &tx,
                self.request.dataset,
                &token.session_digest(),
                &csrf.csrf_digest(),
            )
            .await?;
            if command.expected_config_revision.get() != self.validated_policy.config_revision
                || command.expected_policy_revision.get() != self.validated_policy.policy_revision
            {
                return Err(SessionError::Conflict);
            }
            let digest = command.digest(self.request.dataset, current.principal, key)?;
            let started = now(&tx).await?;
            if !charge_attempts(&tx, self.request.dataset, &current.username, peer, started).await?
            {
                return Err(SessionError::Throttled);
            }
            let account = account_by_name(&tx, self.request.dataset, &current.username)
                .await?
                .ok_or(SessionError::InvalidMetadata)?;
            Ok(ReauthenticationChallenge {
                login: LoginChallenge {
                    dataset: self.request.dataset,
                    incarnation: self.request.incarnation,
                    issued_at_us: started,
                    account: Some(account),
                },
                binding: Binding {
                    session_id: current.id,
                    token_digest: token.session_digest(),
                    csrf_digest: csrf.csrf_digest(),
                    key,
                    digest,
                    grant_revision: revision(&current.account, 3)?,
                    config_revision: self.validated_policy.config_revision,
                    policy_revision: self.validated_policy.policy_revision,
                },
            })
        }
        .await;
        let outcome = terminal(&tx, outcome)?;
        tx.commit().await.map_err(|_| SessionError::CommitUnknown)?;
        self.available = true;
        outcome
    }

    pub async fn finish_browser_reauthentication(
        &mut self,
        verified: VerifiedReauthentication,
    ) -> Result<ReauthenticationProof, SessionError> {
        let challenge = verified.login.0;
        if challenge.dataset != self.request.dataset
            || challenge.incarnation != self.request.incarnation
        {
            return Err(SessionError::Denied);
        }
        let account = challenge.account.ok_or(SessionError::Denied)?;
        self.begin_operation()
            .map_err(|_| SessionError::Unavailable)?;
        let tx = application_transaction(&mut self.client, 809)
            .await
            .map_err(|_| SessionError::Prerequisite)?;
        require(&tx, self.request.dataset, self.identity)
            .await
            .map_err(|_| SessionError::Unavailable)?;
        lock_dataset(&tx, self.request.dataset, self.validated_policy).await?;
        let outcome = async {
            let b = verified.binding;
            let current = command_session(&tx, self.request.dataset, &b.token_digest, &b.csrf_digest).await?;
            let matched = tx.query_opt("SELECT salt=$3::bytea AND password_hash=$4::bytea AND credential_revision=$5::text::numeric AND format='argon2id-v1' AND argon_version=19 AND memory_kib=65536 AND passes=3 AND lanes=1 FROM control.local_credentials WHERE dataset_id=$1::bytea AND principal_id=$2::bytea FOR UPDATE",
                &[&&self.request.dataset.as_bytes()[..], &&account.principal.as_bytes()[..], &&account.salt[..], &&account.password_hash[..], &account.credential_revision.to_string()]).await.map_err(backend)?;
            let time = now(&tx).await?;
            if current.principal != account.principal || current.id != b.session_id
                || revision(&current.account, 1)? != account.credential_revision
                || revision(&current.account, 2)? != account.authentication_revision
                || revision(&current.account, 3)? != b.grant_revision
                || self.validated_policy.config_revision != b.config_revision
                || self.validated_policy.policy_revision != b.policy_revision
                || time >= current.expires
                || time.checked_sub(challenge.issued_at_us).is_none_or(|age| !(0..20_000_000).contains(&age))
                || !matched.as_ref().map(|r| r.try_get::<_, bool>(0).map_err(metadata)).transpose()?.unwrap_or(false) {
                return Err(SessionError::Denied);
            }
            issue_proof(&tx, self.request.dataset, &current, &b, time).await
        }.await;
        let outcome = terminal(&tx, outcome)?;
        tx.commit().await.map_err(|_| SessionError::CommitUnknown)?;
        self.available = true;
        outcome
    }
}

pub(super) struct CommandSession {
    pub(super) principal: PrincipalId,
    pub(super) account: Row,
    pub(super) id: [u8; 16],
    username: String,
    pub(super) expires: i64,
}
async fn command_session(
    tx: &Transaction<'_>,
    dataset: DatasetId,
    token: &[u8; 32],
    csrf: &[u8; 32],
) -> Result<CommandSession, SessionError> {
    let current = authenticated_session(tx, dataset, token, csrf, None).await?;
    require_manager(tx, dataset, current.principal).await?;
    Ok(current)
}

/// Authentication precedes historical command resolution. Fresh permission and
/// proof checks follow it, so a retry cannot reapply a superseded grant.
pub(super) async fn authenticated_session(
    tx: &Transaction<'_>,
    dataset: DatasetId,
    token: &[u8; 32],
    csrf: &[u8; 32],
    target: Option<PrincipalId>,
) -> Result<CommandSession, SessionError> {
    let row = tx.query_opt("SELECT substring(principal_id FROM 1 FOR 17) FROM control.browser_sessions WHERE dataset_id=$1::bytea AND token_digest=$2::bytea", &[&&dataset.as_bytes()[..], &&token[..]]).await.map_err(backend)?.ok_or(SessionError::Denied)?;
    let actor = principal(&row, 0)?;
    if let Some(target) = target {
        // Acquire both before any session/proof row, including self-target.
        tx.query("SELECT principal_id FROM control.local_accounts WHERE dataset_id=$1::bytea AND principal_id IN ($2::bytea,$3::bytea) ORDER BY principal_id FOR UPDATE",
            &[&&dataset.as_bytes()[..], &&actor.as_bytes()[..], &&target.as_bytes()[..]]).await.map_err(backend)?;
    }
    let account = lock_account(tx, dataset, actor).await?;
    let row = tx.query_opt("SELECT substring(s.session_id FROM 1 FOR 17),left(s.credential_revision::text,21),left(s.authentication_revision::text,21),s.csrf_digest=$3::bytea,s.revoked_at_us IS NULL,s.last_seen_at_us,LEAST(s.expires_at_us,s.idle_expires_at_us),left(a.username,65) FROM control.browser_sessions s JOIN control.local_accounts a USING(dataset_id,principal_id) WHERE s.dataset_id=$1::bytea AND s.token_digest=$2::bytea FOR UPDATE OF s",
        &[&&dataset.as_bytes()[..], &&token[..], &&csrf[..]]).await.map_err(backend)?.ok_or(SessionError::Denied)?;
    let time = now(tx).await?;
    let expires = row.try_get::<_, i64>(6).map_err(metadata)?;
    if !account.try_get::<_, bool>(0).map_err(metadata)?
        || !row.try_get::<_, bool>(3).map_err(metadata)?
        || !row.try_get::<_, bool>(4).map_err(metadata)?
        || revision(&account, 1)? != revision(&row, 1)?
        || revision(&account, 2)? != revision(&row, 2)?
        || time < row.try_get::<_, i64>(5).map_err(metadata)?
        || time >= expires
    {
        return Err(SessionError::Denied);
    }
    Ok(CommandSession {
        principal: actor,
        account,
        id: fixed(&row, 0)?,
        username: row.try_get(7).map_err(metadata)?,
        expires,
    })
}

pub(super) async fn require_manager(
    tx: &Transaction<'_>,
    dataset: DatasetId,
    actor: PrincipalId,
) -> Result<(), SessionError> {
    let permitted: bool = tx.query_one("SELECT EXISTS(SELECT 1 FROM control.principal_grants WHERE dataset_id=$1::bytea AND principal_id=$2::bytea AND action='principals.manage' AND resource_kind='dataset' AND resource_id=dataset_id)", &[&&dataset.as_bytes()[..], &&actor.as_bytes()[..]]).await.map_err(backend)?.try_get(0).map_err(metadata)?;
    if !permitted {
        return Err(SessionError::Forbidden);
    }
    Ok(())
}

async fn issue_proof(
    tx: &Transaction<'_>,
    dataset: DatasetId,
    current: &CommandSession,
    binding: &Binding,
    time: i64,
) -> Result<ReauthenticationProof, SessionError> {
    let actor = current.principal;
    // The account lock is already held, BEFORE any proof lock/delete. Expiry
    // cleanup is bounded by the cap and never erases the immutable issuance audit.
    tx.execute("DELETE FROM control.reauthentication_proofs WHERE dataset_id=$1::bytea AND actor_id=$2::bytea AND expires_at_us<=$3::bigint", &[&&dataset.as_bytes()[..], &&actor.as_bytes()[..], &time]).await.map_err(backend)?;
    if let Some(row) = tx.query_opt("SELECT substring(proof_id FROM 1 FOR 17),expires_at_us,request_digest=$3::bytea AND config_revision=$4::text::numeric AND policy_revision=$5::text::numeric AND credential_revision=$6::text::numeric AND authentication_revision=$7::text::numeric AND grant_revision=$8::text::numeric FROM control.reauthentication_proofs WHERE session_id=$1::bytea AND request_key=$2::bytea",
        &[&&current.id[..], &&binding.key.as_bytes()[..], &&binding.digest.as_bytes()[..], &binding.config_revision.to_string(), &binding.policy_revision.to_string(), &revision(&current.account,1)?.to_string(), &revision(&current.account,2)?.to_string(), &binding.grant_revision.to_string()]).await.map_err(backend)? {
        if !row.try_get::<_, bool>(2).map_err(metadata)? { return Err(SessionError::Conflict); }
        return Ok(ReauthenticationProof { id: ReauthenticationId::try_from(fixed::<16>(&row,0)?).map_err(|_| SessionError::InvalidMetadata)?, command_key: binding.key, command_digest: binding.digest, expires_at_us: row.try_get(1).map_err(metadata)? });
    }
    let count: i64 = tx.query_one("SELECT count(*) FROM control.reauthentication_proofs WHERE dataset_id=$1::bytea AND actor_id=$2::bytea", &[&&dataset.as_bytes()[..], &&actor.as_bytes()[..]]).await.map_err(backend)?.try_get(0).map_err(metadata)?;
    if count >= 32 {
        return Err(SessionError::Capacity);
    }
    let id = ReauthenticationId::try_from(random::<16>().map_err(|_| SessionError::Unavailable)?)
        .map_err(|_| SessionError::Unavailable)?;
    let expires = time
        .checked_add(300_000_000)
        .ok_or(SessionError::InvalidMetadata)?
        .min(current.expires);
    let changed = tx.execute("INSERT INTO control.reauthentication_proofs(proof_id,dataset_id,actor_id,session_id,request_key,request_digest,config_revision,policy_revision,credential_revision,authentication_revision,grant_revision,issued_at_us,expires_at_us) VALUES($1::bytea,$2::bytea,$3::bytea,$4::bytea,$5::bytea,$6::bytea,$7::text::numeric,$8::text::numeric,$9::text::numeric,$10::text::numeric,$11::text::numeric,$12::bigint,$13::bigint)",
        &[&&id.as_bytes()[..], &&dataset.as_bytes()[..], &&actor.as_bytes()[..], &&current.id[..], &&binding.key.as_bytes()[..], &&binding.digest.as_bytes()[..], &binding.config_revision.to_string(), &binding.policy_revision.to_string(), &revision(&current.account,1)?.to_string(), &revision(&current.account,2)?.to_string(), &binding.grant_revision.to_string(), &time, &expires]).await.map_err(backend)?;
    if changed == 0 {
        return Err(SessionError::Denied);
    }
    if changed != 1 {
        return Err(SessionError::InvalidMetadata);
    }
    let audit: [u8; 16] = random().map_err(|_| SessionError::Unavailable)?;
    tx.execute("INSERT INTO control.management_audit(audit_id,dataset_id,actor_id,action,outcome,created_at_us,config_revision,subject_id,policy_revision) VALUES($1::bytea,$2::bytea,$3::bytea,'browser.session.reauthenticate','accepted',$4::bigint,$5::text::numeric,$6::bytea,$7::text::numeric)",
        &[&&audit[..], &&dataset.as_bytes()[..], &&actor.as_bytes()[..], &time, &binding.config_revision.to_string(), &&id.as_bytes()[..], &binding.policy_revision.to_string()]).await.map_err(backend)?;
    Ok(ReauthenticationProof {
        id,
        command_key: binding.key,
        command_digest: binding.digest,
        expires_at_us: expires,
    })
}
