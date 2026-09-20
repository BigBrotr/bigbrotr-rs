//! Short fenced transactions surrounding, never containing, password hashing.
use super::{ControlSession, PolicySnapshot, require};
use crate::{
    management::{
        credentials::{random, validate_name},
        sessions::{
            BrowserSecret, IssuedSession, LoginAccount, LoginChallenge, SessionError,
            SessionLifetime, SessionObservation, VerifiedLogin,
        },
    },
    storage::postgres::initialization::application_transaction,
};
use bigbrotr_model::{DatasetId, PrincipalId};
use sha2::{Digest, Sha256};
use std::net::IpAddr;
use tokio_postgres::{Row, Transaction};
use zeroize::Zeroizing;

const HASH_MAX_AGE_US: i64 = 20_000_000;
fn backend(_: tokio_postgres::Error) -> SessionError {
    SessionError::Backend
}
fn metadata(_: tokio_postgres::Error) -> SessionError {
    SessionError::InvalidMetadata
}

impl ControlSession {
    /// Startup prerequisite for the real private listener, before accepting any
    /// credentials. This is not a user session or a policy/action capability.
    pub async fn require_browser_sessions(&mut self) -> Result<(), SessionError> {
        self.begin_operation()
            .map_err(|_| SessionError::Unavailable)?;
        let tx = application_transaction(&mut self.client, 806)
            .await
            .map_err(|_| SessionError::Prerequisite)?;
        require(&tx, self.request.dataset, self.identity)
            .await
            .map_err(|_| SessionError::Unavailable)?;
        lock_dataset(&tx, self.request.dataset, self.validated_policy).await?;
        // PostgreSQL's comma-separated privilege argument means ANY, not ALL.
        // Check each required privilege separately before aggregating.
        let row = tx.query_one("SELECT bool_and(has_table_privilege(CURRENT_USER,t.name,p.privilege)) FROM (VALUES ('control.browser_sessions',ARRAY['SELECT','INSERT','UPDATE','DELETE']),('control.login_buckets',ARRAY['SELECT','INSERT','UPDATE']),('control.local_accounts',ARRAY['SELECT','UPDATE']),('control.local_credentials',ARRAY['SELECT','UPDATE']),('control.principal_grants',ARRAY['SELECT'])) t(name,privileges) CROSS JOIN LATERAL unnest(t.privileges) p(privilege)", &[]).await.map_err(backend)?;
        if !row.try_get::<_, bool>(0).map_err(metadata)? {
            return Err(SessionError::Prerequisite);
        }
        tx.rollback().await.map_err(backend)?;
        self.available = true;
        Ok(())
    }
    /// Charges fixed persistent account/peer buckets before returning any hash
    /// work. The peer is the socket address supplied by the private listener,
    /// never a caller-controlled forwarding header.
    pub async fn begin_browser_login(
        &mut self,
        username: &str,
        peer: IpAddr,
    ) -> Result<LoginChallenge, SessionError> {
        validate_name(username).map_err(|_| SessionError::Denied)?;
        self.begin_operation()
            .map_err(|_| SessionError::Unavailable)?;
        let tx = application_transaction(&mut self.client, 806)
            .await
            .map_err(|_| SessionError::Prerequisite)?;
        require(&tx, self.request.dataset, self.identity)
            .await
            .map_err(|_| SessionError::Unavailable)?;
        lock_dataset(&tx, self.request.dataset, self.validated_policy).await?;
        let now = now(&tx).await?;
        // Account before peer is a fixed lock order, including hash collisions.
        let account_bucket = bucket(b"account", username.as_bytes());
        let peer_bucket = match peer {
            IpAddr::V4(ip) => bucket(b"peer4", &ip.octets()),
            IpAddr::V6(ip) => match ip.to_ipv4_mapped() {
                Some(ip) => bucket(b"peer4", &ip.octets()),
                None => bucket(b"peer6", &ip.octets()),
            },
        };
        let admitted = charge(&tx, self.request.dataset, "account", account_bucket, now, 5).await?
            & charge(&tx, self.request.dataset, "peer", peer_bucket, now, 20).await?;
        let outcome = if admitted {
            let account = account_by_name(&tx, self.request.dataset, username).await?;
            Ok(LoginChallenge {
                dataset: self.request.dataset,
                incarnation: self.request.incarnation,
                issued_at_us: now,
                account,
            })
        } else {
            Err(SessionError::Throttled)
        };
        // Even denial persists the charge. No browser credential is minted here.
        tx.commit().await.map_err(|_| SessionError::CommitUnknown)?;
        self.available = true;
        outcome
    }

    /// Consumes a verified hash exactly once. A lost issuance reply cannot recover
    /// the plaintext token; a subsequent explicit login pays a new attempt and
    /// session slot. There is no automatic retry creating a second credential.
    pub async fn finish_browser_login(
        &mut self,
        verified: VerifiedLogin,
        lifetime: SessionLifetime,
    ) -> Result<IssuedSession, SessionError> {
        let challenge = verified.0;
        if challenge.dataset != self.request.dataset
            || challenge.incarnation != self.request.incarnation
        {
            return Err(SessionError::Denied);
        }
        let account = challenge.account.ok_or(SessionError::Denied)?;
        let session = BrowserSecret::generate()?;
        let csrf = BrowserSecret::generate()?;
        let id: [u8; 16] = random().map_err(|_| SessionError::Unavailable)?;
        self.begin_operation()
            .map_err(|_| SessionError::Unavailable)?;
        let tx = application_transaction(&mut self.client, 806)
            .await
            .map_err(|_| SessionError::Prerequisite)?;
        require(&tx, self.request.dataset, self.identity)
            .await
            .map_err(|_| SessionError::Unavailable)?;
        let policy = lock_dataset(&tx, self.request.dataset, self.validated_policy).await?;
        let outcome = issue(
            &tx,
            self.request.dataset,
            &account,
            challenge.issued_at_us,
            lifetime,
            &id,
            &session,
            &csrf,
            policy,
        )
        .await;
        let outcome = terminal(&tx, outcome)?;
        tx.commit().await.map_err(|_| SessionError::CommitUnknown)?;
        self.available = true;
        outcome.map(|observation| IssuedSession {
            session,
            csrf,
            observation,
        })
    }

    /// A client request may extend idle expiry. Internal policy polling must use
    /// touch=false; it cannot keep an unattended browser session alive.
    pub async fn inspect_browser_session(
        &mut self,
        token: &BrowserSecret,
        touch: bool,
    ) -> Result<SessionObservation, SessionError> {
        self.begin_operation()
            .map_err(|_| SessionError::Unavailable)?;
        let tx = application_transaction(&mut self.client, 806)
            .await
            .map_err(|_| SessionError::Prerequisite)?;
        require(&tx, self.request.dataset, self.identity)
            .await
            .map_err(|_| SessionError::Unavailable)?;
        let policy = lock_dataset(&tx, self.request.dataset, self.validated_policy).await?;
        let outcome = inspect(&tx, self.request.dataset, token, touch, policy).await;
        let outcome = terminal(&tx, outcome)?;
        tx.commit().await.map_err(|_| SessionError::CommitUnknown)?;
        self.available = true;
        outcome
    }

    /// Idempotent only for possession of both original credentials. This records
    /// revocation; it does not assert that future data delivery has drained.
    pub async fn revoke_browser_session(
        &mut self,
        token: &BrowserSecret,
        csrf: &BrowserSecret,
    ) -> Result<(), SessionError> {
        self.begin_operation()
            .map_err(|_| SessionError::Unavailable)?;
        let tx = application_transaction(&mut self.client, 806)
            .await
            .map_err(|_| SessionError::Prerequisite)?;
        require(&tx, self.request.dataset, self.identity)
            .await
            .map_err(|_| SessionError::Unavailable)?;
        lock_dataset(&tx, self.request.dataset, self.validated_policy).await?;
        let outcome = revoke(&tx, self.request.dataset, token, csrf).await;
        let outcome = terminal(&tx, outcome)?;
        tx.commit().await.map_err(|_| SessionError::CommitUnknown)?;
        self.available = true;
        outcome
    }
}

// Business refusal is a completed transaction; transport/schema failures remain
// poisoned. A cancelled future never reaches the owner's available=true branch.
fn terminal<T>(
    _: &Transaction<'_>,
    outcome: Result<T, SessionError>,
) -> Result<Result<T, SessionError>, SessionError> {
    match outcome {
        Err(e @ (SessionError::Denied | SessionError::Capacity | SessionError::InvalidInput)) => {
            Ok(Err(e))
        }
        Err(e) => Err(e),
        Ok(value) => Ok(Ok(value)),
    }
}
async fn lock_dataset(
    tx: &Transaction<'_>,
    dataset: DatasetId,
    expected: PolicySnapshot,
) -> Result<u64, SessionError> {
    let row = tx.query_one("SELECT left(config_revision::text,21),left(policy_revision::text,21) FROM control.datasets WHERE dataset_id=$1::bytea AND config_revision IS NOT NULL AND policy_revision IS NOT NULL FOR UPDATE", &[&&dataset.as_bytes()[..]]).await.map_err(backend)?;
    // A newly observed authoritative head cannot be ignored until the next poll.
    // Reject before login charges, session issuance, touches or revocation.
    if revision(&row, 0)? != expected.config_revision
        || revision(&row, 1)? != expected.policy_revision
    {
        return Err(SessionError::PolicyChanged);
    }
    Ok(expected.policy_revision)
}
async fn now(tx: &Transaction<'_>) -> Result<i64, SessionError> {
    tx.query_one(
        "SELECT (EXTRACT(EPOCH FROM clock_timestamp()) * 1000000)::bigint",
        &[],
    )
    .await
    .map_err(backend)?
    .try_get(0)
    .map_err(metadata)
}
fn bucket(domain: &[u8], value: &[u8]) -> i16 {
    i16::from(
        Sha256::new()
            .chain_update(b"bigbrotr/login-bucket/1\0")
            .chain_update(domain)
            .chain_update([0])
            .chain_update(value)
            .finalize()[0],
    )
}
async fn charge(
    tx: &Transaction<'_>,
    dataset: DatasetId,
    kind: &str,
    bucket: i16,
    now: i64,
    maximum: i32,
) -> Result<bool, SessionError> {
    let row = tx.query_opt(r#"INSERT INTO control.login_buckets AS b(dataset_id,kind,bucket,window_started_at_us,attempts)
        VALUES ($1::bytea,$2::text,$3::smallint,$4::bigint,1)
        ON CONFLICT(dataset_id,kind,bucket) DO UPDATE SET
            window_started_at_us=CASE WHEN $4::numeric-b.window_started_at_us>=60000000 THEN $4::bigint ELSE b.window_started_at_us END,
            attempts=CASE WHEN $4::numeric-b.window_started_at_us>=60000000 THEN 1 ELSE b.attempts+1 END
        WHERE $4::numeric-b.window_started_at_us>=60000000 OR b.attempts<$5::integer RETURNING attempts"#,
        &[&&dataset.as_bytes()[..], &kind, &bucket, &now, &maximum]).await.map_err(backend)?;
    Ok(row.is_some())
}
async fn account_by_name(
    tx: &Transaction<'_>,
    dataset: DatasetId,
    username: &str,
) -> Result<Option<LoginAccount>, SessionError> {
    let row = tx.query_opt(r#"SELECT substring(a.principal_id FROM 1 FOR 17), left(a.credential_revision::text,21),
        left(a.authentication_revision::text,21), substring(c.salt FROM 1 FOR 17), substring(c.password_hash FROM 1 FOR 33),
        a.credential_revision=c.credential_revision AND c.format='argon2id-v1' AND c.argon_version=19
            AND c.memory_kib=65536 AND c.passes=3 AND c.lanes=1
        FROM control.local_accounts a JOIN control.local_credentials c USING(dataset_id,principal_id)
        WHERE a.dataset_id=$1::bytea AND a.username=$2::text"#, &[&&dataset.as_bytes()[..], &username]).await.map_err(backend)?;
    row.map(|row| {
        if !row.try_get::<_, bool>(5).map_err(metadata)? {
            return Err(SessionError::InvalidMetadata);
        }
        Ok(LoginAccount {
            principal: principal(&row, 0)?,
            credential_revision: revision(&row, 1)?,
            authentication_revision: revision(&row, 2)?,
            salt: fixed(&row, 3)?,
            password_hash: Zeroizing::new(fixed(&row, 4)?),
        })
    })
    .transpose()
}

#[allow(clippy::too_many_arguments)]
async fn issue(
    tx: &Transaction<'_>,
    dataset: DatasetId,
    account: &LoginAccount,
    started: i64,
    lifetime: SessionLifetime,
    id: &[u8; 16],
    token: &BrowserSecret,
    csrf: &BrowserSecret,
    policy: u64,
) -> Result<SessionObservation, SessionError> {
    // Account first, credential second. Reset/deactivation uses this same order.
    let row = lock_account(tx, dataset, account.principal).await?;
    let current = tx.query_opt("SELECT salt=$3::bytea AND password_hash=$4::bytea AND credential_revision=$5::text::numeric AND format='argon2id-v1' AND argon_version=19 AND memory_kib=65536 AND passes=3 AND lanes=1 FROM control.local_credentials WHERE dataset_id=$1::bytea AND principal_id=$2::bytea FOR UPDATE", &[&&dataset.as_bytes()[..], &&account.principal.as_bytes()[..], &&account.salt[..], &&account.password_hash[..], &account.credential_revision.to_string()]).await.map_err(backend)?;
    let now = now(tx).await?;
    if !row.try_get::<_, bool>(0).map_err(metadata)?
        || revision(&row, 1)? != account.credential_revision
        || revision(&row, 2)? != account.authentication_revision
        || !current
            .as_ref()
            .map(|r| r.try_get::<_, bool>(0).map_err(metadata))
            .transpose()?
            .unwrap_or(false)
        || now
            .checked_sub(started)
            .is_none_or(|age| !(0..HASH_MAX_AGE_US).contains(&age))
    {
        return Err(SessionError::Denied);
    }
    let expires = now
        .checked_add(lifetime.maximum_us)
        .ok_or(SessionError::InvalidInput)?;
    let idle = now
        .checked_add(lifetime.idle_us)
        .ok_or(SessionError::InvalidInput)?;
    // At most eight rows belong to this account, including terminal rows. Their
    // unpredictable 256-bit credentials cannot be recovered by deleting expiry.
    tx.execute("DELETE FROM control.browser_sessions WHERE dataset_id=$1::bytea AND principal_id=$2::bytea AND (revoked_at_us IS NOT NULL OR idle_expires_at_us<=$3::bigint OR expires_at_us<=$3::bigint)", &[&&dataset.as_bytes()[..], &&account.principal.as_bytes()[..], &now]).await.map_err(backend)?;
    let count: i64 = tx.query_one("SELECT count(*) FROM control.browser_sessions WHERE dataset_id=$1::bytea AND principal_id=$2::bytea", &[&&dataset.as_bytes()[..], &&account.principal.as_bytes()[..]]).await.map_err(backend)?.try_get(0).map_err(metadata)?;
    if count >= 8 {
        return Err(SessionError::Capacity);
    }
    tx.execute(r#"INSERT INTO control.browser_sessions(session_id,dataset_id,principal_id,credential_revision,authentication_revision,token_digest,csrf_digest,issued_at_us,expires_at_us,idle_window_us,last_seen_at_us,idle_expires_at_us)
        VALUES($1::bytea,$2::bytea,$3::bytea,$4::text::numeric,$5::text::numeric,$6::bytea,$7::bytea,$8::bigint,$9::bigint,$10::bigint,$8::bigint,$11::bigint)"#,
        &[&&id[..], &&dataset.as_bytes()[..], &&account.principal.as_bytes()[..], &account.credential_revision.to_string(), &account.authentication_revision.to_string(), &&token.session_digest()[..], &&csrf.csrf_digest()[..], &now, &expires, &lifetime.idle_us, &idle]).await.map_err(backend)?;
    observation(tx, dataset, account.principal, &row, policy, expires, idle).await
}
async fn lock_account(
    tx: &Transaction<'_>,
    dataset: DatasetId,
    principal: PrincipalId,
) -> Result<Row, SessionError> {
    tx.query_opt("SELECT active,left(credential_revision::text,21),left(authentication_revision::text,21),left(grant_revision::text,21) FROM control.local_accounts WHERE dataset_id=$1::bytea AND principal_id=$2::bytea FOR UPDATE", &[&&dataset.as_bytes()[..], &&principal.as_bytes()[..]]).await.map_err(backend)?.ok_or(SessionError::Denied)
}
async fn session_account(
    tx: &Transaction<'_>,
    dataset: DatasetId,
    token: &BrowserSecret,
) -> Result<(PrincipalId, Row), SessionError> {
    let row = tx.query_opt("SELECT substring(principal_id FROM 1 FOR 17) FROM control.browser_sessions WHERE dataset_id=$1::bytea AND token_digest=$2::bytea", &[&&dataset.as_bytes()[..], &&token.session_digest()[..]]).await.map_err(backend)?.ok_or(SessionError::Denied)?;
    let principal = principal(&row, 0)?;
    Ok((principal, lock_account(tx, dataset, principal).await?))
}
async fn inspect(
    tx: &Transaction<'_>,
    dataset: DatasetId,
    token: &BrowserSecret,
    touch: bool,
    policy: u64,
) -> Result<SessionObservation, SessionError> {
    let (principal, account) = session_account(tx, dataset, token).await?;
    let row = tx.query_opt("SELECT substring(session_id FROM 1 FOR 17),left(credential_revision::text,21),left(authentication_revision::text,21),expires_at_us,idle_expires_at_us,idle_window_us,last_seen_at_us,revoked_at_us IS NULL FROM control.browser_sessions WHERE dataset_id=$1::bytea AND token_digest=$2::bytea FOR UPDATE", &[&&dataset.as_bytes()[..], &&token.session_digest()[..]]).await.map_err(backend)?.ok_or(SessionError::Denied)?;
    // wall clock is read AFTER all lock waits. No touch can resurrect expiry.
    let now = now(tx).await?;
    let expires: i64 = row.try_get(3).map_err(metadata)?;
    let mut idle: i64 = row.try_get(4).map_err(metadata)?;
    let seen: i64 = row.try_get(6).map_err(metadata)?;
    if !account.try_get::<_, bool>(0).map_err(metadata)?
        || !row.try_get::<_, bool>(7).map_err(metadata)?
        || revision(&account, 1)? != revision(&row, 1)?
        || revision(&account, 2)? != revision(&row, 2)?
        || now >= expires
        || now >= idle
        || now < seen
    {
        return Err(SessionError::Denied);
    }
    if touch {
        let window: i64 = row.try_get(5).map_err(metadata)?;
        idle = now.checked_add(window).unwrap_or(i64::MAX).min(expires);
        let touched = tx.execute("UPDATE control.browser_sessions SET last_seen_at_us=$3::bigint,idle_expires_at_us=$4::bigint WHERE dataset_id=$1::bytea AND session_id=$2::bytea", &[&&dataset.as_bytes()[..], &&fixed::<16>(&row,0)?[..], &now, &idle]).await.map_err(backend)?;
        if touched != 1 {
            return Err(SessionError::Denied);
        }
    }
    observation(tx, dataset, principal, &account, policy, expires, idle).await
}
async fn revoke(
    tx: &Transaction<'_>,
    dataset: DatasetId,
    token: &BrowserSecret,
    csrf: &BrowserSecret,
) -> Result<(), SessionError> {
    session_account(tx, dataset, token).await?;
    let row = tx.query_opt("SELECT substring(session_id FROM 1 FOR 17),csrf_digest=$3::bytea,revoked_at_us IS NOT NULL FROM control.browser_sessions WHERE dataset_id=$1::bytea AND token_digest=$2::bytea FOR UPDATE", &[&&dataset.as_bytes()[..], &&token.session_digest()[..], &&csrf.csrf_digest()[..]]).await.map_err(backend)?.ok_or(SessionError::Denied)?;
    if !row.try_get::<_, bool>(1).map_err(metadata)? {
        return Err(SessionError::Denied);
    }
    if !row.try_get::<_, bool>(2).map_err(metadata)? {
        tx.execute("UPDATE control.browser_sessions SET revoked_at_us=GREATEST(issued_at_us,(EXTRACT(EPOCH FROM clock_timestamp())*1000000)::bigint) WHERE dataset_id=$1::bytea AND session_id=$2::bytea", &[&&dataset.as_bytes()[..], &&fixed::<16>(&row,0)?[..]]).await.map_err(backend)?;
    }
    Ok(())
}
async fn observation(
    tx: &Transaction<'_>,
    dataset: DatasetId,
    principal: PrincipalId,
    account: &Row,
    policy: u64,
    expires: i64,
    idle: i64,
) -> Result<SessionObservation, SessionError> {
    let rows = tx.query("SELECT CASE WHEN resource_kind='dataset' AND resource_id=dataset_id THEN left(action,65) ELSE 'invalid' END FROM control.principal_grants WHERE dataset_id=$1::bytea AND principal_id=$2::bytea ORDER BY action COLLATE \"C\" LIMIT 8", &[&&dataset.as_bytes()[..], &&principal.as_bytes()[..]]).await.map_err(backend)?;
    let stored_actions: Vec<String> = rows
        .iter()
        .map(|r| r.try_get(0).map_err(metadata))
        .collect::<Result<_, _>>()?;
    const ACTIONS: &[&str] = &[
        "audit.read",
        "budgets.change",
        "data.read",
        "policy.change",
        "principals.manage",
        "roles.control",
        "status.read",
    ];
    if stored_actions.len() > 7
        || stored_actions
            .iter()
            .any(|a| !ACTIONS.contains(&a.as_str()))
        || stored_actions.windows(2).any(|p| p[0] >= p[1])
    {
        return Err(SessionError::InvalidMetadata);
    }
    Ok(SessionObservation {
        principal,
        credential_revision: revision(account, 1)?,
        grant_revision: revision(account, 3)?,
        policy_revision: policy,
        expires_at_us: expires,
        idle_expires_at_us: idle,
        stored_actions,
    })
}
fn fixed<const N: usize>(row: &Row, column: usize) -> Result<[u8; N], SessionError> {
    row.try_get::<_, &[u8]>(column)
        .map_err(metadata)?
        .try_into()
        .map_err(|_| SessionError::InvalidMetadata)
}
fn principal(row: &Row, column: usize) -> Result<PrincipalId, SessionError> {
    PrincipalId::try_from(fixed::<16>(row, column)?).map_err(|_| SessionError::InvalidMetadata)
}
fn revision(row: &Row, column: usize) -> Result<u64, SessionError> {
    let text: &str = row.try_get(column).map_err(metadata)?;
    text.parse::<u64>()
        .ok()
        .filter(|n| *n > 0 && n.to_string() == text)
        .ok_or(SessionError::InvalidMetadata)
}
