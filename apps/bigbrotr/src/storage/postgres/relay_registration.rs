//! Browser command acceptance over the existing immutable URL registry.
//! Authentication and registry mutation share one fenced Control transaction.
use super::{
    ControlSession, PolicySnapshot,
    browser_reauthentication::authenticated_session,
    browser_sessions::*,
    relay_registry::{RelayRegistryError, register_in_locked_transaction},
    require,
};
use crate::{
    management::{
        credentials::random,
        relay_registration::{RegistrationCommand, RegistrationReceipt},
        sessions::{BrowserSecret, SessionError},
    },
    storage::postgres::{ingest_registry::ControlOwner, initialization::application_transaction},
};
use bigbrotr_model::{CommandKey, DatasetId, EndpointId};
use std::cell::Cell;
use tokio_postgres::Transaction;

const HISTORICAL: &str = "
    SELECT c.dataset_id = $3::bytea AND c.action = 'relay.endpoint.register'
            AND c.request_digest = $4::bytea,
        substring(d.endpoint_id FROM 1 FOR 17), c.accepted_at_us
    FROM control.management_commands c
    LEFT JOIN control.relay_registration_commands d USING (actor_id, request_key)
    WHERE c.actor_id = $1::bytea AND c.request_key = $2::bytea";
const AUDIT: &str = "
    INSERT INTO control.management_audit (
        audit_id, dataset_id, actor_id, action, outcome, created_at_us,
        config_revision, subject_id, policy_revision
    ) VALUES (
        $1::bytea, $2::bytea, $3::bytea, 'relay.endpoint.register', 'accepted',
        $4::bigint, $5::text::numeric, $6::bytea, $7::text::numeric
    )";
const COMMAND: &str = "
    INSERT INTO control.management_commands (
        actor_id, request_key, dataset_id, action, request_digest,
        expected_config_revision, policy_revision, audit_id, accepted_at_us
    ) VALUES (
        $1::bytea, $2::bytea, $3::bytea, 'relay.endpoint.register', $4::bytea,
        $5::text::numeric, $6::text::numeric, $7::bytea, $8::bigint
    )";
const DETAIL: &str = "
    INSERT INTO control.relay_registration_commands (
        actor_id, request_key, dataset_id, endpoint_id, session_id,
        actor_credential_revision, actor_authentication_revision, actor_grant_revision,
        owner_resource_id, owner_epoch, owner_incarnation
    ) VALUES (
        $1::bytea, $2::bytea, $3::bytea, $4::bytea, $5::bytea,
        $6::text::numeric, $7::text::numeric, $8::text::numeric,
        $9::bytea, $10::text::numeric, $11::bytea
    )";

impl ControlSession {
    /// Probe the selected consumer before HTTP bind; no implicit DDL or writes.
    pub async fn require_relay_registration(&mut self) -> Result<(), SessionError> {
        self.begin_operation()
            .map_err(|_| SessionError::Unavailable)?;
        let tx = application_transaction(&mut self.client, 25)
            .await
            .map_err(|_| SessionError::Prerequisite)?;
        require(&tx, self.request.dataset, self.identity)
            .await
            .map_err(|_| SessionError::Unavailable)?;
        lock_dataset(&tx, self.request.dataset, self.validated_policy).await?;
        let row = tx.query_one("
            SELECT bool_and(has_table_privilege(CURRENT_USER, t.name, p.privilege))
                AND has_function_privilege(CURRENT_USER,
                    'control.relay_endpoint_candidate(bytea)', 'EXECUTE')
                AND has_function_privilege(CURRENT_USER,
                    'control.relay_registration_digest(control.relay_registration_commands,control.management_commands)',
                    'EXECUTE')
                AND has_function_privilege(CURRENT_USER,
                    'control.command_u64_bytes(control.u64)', 'EXECUTE')
            FROM (VALUES
                ('control.relay_endpoints', ARRAY['SELECT','INSERT']),
                ('control.relay_registration_commands', ARRAY['SELECT','INSERT']),
                ('control.management_commands', ARRAY['SELECT','INSERT']),
                ('control.management_audit', ARRAY['SELECT','INSERT'])
            ) t(name, privileges)
            CROSS JOIN LATERAL unnest(t.privileges) p(privilege)
        ", &[]).await.map_err(backend)?;
        if !row.try_get::<_, bool>(0).map_err(metadata)? {
            return Err(SessionError::Prerequisite);
        }
        tx.rollback().await.map_err(backend)?;
        self.available = true;
        Ok(())
    }

    /// Historical resolution still authenticates the current actor and CSRF.
    /// Cancellation/unknown COMMIT poisons this owner until its driver retires.
    pub async fn accept_relay_registration(
        &mut self,
        token: &BrowserSecret,
        csrf: &BrowserSecret,
        key: CommandKey,
        command: &RegistrationCommand,
        commit_started: &Cell<bool>,
    ) -> Result<RegistrationReceipt, SessionError> {
        self.begin_operation()
            .map_err(|_| SessionError::Unavailable)?;
        let tx = application_transaction(&mut self.client, 25)
            .await
            .map_err(|_| SessionError::Prerequisite)?;
        require(&tx, self.request.dataset, self.identity)
            .await
            .map_err(|_| SessionError::Unavailable)?;
        lock_dataset(&tx, self.request.dataset, self.validated_policy).await?;
        let outcome = accept(
            &tx,
            self.request.dataset,
            self.identity,
            self.validated_policy,
            token,
            csrf,
            key,
            command,
        )
        .await;
        match terminal(&tx, outcome)? {
            Ok(receipt) if receipt.created => {
                commit_started.set(true);
                if let Err(error) = tx.commit().await {
                    if let Some(rejection) = error.as_db_error() {
                        if rejection.code().code() == "BB001" {
                            self.available = true;
                            return Err(SessionError::Denied);
                        }
                        return Err(SessionError::Backend);
                    }
                    return Err(SessionError::CommitUnknown);
                }
                self.available = true;
                Ok(receipt)
            }
            outcome => {
                tx.rollback().await.map_err(backend)?;
                self.available = true;
                outcome
            }
        }
    }
}

#[allow(clippy::too_many_arguments)]
async fn accept(
    tx: &Transaction<'_>,
    dataset_id: DatasetId,
    owner: ControlOwner,
    policy: PolicySnapshot,
    token: &BrowserSecret,
    csrf: &BrowserSecret,
    key: CommandKey,
    command: &RegistrationCommand,
) -> Result<RegistrationReceipt, SessionError> {
    let current = authenticated_session(
        tx,
        dataset_id,
        &token.session_digest(),
        &csrf.csrf_digest(),
        None,
    )
    .await?;
    let digest = command.digest(dataset_id, current.principal, key);
    let dataset = &dataset_id.as_bytes()[..];
    let actor = &current.principal.as_bytes()[..];
    let request_key = &key.as_bytes()[..];
    if let Some(row) = tx
        .query_opt(
            HISTORICAL,
            &[&actor, &request_key, &dataset, &&digest.as_bytes()[..]],
        )
        .await
        .map_err(backend)?
    {
        if !row.try_get::<_, bool>(0).map_err(metadata)? {
            return Err(SessionError::Conflict);
        }
        return Ok(RegistrationReceipt {
            command_key: key,
            endpoint_id: EndpointId::try_from(fixed::<16>(&row, 1)?)
                .map_err(|_| SessionError::InvalidMetadata)?,
            accepted_at_us: row.try_get(2).map_err(metadata)?,
            created: false,
        });
    }
    let permitted: bool = tx
        .query_one(
            "
        SELECT EXISTS (
            SELECT 1 FROM control.principal_grants WHERE dataset_id = $1::bytea
                AND principal_id = $2::bytea AND action = 'policy.change'
                AND resource_kind = 'dataset' AND resource_id = dataset_id
        )",
            &[&dataset, &actor],
        )
        .await
        .map_err(backend)?
        .try_get(0)
        .map_err(metadata)?;
    if !permitted {
        return Err(SessionError::Forbidden);
    }
    if command.expected_config_revision.get() != policy.config_revision
        || command.expected_policy_revision.get() != policy.policy_revision
    {
        return Err(SessionError::Conflict);
    }
    let registration = register_in_locked_transaction(tx, dataset_id, owner, policy, &command.url)
        .await
        .map_err(registry_error)?;
    let time = now(tx).await?;
    if time >= current.expires {
        return Err(SessionError::Denied);
    }
    let audit = random::<16>().map_err(|_| SessionError::Unavailable)?;
    let config_revision = policy.config_revision.to_string();
    let policy_revision = policy.policy_revision.to_string();
    one(tx
        .execute(
            AUDIT,
            &[
                &&audit[..],
                &dataset,
                &actor,
                &time,
                &config_revision,
                &request_key,
                &policy_revision,
            ],
        )
        .await
        .map_err(backend)?)?;
    one(tx
        .execute(
            COMMAND,
            &[
                &actor,
                &request_key,
                &dataset,
                &&digest.as_bytes()[..],
                &config_revision,
                &policy_revision,
                &&audit[..],
                &time,
            ],
        )
        .await
        .map_err(backend)?)?;
    one(tx
        .execute(
            DETAIL,
            &[
                &actor,
                &request_key,
                &dataset,
                &&registration.identity.as_bytes()[..],
                &&current.id[..],
                &revision(&current.account, 1)?.to_string(),
                &revision(&current.account, 2)?.to_string(),
                &revision(&current.account, 3)?.to_string(),
                &&owner.resource_id.as_bytes()[..],
                &owner.epoch.get().to_string(),
                &&owner.process_incarnation.as_bytes()[..],
            ],
        )
        .await
        .map_err(backend)?)?;
    // A new accepted command may point to an old endpoint. Its acceptance time
    // and actor are deliberately independent of the endpoint's original creator.
    Ok(RegistrationReceipt {
        command_key: key,
        endpoint_id: registration.identity,
        accepted_at_us: time,
        created: true,
    })
}
fn one(changed: u64) -> Result<(), SessionError> {
    if changed == 1 {
        Ok(())
    } else {
        Err(SessionError::InvalidMetadata)
    }
}
fn registry_error(error: RelayRegistryError) -> SessionError {
    match error {
        RelayRegistryError::Capacity => SessionError::Capacity,
        RelayRegistryError::InvalidMetadata => SessionError::InvalidMetadata,
        RelayRegistryError::PolicyChanged => SessionError::PolicyChanged,
        RelayRegistryError::AuthorityExpired => SessionError::Denied,
        RelayRegistryError::CommitUnknown => SessionError::CommitUnknown,
        RelayRegistryError::Prerequisite => SessionError::Prerequisite,
        RelayRegistryError::Backend => SessionError::Backend,
        RelayRegistryError::Unavailable | RelayRegistryError::Entropy => SessionError::Unavailable,
    }
}
