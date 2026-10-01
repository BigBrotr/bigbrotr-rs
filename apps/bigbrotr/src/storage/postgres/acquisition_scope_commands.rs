//! Browser command acceptance over existing immutable collection intent.
//! Authentication and registry mutation share one fenced Control transaction.
use super::{
    ControlSession, PolicySnapshot,
    acquisition::{ScopeIntent, ScopeRegistrationError, register_in_locked_transaction},
    browser_reauthentication::authenticated_session,
    browser_sessions::*,
    require,
};
use crate::{
    management::{
        acquisition_scope::{ScopeCommand, ScopeReceipt},
        credentials::random,
        sessions::{BrowserSecret, SessionError},
    },
    storage::postgres::{ingest_registry::ControlOwner, initialization::application_transaction},
};
use bigbrotr_model::{AcquisitionScopeId, CommandKey, DatasetId, PolicyEpoch};
use std::cell::Cell;
use tokio_postgres::Transaction;

const HISTORICAL: &str = "
    SELECT c.dataset_id = $3::bytea AND c.action = 'acquisition.scope.register'
            AND c.request_digest = $4::bytea,
        substring(d.scope_id FROM 1 FOR 17), c.accepted_at_us
    FROM control.management_commands c
    LEFT JOIN control.acquisition_scope_commands d USING (actor_id, request_key)
    WHERE c.actor_id = $1::bytea AND c.request_key = $2::bytea";
const AUDIT: &str = "
    INSERT INTO control.management_audit (
        audit_id, dataset_id, actor_id, action, outcome, created_at_us,
        config_revision, subject_id, policy_revision
    ) VALUES (
        $1::bytea, $2::bytea, $3::bytea, 'acquisition.scope.register', 'accepted',
        $4::bigint, $5::text::numeric, $6::bytea, $7::text::numeric
    )";
const COMMAND: &str = "
    INSERT INTO control.management_commands (
        actor_id, request_key, dataset_id, action, request_digest,
        expected_config_revision, policy_revision, audit_id, accepted_at_us
    ) VALUES (
        $1::bytea, $2::bytea, $3::bytea, 'acquisition.scope.register', $4::bytea,
        $5::text::numeric, $6::text::numeric, $7::bytea, $8::bigint
    )";
const SOURCE_EXISTS: &str = "
    SELECT EXISTS (
        SELECT 1 FROM control.relay_enrollments e
        JOIN control.relay_access_contexts a
            ON a.dataset_id = e.dataset_id AND a.access_scope_id = e.access_scope_id
        WHERE e.dataset_id = $1::bytea AND e.endpoint_id = $2::bytea
            AND e.access_scope_id = $3::bytea AND a.dictionary_kind = 4
            AND a.allocation_domain = $4::bytea AND a.ordinal = $5::text::numeric
    )";
const DETAIL: &str = "
    INSERT INTO control.acquisition_scope_commands (
        actor_id, request_key, dataset_id, scope_id, session_id,
        actor_credential_revision, actor_authentication_revision, actor_grant_revision,
        owner_resource_id, owner_epoch, owner_incarnation
    ) VALUES (
        $1::bytea, $2::bytea, $3::bytea, $4::bytea, $5::bytea,
        $6::text::numeric, $7::text::numeric, $8::text::numeric,
        $9::bytea, $10::text::numeric, $11::bytea
    )";

impl ControlSession {
    /// Probe the selected consumer before HTTP bind; no implicit DDL or writes.
    pub async fn require_acquisition_scope(&mut self) -> Result<(), SessionError> {
        self.begin_operation()
            .map_err(|_| SessionError::Unavailable)?;
        let tx = application_transaction(&mut self.client, 26)
            .await
            .map_err(|_| SessionError::Prerequisite)?;
        require(&tx, self.request.dataset, self.identity)
            .await
            .map_err(|_| SessionError::Unavailable)?;
        lock_dataset(&tx, self.request.dataset, self.validated_policy).await?;
        let row = tx.query_one("
            SELECT bool_and(has_table_privilege(CURRENT_USER, t.name, p.privilege))
                AND has_function_privilege(CURRENT_USER,
                    'control.acquisition_selector_candidate(bytea)', 'EXECUTE')
                AND has_function_privilege(CURRENT_USER,
                    'control.acquisition_scope_digest(control.acquisition_scope_commands,control.management_commands)',
                    'EXECUTE')
                AND has_function_privilege(CURRENT_USER,
                    'control.command_u64_bytes(control.u64)', 'EXECUTE')
            FROM (VALUES
                ('control.acquisition_selectors', ARRAY['SELECT','INSERT']),
                ('control.acquisition_scopes', ARRAY['SELECT','INSERT']),
                ('control.relay_enrollments', ARRAY['SELECT']),
                ('control.relay_access_contexts', ARRAY['SELECT']),
                ('control.acquisition_scope_commands', ARRAY['SELECT','INSERT']),
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
    pub async fn accept_acquisition_scope(
        &mut self,
        token: &BrowserSecret,
        csrf: &BrowserSecret,
        key: CommandKey,
        command: &ScopeCommand,
        commit_started: &Cell<bool>,
    ) -> Result<ScopeReceipt, SessionError> {
        self.begin_operation()
            .map_err(|_| SessionError::Unavailable)?;
        let tx = application_transaction(&mut self.client, 26)
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
    command: &ScopeCommand,
) -> Result<ScopeReceipt, SessionError> {
    let current = authenticated_session(
        tx,
        dataset_id,
        &token.session_digest(),
        &csrf.csrf_digest(),
        None,
    )
    .await?;
    let digest = command.digest(dataset_id, current.principal, key)?;
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
        return Ok(ScopeReceipt {
            command_key: key,
            scope_id: AcquisitionScopeId::try_from(fixed::<16>(&row, 1)?)
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
                AND principal_id = $2::bytea AND action = 'roles.control'
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
    // Scope identity needs an existing complete enrollment/access tuple, but it
    // neither enables a withdrawn relay nor grants current execution authority.
    // Refuse missing input before an FK exception could poison the live owner.
    let source_exists: bool = tx
        .query_one(
            SOURCE_EXISTS,
            &[
                &dataset,
                &&command.endpoint_id.as_bytes()[..],
                &&command.access_scope_id.as_bytes()[..],
                &&command.visibility.allocation_domain.as_bytes()[..],
                &command.visibility.ordinal.get().to_string(),
            ],
        )
        .await
        .map_err(backend)?
        .try_get(0)
        .map_err(metadata)?;
    if !source_exists {
        return Err(SessionError::Conflict);
    }
    let registration = register_in_locked_transaction(
        tx,
        dataset_id,
        owner,
        policy,
        ScopeIntent {
            selector: command.selector(),
            endpoint: command.endpoint_id,
            access: command.access_scope_id,
            visibility: command.visibility,
            policy_epoch: PolicyEpoch::new(command.expected_policy_revision.get()),
        },
        command.document(),
    )
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
    // Acceptance belongs to this command even if the immutable scope already
    // existed. Preserve the scope's original owner and registration time.
    Ok(ScopeReceipt {
        command_key: key,
        scope_id: registration.identity,
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
fn registry_error(error: ScopeRegistrationError) -> SessionError {
    match error {
        ScopeRegistrationError::InvalidInput => SessionError::InvalidInput,
        ScopeRegistrationError::Capacity => SessionError::Capacity,
        ScopeRegistrationError::InvalidMetadata => SessionError::InvalidMetadata,
        ScopeRegistrationError::PolicyChanged => SessionError::PolicyChanged,
        ScopeRegistrationError::AuthorityExpired => SessionError::Denied,
        ScopeRegistrationError::CommitUnknown => SessionError::CommitUnknown,
        ScopeRegistrationError::Prerequisite => SessionError::Prerequisite,
        ScopeRegistrationError::Backend => SessionError::Backend,
        ScopeRegistrationError::Unavailable | ScopeRegistrationError::Entropy => {
            SessionError::Unavailable
        }
    }
}
