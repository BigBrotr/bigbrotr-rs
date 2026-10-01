//! Fenced, session-authenticated operational enrollment. No network work starts
//! here; immutable receipts report acceptance only, including historical retries.
use super::{
    ControlSession, browser_reauthentication::authenticated_session, browser_sessions::*, require,
};
use crate::{
    management::{
        credentials::random,
        grants::Revision,
        relay_enrollment::{EnrollmentCommand, EnrollmentReceipt},
        sessions::{BrowserSecret, SessionError},
    },
    network::DirectPolicy,
    storage::postgres::{ingest_registry::ControlOwner, initialization::application_transaction},
};
use bigbrotr_model::{AccessScopeId, CommandKey, DatasetId, RouteId};
use bigbrotr_protocol::endpoint::Endpoint;
use std::cell::Cell;
use tokio_postgres::{Row, Transaction};

/// Trusted process inputs, supplied from validated deployment roots and the
/// current accepted public-route setting. Never deserialize from a command body.
/// These static limits are not a socket grant or proof of source visibility.
#[derive(Clone, Copy)]
pub struct EnrollmentRoutes<'a> {
    pub public_enabled: bool,
    pub private_ceiling: Option<&'a DirectPolicy>,
}

const HISTORICAL: &str = "
    SELECT c.dataset_id=$3::bytea AND c.action='relay.enrollment.change'
            AND c.request_digest=$4::bytea,
        substring(d.access_scope_id FROM 1 FOR 17), substring(d.route_id FROM 1 FOR 17),
        left(d.generation::text,21), c.accepted_at_us
    FROM control.management_commands c
    LEFT JOIN control.relay_enrollment_changes d USING(actor_id,request_key)
    WHERE c.actor_id=$1::bytea AND c.request_key=$2::bytea";
const ACCESS: &str = "
    SELECT substring(access_scope_id FROM 1 FOR 17) FROM control.relay_access_contexts
    WHERE dataset_id=$1::bytea AND access_class='public' AND dictionary_kind=4
        AND allocation_domain=$2::bytea AND ordinal=$3::text::numeric";
const CURRENT: &str = "
    SELECT e.enabled, left(e.generation::text,21), substring(r.route_bytes FROM 1 FOR 20)
    FROM control.relay_enrollments e JOIN control.network_routes r
        ON r.dataset_id=e.dataset_id AND r.endpoint_id=e.endpoint_id AND r.route_id=e.route_id
    WHERE e.dataset_id=$1::bytea AND e.endpoint_id=$2::bytea AND e.access_scope_id=$3::bytea";
const ROUTE: &str = "
    SELECT substring(route_id FROM 1 FOR 17) FROM control.network_routes
    WHERE dataset_id=$1::bytea AND endpoint_id=$2::bytea AND route_version=1 AND route_bytes=$3::bytea";
const AUDIT: &str = "
    INSERT INTO control.management_audit(audit_id,dataset_id,actor_id,action,outcome,
        created_at_us,config_revision,subject_id,policy_revision)
    VALUES($1::bytea,$2::bytea,$3::bytea,'relay.enrollment.change','accepted',
        $4::bigint,$5::text::numeric,$6::bytea,$7::text::numeric)";
const COMMAND: &str = "
    INSERT INTO control.management_commands(actor_id,request_key,dataset_id,action,
        request_digest,expected_config_revision,policy_revision,audit_id,accepted_at_us)
    VALUES($1::bytea,$2::bytea,$3::bytea,'relay.enrollment.change',$4::bytea,
        $5::text::numeric,$6::text::numeric,$7::bytea,$8::bigint)";
const INSERT_ACCESS: &str = "
    INSERT INTO control.relay_access_contexts(access_scope_id,dataset_id,allocation_domain,
        ordinal,actor_id,request_key)
    VALUES($1::bytea,$2::bytea,$3::bytea,$4::text::numeric,$5::bytea,$6::bytea)";
const INSERT_ROUTE: &str = "
    INSERT INTO control.network_routes(route_id,dataset_id,endpoint_id,route_bytes,actor_id,request_key)
    VALUES($1::bytea,$2::bytea,$3::bytea,$4::bytea,$5::bytea,$6::bytea)";
const DETAIL: &str = "
    INSERT INTO control.relay_enrollment_changes(actor_id,request_key,dataset_id,endpoint_id,
        access_scope_id,route_id,enabled,expected_generation,generation,session_id,
        actor_credential_revision,actor_authentication_revision,actor_grant_revision,
        owner_resource_id,owner_epoch,owner_incarnation)
    VALUES($1::bytea,$2::bytea,$3::bytea,$4::bytea,$5::bytea,$6::bytea,$7::boolean,
        $8::text::numeric,$9::text::numeric,$10::bytea,$11::text::numeric,$12::text::numeric,
        $13::text::numeric,$14::bytea,$15::text::numeric,$16::bytea)";
const INSERT_ENROLLMENT: &str = "
    INSERT INTO control.relay_enrollments(dataset_id,endpoint_id,access_scope_id,route_id,
        enabled,generation,actor_id,request_key)
    SELECT dataset_id,endpoint_id,access_scope_id,route_id,enabled,generation,actor_id,request_key
    FROM control.relay_enrollment_changes WHERE actor_id=$1::bytea AND request_key=$2::bytea";
const UPDATE_ENROLLMENT: &str = "
    UPDATE control.relay_enrollments e SET route_id=d.route_id,enabled=d.enabled,
        generation=d.generation,actor_id=d.actor_id,request_key=d.request_key
    FROM control.relay_enrollment_changes d WHERE d.actor_id=$1::bytea AND d.request_key=$2::bytea
        AND e.dataset_id=d.dataset_id AND e.endpoint_id=d.endpoint_id AND e.access_scope_id=d.access_scope_id
        AND e.generation=d.expected_generation";

impl ControlSession {
    /// Check selected installed-command prerequisites before binding HTTP. This
    /// read transaction grants no user authority and preserves every stored row.
    pub async fn require_relay_enrollment(&mut self) -> Result<(), SessionError> {
        self.begin_operation()
            .map_err(|_| SessionError::Unavailable)?;
        let tx = application_transaction(&mut self.client, 19)
            .await
            .map_err(|_| SessionError::Prerequisite)?;
        require(&tx, self.request.dataset, self.identity)
            .await
            .map_err(|_| SessionError::Unavailable)?;
        lock_dataset(&tx, self.request.dataset, self.validated_policy).await?;
        let row = tx
            .query_one(
                "
            SELECT bool_and(has_table_privilege(CURRENT_USER,t.name,p.privilege))
            FROM (VALUES
                ('control.relay_endpoints',ARRAY['SELECT']),
                ('control.relay_access_contexts',ARRAY['SELECT','INSERT']),
                ('control.network_routes',ARRAY['SELECT','INSERT']),
                ('control.relay_enrollments',ARRAY['SELECT','INSERT','UPDATE']),
                ('control.relay_enrollment_changes',ARRAY['SELECT','INSERT']),
                ('control.management_commands',ARRAY['SELECT','INSERT'])
            ) t(name,privileges)
            CROSS JOIN LATERAL unnest(t.privileges) p(privilege)",
                &[],
            )
            .await
            .map_err(backend)?;
        if !row.try_get::<_, bool>(0).map_err(metadata)? {
            return Err(SessionError::Prerequisite);
        }
        tx.rollback().await.map_err(backend)?;
        self.available = true;
        Ok(())
    }

    /// Caller owns its deadline, cancellation, transport driver and reservation.
    /// A lost COMMIT or cancelled operation poisons the session; resolve on a
    /// freshly acquired owner with the same authenticated actor/key/command.
    #[allow(clippy::too_many_arguments)]
    pub async fn accept_relay_enrollment(
        &mut self,
        token: &BrowserSecret,
        csrf: &BrowserSecret,
        key: CommandKey,
        command: &EnrollmentCommand,
        routes: EnrollmentRoutes<'_>,
        commit_started: &Cell<bool>,
    ) -> Result<EnrollmentReceipt, SessionError> {
        command.validate(self.request.dataset)?;
        self.begin_operation()
            .map_err(|_| SessionError::Unavailable)?;
        let tx = application_transaction(&mut self.client, 19)
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
            routes,
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
    policy: super::PolicySnapshot,
    token: &BrowserSecret,
    csrf: &BrowserSecret,
    key: CommandKey,
    command: &EnrollmentCommand,
    routes: EnrollmentRoutes<'_>,
) -> Result<EnrollmentReceipt, SessionError> {
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
        return receipt(
            command,
            key,
            access_id(&row, 1)?,
            route_id(&row, 2)?,
            revision(&row, 3)?,
            row.try_get(4).map_err(metadata)?,
            false,
        );
    }
    let permitted: bool = tx
        .query_one(
            "
        SELECT EXISTS(SELECT 1 FROM control.principal_grants WHERE dataset_id=$1::bytea
            AND principal_id=$2::bytea AND action='policy.change' AND resource_kind='dataset'
            AND resource_id=dataset_id)",
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
    let endpoint_bytes = &command.endpoint_id.as_bytes()[..];
    let row = tx
        .query_opt(
            "
        SELECT substring(url_bytes FROM 1 FOR 8193) FROM control.relay_endpoints
        WHERE dataset_id=$1::bytea AND endpoint_id=$2::bytea AND normalization_version=1",
            &[&dataset, &endpoint_bytes],
        )
        .await
        .map_err(backend)?
        .ok_or(SessionError::Conflict)?;
    let url = std::str::from_utf8(row.try_get::<_, &[u8]>(0).map_err(metadata)?)
        .map_err(|_| SessionError::InvalidMetadata)?;
    let endpoint = Endpoint::parse(url).map_err(|_| SessionError::InvalidMetadata)?;
    if endpoint.as_str() != url {
        return Err(SessionError::InvalidMetadata);
    }
    // Withdrawal and old receipts deliberately do not depend on removed roots.
    if command.enabled {
        command
            .route
            .validate_enrollment(&endpoint, routes.public_enabled, routes.private_ceiling)
            .map_err(|_| SessionError::Forbidden)?;
    }
    let access_row = tx
        .query_opt(
            ACCESS,
            &[
                &dataset,
                &&command.visibility.allocation_domain.as_bytes()[..],
                &command.visibility.ordinal.get().to_string(),
            ],
        )
        .await
        .map_err(backend)?;
    let existing_access = access_row
        .as_ref()
        .map(|row| access_id(row, 0))
        .transpose()?;
    let previous = if let Some(access) = existing_access {
        tx.query_opt(
            CURRENT,
            &[&dataset, &endpoint_bytes, &&access.as_bytes()[..]],
        )
        .await
        .map_err(backend)?
    } else {
        None
    };
    let route_bytes = command.route.canonical_bytes();
    let generation = match previous {
        Some(row) => {
            let generation = revision(&row, 1)?;
            let same_route = row.try_get::<_, &[u8]>(2).map_err(metadata)? == route_bytes;
            if command.expected_generation.map(Revision::get) != Some(generation)
                || (row.try_get::<_, bool>(0).map_err(metadata)? == command.enabled && same_route)
                || (!command.enabled && !same_route)
            {
                return Err(SessionError::Conflict);
            }
            generation.checked_add(1).ok_or(SessionError::Capacity)?
        }
        None if command.expected_generation.is_none() && command.enabled => 1,
        None => return Err(SessionError::Conflict),
    };
    let route_row = tx
        .query_opt(ROUTE, &[&dataset, &endpoint_bytes, &route_bytes])
        .await
        .map_err(backend)?;
    let existing_route = route_row.as_ref().map(|row| route_id(row, 0)).transpose()?;
    let access = match existing_access {
        Some(id) => id,
        None => AccessScopeId::try_from(fresh()?).map_err(|_| SessionError::Unavailable)?,
    };
    let route = match existing_route {
        Some(id) => id,
        None => RouteId::try_from(fresh()?).map_err(|_| SessionError::Unavailable)?,
    };
    let time = now(tx).await?;
    if time >= current.expires {
        return Err(SessionError::Denied);
    }
    let audit = fresh()?;
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
    if existing_access.is_none() {
        one(tx
            .execute(
                INSERT_ACCESS,
                &[
                    &&access.as_bytes()[..],
                    &dataset,
                    &&command.visibility.allocation_domain.as_bytes()[..],
                    &command.visibility.ordinal.get().to_string(),
                    &actor,
                    &request_key,
                ],
            )
            .await
            .map_err(backend)?)?;
    }
    if existing_route.is_none() {
        one(tx
            .execute(
                INSERT_ROUTE,
                &[
                    &&route.as_bytes()[..],
                    &dataset,
                    &endpoint_bytes,
                    &route_bytes,
                    &actor,
                    &request_key,
                ],
            )
            .await
            .map_err(backend)?)?;
    }
    let expected = command
        .expected_generation
        .map(|value| value.get().to_string());
    one(tx
        .execute(
            DETAIL,
            &[
                &actor,
                &request_key,
                &dataset,
                &endpoint_bytes,
                &&access.as_bytes()[..],
                &&route.as_bytes()[..],
                &command.enabled,
                &expected,
                &generation.to_string(),
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
    let transition = if generation == 1 {
        INSERT_ENROLLMENT
    } else {
        UPDATE_ENROLLMENT
    };
    one(tx
        .execute(transition, &[&actor, &request_key])
        .await
        .map_err(backend)?)?;
    receipt(command, key, access, route, generation, time, true)
}
fn fresh() -> Result<[u8; 16], SessionError> {
    random::<16>().map_err(|_| SessionError::Unavailable)
}
fn one(changed: u64) -> Result<(), SessionError> {
    if changed == 1 {
        Ok(())
    } else {
        Err(SessionError::InvalidMetadata)
    }
}
fn access_id(row: &Row, column: usize) -> Result<AccessScopeId, SessionError> {
    AccessScopeId::try_from(fixed::<16>(row, column)?).map_err(|_| SessionError::InvalidMetadata)
}
fn route_id(row: &Row, column: usize) -> Result<RouteId, SessionError> {
    RouteId::try_from(fixed::<16>(row, column)?).map_err(|_| SessionError::InvalidMetadata)
}
#[allow(clippy::too_many_arguments)]
fn receipt(
    command: &EnrollmentCommand,
    key: CommandKey,
    access: AccessScopeId,
    route: RouteId,
    generation: u64,
    time: i64,
    created: bool,
) -> Result<EnrollmentReceipt, SessionError> {
    Ok(EnrollmentReceipt {
        command_key: key,
        endpoint_id: command.endpoint_id,
        access_scope_id: access,
        route_id: route,
        enabled: command.enabled,
        generation: Revision::try_from(generation.to_string())
            .map_err(|_| SessionError::InvalidMetadata)?,
        accepted_at_us: time,
        created,
    })
}
