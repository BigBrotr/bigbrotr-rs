//! First-source registration in an already initialized control database.
//!
//! Results are checked coordination data, never native ownership or policy grants.
//! The caller owns the configured application connection, driver, deadline and
//! resource charges. After cancellation, discard that connection and resolve the
//! same owned attempt through a fresh connection; never infer rollback from Drop.
use super::{migration_runner, migrations};
use bigbrotr_model::{
    AllocationDomainId, DatasetId, DictionaryKind, Digest256, OwnerEpoch, OwnerResourceId,
    ProcessIncarnation, ProducerSlotId, TopologyGeneration,
};
use std::fmt;
use tokio_postgres::{Client, IsolationLevel, Row, Transaction};

const LOCK_NAMESPACE: i32 = 0x4242_7472;
const LOCK_MIGRATIONS: i32 = 0x4d49_4752;
const LOCK_REGISTRATION: i32 = 0x494e_4752;
const MAX_DOMAINS: usize = 9;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RegistryError {
    InvalidInput,
    Exhausted,
    AttemptUnresolved,
    NoUnresolvedAttempt,
    Backend,
    IncompatibleEngine,
    IncompatibleSchema,
    MigrationPrivileges,
    InvalidMetadata,
    WrongDeployment,
    ControlMismatch,
    HeadsChanged,
    StartingTopologyNotEmpty,
    CommitUnknown,
}
impl fmt::Display for RegistryError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "ingest registration failed: {self:?}")
    }
}
impl std::error::Error for RegistryError {}
fn backend(_: tokio_postgres::Error) -> RegistryError {
    RegistryError::Backend
}

/// Expected registered Control identity. This input does not prove positive
/// ownership; only its real owner may invoke the connected registration command.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ControlOwner {
    pub resource_id: OwnerResourceId,
    pub epoch: OwnerEpoch,
    pub role_instance_id: [u8; 16],
    pub process_incarnation: ProcessIncarnation,
}

/// The immutable identity of one initial registration. Every inserted owner,
/// source and producer epoch is one. A populated topology or an existing owner
/// requires its separate closure/handoff operation, not this initializer.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct InitialRegistration {
    pub dataset_id: DatasetId,
    pub instance_id: [u8; 16],
    pub control: ControlOwner,
    pub ingest_resource_id: OwnerResourceId,
    pub ingest_role_instance_id: [u8; 16],
    pub process_incarnation: ProcessIncarnation,
    pub shard_id: u32,
    pub config_revision: u64,
    pub policy_revision: u64,
    pub config_digest: Digest256,
    pub policy_digest: Digest256,
    pub previous_topology: Option<TopologyGeneration>,
    pub allocation_domains: Vec<(DictionaryKind, AllocationDomainId)>,
    pub producer_slot_id: ProducerSlotId,
    pub outstanding_request_limit: u32,
    pub created_at_us: i64,
    pub lease_expires_at_us: i64,
}
impl InitialRegistration {
    pub fn target_topology(&self) -> Result<TopologyGeneration, RegistryError> {
        match self.previous_topology {
            None => Ok(TopologyGeneration::new(0)),
            Some(previous) => previous
                .get()
                .checked_add(1)
                .map(TopologyGeneration::new)
                .ok_or(RegistryError::Exhausted),
        }
    }
    fn validate(&self) -> Result<(), RegistryError> {
        self.target_topology()?;
        if self.instance_id == [0; 16]
            || self.control.role_instance_id == [0; 16]
            || self.ingest_role_instance_id == [0; 16]
            || self.ingest_role_instance_id == self.control.role_instance_id
            || self.ingest_resource_id == self.control.resource_id
            || self.config_revision == 0
            || self.policy_revision == 0
            || self.outstanding_request_limit == 0
            || self.lease_expires_at_us <= self.created_at_us
            || self.allocation_domains.len() > MAX_DOMAINS
        {
            return Err(RegistryError::InvalidInput);
        }
        let mut previous = 0;
        for (index, &(kind, domain)) in self.allocation_domains.iter().enumerate() {
            if kind as u16 <= previous
                || self.allocation_domains[..index]
                    .iter()
                    .any(|entry| entry.1 == domain)
            {
                return Err(RegistryError::InvalidInput);
            }
            previous = kind as u16;
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AttemptState {
    Ready,
    /// The future may have been cancelled, including before COMMIT dispatch.
    InFlight,
    CommitDispatched,
    Complete,
}

/// A result describes registry state only. `current` compares heads/live row
/// states at inspection; it is not a lease, an authenticated action grant or a
/// claim that the native store matches. Even true must not activate a writer.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RegistrationObservation {
    Absent,
    Matching { current: bool },
    Conflict,
}

/// Owns the exact identity across timeout and unknown COMMIT. Not Clone or
/// Deserialize. Input cannot be changed while an earlier attempt is unresolved.
#[derive(Debug)]
pub struct RegistrationAttempt {
    specification: InitialRegistration,
    state: AttemptState,
}
impl RegistrationAttempt {
    pub fn new(specification: InitialRegistration) -> Result<Self, RegistryError> {
        specification.validate()?;
        Ok(Self {
            specification,
            state: AttemptState::Ready,
        })
    }
    pub const fn state(&self) -> AttemptState {
        self.state
    }
    pub fn specification(&self) -> &InitialRegistration {
        &self.specification
    }

    pub async fn register(
        &mut self,
        client: &mut Client,
    ) -> Result<RegistrationObservation, RegistryError> {
        if self.state != AttemptState::Ready {
            return Err(RegistryError::AttemptUnresolved);
        }
        self.state = AttemptState::InFlight;
        let tx = transaction(client).await?;
        let result = async {
            validate_backend(&tx).await?;
            let state = observe(&tx, &self.specification).await?;
            if state.observation != RegistrationObservation::Absent {
                return Ok(state.observation);
            }
            if !state.can_register {
                return Err(RegistryError::HeadsChanged);
            }
            migrations::require_applied(&migration_runner::read_ledger(&tx).await
                .map_err(|_| RegistryError::IncompatibleSchema)?, 7)
                .map_err(|_| RegistryError::IncompatibleSchema)?;
            let owner = self.specification.control;
            tx.query_one("SELECT control.require_control_owner($1::bytea, $2::bytea, $3::text::numeric, $4::bytea)",
                &[&&self.specification.dataset_id.as_bytes()[..], &&owner.resource_id.as_bytes()[..],
                  &owner.epoch.get().to_string(), &&owner.process_incarnation.as_bytes()[..]])
                .await.map_err(|_| RegistryError::ControlMismatch)?;
            insert(&tx, &self.specification).await?;
            Ok(RegistrationObservation::Matching { current: true })
        }
        .await;
        match result {
            Ok(observation) => {
                // Even an idempotent/no-write transaction follows the same bounded
                // terminal protocol. No successful result escapes before it ends.
                self.state = AttemptState::CommitDispatched;
                tx.commit()
                    .await
                    .map_err(|_| RegistryError::CommitUnknown)?;
                self.state = AttemptState::Complete;
                Ok(observation)
            }
            Err(error) => {
                tx.rollback()
                    .await
                    .map_err(|_| RegistryError::CommitUnknown)?;
                self.state = AttemptState::Ready;
                Err(error)
            }
        }
    }

    /// Use a fresh application connection after the original future terminated.
    /// Its transaction lock excludes the old possible COMMIT before a new read.
    /// An absent result permits retry of this same immutable specification only.
    pub async fn resolve(
        &mut self,
        client: &mut Client,
    ) -> Result<RegistrationObservation, RegistryError> {
        if !matches!(
            self.state,
            AttemptState::InFlight | AttemptState::CommitDispatched
        ) {
            return Err(RegistryError::NoUnresolvedAttempt);
        }
        let observation = inspect(client, &self.specification).await?;
        self.state = match observation {
            RegistrationObservation::Absent => AttemptState::Ready,
            _ => AttemptState::Complete,
        };
        Ok(observation)
    }
}

/// Bounded inspection under the same serialization as register/resolve. Row locks
/// protect coordination reads; no dataset, owner or producer is created here.
pub async fn inspect(
    client: &mut Client,
    specification: &InitialRegistration,
) -> Result<RegistrationObservation, RegistryError> {
    specification.validate()?;
    let tx = transaction(client).await?;
    validate_backend(&tx).await?;
    let observed = observe(&tx, specification).await?;
    tx.rollback().await.map_err(backend)?;
    Ok(observed.observation)
}

async fn transaction(client: &mut Client) -> Result<Transaction<'_>, RegistryError> {
    let tx = client
        .build_transaction()
        .isolation_level(IsolationLevel::ReadCommitted)
        .start()
        .await
        .map_err(backend)?;
    tx.batch_execute("SET LOCAL search_path = pg_catalog; SET LOCAL statement_timeout = '20s'; SET LOCAL lock_timeout = '5s'; SET LOCAL idle_in_transaction_session_timeout = '10s'; SET LOCAL synchronous_commit = on")
        .await.map_err(backend)?;
    tx.query_one(
        "SELECT pg_catalog.pg_advisory_xact_lock_shared($1, $2)",
        &[&LOCK_NAMESPACE, &LOCK_MIGRATIONS],
    )
    .await
    .map_err(backend)?;
    // A separate command before every lookup is essential: Read Committed then
    // takes the lookup snapshot after any previous holder finished its COMMIT.
    tx.query_one(
        "SELECT pg_catalog.pg_advisory_xact_lock($1, $2)",
        &[&LOCK_NAMESPACE, &LOCK_REGISTRATION],
    )
    .await
    .map_err(backend)?;
    Ok(tx)
}

async fn validate_backend(tx: &Transaction<'_>) -> Result<(), RegistryError> {
    let row = tx.query_one("SELECT pg_catalog.current_setting('server_version_num')::integer, pg_catalog.current_setting('fsync') = 'on', pg_catalog.current_setting('full_page_writes') = 'on'", &[])
        .await.map_err(backend)?;
    if row.try_get::<_, i32>(0).map_err(backend)? != 160015
        || !boolean(&row, 1)?
        || !boolean(&row, 2)?
    {
        return Err(RegistryError::IncompatibleEngine);
    }
    // Registration is an application operation. Refuse schema creation rights,
    // superusers and effective membership in the schema/table owning role.
    let row = tx.query_one("SELECT EXISTS (SELECT 1 FROM pg_catalog.pg_roles WHERE rolname = CURRENT_USER AND rolsuper) OR pg_catalog.has_schema_privilege(CURRENT_USER, 'control', 'CREATE') OR EXISTS (SELECT 1 FROM pg_catalog.pg_namespace WHERE nspname = 'control' AND pg_catalog.pg_has_role(CURRENT_USER, nspowner, 'USAGE')) OR EXISTS (SELECT 1 FROM pg_catalog.pg_class c JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace WHERE n.nspname = 'control' AND c.relkind IN ('r', 'p') AND pg_catalog.pg_has_role(CURRENT_USER, c.relowner, 'USAGE'))", &[])
        .await.map_err(backend)?;
    if boolean(&row, 0)? {
        return Err(RegistryError::MigrationPrivileges);
    }
    let applied = migration_runner::read_ledger(tx)
        .await
        .map_err(|_| RegistryError::IncompatibleSchema)?;
    migrations::require_applied(&applied, 2).map_err(|_| RegistryError::IncompatibleSchema)?;
    Ok(())
}

struct Observed {
    observation: RegistrationObservation,
    can_register: bool,
}
#[derive(Clone, Copy)]
struct Presence {
    exists: bool,
    matches: bool,
    live: bool,
}
fn presence(row: Option<Row>) -> Result<Presence, RegistryError> {
    match row {
        None => Ok(Presence {
            exists: false,
            matches: false,
            live: false,
        }),
        Some(row) => Ok(Presence {
            exists: true,
            matches: boolean(&row, 0)?,
            live: boolean(&row, 1)?,
        }),
    }
}
fn boolean(row: &Row, column: usize) -> Result<bool, RegistryError> {
    row.try_get(column)
        .map_err(|_| RegistryError::InvalidMetadata)
}
fn fixed_bytes<const N: usize>(value: &[u8]) -> Result<[u8; N], RegistryError> {
    value.try_into().map_err(|_| RegistryError::InvalidMetadata)
}
fn nonzero_id(value: &[u8]) -> Result<[u8; 16], RegistryError> {
    let value = fixed_bytes(value)?;
    if value == [0; 16] {
        return Err(RegistryError::InvalidMetadata);
    }
    Ok(value)
}
fn decimal(value: &str) -> Result<u64, RegistryError> {
    if value.is_empty()
        || value.len() > 20
        || (value.len() > 1 && value.starts_with('0'))
        || !value.bytes().all(|byte| byte.is_ascii_digit())
    {
        return Err(RegistryError::InvalidMetadata);
    }
    value.parse().map_err(|_| RegistryError::InvalidMetadata)
}
fn optional_decimal(row: &Row, column: usize) -> Result<Option<u64>, RegistryError> {
    row.try_get::<_, Option<&str>>(column)
        .map_err(backend)?
        .map(decimal)
        .transpose()
}

async fn observe(tx: &Transaction<'_>, s: &InitialRegistration) -> Result<Observed, RegistryError> {
    let dataset = &s.dataset_id.as_bytes().as_slice();
    let resource = &s.ingest_resource_id.as_bytes().as_slice();
    let incarnation = &s.process_incarnation.as_bytes().as_slice();
    let role = &s.ingest_role_instance_id.as_slice();
    let control_resource = &s.control.resource_id.as_bytes().as_slice();
    let control_incarnation = &s.control.process_incarnation.as_bytes().as_slice();
    let control_epoch = s.control.epoch.get().to_string();
    let config = s.config_revision.to_string();
    let policy = s.policy_revision.to_string();
    let target = s.target_topology()?.get().to_string();
    let previous = s.previous_topology.map(|value| value.get().to_string());
    let slot = &s.producer_slot_id.as_bytes().as_slice();
    let shard = i64::from(s.shard_id);
    let row = tx.query_opt("SELECT substring(instance_id FROM 1 FOR 17), left(topology_generation::text,21), left(config_revision::text,21), left(policy_revision::text,21) FROM control.datasets WHERE dataset_id = $1::bytea FOR SHARE", &[dataset])
        .await.map_err(backend)?.ok_or(RegistryError::WrongDeployment)?;
    if nonzero_id(row.try_get::<_, &[u8]>(0).map_err(backend)?)? != s.instance_id {
        return Err(RegistryError::WrongDeployment);
    }
    let topology = optional_decimal(&row, 1)?;
    let config_head = optional_decimal(&row, 2)?;
    let policy_head = optional_decimal(&row, 3)?;
    let row = tx.query_opt("SELECT substring(c.document_digest FROM 1 FOR 33), substring(p.document_digest FROM 1 FOR 33), c.schema_version, p.schema_version FROM control.config_revisions c JOIN control.policy_revisions p ON p.dataset_id = c.dataset_id AND p.config_revision = c.revision WHERE c.dataset_id = $1::bytea AND c.revision = $2::text::numeric AND p.revision = $3::text::numeric", &[dataset, &config, &policy])
        .await.map_err(backend)?.ok_or(RegistryError::HeadsChanged)?;
    if fixed_bytes::<32>(row.try_get::<_, &[u8]>(0).map_err(backend)?)?
        != *s.config_digest.as_bytes()
        || fixed_bytes::<32>(row.try_get::<_, &[u8]>(1).map_err(backend)?)?
            != *s.policy_digest.as_bytes()
        || row.try_get::<_, i32>(2).map_err(backend)? != 1
        || row.try_get::<_, i32>(3).map_err(backend)? != 1
    {
        return Err(RegistryError::HeadsChanged);
    }
    let control = presence(tx.query_opt("SELECT o.dataset_id = $1::bytea AND o.role = 'control' AND o.role_instance_id = $4::bytea AND o.process_incarnation = $5::bytea AND r.dataset_id = $1::bytea AND r.role = 'control', CASE o.state WHEN 'active' THEN r.retired_at_us IS NULL WHEN 'draining' THEN false WHEN 'released' THEN false END FROM control.owner_epochs o JOIN control.role_instances r ON r.role_instance_id = o.role_instance_id WHERE o.resource_id = $2::bytea AND o.epoch = $3::text::numeric FOR SHARE OF o,r", &[dataset, control_resource, &control_epoch, &s.control.role_instance_id.as_slice(), control_incarnation])
        .await.map_err(backend)?)?;
    if !control.matches {
        return Err(RegistryError::ControlMismatch);
    }
    if let Some(previous) = &previous {
        let row = tx.query_opt("SELECT sealed, EXISTS (SELECT 1 FROM control.topology_streams WHERE dataset_id = $1::bytea AND topology_generation = $2::text::numeric) FROM control.topology_generations WHERE dataset_id = $1::bytea AND generation = $2::text::numeric FOR SHARE", &[dataset, previous])
            .await.map_err(backend)?.ok_or(RegistryError::StartingTopologyNotEmpty)?;
        if !boolean(&row, 0)? || boolean(&row, 1)? {
            return Err(RegistryError::StartingTopologyNotEmpty);
        }
    }
    let prerequisites = control.live
        && config_head == Some(s.config_revision)
        && policy_head == Some(s.policy_revision);
    let owner = presence(tx.query_opt("SELECT dataset_id = $1::bytea AND role = 'ingest' AND role_instance_id = $3::bytea AND process_incarnation = $4::bytea AND acquired_at_us = $5::bigint, CASE state WHEN 'active' THEN true WHEN 'draining' THEN false WHEN 'released' THEN false END FROM control.owner_epochs WHERE resource_id = $2::bytea AND epoch = 1 FOR SHARE", &[dataset, resource, role, incarnation, &s.created_at_us])
        .await.map_err(backend)?)?;
    let role_row = presence(tx.query_opt("SELECT dataset_id = $1::bytea AND role = 'ingest' AND created_at_us = $3::bigint, retired_at_us IS NULL FROM control.role_instances WHERE role_instance_id = $2::bytea FOR SHARE", &[dataset, role, &s.created_at_us])
        .await.map_err(backend)?)?;
    let stream = presence(tx.query_opt("SELECT owner_resource_id = $3::bytea AND owner_role = 'ingest' AND owner_incarnation = $4::bytea AND opened_at_us = $5::bigint, CASE state WHEN 'open' THEN true WHEN 'closing' THEN false WHEN 'closed' THEN false END FROM control.source_streams WHERE dataset_id = $1::bytea AND shard_id = $2::bigint AND owner_epoch = 1 FOR SHARE", &[dataset, &shard, resource, incarnation, &s.created_at_us])
        .await.map_err(backend)?)?;
    let generation = presence(tx.query_opt("SELECT previous_generation IS NOT DISTINCT FROM $3::text::numeric AND created_at_us = $4::bigint AND sealed, sealed FROM control.topology_generations WHERE dataset_id = $1::bytea AND generation = $2::text::numeric FOR SHARE", &[dataset, &target, &previous, &s.created_at_us])
        .await.map_err(backend)?)?;
    let members = tx.query("SELECT shard_id::bigint, left(owner_epoch::text,21), handoff_last_complete_sequence IS NULL FROM control.topology_streams WHERE dataset_id = $1::bytea AND topology_generation = $2::text::numeric LIMIT 2", &[dataset, &target])
        .await.map_err(backend)?;
    let member_matches = if let [row] = members.as_slice() {
        row.try_get::<_, i64>(0).map_err(backend)? == shard
            && decimal(row.try_get(1).map_err(backend)?)? == 1
            && boolean(row, 2)?
    } else {
        false
    };
    let domains = tx.query("SELECT dictionary_kind, substring(allocation_domain FROM 1 FOR 17), left(owner_epoch::text,21), owner_role = 'ingest', substring(owner_incarnation FROM 1 FOR 17), created_at_us, CASE state WHEN 'active' THEN true WHEN 'closed' THEN false END FROM control.allocation_domains WHERE dataset_id = $1::bytea AND owner_resource_id = $2::bytea LIMIT 10 FOR SHARE", &[dataset, resource])
        .await.map_err(backend)?;
    let mut found_domains = Vec::with_capacity(domains.len());
    let mut domains_live = true;
    let mut domains_identity = domains.len() <= MAX_DOMAINS;
    for row in &domains {
        let kind = u16::try_from(row.try_get::<_, i16>(0).map_err(backend)?)
            .ok()
            .and_then(|value| DictionaryKind::try_from(value).ok())
            .ok_or(RegistryError::InvalidMetadata)?;
        let domain = AllocationDomainId::try_from(nonzero_id(row.try_get(1).map_err(backend)?)?)
            .map_err(|_| RegistryError::InvalidMetadata)?;
        domains_identity &= decimal(row.try_get(2).map_err(backend)?)? == 1
            && boolean(row, 3)?
            && nonzero_id(row.try_get(4).map_err(backend)?)? == *s.process_incarnation.as_bytes()
            && row.try_get::<_, i64>(5).map_err(backend)? == s.created_at_us;
        domains_live &= boolean(row, 6)?;
        found_domains.push((kind, domain));
    }
    found_domains.sort_unstable_by_key(|entry| entry.0 as u16);
    // A desired domain already registered to another resource/kind is a
    // conflict, not healthy absence followed by a vague uniqueness failure.
    let mut conflicting_domain = false;
    for &(kind, domain) in &s.allocation_domains {
        let row = tx.query_one("SELECT EXISTS (SELECT 1 FROM control.allocation_domains WHERE dataset_id = $1::bytea AND allocation_domain = $2::bytea AND (dictionary_kind <> $3::smallint OR owner_resource_id <> $4::bytea))", &[dataset, &domain.as_bytes().as_slice(), &(kind as i16), resource])
            .await.map_err(backend)?;
        conflicting_domain |= boolean(&row, 0)?;
    }
    let slot_row = presence(tx.query_opt("SELECT dataset_id = $1::bytea AND registered_by_resource_id = $3::bytea AND registered_by_epoch = $4::text::numeric AND registered_by_role = 'control' AND registered_by_incarnation = $5::bytea AND created_at_us = $6::bigint, true FROM control.producer_slots WHERE producer_slot_id = $2::bytea", &[dataset, slot, control_resource, &control_epoch, control_incarnation, &s.created_at_us])
        .await.map_err(backend)?)?;
    let producer = presence(tx.query_opt("SELECT dataset_id = $1::bytea AND producer_role_instance_id = $3::bytea AND producer_role = 'ingest' AND producer_incarnation = $4::bytea AND owner_resource_id = $5::bytea AND owner_epoch = 1 AND owner_role = 'ingest' AND owner_incarnation = $4::bytea AND outstanding_request_limit = $6::bigint AND opened_at_us = $7::bigint, CASE state WHEN 'open' THEN true WHEN 'closing' THEN false WHEN 'retired' THEN false END FROM control.producer_epochs WHERE producer_slot_id = $2::bytea AND producer_epoch = 1 FOR SHARE", &[dataset, slot, role, incarnation, resource, &i64::from(s.outstanding_request_limit), &s.created_at_us])
        .await.map_err(backend)?)?;
    let required = [owner, role_row, stream, generation, slot_row, producer];
    let any = required.iter().any(|row| row.exists)
        || !members.is_empty()
        || !domains.is_empty()
        || conflicting_domain;
    let observation = if !any {
        RegistrationObservation::Absent
    } else if required.iter().all(|row| row.matches)
        && member_matches
        && domains_identity
        && !conflicting_domain
        && found_domains == s.allocation_domains
    {
        RegistrationObservation::Matching {
            current: prerequisites
                && topology == Some(s.target_topology()?.get())
                && required.iter().all(|row| row.live)
                && domains_live,
        }
    } else {
        RegistrationObservation::Conflict
    };
    Ok(Observed {
        observation,
        can_register: prerequisites && topology == s.previous_topology.map(|value| value.get()),
    })
}

async fn insert(tx: &Transaction<'_>, s: &InitialRegistration) -> Result<(), RegistryError> {
    // Dataset/Control rows were locked by observe. Upgrade dataset before any
    // new rows: the final CAS and all registrations share this transaction.
    let dataset = &s.dataset_id.as_bytes().as_slice();
    let resource = &s.ingest_resource_id.as_bytes().as_slice();
    let incarnation = &s.process_incarnation.as_bytes().as_slice();
    let role = &s.ingest_role_instance_id.as_slice();
    let target = s.target_topology()?.get().to_string();
    let previous = s.previous_topology.map(|value| value.get().to_string());
    tx.query_one(
        "SELECT dataset_id FROM control.datasets WHERE dataset_id = $1::bytea FOR UPDATE",
        &[dataset],
    )
    .await
    .map_err(backend)?;
    tx.execute("INSERT INTO control.role_instances (role_instance_id,dataset_id,role,created_at_us) VALUES ($1::bytea,$2::bytea,'ingest',$3::bigint)", &[role, dataset, &s.created_at_us]).await.map_err(backend)?;
    tx.execute("INSERT INTO control.owner_epochs (resource_id,epoch,dataset_id,role,role_instance_id,process_incarnation,acquired_at_us,lease_expires_at_us,state) VALUES ($1::bytea,1,$2::bytea,'ingest',$3::bytea,$4::bytea,$5::bigint,$6::bigint,'active')", &[resource, dataset, role, incarnation, &s.created_at_us, &s.lease_expires_at_us]).await.map_err(backend)?;
    tx.execute("INSERT INTO control.topology_generations (dataset_id,generation,previous_generation,created_at_us) VALUES ($1::bytea,$2::text::numeric,$3::text::numeric,$4::bigint)", &[dataset, &target, &previous, &s.created_at_us]).await.map_err(backend)?;
    tx.execute("INSERT INTO control.source_streams (dataset_id,shard_id,owner_epoch,owner_resource_id,owner_incarnation,opened_at_us) VALUES ($1::bytea,$2::bigint,1,$3::bytea,$4::bytea,$5::bigint)", &[dataset, &i64::from(s.shard_id), resource, incarnation, &s.created_at_us]).await.map_err(backend)?;
    tx.execute("INSERT INTO control.topology_streams (dataset_id,topology_generation,shard_id,owner_epoch) VALUES ($1::bytea,$2::text::numeric,$3::bigint,1)", &[dataset, &target, &i64::from(s.shard_id)]).await.map_err(backend)?;
    for &(kind, domain) in &s.allocation_domains {
        tx.execute("INSERT INTO control.allocation_domains (dataset_id,dictionary_kind,allocation_domain,owner_resource_id,owner_epoch,owner_incarnation,created_at_us) VALUES ($1::bytea,$2::smallint,$3::bytea,$4::bytea,1,$5::bytea,$6::bigint)", &[dataset, &(kind as i16), &domain.as_bytes().as_slice(), resource, incarnation, &s.created_at_us]).await.map_err(backend)?;
    }
    let slot = &s.producer_slot_id.as_bytes().as_slice();
    tx.execute("INSERT INTO control.producer_slots (producer_slot_id,dataset_id,registered_by_resource_id,registered_by_epoch,registered_by_incarnation,created_at_us) VALUES ($1::bytea,$2::bytea,$3::bytea,$4::text::numeric,$5::bytea,$6::bigint)", &[slot, dataset, &s.control.resource_id.as_bytes().as_slice(), &s.control.epoch.get().to_string(), &s.control.process_incarnation.as_bytes().as_slice(), &s.created_at_us]).await.map_err(backend)?;
    tx.execute("INSERT INTO control.producer_epochs (producer_slot_id,producer_epoch,dataset_id,producer_role_instance_id,producer_role,producer_incarnation,owner_resource_id,owner_epoch,owner_incarnation,outstanding_request_limit,opened_at_us) VALUES ($1::bytea,1,$2::bytea,$3::bytea,'ingest',$4::bytea,$5::bytea,1,$4::bytea,$6::bigint,$7::bigint)", &[slot, dataset, role, incarnation, resource, &i64::from(s.outstanding_request_limit), &s.created_at_us]).await.map_err(backend)?;
    tx.execute("UPDATE control.topology_generations SET sealed = true WHERE dataset_id = $1::bytea AND generation = $2::text::numeric", &[dataset, &target]).await.map_err(backend)?;
    let changed = tx.execute("UPDATE control.datasets SET topology_generation = $2::text::numeric WHERE dataset_id = $1::bytea AND topology_generation IS NOT DISTINCT FROM $3::text::numeric AND config_revision = $4::text::numeric AND policy_revision = $5::text::numeric", &[dataset, &target, &previous, &s.config_revision.to_string(), &s.policy_revision.to_string()]).await.map_err(backend)?;
    if changed != 1 {
        return Err(RegistryError::HeadsChanged);
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn database_scalar_decoding_preserves_full_domain_and_rejects_coercion() {
        for value in [0, 1, i64::MAX as u64, (i64::MAX as u64) + 1, u64::MAX] {
            assert_eq!(decimal(&value.to_string()).unwrap(), value);
        }
        for value in [
            "",
            "00",
            "01",
            "+1",
            "-0",
            "1.0",
            "1e0",
            " 1",
            "1\n",
            "18446744073709551616",
            "１２",
        ] {
            assert_eq!(decimal(value), Err(RegistryError::InvalidMetadata));
        }
        assert_eq!(nonzero_id(&[1; 16]).unwrap(), [1; 16]);
        for bytes in [&[0; 16][..], &[1; 15], &[1; 17]] {
            assert_eq!(nonzero_id(bytes), Err(RegistryError::InvalidMetadata));
        }
        assert_eq!(fixed_bytes::<32>(&[0; 32]).unwrap(), [0; 32]);
        assert!(fixed_bytes::<32>(&[1; 33]).is_err());
    }
}
