//! Initial physical Index owner. Metadata planning and PG authority remain in
//! their existing owners. These operations install/inspect actual CH tables and
//! emit opaque proofs only after complete bounded typed readback.
use super::{
    lane::{INDEX_LANE_STACK_RESERVATION, OwnedIndexLane},
    rows::{MAX_CORE_FACT_BYTES, MAX_TAG_READBACK_ALLOCATION_BYTES},
    schema::IndexTables,
    verified::{VerifiedIndexPlan, VerifiedInstallation},
};
use crate::{
    runtime::OwnedPermit,
    storage::{
        indexer::{custody::HeldIndexCustody, plan::PlannedBatch},
        postgres::{IndexTarget, PreparedIndexPlan},
    },
};
use std::{
    fs::File,
    future::Future,
    num::NonZeroUsize,
    sync::Arc,
    time::{Duration, Instant},
};

mod inspect;
mod operation;
mod readback;
mod recovery;
mod write;
pub use super::lane::{
    JoinedIndexLane as JoinedIndexOperation, LaneError as IndexOperationError,
    LaneOutcome as IndexOperationOutcome,
};
pub use operation::IndexOperation;
use operation::QueryOwner;
pub use operation::{IndexConnection, IndexEngineSlot, IndexOperationId};
pub use recovery::{IndexRecoveryVerifier, RecoveryResult, required_recovery_metadata_bytes};

pub(super) const MAX_QUERY_BYTES: usize = 256 * 1024;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PhysicalError {
    InvalidInput,
    InsufficientReservation,
    Allocation,
    Limit,
    Custody,
    Source,
    Rows,
    Schema,
    IncompatibleEngine,
    Query,
    RemoteUnresolved,
    Unavailable,
    Control,
    Sequence,
}
impl std::fmt::Display for PhysicalError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "initial Index physical operation failed: {self:?}")
    }
}
impl std::error::Error for PhysicalError {}

/// Local requested-allocation bounds and an independent remote query ceiling.
/// Allocator overhead/process RSS and whole-engine capacity still belong to the
/// configured process envelopes; these are not measured resident-memory claims.
#[derive(Debug, Clone, Copy)]
pub struct PhysicalLimits {
    response_bytes: NonZeroUsize,
    request_bytes: u64,
    max_rows: u64,
    query_memory_bytes: u64,
    query_timeout: Duration,
}
impl PhysicalLimits {
    pub fn new(
        response_bytes: NonZeroUsize,
        request_bytes: u64,
        max_rows: u64,
        query_memory_bytes: u64,
        query_timeout: Duration,
    ) -> Result<Self, PhysicalError> {
        if request_bytes == 0
            || max_rows == 0
            || max_rows == u64::MAX
            || query_memory_bytes == 0
            || query_timeout.as_secs() == 0
            || query_timeout.subsec_nanos() != 0
        {
            return Err(PhysicalError::InvalidInput);
        }
        let value = Self {
            response_bytes,
            request_bytes,
            max_rows,
            query_memory_bytes,
            query_timeout,
        };
        value.required_local_data_bytes()?;
        Ok(value)
    }
    pub fn required_local_data_bytes(self) -> Result<u64, PhysicalError> {
        // Staged complete wire response plus the original raw/assembled
        // response and owned scalar-row overlap; separately
        // bounded decoded nested tags; worst core-row serialization growth;
        // bounded schema/query/plan metadata and credential/driver staging.
        (self.response_bytes.get() as u64)
            .checked_mul(5)
            .and_then(|n| n.checked_add(MAX_TAG_READBACK_ALLOCATION_BYTES as u64))
            .and_then(|n| n.checked_add(4 * MAX_CORE_FACT_BYTES as u64))
            .and_then(|n| n.checked_add(2 * MAX_QUERY_BYTES as u64 + 2 * 1024 * 1024))
            .ok_or(PhysicalError::Limit)
    }
    pub fn required_local_native_bytes(self) -> u64 {
        INDEX_LANE_STACK_RESERVATION
    }
    pub fn required_engine_data_bytes(self) -> u64 {
        self.query_memory_bytes
    }
    pub fn query_timeout(self) -> Duration {
        self.query_timeout
    }
    fn check_local(self, permit: &OwnedPermit) -> Result<(), PhysicalError> {
        let units = permit.request().units();
        if units.data_bytes() < self.required_local_data_bytes()?
            || units.native_bytes() < self.required_local_native_bytes()
            || units.items() == 0
            || units.control_bytes() != 0
        {
            return Err(PhysicalError::InsufficientReservation);
        }
        Ok(())
    }
}

/// Clones share the same service-owned remote slot, credentials and immutable
/// table manifest. No ambient client or runtime handle is retained here.
#[derive(Clone)]
pub struct IndexAdapter {
    tables: Arc<IndexTables>,
    connection: Arc<IndexConnection>,
    limits: PhysicalLimits,
    slot: Arc<IndexEngineSlot>,
}
impl IndexAdapter {
    pub fn new(
        tables: Arc<IndexTables>,
        connection: Arc<IndexConnection>,
        limits: PhysicalLimits,
        slot: Arc<IndexEngineSlot>,
    ) -> Self {
        Self {
            tables,
            connection,
            limits,
            slot,
        }
    }
    pub fn tables(&self) -> &IndexTables {
        &self.tables
    }
    pub fn limits(&self) -> PhysicalLimits {
        self.limits
    }
    pub fn unresolved_query(&self) -> Option<String> {
        self.slot.unresolved_query()
    }

    #[allow(clippy::too_many_arguments)]
    fn start<T, F, Work>(
        &self,
        custody: &HeldIndexCustody,
        local: OwnedPermit,
        engine: OwnedPermit,
        operation: IndexOperationId,
        deadline: Instant,
        source: Option<Arc<PlannedBatch>>,
        metadata: Option<Arc<OwnedPermit>>,
        work: F,
    ) -> Result<IndexOperation<T>, PhysicalError>
    where
        T: Send + 'static,
        F: FnOnce(QueryOwner, File) -> Work + Send + 'static,
        Work: Future<Output = Result<T, PhysicalError>> + 'static,
    {
        self.tables
            .check_binding(custody.binding())
            .map_err(|_| PhysicalError::Schema)?;
        self.limits.check_local(&local)?;
        let retained = custody.retain_lane().map_err(|_| PhysicalError::Custody)?;
        self.slot.reserve(operation, engine, self.limits)?;
        let connection = Arc::clone(&self.connection);
        let tables = Arc::clone(&self.tables);
        let limits = self.limits;
        let slot = Arc::clone(&self.slot);
        // Client and its hidden tasks are created only on the dedicated runtime.
        let lane = OwnedIndexLane::start(custody, local, deadline, move || {
            let client = connection.client(tables.database(), limits);
            work(QueryOwner::new(client, slot, operation), retained)
        });
        match lane {
            Ok(lane) => Ok(IndexOperation::new(
                lane,
                source,
                metadata,
                Arc::clone(&self.slot),
                operation,
            )),
            Err(_) => {
                self.slot.joined(operation);
                Err(PhysicalError::Unavailable)
            }
        }
    }

    /// Explicit initializer only: requires its separate initialization account.
    /// Partial/foreign/nonempty existing databases are refused and preserved.
    pub fn install_fresh(
        &self,
        custody: &HeldIndexCustody,
        local: OwnedPermit,
        engine: OwnedPermit,
        operation: IndexOperationId,
        deadline: Instant,
    ) -> Result<IndexOperation<VerifiedInstallation>, PhysicalError> {
        let tables = Arc::clone(&self.tables);
        let connection = Arc::clone(&self.connection);
        let binding = *custody.binding();
        self.start(
            custody,
            local,
            engine,
            operation,
            deadline,
            None,
            None,
            move |mut owner, retained| async move {
                inspect::engine(&mut owner, &tables, connection.user(), false).await?;
                inspect::empty_database(&mut owner, &tables).await?;
                for target in IndexTarget::ALL {
                    owner.execute(&tables.create_table(target)).await?;
                }
                inspect::installed(&mut owner, &tables, true).await?;
                Ok(VerifiedInstallation::observed(binding, retained))
            },
        )
    }

    /// Only the actual persisted PreparedIndexPlan can enter the ordinary writer.
    /// The full source/mapping reservation lives outside the cancellable future.
    #[allow(clippy::too_many_arguments)]
    pub fn write_and_verify(
        &self,
        custody: &HeldIndexCustody,
        prepared: &PreparedIndexPlan,
        planned: PlannedBatch,
        local: OwnedPermit,
        engine: OwnedPermit,
        operation: IndexOperationId,
        deadline: Instant,
    ) -> Result<IndexOperation<VerifiedIndexPlan>, PhysicalError> {
        if prepared.plan() != planned.plan() || planned.plan().binding() != custody.binding() {
            return Err(PhysicalError::Source);
        }
        self.replay_and_verify(
            custody,
            Arc::new(planned),
            local,
            engine,
            operation,
            deadline,
            true,
            None,
        )
    }
    #[allow(clippy::too_many_arguments)]
    fn replay_and_verify(
        &self,
        custody: &HeldIndexCustody,
        planned: Arc<PlannedBatch>,
        local: OwnedPermit,
        engine: OwnedPermit,
        operation: IndexOperationId,
        deadline: Instant,
        guard_after: bool,
        metadata: Option<Arc<OwnedPermit>>,
    ) -> Result<IndexOperation<VerifiedIndexPlan>, PhysicalError> {
        let tables = Arc::clone(&self.tables);
        let connection = Arc::clone(&self.connection);
        let limits = self.limits;
        let source = Arc::clone(&planned);
        self.start(
            custody,
            local,
            engine,
            operation,
            deadline,
            Some(planned),
            metadata,
            move |mut owner, retained| async move {
                inspect::engine(&mut owner, &tables, connection.user(), true).await?;
                inspect::installed(&mut owner, &tables, false).await?;
                write::batch(&mut owner, &tables, &source, limits).await?;
                let plan = source.plan();
                let digest = readback::plan(&mut owner, &tables, plan, limits).await?;
                if guard_after {
                    readback::inside_cut(
                        &mut owner,
                        &tables,
                        plan.binding().generation().as_bytes(),
                        plan.after(),
                    )
                    .await?;
                }
                Ok(VerifiedIndexPlan::observed(
                    *plan.binding(),
                    plan.digest(),
                    digest,
                    retained,
                ))
            },
        )
    }
}
