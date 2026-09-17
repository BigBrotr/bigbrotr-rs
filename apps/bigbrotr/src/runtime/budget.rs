use std::{
    collections::VecDeque,
    sync::{Arc, Condvar, Mutex, MutexGuard},
    time::Instant,
};

/// Finite resource limits enforced by a [`ResourceBudget`].
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct ResourceLimits {
    data_bytes: u64,
    items: u64,
    native_bytes: u64,
    control_bytes: u64,
}

impl ResourceLimits {
    /// Constructs limits for data bytes, item count, native bytes, and the
    /// independently reserved control bytes.
    pub const fn new(data_bytes: u64, items: u64, native_bytes: u64, control_bytes: u64) -> Self {
        Self {
            data_bytes,
            items,
            native_bytes,
            control_bytes,
        }
    }

    /// Returns the data-byte limit or current use.
    pub const fn data_bytes(self) -> u64 {
        self.data_bytes
    }

    /// Returns the item limit or current use.
    pub const fn items(self) -> u64 {
        self.items
    }

    /// Returns the native-byte limit or current use.
    pub const fn native_bytes(self) -> u64 {
        self.native_bytes
    }

    /// Returns the control-byte limit or current use.
    pub const fn control_bytes(self) -> u64 {
        self.control_bytes
    }

    fn checked_add(self, other: Self) -> Option<Self> {
        Some(Self::new(
            self.data_bytes.checked_add(other.data_bytes)?,
            self.items.checked_add(other.items)?,
            self.native_bytes.checked_add(other.native_bytes)?,
            self.control_bytes.checked_add(other.control_bytes)?,
        ))
    }

    fn checked_sub(self, other: Self) -> Option<Self> {
        Some(Self::new(
            self.data_bytes.checked_sub(other.data_bytes)?,
            self.items.checked_sub(other.items)?,
            self.native_bytes.checked_sub(other.native_bytes)?,
            self.control_bytes.checked_sub(other.control_bytes)?,
        ))
    }
}

/// Bounds the number of threads admitted to each synchronous wait lane.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct WaiterLimits {
    data: usize,
    control: usize,
}

impl WaiterLimits {
    /// Constructs independent data and control waiter limits.
    pub const fn new(data: usize, control: usize) -> Self {
        Self { data, control }
    }

    /// Returns the data-lane waiter limit.
    pub const fn data(self) -> usize {
        self.data
    }

    /// Returns the control-lane waiter limit.
    pub const fn control(self) -> usize {
        self.control
    }
}

/// A statically checked process-memory envelope and its admission limits.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ProcessEnvelope {
    memory_max_bytes: u64,
    baseline_bytes: u64,
    resources: ResourceLimits,
    waiters: WaiterLimits,
}

impl ProcessEnvelope {
    /// Checks that baseline, data, native, and control memory fit the process
    /// maximum without arithmetic overflow.
    pub fn new(
        memory_max_bytes: u64,
        baseline_bytes: u64,
        resources: ResourceLimits,
        waiters: WaiterLimits,
    ) -> Result<Self, EnvelopeError> {
        let required_bytes = baseline_bytes
            .checked_add(resources.data_bytes)
            .and_then(|value| value.checked_add(resources.native_bytes))
            .and_then(|value| value.checked_add(resources.control_bytes))
            .ok_or(EnvelopeError::MemoryArithmeticOverflow)?;
        if required_bytes > memory_max_bytes {
            return Err(EnvelopeError::MemoryLimitExceeded {
                required_bytes,
                memory_max_bytes,
            });
        }
        Ok(Self {
            memory_max_bytes,
            baseline_bytes,
            resources,
            waiters,
        })
    }

    /// Returns the process memory maximum used for static validation.
    pub const fn memory_max_bytes(self) -> u64 {
        self.memory_max_bytes
    }

    /// Returns the baseline memory reservation.
    pub const fn baseline_bytes(self) -> u64 {
        self.baseline_bytes
    }

    /// Returns the enforceable admission limits.
    pub const fn resources(self) -> ResourceLimits {
        self.resources
    }

    /// Returns the independent wait-lane bounds.
    pub const fn waiters(self) -> WaiterLimits {
        self.waiters
    }
}

/// Failure while validating a process-memory envelope.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum EnvelopeError {
    /// The memory components overflowed `u64` while being summed.
    MemoryArithmeticOverflow,
    /// The checked components exceeded the configured process maximum.
    MemoryLimitExceeded {
        /// Sum of baseline, data, native, and control bytes.
        required_bytes: u64,
        /// Configured process maximum.
        memory_max_bytes: u64,
    },
}

/// A checked request for either data-plane resources or protected control
/// capacity.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ResourceRequest {
    units: ResourceLimits,
    lane: Lane,
}

impl ResourceRequest {
    /// Constructs a non-empty data request.
    pub fn data(bytes: u64, items: u64, native_bytes: u64) -> Result<Self, RequestError> {
        if bytes == 0 && items == 0 && native_bytes == 0 {
            return Err(RequestError::Empty);
        }
        Ok(Self {
            units: ResourceLimits::new(bytes, items, native_bytes, 0),
            lane: Lane::Data,
        })
    }

    /// Constructs a non-empty control request.
    pub fn control(bytes: u64) -> Result<Self, RequestError> {
        if bytes == 0 {
            return Err(RequestError::Empty);
        }
        Ok(Self {
            units: ResourceLimits::new(0, 0, 0, bytes),
            lane: Lane::Control,
        })
    }

    fn units(self) -> ResourceLimits {
        self.units
    }

    fn fits_limit(self, limits: ResourceLimits) -> bool {
        match self.lane {
            Lane::Data => {
                self.units.data_bytes <= limits.data_bytes
                    && self.units.items <= limits.items
                    && self.units.native_bytes <= limits.native_bytes
            }
            Lane::Control => self.units.control_bytes <= limits.control_bytes,
        }
    }

    fn lane(self) -> Lane {
        self.lane
    }
}

/// Failure while constructing a resource request.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum RequestError {
    /// The request did not reserve any resource.
    Empty,
}

/// Admission failure for a resource request.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum AcquireError {
    /// The budget is permanently closed.
    Closed,
    /// This request cannot fit the current limit even with no live work.
    RequestExceedsLimit,
    /// Capacity is unavailable for an immediate acquisition.
    Unavailable,
    /// The request's bounded wait lane is full.
    WaitQueueFull,
    /// The caller's explicit deadline elapsed before admission.
    DeadlineExceeded,
    /// Internal resource accounting would overflow.
    ArithmeticOverflow,
}

/// Failure while waiting for a closed budget to drain.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum DrainError {
    /// Draining is meaningful only after the budget is closed.
    NotClosed,
    /// Live permits or joinable waiters remained at the deadline.
    DeadlineExceeded,
}

/// A cloneable admission budget. Capacity stays owned until its permit drops.
#[derive(Clone)]
pub struct ResourceBudget {
    inner: Arc<Inner>,
}

/// Exclusive ownership of capacity admitted from a [`ResourceBudget`].
pub struct OwnedPermit {
    inner: Arc<Inner>,
    request: ResourceRequest,
}

impl std::fmt::Debug for OwnedPermit {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("OwnedPermit")
            .field("request", &self.request)
            .finish_non_exhaustive()
    }
}

impl OwnedPermit {
    /// Returns the exact request held by this permit.
    pub const fn request(&self) -> ResourceRequest {
        self.request
    }
}

impl Drop for OwnedPermit {
    fn drop(&mut self) {
        let mut state = self.inner.lock();
        state.used = state
            .used
            .checked_sub(self.request.units())
            .expect("owned permit release must match prior admission");
        state.active_permits = state
            .active_permits
            .checked_sub(1)
            .expect("owned permit count must be positive");
        self.inner.changed.notify_all();
    }
}

/// An instantaneous view of budget state for diagnostics and shutdown checks.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct BudgetSnapshot {
    limits: ResourceLimits,
    used: ResourceLimits,
    active_permits: u64,
    closed: bool,
    waiting_data: usize,
    waiting_control: usize,
}

impl BudgetSnapshot {
    /// Returns the live resource limits.
    pub const fn limits(self) -> ResourceLimits {
        self.limits
    }

    /// Returns capacity currently held by owned permits.
    pub const fn used(self) -> ResourceLimits {
        self.used
    }

    /// Returns the count of live owned permits.
    pub const fn active_permits(self) -> u64 {
        self.active_permits
    }

    /// Reports whether the budget has been permanently closed.
    pub const fn closed(self) -> bool {
        self.closed
    }

    /// Returns the number of admitted data-lane waiters.
    pub const fn waiting_data(self) -> usize {
        self.waiting_data
    }

    /// Returns the number of admitted control-lane waiters.
    pub const fn waiting_control(self) -> usize {
        self.waiting_control
    }

    /// Reports whether live data usage exceeds any current data limit.
    pub const fn data_over_limit(self) -> bool {
        self.used.data_bytes > self.limits.data_bytes
            || self.used.items > self.limits.items
            || self.used.native_bytes > self.limits.native_bytes
    }
}

impl ResourceBudget {
    /// Constructs an open budget from a checked process envelope.
    pub fn new(envelope: ProcessEnvelope) -> Self {
        Self {
            inner: Arc::new(Inner {
                state: Mutex::new(State {
                    memory_max_bytes: envelope.memory_max_bytes,
                    baseline_bytes: envelope.baseline_bytes,
                    limits: envelope.resources,
                    waiter_limits: envelope.waiters,
                    used: ResourceLimits::default(),
                    active_permits: 0,
                    closed: false,
                    data_waiters: VecDeque::new(),
                    control_waiters: VecDeque::new(),
                }),
                changed: Condvar::new(),
            }),
        }
    }

    /// Attempts immediate admission without joining a wait lane.
    pub fn try_acquire(&self, request: ResourceRequest) -> Result<OwnedPermit, AcquireError> {
        let mut state = self.inner.lock();
        Self::check_open_and_possible(&state, request)?;
        if !state.queue(request.lane()).is_empty() || !state.can_admit(request)? {
            return Err(AcquireError::Unavailable);
        }
        state.grant(request)?;
        Ok(OwnedPermit {
            inner: Arc::clone(&self.inner),
            request,
        })
    }

    /// Waits in the request's bounded FIFO lane until admitted or the explicit
    /// deadline is reached.
    pub fn acquire_until(
        &self,
        request: ResourceRequest,
        deadline: Instant,
    ) -> Result<OwnedPermit, AcquireError> {
        let mut state = self.inner.lock();
        Self::check_open_and_possible(&state, request)?;
        if Instant::now() >= deadline {
            return Err(AcquireError::DeadlineExceeded);
        }

        let lane = request.lane();
        if state.queue(lane).len() >= state.waiter_limit(lane) {
            return Err(AcquireError::WaitQueueFull);
        }
        let token = Arc::new(());
        state.queue_mut(lane).push_back(Arc::clone(&token));

        loop {
            if state.closed {
                state.remove_waiter(lane, &token);
                self.inner.changed.notify_all();
                return Err(AcquireError::Closed);
            }
            if !request.fits_limit(state.limits) {
                state.remove_waiter(lane, &token);
                self.inner.changed.notify_all();
                return Err(AcquireError::RequestExceedsLimit);
            }
            let now = Instant::now();
            if now >= deadline {
                state.remove_waiter(lane, &token);
                self.inner.changed.notify_all();
                return Err(AcquireError::DeadlineExceeded);
            }

            let is_front = state
                .queue(lane)
                .front()
                .is_some_and(|front| Arc::ptr_eq(front, &token));
            let can_admit = match state.can_admit(request) {
                Ok(can_admit) => can_admit,
                Err(error) => {
                    state.remove_waiter(lane, &token);
                    self.inner.changed.notify_all();
                    return Err(error);
                }
            };
            if is_front && can_admit {
                if let Err(error) = state.grant(request) {
                    state.remove_waiter(lane, &token);
                    self.inner.changed.notify_all();
                    return Err(error);
                }
                state.queue_mut(lane).pop_front();
                self.inner.changed.notify_all();
                return Ok(OwnedPermit {
                    inner: Arc::clone(&self.inner),
                    request,
                });
            }

            let remaining = deadline.saturating_duration_since(now);
            let (next_state, _) = self
                .inner
                .changed
                .wait_timeout(state, remaining)
                .expect("resource budget mutex must not be poisoned");
            state = next_state;
        }
    }

    /// Replaces live limits. Existing permits retain ownership; subsequent
    /// admission observes the new limits.
    pub fn set_limits(&self, limits: ResourceLimits) -> Result<(), EnvelopeError> {
        let mut state = self.inner.lock();
        // A reduced limit cannot release bytes still owned by existing permits.
        // Charge both retained use and newly available capacity before allowing
        // a transfer between lanes or between data and native allocations.
        let transition = ResourceLimits::new(
            limits.data_bytes.max(state.used.data_bytes),
            limits.items.max(state.used.items),
            limits.native_bytes.max(state.used.native_bytes),
            limits.control_bytes.max(state.used.control_bytes),
        );
        ProcessEnvelope::new(
            state.memory_max_bytes,
            state.baseline_bytes,
            transition,
            state.waiter_limits,
        )?;
        state.limits = limits;
        self.inner.changed.notify_all();
        Ok(())
    }

    /// Permanently closes admission and wakes every joinable waiter.
    pub fn close(&self) {
        let mut state = self.inner.lock();
        state.closed = true;
        self.inner.changed.notify_all();
    }

    /// Waits for a closed budget to have no live permits or queued waiters.
    pub fn drain_until(&self, deadline: Instant) -> Result<(), DrainError> {
        let mut state = self.inner.lock();
        if !state.closed {
            return Err(DrainError::NotClosed);
        }
        loop {
            if state.active_permits == 0
                && state.data_waiters.is_empty()
                && state.control_waiters.is_empty()
            {
                return Ok(());
            }
            let now = Instant::now();
            if now >= deadline {
                return Err(DrainError::DeadlineExceeded);
            }
            let remaining = deadline.saturating_duration_since(now);
            let (next_state, _) = self
                .inner
                .changed
                .wait_timeout(state, remaining)
                .expect("resource budget mutex must not be poisoned");
            state = next_state;
        }
    }

    /// Returns an instantaneous diagnostic snapshot.
    pub fn snapshot(&self) -> BudgetSnapshot {
        let state = self.inner.lock();
        BudgetSnapshot {
            limits: state.limits,
            used: state.used,
            active_permits: state.active_permits,
            closed: state.closed,
            waiting_data: state.data_waiters.len(),
            waiting_control: state.control_waiters.len(),
        }
    }

    fn check_open_and_possible(
        state: &State,
        request: ResourceRequest,
    ) -> Result<(), AcquireError> {
        if state.closed {
            return Err(AcquireError::Closed);
        }
        if !request.fits_limit(state.limits) {
            return Err(AcquireError::RequestExceedsLimit);
        }
        Ok(())
    }
}

struct Inner {
    state: Mutex<State>,
    changed: Condvar,
}

impl Inner {
    fn lock(&self) -> MutexGuard<'_, State> {
        self.state
            .lock()
            .expect("resource budget mutex must not be poisoned")
    }
}

struct State {
    memory_max_bytes: u64,
    baseline_bytes: u64,
    limits: ResourceLimits,
    waiter_limits: WaiterLimits,
    used: ResourceLimits,
    active_permits: u64,
    closed: bool,
    data_waiters: VecDeque<Arc<()>>,
    control_waiters: VecDeque<Arc<()>>,
}

impl State {
    fn queue(&self, lane: Lane) -> &VecDeque<Arc<()>> {
        match lane {
            Lane::Data => &self.data_waiters,
            Lane::Control => &self.control_waiters,
        }
    }

    fn queue_mut(&mut self, lane: Lane) -> &mut VecDeque<Arc<()>> {
        match lane {
            Lane::Data => &mut self.data_waiters,
            Lane::Control => &mut self.control_waiters,
        }
    }

    fn waiter_limit(&self, lane: Lane) -> usize {
        match lane {
            Lane::Data => self.waiter_limits.data,
            Lane::Control => self.waiter_limits.control,
        }
    }

    fn remove_waiter(&mut self, lane: Lane, token: &Arc<()>) {
        if let Some(index) = self
            .queue(lane)
            .iter()
            .position(|queued| Arc::ptr_eq(queued, token))
        {
            self.queue_mut(lane).remove(index);
        }
    }

    fn can_admit(&self, request: ResourceRequest) -> Result<bool, AcquireError> {
        let next = self
            .used
            .checked_add(request.units())
            .ok_or(AcquireError::ArithmeticOverflow)?;
        Ok(match request.lane() {
            Lane::Data => {
                next.data_bytes <= self.limits.data_bytes
                    && next.items <= self.limits.items
                    && next.native_bytes <= self.limits.native_bytes
            }
            Lane::Control => next.control_bytes <= self.limits.control_bytes,
        })
    }

    fn grant(&mut self, request: ResourceRequest) -> Result<(), AcquireError> {
        let next_used = self
            .used
            .checked_add(request.units())
            .ok_or(AcquireError::ArithmeticOverflow)?;
        let next_active = self
            .active_permits
            .checked_add(1)
            .ok_or(AcquireError::ArithmeticOverflow)?;
        self.used = next_used;
        self.active_permits = next_active;
        Ok(())
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum Lane {
    Data,
    Control,
}
