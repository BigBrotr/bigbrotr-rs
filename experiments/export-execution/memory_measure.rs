//! Standalone research allocator observation. Compile directly with pinned rustc.
//! Cargo/product forbid(unsafe_code) is unchanged; unsafe delegation is here only.
#![deny(unsafe_op_in_unsafe_fn)]
#[cfg(not(target_pointer_width = "64"))]
compile_error!("this finite measurement is qualified only for 64-bit targets");

#[allow(dead_code)]
#[forbid(unsafe_code)]
mod memory;

use std::{
    alloc::{GlobalAlloc, Layout, System},
    sync::atomic::{AtomicBool, AtomicU64, Ordering::SeqCst},
};

const SCOPE: &str = "whole-probe-rust-requested-live/1";
struct Counts {
    live: AtomicU64,
    peak: AtomicU64,
    invalid: AtomicBool,
}
impl Counts {
    const fn new() -> Self {
        Self {
            live: AtomicU64::new(0),
            peak: AtomicU64::new(0),
            invalid: AtomicBool::new(false),
        }
    }
    fn add(&self, bytes: usize) {
        match self
            .live
            .fetch_update(SeqCst, SeqCst, |old| old.checked_add(bytes as u64))
        {
            Ok(old) => {
                self.peak.fetch_max(old + bytes as u64, SeqCst);
            }
            Err(_) => {
                self.invalid.store(true, SeqCst);
            }
        }
    }
    fn remove(&self, bytes: usize) {
        if self
            .live
            .fetch_update(SeqCst, SeqCst, |old| old.checked_sub(bytes as u64))
            .is_err()
        {
            self.invalid.store(true, SeqCst);
        }
    }
    fn resized(&self, old: usize, new: usize) {
        if new >= old {
            self.add(new - old);
        } else {
            self.remove(old - new);
        }
    }
    fn snapshot(&self) -> memory::AllocationSnapshot {
        let live = self.live.load(SeqCst);
        // Concurrent snapshots are observations, not an atomic multi-field cut.
        // Never report peak<live while another allocator's peak update is pending.
        let peak = self.peak.fetch_max(live, SeqCst).max(live);
        memory::AllocationSnapshot {
            scope: SCOPE,
            live_bytes: live,
            peak_bytes: peak,
            accounting_valid: !self.invalid.load(SeqCst),
        }
    }
}
struct Meter<A> {
    allocator: A,
    counts: Counts,
}
impl<A> Meter<A> {
    const fn new(allocator: A) -> Self {
        Self {
            allocator,
            counts: Counts::new(),
        }
    }
}

// GlobalAlloc's caller owns each live block and supplies its matching layout.
// Delegation preserves that contract. Counting runs for the complete process;
// no allocation can cross a disabled/reset counter window.
unsafe impl<A: GlobalAlloc> GlobalAlloc for Meter<A> {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        let value = unsafe { self.allocator.alloc(layout) };
        if !value.is_null() {
            self.counts.add(layout.size());
        }
        value
    }
    unsafe fn alloc_zeroed(&self, layout: Layout) -> *mut u8 {
        let value = unsafe { self.allocator.alloc_zeroed(layout) };
        if !value.is_null() {
            self.counts.add(layout.size());
        }
        value
    }
    unsafe fn dealloc(&self, value: *mut u8, layout: Layout) {
        unsafe { self.allocator.dealloc(value, layout) };
        // Keep the old charge until actual deallocation returns.
        self.counts.remove(layout.size());
    }
    unsafe fn realloc(&self, value: *mut u8, layout: Layout, new_size: usize) -> *mut u8 {
        let result = unsafe { self.allocator.realloc(value, layout, new_size) };
        // Failure preserves the original block. System-internal transient
        // old+new overlap and usable-size overhead are explicitly unmeasured.
        if !result.is_null() {
            self.counts.resized(layout.size(), new_size);
        }
        result
    }
}
#[global_allocator]
static ALLOCATOR: Meter<System> = Meter::new(System);
fn snapshot() -> memory::AllocationSnapshot {
    ALLOCATOR.counts.snapshot()
}

fn main() -> std::process::ExitCode {
    let args: Vec<_> = std::env::args().collect();
    if args.len() != 3 || args[1] != "measure" {
        eprintln!("usage: export-memory-measure measure /lab/case.json");
        return std::process::ExitCode::from(2);
    }
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("measurement runtime");
    let outcome = runtime.block_on(memory::run_reduced(
        std::path::Path::new(&args[2]),
        snapshot,
    ));
    // Natural destruction includes outstanding blocking threads. If it cannot
    // finish, the finite outer owner retains UNKNOWN; no terminal cut is emitted.
    drop(runtime);
    let observed = snapshot();
    let threshold = outcome.as_ref().ok().copied();
    let status = if outcome.is_err() {
        "UNKNOWN_OR_FAILED"
    } else if !observed.accounting_valid {
        "INVALID_ALLOCATION_ACCOUNTING"
    } else if threshold.is_some_and(|limit| observed.peak_bytes <= limit) {
        "OBSERVED_WITHIN_MANAGER_REQUEST_BUDGET"
    } else {
        "INCONCLUSIVE_WHOLE_PARENT_EXCEEDS_MANAGER"
    };
    // Measurement ends at the snapshot above, after runtime destruction.
    // Formatting/emitting this terminal record is outside the reported cut.
    println!(
        "{}",
        serde_json::json!({
            "event":"export-allocation-terminal","profile":"reduced-plaintext-manager/1",
            "cut":"after-runtime-drop-before-terminal-format","status":status,
            "catalog_manager_bytes":threshold,"rust_allocations":observed,
            "operation_error":outcome.as_ref().err(),
            "exclusions":["allocator-overhead-and-usable-size","transient-realloc-overlap",
                "native-and-TLS-allocators","child-processes","kernel-and-RSS",
                "terminal-record-formatting-after-cut"]
        })
    );
    if outcome.is_err() || !observed.accounting_valid {
        std::process::ExitCode::from(4)
    } else {
        // Inconclusive allocation attribution does not manufacture product failure.
        std::process::ExitCode::SUCCESS
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    fn layout(bytes: usize) -> Layout {
        Layout::from_size_align(bytes, 8).unwrap()
    }
    fn exact(meter: &Meter<impl GlobalAlloc>, live: u64, peak: u64) {
        let value = meter.counts.snapshot();
        assert!(value.accounting_valid);
        assert_eq!((value.live_bytes, value.peak_bytes), (live, peak));
    }
    #[test]
    fn preexisting_and_zeroed_allocations_never_cross_a_window() {
        let meter = Meter::new(System);
        let first = unsafe { meter.alloc(layout(23)) };
        assert!(!first.is_null());
        // The first observation begins after allocation; it still owns 23 bytes.
        exact(&meter, 23, 23);
        let second = unsafe { meter.alloc_zeroed(layout(41)) };
        assert!(!second.is_null());
        assert!(
            unsafe { std::slice::from_raw_parts(second, 41) }
                .iter()
                .all(|b| *b == 0)
        );
        exact(&meter, 64, 64);
        unsafe {
            meter.dealloc(first, layout(23));
            meter.dealloc(second, layout(41));
        }
        exact(&meter, 0, 64);
    }
    #[test]
    fn successful_realloc_growth_and_shrink_preserve_original_content() {
        let meter = Meter::new(System);
        let original = unsafe { meter.alloc(layout(16)) };
        assert!(!original.is_null());
        unsafe {
            original.write(91);
        }
        let grown = unsafe { meter.realloc(original, layout(16), 4096) };
        assert!(!grown.is_null());
        assert_eq!(unsafe { grown.read() }, 91);
        exact(&meter, 4096, 4096);
        let shrunk = unsafe { meter.realloc(grown, layout(4096), 8) };
        assert!(!shrunk.is_null());
        assert_eq!(unsafe { shrunk.read() }, 91);
        exact(&meter, 8, 4096);
        unsafe {
            meter.dealloc(shrunk, layout(8));
        }
        exact(&meter, 0, 4096);
    }
    // Deterministic delegate failure; do not depend on host OOM/overcommit.
    struct Fail {
        allocations: AtomicBool,
        reallocations: AtomicBool,
    }
    unsafe impl GlobalAlloc for Fail {
        unsafe fn alloc(&self, l: Layout) -> *mut u8 {
            if self.allocations.load(SeqCst) {
                std::ptr::null_mut()
            } else {
                unsafe { System.alloc(l) }
            }
        }
        unsafe fn alloc_zeroed(&self, l: Layout) -> *mut u8 {
            if self.allocations.load(SeqCst) {
                std::ptr::null_mut()
            } else {
                unsafe { System.alloc_zeroed(l) }
            }
        }
        unsafe fn dealloc(&self, p: *mut u8, l: Layout) {
            unsafe { System.dealloc(p, l) }
        }
        unsafe fn realloc(&self, p: *mut u8, l: Layout, n: usize) -> *mut u8 {
            if self.reallocations.load(SeqCst) {
                std::ptr::null_mut()
            } else {
                unsafe { System.realloc(p, l, n) }
            }
        }
    }
    #[test]
    fn allocation_and_reallocation_failure_leave_the_original_charge() {
        let meter = Meter::new(Fail {
            allocations: AtomicBool::new(true),
            reallocations: AtomicBool::new(true),
        });
        assert!(unsafe { meter.alloc(layout(16)) }.is_null());
        assert!(unsafe { meter.alloc_zeroed(layout(16)) }.is_null());
        exact(&meter, 0, 0);
        meter.allocator.allocations.store(false, SeqCst);
        let original = unsafe { meter.alloc(layout(16)) };
        assert!(!original.is_null());
        unsafe {
            original.write(37);
        }
        assert!(unsafe { meter.realloc(original, layout(16), 64) }.is_null());
        assert_eq!(unsafe { original.read() }, 37);
        exact(&meter, 16, 16);
        unsafe {
            meter.dealloc(original, layout(16));
        }
        exact(&meter, 0, 16);
    }
    #[test]
    fn allocation_can_outlive_its_origin_thread_and_first_observation() {
        let meter = Meter::new(System);
        std::thread::scope(|scope| {
            let address = scope
                .spawn(|| {
                    let value = unsafe { meter.alloc(layout(73)) };
                    assert!(!value.is_null());
                    value as usize
                })
                .join()
                .unwrap();
            exact(&meter, 73, 73);
            let meter = &meter;
            scope
                .spawn(move || unsafe { meter.dealloc(address as *mut u8, layout(73)) })
                .join()
                .unwrap();
        });
        exact(&meter, 0, 73);
    }
    #[test]
    fn concurrent_live_blocks_have_a_known_independent_sum() {
        let meter = Meter::new(System);
        let ready = std::sync::Barrier::new(5);
        let release = std::sync::Barrier::new(5);
        std::thread::scope(|scope| {
            for size in [64, 128, 256, 512] {
                let (meter, ready, release) = (&meter, &ready, &release);
                scope.spawn(move || {
                    let value = unsafe { meter.alloc(layout(size)) };
                    assert!(!value.is_null());
                    ready.wait();
                    release.wait();
                    unsafe {
                        meter.dealloc(value, layout(size));
                    }
                });
            }
            ready.wait();
            exact(&meter, 960, 960);
            release.wait();
        });
        exact(&meter, 0, 960);
    }
    #[test]
    fn snapshot_publishes_live_before_delayed_allocator_peak_update() {
        let counts = Counts::new();
        // Model the exact intermediate state between a successful live CAS and
        // its allocator peak update; no real invalid allocation is performed.
        counts.live.store(73, SeqCst);
        let first = counts.snapshot();
        assert_eq!((first.live_bytes, first.peak_bytes), (73, 73));
        counts.remove(73);
        assert_eq!(counts.snapshot().peak_bytes, 73);
        counts.peak.fetch_max(73, SeqCst); // Delayed allocator publication.
        assert_eq!(counts.snapshot().peak_bytes, 73);
    }
    #[test]
    fn arithmetic_invalidity_is_permanent_without_real_invalid_allocations() {
        let underflow = Counts::new();
        underflow.remove(1);
        underflow.add(1);
        assert!(!underflow.snapshot().accounting_valid);
        let overflow = Counts::new();
        overflow.add(usize::MAX);
        overflow.add(1);
        overflow.remove(usize::MAX);
        assert!(!overflow.snapshot().accounting_valid);
    }
}
