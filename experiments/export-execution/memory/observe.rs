//! Caller markers and sampled logical grants; neither is an RSS measurement.
use super::*;
use std::{io::BufWriter, sync::atomic::AtomicU64, thread};

/// Safe observation seam; the standalone research wrapper owns the allocator.
/// These are requested Rust bytes, never allocator usable size or process RSS.
#[derive(Clone, Copy, serde::Serialize)]
pub(crate) struct AllocationSnapshot {
    pub scope: &'static str,
    pub live_bytes: u64,
    pub peak_bytes: u64,
    pub accounting_valid: bool,
}
pub(crate) type SnapshotFn = fn() -> AllocationSnapshot;

pub(super) struct Observer {
    origin: Instant,
    phase: Arc<AtomicU64>,
    stop: Arc<AtomicBool>,
    join: Option<thread::JoinHandle<Result<()>>>,
    snapshot: Option<SnapshotFn>,
}
impl Observer {
    pub fn start(path: &Path, budget: &ResourceBudget) -> Result<Self> {
        Self::observed(path, budget, None)
    }
    pub fn observed(
        path: &Path,
        budget: &ResourceBudget,
        snapshot: Option<SnapshotFn>,
    ) -> Result<Self> {
        let file = fs::OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(path)
            .map_err(error)?;
        let origin = Instant::now();
        let phase = Arc::new(AtomicU64::new(0));
        let stop = Arc::new(AtomicBool::new(false));
        let (sample_phase, sample_stop, budget) = (phase.clone(), stop.clone(), budget.clone());
        let join = thread::spawn(move || {
            let mut output = BufWriter::with_capacity(4096, file);
            // Fixed24k sample ceiling, nominal5ms cadence. The outer observer
            // separately bounds wall time; no retained sample vector here.
            for index in 0..=24_000 {
                let budget_snapshot = budget.snapshot();
                let used = budget_snapshot.used();
                let mut row = serde_json::json!({
                    "index":index,"elapsed_us":origin.elapsed().as_micros(),
                    "phase":sample_phase.load(Ordering::Acquire),
                    "data_bytes":used.data_bytes(),"native_bytes":used.native_bytes(),
                    "control_bytes":used.control_bytes(),"items":used.items(),
                    "permits":budget_snapshot.active_permits()
                });
                if let Some(snapshot) = snapshot {
                    row["rust_allocations"] = serde_json::to_value(snapshot()).map_err(error)?;
                }
                serde_json::to_writer(&mut output, &row).map_err(error)?;
                drop(row); // Preserve the default temporary lifetime before write/sleep.
                output.write_all(b"\n").map_err(error)?;
                if sample_stop.load(Ordering::Acquire) {
                    output.flush().map_err(error)?;
                    output.get_ref().sync_all().map_err(error)?;
                    return Ok(());
                }
                thread::sleep(Duration::from_millis(5));
            }
            Err("ledger sampler finite deadline exceeded".into())
        });
        Ok(Self {
            origin,
            phase,
            stop,
            join: Some(join),
            snapshot,
        })
    }
    pub fn reduced(&self) -> bool {
        self.snapshot.is_some()
    }
    pub fn note(&self, name: &str, fields: impl FnOnce() -> serde_json::Value) -> Result<()> {
        if self.reduced() {
            self.mark(name, fields())
        } else {
            Ok(())
        }
    }
    pub fn mark(&self, name: &str, fields: serde_json::Value) -> Result<()> {
        let phase = self.phase.fetch_add(1, Ordering::AcqRel) + 1;
        let mut row = serde_json::json!({"event":"export-measure","phase":phase,"name":name,
            "elapsed_us":self.origin.elapsed().as_micros(),"fields":fields});
        if let Some(snapshot) = self.snapshot {
            row["rust_allocations"] = serde_json::to_value(snapshot()).map_err(error)?;
        }
        emit(row)
    }
    pub fn finish(mut self) -> Result<()> {
        self.stop.store(true, Ordering::Release);
        self.join
            .take()
            .ok_or("missing sampler")?
            .join()
            .map_err(|_| "sampler panic")?
    }
}
impl Drop for Observer {
    fn drop(&mut self) {
        self.stop.store(true, Ordering::Release);
        if let Some(join) = self.join.take() {
            let _ = join.join();
        }
    }
}
pub(super) fn emit(value: serde_json::Value) -> Result<()> {
    let mut stdout = std::io::stdout().lock();
    serde_json::to_writer(&mut stdout, &value).map_err(error)?;
    stdout.write_all(b"\n").map_err(error)?;
    stdout.flush().map_err(error)
}
