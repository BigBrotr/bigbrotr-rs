//! Finite-only final-job schedules. The harness owns the actual row lock; this
//! helper observes the existing lease and never manufactures a database wait.
use super::*;

const POLL: Duration = Duration::from_millis(5);
const GATE_TIMEOUT: Duration = Duration::from_secs(10);
const PREAGE_TIMEOUT: Duration = Duration::from_secs(22);

pub(super) struct Wait {
    mode: &'static str,
    prefix: PathBuf,
    go: PathBuf,
    cancel: PathBuf,
}

impl Wait {
    pub(super) fn new(mode: &str, prefix: &Path) -> Self {
        let mode = match mode {
            "cancel" => "cancel",
            "expiry" => "expiry",
            _ => panic!("expected explicit cancel or expiry wait mode"),
        };
        let text = prefix.to_str().expect("UTF-8 gate prefix");
        assert!(prefix.is_absolute() && text.len() <= 512 && prefix.file_name().is_some());
        assert!(prefix.components().all(|part| matches!(
            part,
            std::path::Component::RootDir | std::path::Component::Normal(_)
        )));
        assert!(prefix.parent().unwrap().is_dir());
        let path = |suffix: &str| {
            let mut name = prefix.as_os_str().to_owned();
            name.push(suffix);
            PathBuf::from(name)
        };
        let result = Self {
            mode,
            prefix: prefix.into(),
            go: path(".go"),
            cancel: path(".cancel"),
        };
        // Fail before native inspection or Control acquisition on stale files.
        assert!(!present(&result.go).expect("valid initially absent go file"));
        assert!(!present(&result.cancel).expect("valid initially absent cancel file"));
        result
    }

    pub(super) async fn gate(&self, probe: &Probe, owner: &ControlSession) -> Result<i64, Failure> {
        let refreshed = clock(probe, owner).await;
        self.emit_clock("refreshed", &refreshed);
        let (_, deadline) = refreshed?;
        self.emit_gate("ready");
        let released = tokio::time::timeout(GATE_TIMEOUT, async {
            loop {
                if present(&self.cancel)? {
                    return Err(std::io::Error::other("cancel preceded final dispatch"));
                }
                if present(&self.go)? {
                    return Ok(());
                }
                tokio::time::sleep(POLL).await;
            }
        })
        .await
        .map_err(|_| Failure::at("retirement-wait-gate", None, None, "GateTimeout"))?;
        released.map_err(|error| Failure::at("retirement-wait-gate", None, None, error))?;
        self.emit_gate("released");
        Ok(deadline)
    }

    pub(super) async fn dispatch(
        &self,
        probe: &Probe,
        owner: &ControlSession,
        deadline: i64,
    ) -> Result<(), Failure> {
        // This is outside any product transaction. There is no refresh between
        // the retained final proof and its consumption by the actual call.
        let observed = tokio::time::timeout(PREAGE_TIMEOUT, async {
            loop {
                let time = clock(probe, owner).await?;
                if time.1 != deadline {
                    return Err(Failure::at(
                        "retirement-wait-clock",
                        None,
                        None,
                        "LeaseChanged",
                    ));
                }
                if self.mode == "cancel" || time.1 - time.0 <= 2_000_000 {
                    return Ok(time);
                }
                // A sleep is not authority evidence. Sample actual PostgreSQL
                // UTC again after at most one second, approaching the same
                // deadline without thousands of observer round trips.
                let sleep_us = u64::try_from((time.1 - time.0 - 2_000_000).min(1_000_000))
                    .expect("positive remaining preage interval");
                tokio::time::sleep(Duration::from_micros(sleep_us)).await;
            }
        })
        .await
        .unwrap_or_else(|_| {
            Err(Failure::at(
                "retirement-wait-clock",
                None,
                None,
                "PreageTimeout",
            ))
        });
        self.emit_clock("dispatch", &observed);
        let (now, expires) = observed?;
        if (self.mode == "expiry" && !(1_000_000..=2_000_000).contains(&(expires - now)))
            || (self.mode == "cancel" && now >= expires)
        {
            return Err(Failure::at(
                "retirement-wait-clock",
                None,
                None,
                "MissedLiveDispatchWindow",
            ));
        }
        if present(&self.cancel)
            .map_err(|error| Failure::at("retirement-wait-gate", None, None, error))?
        {
            return Err(Failure::at(
                "retirement-wait-gate",
                None,
                None,
                "CancelBeforeDispatch",
            ));
        }
        Ok(())
    }

    pub(super) async fn pending<T>(&self, probe: &Probe, future: impl Future<Output = T>) -> T {
        if self.mode == "expiry" {
            return future.await;
        }
        tokio::pin!(future);
        let mut applied = false;
        loop {
            tokio::select! {
                // Poll the actual operation first. Applying cancellation never
                // replaces or drops it: the same pinned future must return.
                biased;
                result = &mut future => return result,
                () = tokio::time::sleep(POLL), if !applied => {
                    let signal = present(&self.cancel).expect("invalid cancel file; custody unknown");
                    if signal {
                        probe.cancelled.store(true, Ordering::Relaxed);
                        applied = true;
                        emit(serde_json::json!({"event":"lineage-retirement-wait-cancel",
                            "mode":self.mode,"phase":"applied",
                            "cancelled":probe.cancelled.load(Ordering::Relaxed)}));
                    }
                }
            }
        }
    }

    pub(super) async fn returned(
        &self,
        probe: &Probe,
        owner: &ControlSession,
        deadline: i64,
    ) -> Result<(), Failure> {
        let observed = clock(probe, owner).await;
        self.emit_clock("returned", &observed);
        if observed?.1 != deadline {
            return Err(Failure::at(
                "retirement-wait-clock",
                None,
                None,
                "LeaseChanged",
            ));
        }
        Ok(())
    }

    fn emit_gate(&self, phase: &str) {
        emit(
            serde_json::json!({"event":"lineage-retirement-wait-gate","mode":self.mode,
            "phase":phase,"prefix":self.prefix,"gate_timeout_ms":GATE_TIMEOUT.as_millis(),
            "poll_ms":POLL.as_millis()}),
        );
    }

    fn emit_clock(&self, stage: &str, result: &Result<(i64, i64), Failure>) {
        emit(
            serde_json::json!({"event":"lineage-retirement-wait-clock","mode":self.mode,
            "stage":stage,"server_us":result.as_ref().ok().map(|v|v.0),
            "control_deadline_us":result.as_ref().ok().map(|v|v.1),
            "remaining_us":result.as_ref().ok().map(|v|v.1-v.0),
            "error":result.as_ref().err().map(|e|&e.error)}),
        );
    }
}

// The finite protocol reads only metadata for an exact zero-byte signal. It
// never opens a FIFO/device, follows a symlink, consumes content, or deletes it.
fn present(path: &Path) -> std::io::Result<bool> {
    match std::fs::symlink_metadata(path) {
        Ok(metadata) if metadata.file_type().is_file() && metadata.len() == 0 => Ok(true),
        Ok(_) => Err(std::io::Error::other(
            "signal must be an empty regular nonsymlink file",
        )),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(false),
        Err(error) => Err(error),
    }
}

async fn clock(probe: &Probe, owner: &ControlSession) -> Result<(i64, i64), Failure> {
    let id = owner.identity();
    let row = probe
        .bounded(
            "retirement-wait-clock",
            probe.observer.query_one(
                "SELECT (EXTRACT(EPOCH FROM pg_catalog.clock_timestamp())*1000000)::bigint,
                o.lease_expires_at_us
         FROM control.owner_epochs o JOIN pg_catalog.pg_stat_activity a
           ON a.pid=o.control_backend_pid AND a.backend_start=o.control_backend_start
         WHERE o.resource_id=$1::bytea AND o.epoch=$2::text::numeric
           AND o.process_incarnation=$3::bytea AND o.role='control' AND o.state='active'
           AND a.datname=current_database()",
                &[
                    &&id.resource_id.as_bytes()[..],
                    &id.epoch.get().to_string(),
                    &&id.process_incarnation.as_bytes()[..],
                ],
            ),
        )
        .await
        .map_err(|error| Failure::at("retirement-wait-clock", None, None, error))?;
    Ok((row.get(0), row.get(1)))
}
