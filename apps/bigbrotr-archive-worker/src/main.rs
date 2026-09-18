//! Private finite worker entrypoint. The application owns process supervision.
use bigbrotr_archive_worker::{
    engine::{Engine, EngineLimits},
    prepare::PreparedBatch,
};
use bigbrotr_model::codec::FactDecodeLimits;
use bigbrotr_protocol::archive_batch::Limits;
use std::{io::Write, path::Path, process::ExitCode, sync::atomic::AtomicBool};

/// The synchronous spawning thread retains the worker until positive reap.
/// Install before opening the engine or touching a job root. No subsequent
/// fork, exec or credential change may clear the Linux parent-death setting.
fn bind_parent() -> Result<(), &'static str> {
    const ERROR: &str = "private archive parent unavailable";
    let expected = std::env::var("BIGBROTR_ARCHIVE_PARENT_PID")
        .ok()
        .and_then(|value| value.parse::<i32>().ok())
        .filter(|value| *value > 0)
        .and_then(rustix::process::Pid::from_raw)
        .ok_or(ERROR)?;
    if rustix::process::getppid() != Some(expected) {
        return Err(ERROR);
    }
    #[cfg(target_os = "linux")]
    rustix::process::set_parent_process_death_signal(Some(rustix::process::Signal::KILL))
        .map_err(|_| ERROR)?;
    // Death before prctl is not retroactively signalled. The expected PID comes
    // from the spawning process, so reparenting cannot silently adopt a new
    // owner. Mac checks initial parentage only; it has no Linux death signal.
    if rustix::process::getppid() != Some(expected) {
        return Err(ERROR);
    }
    Ok(())
}

fn run() -> Result<(), Box<dyn std::error::Error>> {
    bind_parent()?;
    let arguments: Vec<_> = std::env::args_os().take(10).collect();
    let inspect_engine = arguments.len() == 7 && arguments[1] == "inspect-engine";
    let bootstrap_catalog = arguments.len() == 7 && arguments[1] == "bootstrap-catalog";
    let inspect_batch = arguments.len() == 9 && arguments[1] == "inspect-batch";
    if !inspect_engine && !inspect_batch && !bootstrap_catalog {
        return Err("usage: bigbrotr-archive-worker {bootstrap-catalog BUNDLE_DIR EMPTY_TEMP_DIR MEMORY_BYTES TEMP_BYTES THREADS | inspect-engine BUNDLE_DIR EMPTY_TEMP_DIR MEMORY_BYTES TEMP_BYTES THREADS | inspect-batch BUNDLE_DIR EMPTY_TEMP_DIR MEMORY_BYTES TEMP_BYTES THREADS MAX_RECORDS MAX_ENCODED_BYTES}".into());
    }
    let number = |i: usize| -> Result<u64, Box<dyn std::error::Error>> {
        Ok(arguments[i].to_str().ok_or("invalid limit")?.parse()?)
    };
    let limits = EngineLimits {
        memory_bytes: number(4)?,
        temp_bytes: number(5)?,
        threads: u16::try_from(number(6)?)?,
    };
    let engine = Engine::open(Path::new(&arguments[3]), limits)?;
    engine.load_bundle(Path::new(&arguments[2]))?;
    let inspection = if bootstrap_catalog {
        let request =
            bigbrotr_archive_worker::catalog::BootstrapRequest::read(std::io::stdin().lock())?;
        engine.bootstrap_catalog(request)?;
        serde_json::json!({"format":"bigbrotr-native-bootstrap/1","status":"initialized"})
    } else if inspect_batch {
        let limits = Limits {
            max_records: usize::try_from(number(7)?)?,
            max_encoded_bytes: usize::try_from(number(8)?)?,
            fact: FactDecodeLimits::default(),
        };
        let prepared = PreparedBatch::prepare(
            engine,
            std::io::stdin().lock(),
            limits,
            &AtomicBool::new(false),
        )?;
        serde_json::json!({"format":"bigbrotr-private-preparation/1",
            "first":prepared.batch().first(), "last":prepared.batch().last(),
            "batch_digest":prepared.batch().digest(), "facts":prepared.batch().records().len(),
            "staged_counts":prepared.counts()?})
    } else {
        engine.inspection()?
    };
    let result = serde_json::to_vec(&inspection)?;
    let mut output = std::io::stdout().lock();
    output.write_all(&result)?;
    output.write_all(b"\n")?;
    output.flush()?;
    Ok(())
}

fn main() -> ExitCode {
    match run() {
        Ok(()) => ExitCode::SUCCESS,
        Err(error) => {
            // Only our redacted errors and bounded numeric parsing reach here.
            let _ = writeln!(std::io::stderr().lock(), "{error}");
            ExitCode::FAILURE
        }
    }
}
