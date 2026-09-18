//! Private finite worker entrypoint. The application owns process supervision.
use bigbrotr_archive_worker::{
    engine::{Engine, EngineLimits},
    prepare::PreparedBatch,
};
use bigbrotr_model::codec::FactDecodeLimits;
use bigbrotr_protocol::archive_batch::Limits;
use std::{io::Write, path::Path, process::ExitCode, sync::atomic::AtomicBool};

fn run() -> Result<(), Box<dyn std::error::Error>> {
    let arguments: Vec<_> = std::env::args_os().take(10).collect();
    let inspect_engine = arguments.len() == 7 && arguments[1] == "inspect-engine";
    let inspect_batch = arguments.len() == 9 && arguments[1] == "inspect-batch";
    if !inspect_engine && !inspect_batch {
        return Err("usage: bigbrotr-archive-worker {inspect-engine BUNDLE_DIR EMPTY_TEMP_DIR MEMORY_BYTES TEMP_BYTES THREADS | inspect-batch BUNDLE_DIR EMPTY_TEMP_DIR MEMORY_BYTES TEMP_BYTES THREADS MAX_RECORDS MAX_ENCODED_BYTES}".into());
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
    let inspection = if inspect_batch {
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
