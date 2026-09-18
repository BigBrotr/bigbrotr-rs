//! Private finite worker entrypoint. The application owns process supervision.
use bigbrotr_archive_worker::engine::{Engine, EngineLimits};
use std::{io::Write, path::Path, process::ExitCode};

fn run() -> Result<(), Box<dyn std::error::Error>> {
    let arguments: Vec<_> = std::env::args_os().take(8).collect();
    if arguments.len() != 7 || arguments[1] != "inspect-engine" {
        return Err("usage: bigbrotr-archive-worker inspect-engine BUNDLE_DIR EMPTY_TEMP_DIR MEMORY_BYTES TEMP_BYTES THREADS".into());
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
    let result = serde_json::to_vec(&engine.inspection()?)?;
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
