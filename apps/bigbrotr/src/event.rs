use bigbrotr_protocol::{EventLimits, verify_json};
use std::{
    io::{self, Read, Write},
    process::ExitCode,
};

pub fn verify_stdin() -> ExitCode {
    let limits = EventLimits::default();
    // Fixed upper capacity avoids reallocating a wire-sized input; one extra byte
    // distinguishes EOF at the limit from an oversized stream without reading it.
    let mut bytes = Vec::with_capacity(limits.wire_bytes + 1);
    if io::stdin()
        .lock()
        .take((limits.wire_bytes + 1) as u64)
        .read_to_end(&mut bytes)
        .is_err()
    {
        let _ = writeln!(
            io::stderr().lock(),
            "error: could not read event from stdin"
        );
        return ExitCode::FAILURE;
    }
    let verified = match verify_json(&bytes, limits) {
        Ok(event) => event,
        Err(error) => {
            let _ = writeln!(io::stderr().lock(), "error: {error}");
            return ExitCode::from(if error.is_limit() { 4 } else { 3 });
        }
    };
    let mut stdout = io::stdout().lock();
    if serde_json::to_writer(&mut stdout, verified.event()).is_err()
        || stdout
            .write_all(b"\n")
            .and_then(|()| stdout.flush())
            .is_err()
    {
        let _ = writeln!(io::stderr().lock(), "error: could not write verified event");
        return ExitCode::FAILURE;
    }
    ExitCode::SUCCESS
}
