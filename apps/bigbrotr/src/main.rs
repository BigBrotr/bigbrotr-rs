mod cli;
mod config;
mod event;

use std::{
    io::{self, Write},
    process::ExitCode,
};

fn main() -> ExitCode {
    let matches = cli::command().get_matches();
    match matches.subcommand_name() {
        Some("config") => config::execute(matches.subcommand_matches("config").unwrap(), false),
        Some("doctor") => config::execute(matches.subcommand_matches("doctor").unwrap(), true),
        Some("event") => event::verify_stdin(),
        Some("build-info") => {
            let mut stdout = io::stdout().lock();
            if writeln!(
                stdout,
                "{}",
                include_str!(concat!(env!("OUT_DIR"), "/build-info.json"))
            )
            .and_then(|()| stdout.flush())
            .is_err()
            {
                let _ = writeln!(
                    io::stderr().lock(),
                    "error: could not write requested build information"
                );
                return ExitCode::FAILURE;
            }
            ExitCode::SUCCESS
        }
        _ => unreachable!("clap requires a known subcommand"),
    }
}
