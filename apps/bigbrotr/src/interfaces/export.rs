//! Finite operator-owned destination binding; private jobs consume it separately.
use bigbrotr::export::destination::LocalDestination;
use clap::{Arg, ArgMatches, Command};
use std::{
    path::{Path, PathBuf},
    process::ExitCode,
    sync::Arc,
};

fn check_placement(config: &crate::config::Resolved, root: &Path) -> Result<(), (u8, String)> {
    for key in config.selected_storage_roots() {
        let other = Path::new(config.text(key).expect("selected root"));
        match other.try_exists() {
            Ok(false) => (),
            Ok(true) => bigbrotr::storage::archive::LocalCoordinator::require_disjoint(root, other)
                .map_err(|_| {
                    (
                        4,
                        "export destination storage placement cannot be verified".into(),
                    )
                })?,
            Err(_) => return Err((4, "selected storage root cannot be inspected".into())),
        }
    }
    Ok(())
}

/// Deployment-selected custody is acquired before Control startup and retained
/// outside its operation futures through actual backend-driver destruction.
pub(super) fn prepare_destination(
    config: &crate::config::Resolved,
) -> Result<Option<Arc<LocalDestination>>, (u8, String)> {
    let Some(root) = config.text("export.destination.root") else {
        return Ok(None);
    };
    let root = Path::new(root);
    check_placement(config, root)?;
    let id = config
        .text("export.destination.id")
        .expect("validated paired destination")
        .parse()
        .map_err(|_| (3, "invalid export destination identity".into()))?;
    let dataset = config
        .text("topology.dataset_id")
        .expect("validated dataset")
        .parse()
        .map_err(|_| (3, "invalid dataset identity".into()))?;
    LocalDestination::open(root, dataset, id)
        .map(|value| Some(Arc::new(value)))
        .map_err(|_| {
            (
                4,
                "export destination unavailable; explicit initialization required".into(),
            )
        })
}

pub fn command() -> Command {
    let mut command = Command::new("export")
        .about("Prepare and inspect an explicitly approved private export destination")
        .subcommand_required(true)
        .disable_help_subcommand(true);
    for (name, about) in [
        (
            "destination-initialize",
            "Bind an existing empty private directory to this dataset and destination ID",
        ),
        (
            "destination-inspect",
            "Inspect an existing destination without creating or repairing state",
        ),
    ] {
        command = command.subcommand(Command::new(name).about(about)
            .arg(Arg::new("file").long("file").required(true).value_parser(clap::value_parser!(PathBuf)))
            .after_help("Requires paired export.destination.id/root in the deployment file. Root must already exist, be owned by this process user, mode0700 and disjoint from selected writable stores; symlinks are refused. Initialization syncs a private identity marker and directory; identical complete retries rewrite the verified same bytes through a fresh synced inode, partial/conflicting state is retained and refused. This finite command contacts no database and grants no job, scope or publication authority. No export execution is implemented by this command. Exit:0 bound,1 output I/O,2 usage,3 invalid configuration,4 unavailable/conflicting storage."));
    }
    command
}
pub fn execute(args: &ArgMatches) -> ExitCode {
    let Ok(mut output) = super::lifecycle_output::LifecycleOutput::start() else {
        return ExitCode::FAILURE;
    };
    let result = (|| {
        let (operation, args) = args.subcommand().expect("required operation");
        let config = crate::config::load(args.get_one::<PathBuf>("file").unwrap())?;
        let id = config
            .text("export.destination.id")
            .ok_or_else(|| (3, "missing export destination identity".into()))?
            .parse()
            .map_err(|_| (3, "invalid export destination identity".into()))?;
        let root = Path::new(
            config
                .text("export.destination.root")
                .ok_or_else(|| (3, "missing export destination root".into()))?,
        );
        check_placement(&config, root)?;
        let dataset = config
            .text("topology.dataset_id")
            .expect("validated dataset")
            .parse()
            .map_err(|_| (3, "invalid dataset identity".into()))?;
        let destination = if operation == "destination-initialize" {
            LocalDestination::initialize(root, dataset, id)
        } else {
            LocalDestination::open(root, dataset, id)
        }
        .map_err(|e| (4, e.to_string()))?;
        Ok::<_, (u8, String)>(
            serde_json::json!({"format":"bigbrotr-export-destination-observation/1", "state":"bound",
            "dataset_id":destination.dataset(), "destination_id":destination.id(), "binding_sha256":destination.binding_digest(),
            "job_authority":"not-granted"}),
        )
    })();
    let code = match result {
        Ok(value) => {
            output.emit(value, false);
            0
        }
        Err((code, error)) => {
            output.emit(format_args!("error: {error}"), true);
            code
        }
    };
    let delivered = output.finish();
    std::process::exit(i32::from(if delivered { code } else { 1 }));
}
