pub fn command() -> clap::Command {
    clap::Command::new("BigBrotr")
        .bin_name("bigbrotr")
        .version(concat!(env!("CARGO_PKG_VERSION"), " (", env!("BIGBROTR_BUILD_TARGET"), ")"))
        .about("BigBrotr: verifiable Nostr archive and observatory")
        .after_help("Implementation in progress. Use build-info to inspect this artifact's available capabilities.")
        .arg_required_else_help(true)
        .subcommand_required(true)
        .disable_help_subcommand(true)
        .subcommand(clap::Command::new("build-info").about("Print embedded build identity as JSON"))
}
