# PostgreSQL Control backend acceptance

This isolated P01-T06/P02-T02 harness exercises the actual Rust adapter. It is
not a product service, native admission writer or human authorization test.
The complete selected product and runtime policy consumer remain required.

Use clean exact Git source and a fresh inventoried Tux lab, as documented in
[development and delivery](../../docs/engineering/development-and-delivery.md).
The shared helper builds the distinct acceptance executable against the same
source manifest, lockfile, native toolchain and release target as the actual CLI.

```sh
python3 tools/build.py --output /owned/build --jobs 2
python3 experiments/ingest-registry/build_probe.py --build /owned/build \
  --example control_owner_probe
python3 experiments/control-migrations/lab.py setup \
  --root /owned/runtime --build /owned/build --source "$PWD" --run-name controlowner
python3 experiments/control-owner/run.py --root /owned/runtime --extended
python3 experiments/control-owner/sql_schedules.py --root /owned/runtime --source "$PWD"
python3 experiments/control-migrations/lab.py stop --root /owned/runtime
```

These are example paths, not permission to overwrite an existing environment.
The lab has an internal network, no host ports and protected synthetic credentials.
The runner records partial results and preserves failures. The initial matrix
covers an atomic legacy-history upgrade refusal, upgrade data preservation,
missing/incorrect policy/deployment, concurrent Control refusal, same-tuple
connection impersonation, actual backend termination, stale-owner rejection,
same-resource successor epochs, graceful release and actual Ingest registration.
An external psql observer checks every table, the complete mutation set and an
independent exclusion-digest calculation. The probe's complete frozen 215-setting
policy validator is an independent fixture oracle; it does not qualify the later
runtime integration with the actual root-configuration validator.

The supplemental SQL runner tests six attributed-owner refusals and compares
transaction-held exclusion with the session-unlock negative control.
[Additional required cases](cases.md) remain separate gates. A successful run is
not evidence for unexecuted COMMIT fault schedules, paused clients past TTL,
PostgreSQL restart, native handoff, load, arm64 or release journeys.

The extended matrix adds three acquisition COMMIT boundaries (before forwarding,
after server completion and cancellation while held), fresh-incarnation recovery,
two actual container pause/resume cases exceeding the persisted 60-second TTL,
and PostgreSQL SIGKILL/restart with a same-resource successor. These remain
proposed until the exact revision's native report records each result. Five
separately labeled clients are stopped-retained in finally blocks; no host ports
are published. Release-COMMIT and refresh cancellation remain separate cases.
