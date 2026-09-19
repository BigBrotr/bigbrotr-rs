# Actual Control process acceptance

P01-T04/T06 executable integration, using the existing exact-Git native builder
and isolated PostgreSQL laboratory. This is acceptance orchestration, not product
installation. Inventory the host before creating a fresh run; retained labs must
not be overwritten. The source checkout must be clean and match the build receipt.

```sh
python3 tools/build.py --output /owned/build --jobs 2
python3 experiments/control-migrations/lab.py setup --root /owned/runtime \
  --build /owned/build --source "$PWD" --run-name controlruntime
python3 experiments/control-runtime/run.py --root /owned/runtime
python3 experiments/control-migrations/lab.py stop --root /owned/runtime
```

Required scenarios use the real `serve --roles control` executable: no activated
policy; mismatched dataset, instance and authority mode; correctly hashed but
semantically invalid configuration/security documents; enabled but unimplemented
administration refusal; concurrent owner refusal;
policy polling without scheduling-row writes; actual heartbeat renewal; SIGINT
and SIGTERM release with exact owner/role history; managed file drift; pause
past policy TTL with refusal to renew on resume; interrupted policy query;
PostgreSQL loss and explicit process restart after engine recovery.

Inspect all Control relations and backend cleanup independently, including
literal epoch progression and separately computed exclusion digests. Observe
actual container exits and OOM state. Keep partial failures, source/artifact
identities and owned resource records. A proposal here is not a PASS.

Full native Ingest writer handoff, principals/action grants, public stream
revocation, automatic service recovery, maximum document load, slow lifecycle
output, arm64 and complete product/release journeys remain separate gates.

`monitor_regressions.py --root /owned/runtime` uses a fresh lab to hold actual
PostgreSQL locks across slow successful acquisition/poll responses, then observes
continued healthy supervision. It fills a real stdout pipe, observes a database
heartbeat without draining it, sends SIGTERM and checks release, backend closure,
process exit and pipe EOF. A separately full stderr pipe exercises error exit.
It also investigates MON-005 using 200,000 configured endpoint entries, matching
the actual CLI policy digest to independent bytes and measuring their PostgreSQL
JSONB rendering. Its initial counterexample result is `CONFIRMED_UNFIXED`, not
configuration acceptance. [Native execution at 2c0dfc1](../../docs/implementation/evidence/p01-monitor-corrections-native-20260918.json)
passes the four process regressions and the earlier sixteen scenarios. The
200,000-entry candidate confirms MON-005: 10,407,457 compact bytes become
10,607,924 JSONB text bytes; initialization fails without changing the database.
That historical finding was subsequently corrected and qualified by the
[JSONB limit cases](../../docs/implementation/evidence/p01-jsonb-limit-native-20260918.json).
The original monitor script retains its counterexample semantics for the named
historical source; it is not the current correction acceptance runner.

[Executed native evidence](../../docs/implementation/evidence/p01-control-runtime-native-20260918.json)
records the 16-case Linux amd64/PG16.15 run at `6be8515`, exact source/artifact
audits, all seven product process exits, 240 independently inspected relations
and retained isolated resources. The same record separately qualifies the common
foundation CI at `6314639`; that earlier source lacked the administration gate
correction and is not the qualified runtime artifact.


`archive_inventory.py --root /owned/archive-inventory-run` uses the exact-source
`native_archive_inventory.py` build and a fresh inventoried PG laboratory. It
runs the actual CLI migration/initialization/activation, seeds 130 immutable
attempts through the real fenced adapter, and checks three-page startup/restart,
empty history, an old executable negative control, historical catalog mismatch,
a blocked startup scan cancelled by SIGTERM with driver closure, and explicit
migration0009 refusal. The adapter probe additionally exercises a fixed 132-row
cutoff with two later issuances, three page boundaries, numeric continuity,
owner-bound cursors, historical corruption and blocked-page cancellation.
The harness retains exact output and identities. This is an inventory consumer,
not Archive execution, receipt resolution, source-policy or M1 qualification.

The scan extension is selected with `archive-attempt-probe --scan`; its default
two-row fixture remains the input for existing catalog activation/recovery
consumers. The initial native inventory artifact used the extension by default;
the later fixture-only switch preserves those existing consumers without changing
product code. Record artifact and harness revisions separately for a harness-only
rerun (`--artifact-revision`), and use a fresh `--run-name` within an active lab.
Protected CLI secret files must be owned by the runner with mode 0400/0600.

The current archive_inventory matrix additionally exercises explicit coordinator
state registration with the actual CLI, missing/partial bindings, identical retry,
live-owner exclusion and a substituted state root. Its native fixture creates only
a local initialization reservation; it does not initialize a scientific catalog.
Existing populated histories remain observation-only. The matrix retains actual
PostgreSQL cancellation/residual-fence cases; no receipt resolution is claimed.
