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
That finding remains open pending bounded refusal and near-limit acceptance.

[Executed native evidence](../../docs/implementation/evidence/p01-control-runtime-native-20260918.json)
records the 16-case Linux amd64/PG16.15 run at `6be8515`, exact source/artifact
audits, all seven product process exits, 240 independently inspected relations
and retained isolated resources. The same record separately qualifies the common
foundation CI at `6314639`; that earlier source lacked the administration gate
correction and is not the qualified runtime artifact.
