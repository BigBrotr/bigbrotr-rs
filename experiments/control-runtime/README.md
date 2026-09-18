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
semantically invalid configuration/security documents; concurrent owner refusal;
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
