# First signed event on Linux

Use the maintained `p01-first-launch-tutorial` case to initialize a fresh installation,
acquire a controlled signed event, publish it to Archive, stop the initial owner,
then read and export that event through ordinary Control. The fixture, relay,
authenticated clients and orchestration are supplied by the repository. The
candidate bundle supplies the application, native worker and installation files.
Keep the checkout as well as the bundle; the bundle alone is not this acceptance
runner.

This is a development candidate procedure. Run the case against the exact
candidate you intend to use; its result applies to those recorded input identities
and declared checks. It adds outside-checkout execution, owned TCP listener
observations, complete controlled-peer traffic and actual unsupported-capability
refusals to the existing first-event journey. It does not repeat the separately
qualified Ingest successor/resend, Index, outage or native/C API census journeys,
and does not qualify general capacity or the complete product. See
[practical verification](https://github.com/BigBrotr/bigbrotr-rs/blob/main/docs/development/verification.md#fresh-installed-first-launch-case)
for the detailed case contract.

The repository is public. The candidate manifest's historical
`visibility: "private"` field describes the local candidate format; it does not
describe repository visibility, authenticate a release or protect generated
credentials. There is no official release download in this procedure.

The recipe uses a dedicated PostgreSQL instance, application data and protected
credentials. It binds administration only to `127.0.0.1:18081`, PostgreSQL only to
`127.0.0.1:15432`, and explicitly permits the controlled relay at
`ws://127.0.0.1:7447/installed-fixture`. These ports must be free. Application
containers use Linux host networking because the current private listener accepts
loopback only. No public endpoint, signing key or automatic relay enrollment is
configured. Adjacent Index, Discovery, Monitor and public services are disabled.

## Prepare and run the maintained journey

Use native Linux amd64, Python 3.11+, Git, a non-root account with access to a
local Docker daemon, and Docker Compose v2. The extracted executable must also
run on that host for the CLI clients. Keep ports 15432, 18081 and 7447 free and
provide more than 65 GiB free completion space. Linux `/proc` must expose the
owned application and client process identities, descriptors and TCP tables;
inaccessible observations cannot pass. Application plus native work is
limited to 6 GiB with no swap, and PostgreSQL to 1.5 GiB; these are configured
grants, not measured workload capacity. History has a 1 GiB execution grant,
export 256 MiB and the Archive worker 512 MiB. The history grant also covers
bounded response and coordination work.

Keep the repository as an explicitly declared input. Create a writable directory
such as `/absolute/first-event` outside every working checkout for the build,
candidate, execution and evidence. Start from a clean committed checkout:

```sh
git clone https://github.com/BigBrotr/bigbrotr-rs.git
cd bigbrotr-rs
```

Select the committed revision you intend to exercise before building. Install the
[build prerequisites](https://github.com/BigBrotr/bigbrotr-rs/blob/main/docs/development/build.md)
and choose fresh absolute build/candidate paths. The builder obtains pinned native
assets over HTTPS, or accepts an explicitly prepared verified bundle through
`--archive-bundle`; see the
[native bundle instructions](https://github.com/BigBrotr/bigbrotr-rs/blob/main/packaging/archive-worker/README.md).
Locked Rust dependencies and toolchains may also need build-time network access.
The tutorial's Nostr-signing exclusion is bound to the reviewed `c656c518` product code,
dependencies and configuration. Documentation/acceptance-only revisions may reuse
that code profile; changed product inputs require a fresh reachable-path review.
The case refuses an unreviewed profile instead of assuming it cannot sign.
Retain the artifact's source commit in the declared checkout for those recorded
source comparisons; the normal full clone provides the published history.

```sh
python3 tools/build.py --output /absolute/first-event/build
python3 packaging/release/assemble.py \
  --artifacts /absolute/first-event/build \
  --source-manifest /absolute/first-event/build/source-manifest.json \
  --output /absolute/first-event/candidate --include-journey
```

The assembler requires a clean committed checkout and an output outside its
checkout and artifact inputs. Existing trusted build bytes and their matching
receipts can be reused when their required identities still agree. Product,
assembly and acceptance sources remain separate. A recipe/tutorial change needs
a fresh assembly and qualification of that candidate, even when the product
binary and worker are unchanged. The unsigned manifest binds the archive and
`members.json` inventory; use trusted build custody rather than treating hashes
as release authentication.

Acquire the exact runtime images before acceptance; the case does not pull them:

```sh
docker pull --platform linux/amd64 postgres@sha256:f1c3376c26f2609ab9f29f71f824103fe2fcd8ee0346485cb6122a4f93df6f94
docker pull --platform linux/amd64 gcr.io/distroless/cc-debian13@sha256:732d2fe38bb1996cb47f8218b48dc840967684d73c5483894ff2ed638068c1b6
BIGBROTR_SOURCE=$(pwd -P)
cd /absolute/first-event
python3 "$BIGBROTR_SOURCE/tests/acceptance/run.py" p01-first-launch-tutorial \
  --candidate-manifest /absolute/first-event/candidate/candidate-manifest.json \
  --source-root "$BIGBROTR_SOURCE" --evidence /absolute/first-event/tutorial-evidence
```

Choose a new evidence path on each attempt; do not precreate or reuse it. The case
verifies and extracts the candidate into its own `bundle` child. A fresh evidence
directory must be outside every working checkout for this tutorial variant, as
must the execution working directory. Use canonical absolute paths without
symlink components. The original `p01-first-launch` case still permits a fresh
Git-ignored `.research` descendant when outside-checkout qualification is not
selected. Intake rejects source
equality/ancestor paths, other checkout paths, input collisions and existing
extraction destinations. This extraction allowance does not relax the assembler's
output restriction.

The case builds only the small runtime wrapper from verified candidate bytes,
using the pinned local base with `--no-cache --network=none --pull=false`; it performs no
Rust/product build. It generates isolated identities and protected credentials,
applies explicit provisioning/migrations/activation/enrollment, and supplies the
controlled signed/forged/duplicate relay traffic. After complete publication and
graceful shutdown it starts ordinary Control, enables explicit scope grants,
checks the exact signed history and standalone JSONL export, and joins/stops its
owned processes and containers. A running container or an accepted/queued job
alone never establishes publication or role readiness.

The observed initial and ordinary application TCP listeners must be exactly
`127.0.0.1:18081` for their actual owned PIDs; the independent peer must listen
only on `127.0.0.1:7447`. PostgreSQL must have its actual loopback 15432 published
binding and accept a TCP connection. These are bounded snapshots of the owned
processes and declared database route, not a host-wide egress audit.

The selected code profile has no reachable Nostr signer: the event command
verifies existing signatures, and acquisition emits only `REQ` and `CLOSE`.
The real peer checks every accepted connection through its protocol close and
TCP EOF, including quiet cancellation, while the signed acquisition and 353-byte
export make the observation nonempty. Any peer error, incomplete shutdown or
unmatched accepted connection fails this claim. The case also tries an assigned,
enabled but unwired Monitor role and active-write enablement; both must produce
their exact exit 3 refusals without startup or Ready output. The claim concerns
Nostr signing and outbound Nostr event publication. Session HMAC, TLS mechanisms
and the required Archive SQL publication have different meanings.

`initialized` with `ready:false` identifies the fresh owning lifetime; an
accepted/queued acquisition job does not report durable traversal completion.
Authenticated role observations describe only selected capabilities. Ordinary
Control opens no Ingest writer, and reports private history/export observations
under their own current authority. An absent role cannot be inferred Ready.
Keep Index and public/signing services disabled in this tutorial; live Index
succession remains full-product work under P02-T06, separate from this first-event
procedure and the already qualified finite continuation.

By default it creates a fresh labeled bridge for PostgreSQL. If you need to borrow
an existing local bridge, explicitly append `--compose-network EXISTING_BRIDGE`;
the case records its identity and endpoints, leaves unrelated containers alone
and never removes the borrowed bridge.

Inspect `result.json`: exit 0 means PASS for the declared case, 1 means FAIL and 2
means NOT_RUN. Missing prerequisites and unreached stages cannot become PASS.
The `capability-negatives/verdict.json`, `initial/tutorial-listeners.json`,
`initial/tutorial-peer-verdict.json`, `ordinary/tutorial-listeners.json` and
`ordinary/tutorial-verdict.json` records describe the added checks.
Cleanup retains stopped containers, image, network, data and logs for inspection;
review `resources.json` before later manual retirement. The evidence root is mode
0700 and contains owner tokens, password/session files and full SQL captures.
Keep it protected and never upload it wholesale, including to public Actions.
Review any selected summary before publication. Detailed failure, cleanup and
scope rules belong to the shared verification guide linked above.

## Initialize the recipe manually

For an operator inspecting the explicit bootstrap commands, use a verified
extracted bundle, such as the case's `bundle` directory after its candidate check
passes. Prepare a separate new state directory and Compose project. These commands
initialize an instance; the maintained case above owns the complete controlled
fixture journey and always creates its own fresh state. Do not run the two
procedures concurrently on the fixed ports or point them at the same instance.

Set absolute paths to the extracted bundle and a **new sibling** state directory,
and choose unique local project/image names. Keep the state path stable after
initialization. Retain the runtime-image and resource prerequisites above.

```sh
export BIGBROTR_BUNDLE=/absolute/first-event/bundle
export BIGBROTR_STATE=/absolute/first-event/instance
export BIGBROTR_PROJECT=bigbrotr-first-event
export BIGBROTR_IMAGE=bigbrotr-local:first-event
export BIGBROTR_OWNER="$(python3 -c 'import secrets; print(secrets.token_hex(16))')"
export BIGBROTR_UID="$(id -u)"
export BIGBROTR_GID="$(id -g)"
umask 077
python3 - <<'PY'
import json, os, secrets, shutil, stat
from pathlib import Path
bundle, state = (Path(os.environ[n]) for n in ('BIGBROTR_BUNDLE', 'BIGBROTR_STATE'))
assert os.geteuid() != 0 and bundle.is_absolute() and state.is_absolute()
assert '..' not in state.parts and state != bundle and bundle not in state.parents
for p in (bundle, *bundle.parents, *state.parents):
    assert stat.S_ISDIR(p.lstat().st_mode), 'linked or missing parent'
assert shutil.disk_usage(state.parent).free > 65 * 1024**3
state.mkdir(mode=0o700)
for name in ('config', 'data', 'postgres', 'secrets', 'secrets/runtime', 'secrets/operator',
             'data/archive', 'data/archive-state', 'data/index',
             'data/scratch', 'data/export', 'observations'):
    (state / name).mkdir(mode=0o700)
for name in ('runtime/control', 'runtime/catalog', 'runtime/archive-read',
             'operator/bootstrap', 'operator/migrator', 'operator/owner-password'):
    with (state / 'secrets' / name).open('x') as f:
        f.write(secrets.token_hex(32))
text = (bundle / 'config/recipes/acceptance.toml').read_text()
ids = {name: secrets.token_hex(16) for name in ('instance', 'dataset', 'catalog', 'activation', 'owner')}
for name, digit in (('instance', '1'), ('dataset', '2'), ('catalog', '3')):
    assert text.count(digit * 32) == 1
    text = text.replace(digit * 32, ids[name])
(state / 'config/instance.toml').write_text(text)
(state / 'operation-ids.json').write_text(json.dumps(ids, indent=2) + '\n')
PY
docker build --no-cache --network=none --pull=false -f "$BIGBROTR_BUNDLE/packaging/image/Dockerfile" \
  -t "$BIGBROTR_IMAGE" "$BIGBROTR_BUNDLE"
dc() { docker compose --env-file /dev/null -f "$BIGBROTR_BUNDLE/packaging/compose/compose.yaml" "$@"; }
dc --profile operator config --quiet
dc run --rm --no-deps app build-info
dc run --rm --no-deps app archive verify-worker --root /opt/bigbrotr/archive-worker
dc run --rm --no-deps app config validate --file /etc/bigbrotr/instance.toml
```

The image consumes existing binaries; it does not compile the product. Its pinned
[Distroless C++ runtime](https://github.com/GoogleContainerTools/distroless) has no
shell or package manager. Administrative SQL uses the separate PostgreSQL image.
Only explicit initialization commands receive `/run/operator`; the ordinary
application does not receive the bootstrap, migrator or owner-password files.

Start only the fresh database, create the separate roles/databases, then apply the
compiled Control migrations with the migrator. The operator grants the explicitly
listed runtime rights after migration; the application owns no schema and cannot
write the migration ledger. The catalog manager has its separately required
`CREATEROLE` privilege; the archive reader has no write rights.

```sh
dc up -d --wait database
dc exec -T --user 0 database psql -X -U postgres -d postgres \
  -v ON_ERROR_STOP=1 -v stage=bootstrap -f /opt/bigbrotr/provision.sql
dc run --rm --no-deps initialize migrate plan --file /etc/bigbrotr/instance.toml \
  --user bigbrotr_migrator --password-ref file:/run/operator/migrator --target 41
dc run --rm --no-deps initialize migrate apply --file /etc/bigbrotr/instance.toml \
  --user bigbrotr_migrator --password-ref file:/run/operator/migrator --target 41
dc exec -T --user 0 database psql -X -U postgres -d postgres \
  -v ON_ERROR_STOP=1 -v stage=control -f /opt/bigbrotr/provision.sql
dc run --rm --no-deps app config initialize --file /etc/bigbrotr/instance.toml
activation_id=$(python3 -c 'import json,os; print(json.load(open(os.environ["BIGBROTR_STATE"]+"/operation-ids.json"))["activation"])')
dc run --rm --no-deps app config activate-initial --file /etc/bigbrotr/instance.toml \
  --request-id "$activation_id" --expected-revision 1
```

Keep each result and the generated operation IDs. Resolve an unknown outcome
through the owning command's status/inspection procedure before an identical
retry. Owner-token issuance cannot recover a lost secret: inspect its status,
explicitly revoke that challenge, then issue a new request ID. Do not replace
the database, rewrite accepted policy or bypass immutable-history checks.
Provisioning SQL is for an empty instance and refuses duplicate creation. If it
stops partway, preserve and inspect that partial instance before continuing.

Enroll the first owner locally, then initialize the Archive catalog, local state
and Control coordinator. The owner token is returned once; protect its original
response and the extracted token. Enrollment grants administration, but does not
implicitly grant private history or export access.

```sh
owner_id=$(python3 -c 'import json,os; print(json.load(open(os.environ["BIGBROTR_STATE"]+"/operation-ids.json"))["owner"])')
dc run --rm --no-deps initialize access owner-token issue \
  --file /etc/bigbrotr/instance.toml --username alice --request-id "$owner_id" \
  > "$BIGBROTR_STATE/observations/owner-token.json"
python3 - <<'PY'
import json, os, re
from pathlib import Path
state = Path(os.environ['BIGBROTR_STATE'])
issued = json.loads((state / 'observations/owner-token.json').read_text())
assert re.fullmatch('[0-9a-f]{64}', issued['token'])
with (state / 'secrets/operator/owner-token').open('x') as f:
    f.write(issued['token'])
PY
challenge_id=$(python3 -c 'import json,os; print(json.load(open(os.environ["BIGBROTR_STATE"]+"/observations/owner-token.json"))["challenge_id"])')
dc run --rm --no-deps initialize access enroll-owner --file /etc/bigbrotr/instance.toml \
  --challenge-id "$challenge_id" --token-ref file:/run/operator/owner-token \
  --password-ref file:/run/operator/owner-password \
  > "$BIGBROTR_STATE/observations/owner-enrollment.json"
dc run --rm --no-deps app access owner-token status --file /etc/bigbrotr/instance.toml \
  > "$BIGBROTR_STATE/observations/owner-status.json"
dc run --rm --no-deps initialize archive initialize --file /etc/bigbrotr/instance.toml \
  --initializer-user postgres --initializer-password-ref file:/run/operator/bootstrap \
  --worker-root /opt/bigbrotr/archive-worker
dc run --rm --no-deps app archive state-initialize --file /etc/bigbrotr/instance.toml
dc run --rm --no-deps app archive coordinator-initialize --file /etc/bigbrotr/instance.toml
dc exec -T --user 0 database psql -X -U postgres -d postgres \
  -v ON_ERROR_STOP=1 -v stage=reader -f /opt/bigbrotr/provision.sql
```

Retain the generated `principal_id` from enrollment and verify that status names
the same account. The owner does not implicitly receive private history/export
authority. Catalog initialization retires its temporary bootstrap identity;
coordinator initialization reports no publication authority and `ready:false`.
The application has not yet collected an event.

The maintained case supplies source registration, finite acquisition and explicit
`history.read`, `export` and `private-export` grants. Its initial owning lifetime
uses `serve --file /etc/bigbrotr/instance.toml --roles control,ingest,sync,archive
--initialize-ingest`; this is fresh-only and cannot restart an existing Ingest
directory. After that owner closes, it enables the deployment reader and binds an
export destination before `serve --roles control`. Ordinary Control opens no
Ingest store. The current manual API/command contracts are in
[Control initialization](https://github.com/BigBrotr/bigbrotr-rs/blob/main/docs/operations/control-initialization.md),
[owner enrollment](https://github.com/BigBrotr/bigbrotr-rs/blob/main/docs/user/administration/enrollment.md) and
[initial Ingest](https://github.com/BigBrotr/bigbrotr-rs/blob/main/docs/reference/ingest-store.md#explicit-initial-owning-lifetime),
with separate [scope grants](https://github.com/BigBrotr/bigbrotr-rs/blob/main/docs/user/administration/scope-grants.md),
[private history](https://github.com/BigBrotr/bigbrotr-rs/blob/main/docs/user/api/history.md) and
[export](https://github.com/BigBrotr/bigbrotr-rs/blob/main/docs/operations/export.md).

After the manual commands finish, stop the database with `dc stop database`. If
you started an owning application lifetime, stop and join that container first.
Keep the state directory, logs and candidate identity. No automatic restart
policy or volume-removal command is part of this first-event recipe.
