# First signed event on a private Linux installation

This M1 candidate is under qualification on Linux amd64. The native bundle can
be relocated and its worker bytes verified. The complete clean-volume journey,
connected readiness and restart/resend acceptance are still open; see the current
[checkpoint](https://github.com/BigBrotr/bigbrotr-rs/blob/main/docs/implementation/progress.md). Do not infer readiness from a running
container, successful initialization or the bundle manifest.

Use the exact private candidate and detached manifest supplied by the integrating
owner. Verify their recorded hashes, extract into a new directory outside the
checkout, and keep the bundle read-only. The supplied `members.json` lists every
regular file except itself; the detached manifest binds its hash and the archive.
These unsigned local records establish byte identity, not release authenticity.

The recipe uses a dedicated PostgreSQL instance, application data and protected
credentials. It binds administration only to `127.0.0.1:18081`, PostgreSQL only to
`127.0.0.1:15432`, and explicitly permits the controlled relay at
`ws://127.0.0.1:7447/installed-fixture`. These ports must be free. Application
containers use Linux host networking because the current private listener accepts
loopback only. No public endpoint, signing key or automatic relay enrollment is
configured. Adjacent Index, Discovery, Monitor and public services are disabled.

## Prepare a new instance

Set absolute paths to the extracted bundle and a **new sibling** state directory,
and choose unique local project/image names. Keep the state path stable after
initialization. You need Docker Compose, Python 3 and at least the configured
64 GiB free completion reserve plus expected growth. The example bounds application
and native work together to 6 GiB with no swap, and PostgreSQL to 1.5 GiB; these
are configured grants, not measured workload capacity.
History uses a 1 GiB execution grant with the 512 MiB Archive worker; export has
its separate 256 MiB grant. The history grant must also cover the bounded response
and coordination reserves; the export limit is not a history limit.

```sh
export BIGBROTR_BUNDLE=/absolute/first-event/bundle
export BIGBROTR_STATE=/absolute/first-event/instance
export BIGBROTR_PROJECT=bigbrotr-first-event
export BIGBROTR_IMAGE=bigbrotr-private:first-event
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
docker build --network=none -f "$BIGBROTR_BUNDLE/packaging/image/Dockerfile" \
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

## Initialize through the actual application commands

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
  --user bigbrotr_migrator --password-ref file:/run/operator/migrator --target 40
dc run --rm --no-deps initialize migrate apply --file /etc/bigbrotr/instance.toml \
  --user bigbrotr_migrator --password-ref file:/run/operator/migrator --target 40
dc exec -T --user 0 database psql -X -U postgres -d postgres \
  -v ON_ERROR_STOP=1 -v stage=control -f /opt/bigbrotr/provision.sql
dc run --rm --no-deps app config initialize --file /etc/bigbrotr/instance.toml
activation_id=$(python3 -c 'import json,os; print(json.load(open(os.environ["BIGBROTR_STATE"]+"/operation-ids.json"))["activation"])')
dc run --rm --no-deps app config activate-initial --file /etc/bigbrotr/instance.toml \
  --request-id "$activation_id" --expected-revision 1
```

Keep each result and the generated operation IDs. Resolve an unknown outcome by
retrying the identical application command and ID; do not replace the database,
rewrite accepted policy or bypass immutable-history checks. Provisioning SQL is
for an empty instance and deliberately refuses duplicate creation. If it stops
partway, preserve and inspect that partial instance before continuing.

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
  --password-ref file:/run/operator/owner-password
dc run --rm --no-deps app access owner-token status --file /etc/bigbrotr/instance.toml
dc run --rm --no-deps initialize archive initialize --file /etc/bigbrotr/instance.toml \
  --initializer-user postgres --initializer-password-ref file:/run/operator/bootstrap \
  --worker-root /opt/bigbrotr/archive-worker
dc run --rm --no-deps app archive state-initialize --file /etc/bigbrotr/instance.toml
dc run --rm --no-deps app archive coordinator-initialize --file /etc/bigbrotr/instance.toml
dc exec -T --user 0 database psql -X -U postgres -d postgres \
  -v ON_ERROR_STOP=1 -v stage=reader -f /opt/bigbrotr/provision.sql
```

These finite commands have passed on a fresh instance with the recorded candidate
bytes and explicit grants. Catalog initialization retires its temporary bootstrap
identity; coordinator initialization reports no publication authority and
`ready:false`. The application has not yet collected an event.

Controlled acquisition and the read/export/restart sequence will be completed here
from the connected acceptance result. They are not yet qualified by this candidate;
the current command contracts
remain in [Control initialization](https://github.com/BigBrotr/bigbrotr-rs/blob/main/docs/operations/control-initialization.md),
[owner enrollment](https://github.com/BigBrotr/bigbrotr-rs/blob/main/docs/user/administration/enrollment.md) and
[initial Ingest](https://github.com/BigBrotr/bigbrotr-rs/blob/main/docs/reference/ingest-store.md#explicit-initial-owning-lifetime).

Stop owned application containers gracefully before stopping the database. Keep
the state directory, logs and candidate identity. No automatic restart policy or
volume-removal command is part of this first-event recipe.
