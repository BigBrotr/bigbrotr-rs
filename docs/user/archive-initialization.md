# Explicit local archive initialization

The implementation candidate provides `archive initialize` and `archive recover`.
They initialize an empty scientific catalog and its commit guards; they do not
start Archive, activate a writer, admit events or establish protection/readiness.
The current supported custody placement is same-boot Linux ext4. Cross-boot and
restore mappings remain implementation work.

Prepare these inputs explicitly:

- A statically valid configuration selecting local Archive, with independently
  chosen dataset/catalog IDs and the expected catalog database and data root.
- An empty PostgreSQL 16.15 database with fsync/full_page_writes enabled. The
  restricted bootstrap must not inherit PUBLIC database CREATE or bypass grants.
- A separate privileged initializer login. Its password comes from a protected
  `file:/absolute/path` reference; never place password bytes in arguments.
- A configured catalog manager with LOGIN, CREATEROLE and NOINHERIT, without
  superuser, CREATEDB, replication, bypass-RLS or inherited administrative roles.
  The initializer does not promote or create this manager.
- An existing empty private data directory, plus an existing private scratch
  directory owned by the command UID. Both paths must be canonical and disjoint;
  scratch must also be disjoint from the immutable installed worker bundle.
- A main executable built with the authenticated worker identity and its matching
  bundle. An ordinary unbound Cargo executable cannot initialize a native catalog.

Protected credential files must meet the
[credential contract](../security/credentials.md). Configuration validation stays
offline. The finite initializer then reads credentials, verifies the bundle and
checks its allowances before permanently reserving the database/root pair.
The native mutual-TLS candidate loads bounded protected certificate/key files
before reservation and freezes them in anonymous sealed Linux descriptors. Its
16 MiB application allowance stays charged through actual worker termination.
The [native matrix](../implementation/evidence/p02-native-mtls-20260919.json)
qualifies initialization and receipt observation at `50d9496`. It never falls back to
plaintext. Native CA paths are limited to 1023 bytes by the pinned libpq; longer
paths refuse before reserving the database/root pair. Server HBA must combine
SCRAM with explicit client-certificate validation when mTLS is required. A shared
certificate does not make its CN equal to every generated bootstrap/writer login;
configure a compatible server policy or mapping.

```sh
bigbrotr archive initialize --file /etc/bigbrotr/instance.toml \
  --initializer-user archive_installer \
  --initializer-password-ref file:/run/secrets/archive-installer
```

By default, `archive-worker/` is beside the main executable. Use `--worker-root`
only to select another canonical matching installed bundle. The initial profile
requires at least 512 MiB in `archive.worker_memory_bytes`: one quarter is engine
memory and the remainder covers native overhead. Initialization uses one worker,
one engine thread and a 64 MiB temporary-file setting. Its application inspection
grant includes 64 MiB plus connection preparation; application and native quotas
remain separate. These declared bounds are not measured RSS or disk-capacity
certificates. The process and filesystem limits of the deployment still apply.

Successful initialize means the native child has joined, the exact temporary bootstrap login
has been retired, remaining SQL-capable sessions have drained, the guard/schema ownership has
been installed for the runtime manager, and a fresh manager connection has verified
the installation. The JSON report retains `ready:false` and grants no runtime
authority. The immutable local marker and PostgreSQL reservation roles remain.
Private `initialize-<nonce>` scratch directories are retained for explicit cleanup
after verifying process exclusion; do not remove an active worker's scratch.

The initializer gives the manager CONNECT on the catalog database and USAGE on
`public`, both WITH GRANT OPTION. Each new writer receives those privileges and
native table DML without delegation. Database and `public` schema ownership stay
unchanged; PUBLIC access is unnecessary. Installation inspection refuses a manager
missing either ordinary access or delegation, before creating another writer.

Existing installations from before this requirement need an explicit privileged
ACL upgrade while Archive is quiescent. Connect `psql -X -v ON_ERROR_STOP=1` to the
exact catalog database as its trusted administrator, with `catalog_database` and
`catalog_manager` variables set to the verified database and configured manager
names. Apply these grants, then use the normal recovery command below for fresh
inspection. Recovery itself does not repair existing ACLs or re-enable writers.

```sql
-- Runtime catalog manager delegation.
GRANT CONNECT ON DATABASE :"catalog_database" TO :"catalog_manager" WITH GRANT OPTION;
GRANT USAGE ON SCHEMA public TO :"catalog_manager" WITH GRANT OPTION;
```

This changes only ACLs; applied migration bytes/checksums and catalog data are
unchanged. Preserve earlier attempts and their credential lifetimes. A previously
committed but unusable writer must follow ordinary fencing/resolution and a new
retained intent; these grants do not authorize reusing or resetting its password.
The [installed publication evidence](../implementation/evidence/p02-installed-publication-native-20260919.json)
at `61271e2` executes these grants after separately removing each grant option on
a fresh installed catalog, and verifies six signed publication/fencing journeys.
It does not qualify a populated historical upgrade or the full Archive service.

If a response is lost or a command exits unsuccessfully after reservation, preserve
the same configuration identities, database, root and marker:

```sh
bigbrotr archive recover --file /etc/bigbrotr/instance.toml \
  --initializer-user archive_installer \
  --initializer-password-ref file:/run/secrets/archive-installer
```

Recovery never generates a replacement bootstrap credential or launches another
native initializer. It establishes exclusion of the previous local custodied
clients through reacquisition, rather than reaping another coordinator's child.
Late, previously invisible selected-client backends may still appear, but cannot
already have application SQL queued after positive client destruction. It can
finish installation from complete scientific state or
inspect an already installed matching catalog. Partial, altered or incompatible
state refuses. A committed reservation with a still-pristine database is stranded:
recovery retires its bootstrap and requires a new database/root pair. An absent
database reservation also does not authorize reuse of the retained local marker.
Do not delete markers, reset passwords or manually adopt tables to bypass refusal.

SQL phases have a 30-second deadline; native supervision has a 120-second deadline.
SIGINT/SIGTERM cancel the active operation and wait for owned transport/supervisor
termination. A timeout remains unresolved until recovery establishes actual state.
Exit codes are 0 for installed and freshly inspected, 1 for local/output failure,
2 for usage, 3 for configuration/preflight refusal, 4 for unresolved/incompatible
state, and 130/143 for interruption. A blocked output destination can produce exit1
after a successful durable installation; run recovery to establish its result.

See the [owning archive contract](../specification/archive-storage-contract.md)
and [current implementation checkpoint](../implementation/progress.md) for the
revision-specific validation boundary and outstanding service/release work.
The [native core evidence](../implementation/evidence/p02-initialization-cli-native-20260919.json)
records 17 executed scenarios and preserves the failed first fixture. The separate
[drain evidence](../implementation/evidence/p02-initialization-drain-native-20260919.json)
exercises five actual recovery cases with SQL remaining on PostgreSQL after client
destruction, including another database, timeout and both signals. The producer is
a controlled tokio-postgres fixture. The [25 TLS/mTLS cases](../implementation/evidence/p02-native-mtls-20260919.json)
also cover actual initialization/recovery, server-enforced certificate validation,
refusals with unchanged durable state, and cancellation of a blocked native
receipt query. [Nine late-fault cases](../implementation/evidence/p02-initialization-late-native-20260919.json)
at `3ad4c64` cover lost scientific/guard commit replies and coordinator/native
process crashes, with recovery while the worker bundle is unavailable. An
incomplete schema remains refused; complete committed state is installed or
recognized without changing native data or the permanent root marker. Signed
mTLS publication, full concurrent/exhaustion/restore and service acceptance
remain open; the guide does not advertise complete installation support.
