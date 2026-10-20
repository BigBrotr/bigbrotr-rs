# Fresh signed-event setup composition

The original source-candidate status is superseded by the scoped results in the
[recovery report](../../docs/implementation/evidence/p02-producer-recovery-native-20260920.json),
under `archive_export_memory_source`. This composes the existing
[accept-only probe](ACCEPT.md) without adding a scheduler mode or changing
production policy, migration bytes, deadlines or the packaged worker.

Root invokes three finite commands in separately inventoried setup environments:

```text
python3 -B experiments/export-execution/memory_setup.py publish /lab/setup-input.json
python3 -B experiments/export-execution/memory_setup.py continue /lab/fresh-root /lab/publication-custody.json ROOT_VERIFIED_RECEIPT_SHA256
python3 -B experiments/export-execution/memory_setup_accept.py /lab/fresh-root /sys/fs/cgroup
```

The first command requires a nonexistent fresh root and empty Control/catalog
databases. It applies current migrations, grants operator-provisioned application
privileges, then calls real configuration initialization, initial activation,
owner-token enrollment, Archive/state/coordinator and destination initialization.
There is no SQL authority seed or `session_replication_role` bypass. Every
principal grant comes from enrollment; each visibility grant uses real HTTP
reauthentication and the scope-grant consumer.

Before initialization, the fixture exclusively creates three mode-0400 TOML
profiles and records their sizes and SHA256 hashes in `profiles.json`:

| Profile | Actual consumers | Selection |
|---|---|---|
| `destination.toml` | Migration, configuration/authority initialization, enrollment, Archive/state/coordinator/destination initialization and scientific census | Exact supplied recipe, including both destination fields; reader disabled |
| `publisher.toml` | Initial `serve --initialize-ingest` | Omits both `export.destination.id` and `export.destination.root`; all other parsed values equal the supplied recipe |
| `ordinary.toml` | Ordinary Control for the three authenticated grants | Supplied destination-bearing recipe with `archive.reader.enabled=true` |

All three files undergo real `config validate` before initialization. Their
reported document hashes must match the frozen files, and their complete
`initial_policy` candidate reports must agree. These deployment-root selections
are excluded from persisted policy; real initialization and activation still
establish authority. Every `--file` call checks its profile's bytes and read-only
mode immediately before launching and retains `<call>.profile.json`; the
positional scientific-census call does the same. No live configuration is edited.
The acceptance follow-up reauthenticates all three pins and the original recipe.

The `publish` phase owns actual Sync/Ingest/Archive publication of the selected
frozen signed event (94,577 bytes by default). Its relay supplies a narrowly selected callback:
the existing handshake, masked REQ/CLOSE checks, three-second socket and small
EOSE encoder stay intact. Each large transmitted frame and its metadata are
retained. Independent event reconstruction, native facts, complete producer
inputs/outcomes, scientific families, native receipts and frontier digests are
checked in the continuation after actual publisher termination and root's
authenticated custody receipt. The final frontier and snapshot
come from this fresh publication and its independently expected batches.
The old small event's fixed frontier/snapshot are not inputs.

After the publisher and relay join naturally, `publish` retains the full raw
captures, the original physical baseline and a closed `publication.json` with
status `PUBLISHED_AWAITING_ROOT_CUSTODY`. It then exits before either native
observer. Root must authenticate the complete process trace, natural termination
of the phase container/tracer, empty prior cgroup, backend absence and unchanged
physical mounts before starting `continue` in a fresh owned container.

The continuation authenticates every original evidence pin, the source/inventory
and all three configuration profiles. It compares physical files to the original
preobserver baseline and recaptures all 75 Control tables/xmin and the catalog
before either native observer. Ordinary Control then receives only the three
scope-grant requests. The complete
published files, native catalog and original Control rows/xmin survive that
stage. The scheduler is joined before `accept.json` is written. A successful
continuation emits `PREPARED_FOR_ROOT_ACCEPTANCE`; it has accepted no Export.

The final command authenticates the prepared state, waits for the observed
account login bucket's natural 60-second expiry using PostgreSQL time, and invokes
the exact accept-only probe once. Two logins plus three reauthentications can fill
the five-attempt bucket. It does not edit the bucket, retry a denial, renew a pin
or change any deadline. Each publication, continuation and acceptance invocation
has its own 120-second wrapper budget; root's between-phase custody audit is
outside those budgets. Continuation authentication consumes its phase budget.
Individual
CLI operations default to 30 seconds, SQL to eight seconds, publication observation
to 50 seconds, and normal Serve joining to eight seconds. Root must impose its
own outer process/container deadline and retain unresolved descendants.

The acceptance oracle checks full captures of all 75 Control tables, including
xmin, and the complete catalog. It expects one new command/audit, one Accepted
revision-1 unreserved job, its next admission ordinal, exact complete input bounds
and distinct reader/Export leases and claims. Existing publication, source,
scope-grant history and physical files remain exact. The temporary reader is
released; the Export retention obligation remains active. Requested pin TTL is
30 seconds, clamped by the actual five-second Control lease. Control refresh does
not extend a pin. An active retained row is not current execution permission.

Only a known, independently checked acceptance prepares `case.json`,
`science.json` and `run.json` for the unchanged [measurement runner](MEMORY.md).
The acceptance command only records the absolute path visible to the future
observer; `/sys/fs/cgroup` selects the root of its private cgroup namespace.
It never enters, creates, reserves or measures that cgroup. Before measurement,
root must provision and authenticate a genuinely fresh dedicated cgroup and run
the original runner and oracle there. This packet makes no memory, five-export
or 256 MiB claim.

`memory_lab.py --root FRESH_EXTERNAL_ROOT --build QUALIFIED_ARTIFACT_DIRECTORY
--probe QUALIFIED_PROBE_DIRECTORY --instrument QUALIFIED_TRACER_DIRECTORY`
composes these phases from a clean source checkout. It creates fresh isolated
PostgreSQL and credentials, then separate finite containers for configuration,
publication, continuation and acceptance. Complete process traces and external
container/cgroup/backend/mount checks gate each transition. The retained client
image includes the exact recorded `psql` executable; `client.Dockerfile` describes
its research-only construction, with installed package identities in the image
build evidence. The executor ends after independently checked acceptance and
stops its containers; it does not run measurement. Its first candidate retains
the full 1 MiB event envelope and a 1 GiB policy export cap, not a qualified
256 MiB result. Preserve any accepted job even if cleanup fails; `cleanup-required`
is a nonzero executor outcome and never permission to retry acceptance.

## Publication custody receipt

Root produces `publication-custody.json` outside `fresh-root` after the first
phase's entire container and tracer have terminated. The continuation requires
its exact SHA256 as a command argument. It never creates or approves that receipt.
The JSON object has exactly the following fields; every file pin is
`{path,bytes,sha256}` naming a canonical protected absolute regular file visible
at the same path in the continuation container:

| Field | Required value |
|---|---|
| `format`, `status` | `bigbrotr-export-publication-custody/1`, `VERIFIED_FOR_NATIVE_REOPEN` |
| `root` | Original canonical fresh-root path |
| `publication` | Exact pin for `root/publication.json`, at most 16 MiB |
| `source_manifest`, `inventory` | Exactly the original setup-input pins |
| `container_id` | Complete 64-character lowercase hexadecimal prior container ID |
| `cgroup_path` | Actual absolute host cgroup path recorded by root |
| `tracer` | Actual authenticated executable pin for the process-only tracer |
| `identity` | `trace-birth-exec-lineage` |
| `evidence` | Exactly six pins: `trace`, `trace_audit`, `container`, `cgroup`, `backends`, `mounts`; each nonempty and at most 16 MiB |
| `checks` | Exactly `natural_exit`, `empty_cgroup`, `backends_absent`, `physical_roots_unchanged`, `complete_worker_lifetimes`, `no_other_credential_holders`, each JSON `true` |

The full process-only trace must start before any phase child, identify each
transient worker's birth/exec timestamp and lineage, and retain its exit and
parent wait outcome. These traced lifecycle identities are distinct from
`/proc/PID/stat` start ticks. Polling samples do not establish completeness.
Root must independently audit the pinned trace and tracer identity, actual
terminal container state, empty prior cgroup, backend exclusion, original
physical mounts and absence of other credential-capable holders. The continuation
checks the closed receipt and every pin, but does not replace that external
semantic audit with an interpretation of arbitrary evidence JSON or Boolean
claims. The caller-supplied receipt hash is the explicit root authorization
boundary; this is a trusted root fixture, not an untrusted receipt service.

`publication.json` pins every original non-native evidence file, including
configuration, initialization, sessions, SQL queries/results, PID/terminal records
and complete relay frames. Native roots retain their original physical census;
scratch retains its separate publication census. The evidence inventory allows
8,192 entries, 16 MiB per evidence file and 256 MiB total. Keep outer trace/audit files
outside `fresh-root`; adding files there before continuation is refused.
`continuation-started.json` is exclusively created before authentication and never
removed. A validation failure preserves `continuation-failure.json` with
`UNKNOWN_PRESERVED`; neither a new receipt nor another invocation retries it.
The continuation records `authenticated-custody.json`, and its successful result
binds both the original publication and root receipt hashes.

Root must stop and authenticate the continuation environment before acceptance,
and the acceptance environment before measurement. The publication receipt only
authorizes the publication-to-observer transition. Later process/container/backend
custody and the fresh measurement cgroup remain root-owned prerequisites.

## Closed setup descriptor

`setup-input.json` is a protected absolute JSON file with precisely these fields:

| Field | Required value |
|---|---|
| `format` | `bigbrotr-export-fresh-setup/1` |
| `name` | 1–64 ASCII letters, digits or hyphens |
| `root` | Canonical absolute nonexistent root under an existing owned parent |
| `application_source` | Actual 40-character source revision from the passed application's clean `build-info` |
| `application`, `probe`, `ingest_probe`, `psql` | Each `{path,bytes,sha256}` naming the actual executable; the probes include this fixture's required modes |
| `config` | `{path,bytes,sha256}` for the explicit root-prepared destination-bearing TOML recipe, with a separate `[export.destination]` section containing exactly `id` and `root` |
| `recipe` | `{path,bytes,sha256}` for the selected frozen input. Default `archive-event-response.json` SHA256 is `815bf9e7e0fbcda73a77fbc3f91bebe0ec5c5f356e01bd665f695a8edb5af963`; supported boundary literals are listed below. |
| `event_kind` | Optional; omission preserves `frozen-archive-recipe`. Explicit `frozen-signed-event` selects a complete pinned boundary literal, never a newly signed runtime value. |
| `worker` | `{root,bytes,sha256}` for the unchanged qualified worker executable; root must be the application's adjacent `archive-worker` directory, which ordinary Serve actually selects |
| `inventory`, `source_manifest` | Each `{path,bytes,sha256}` for root's authenticated environment/build/source evidence; pinning the receipt alone does not validate its contents |
| `operator`, `migrator` | Each `{user,password_ref}` for distinct responsibilities provisioned outside this script |
| `username`, `password_ref` | Actual fresh owner username and protected `file:/absolute/path` password |
| `keys` | Ten distinct nonzero 16-byte lowercase hexadecimal command keys: `activate`, `owner`, `endpoint`, `enrollment`, `scope`, `acquisition`, `accept-export`, `history.read`, `export`, `private-export` |
| `limits` | The exact closed [measurement limits](MEMORY.md), all positive integers; retain `row_bytes=1048576` and `disk_reserve_bytes=68719476736` |

All artifact sizes and hashes are supplied from real files. There are no default
future ELF hashes or job IDs. Protected input files are regular, canonical,
single-link and not group/world writable. Credential files must additionally be
private. Secret-bearing raw CLI, SQL, session and token evidence stays under the
fresh mode-0700 root; do not publish that directory or commit its contents.

The configuration is generated with the real existing recipe owner and frozen
before initialization. It must select managed authority; local roles
`control,ingest,sync,archive`; administration on private port 18081; the initial
publisher enabled and reader disabled; and the existing signed Sync profile
whose request is `{"kinds":[1],"limit":3}`, provenance enabled, history floor zero
and ephemeral retention. Retain the qualified native/resource/reader profiles
and the full event envelope. It must bind `storage.ingest_root=root/store`,
`archive_root=root/data`, `archive_state_root=root/state`,
`scratch_root=root/scratch` and `export.destination.root=root/destination`.
Both PostgreSQL connections use the explicitly isolated loopback
`127.0.0.1:5432`, local-plaintext/loopback TLS boundary, distinct new database
names, narrow application/manager roles and a SELECT-only reader. The reader
password is the existing census's 64-character lowercase hexadecimal profile.
The three profiles above are frozen before initialization; selecting the existing
ordinary-reader path later does not edit any file or accepted policy document.

## Root prerequisites and retained limits

Root owns engine/container/network creation, exact PostgreSQL 16.15 configuration,
role/credential/ACL provisioning, operator authorization, complete native bundle
verification and clean build provenance. The inventory must bind their actual
identities, physical mounts, cgroups and filesystem ownership. Supply the
application-adjacent unchanged worker bundle with the pinned library, header and
extensions. Authenticate every Python dependency against the source manifest;
the wrapper hashes that manifest but does not replace root's source review.

This is one known initial single-stream publication, not a general setup runner.
The independent census keeps its existing count/native budgets. The original
large-event invocation permits 262,144 bytes for the test CBOR decoder and a
scientific JSON row strictly below that size; their old defaults remain 65,536.
The explicit supported-boundary selection below uses larger observation bounds.
The unchanged native dump
allows fewer than 2,048 records and values below 1 MiB. Root-file census permits
8,192 entries, 64 MiB per file and 256 MiB total per root. Child stdout is capped
at 16 MiB, stderr at 1 MiB and SQL input at 64 KiB. These are fixture bounds,
not new product limits or measured reservations.

The wrapper retains direct-child PID/start/executable, process-group exclusion,
return status and fresh Control/catalog backend absence before subsequent opens.
Root must separately retain complete transient native-worker PID/start/exit and
cgroup custody; process-group absence does not establish that no descendant
escaped. External custody is required before invocation and again before
the publication continuation or reopening after any uncertainty. Direct SQL observations cannot exclude an
invisible startup-capable credential holder on their own.

The strict physical comparison starts before either native observer. The existing
RocksDB dump uses default read-only options; any incidental diagnostic-file change
will therefore refuse this candidate. Whether that consumer needs a separately
reviewed diagnostic allowance remains unexecuted. Do not silently move the
baseline after readback or normalize such differences.

Any timeout, forced exit, missing identity, failed join, changed bytes, unknown
COMMIT or oracle refusal preserves `UNKNOWN_PRESERVED`. No source-root cleanup,
replacement owner, copied native root, automatic retry or compensating SQL follows.
Even a failed accept probe may have committed its job and retention pin: retain
the original command key and resolve actual outcome under root custody. The
presence of prepared files, an `Accepted` telemetry marker or AST validity is
not runtime qualification.

## Supported signed-input boundary cases

Use the existing `memory_lab.py` arguments with `--signed-input content-heavy`
or `--signed-input tag-heavy`; omit this option to preserve `archive-sample`.
Each case needs a new inventoried root and genuine publication. For five ordinary
Serve exports, retain `--installed-five --export-memory-mib 256` and the same
qualified application/build/probe/tracer inputs. No application or probe rebuild,
policy change, deadline change, custody shortcut or smaller export row profile is
part of this selection.

Both new inputs are compact 262,144-byte complete signed JSON objects, without
newline. Public test key 3, created_at 1 and kind 1 are fixed:

| Input under `tests/fixtures/protocol/` | Shape | SHA256 |
|---|---|---|
| `archive-event-content-heavy.json` | Empty tags; 261,811 ASCII content bytes; 18 whole-envelope lexical nodes | `b156a704a9d57837b1a49f272b646cb1e2a68d9d826ba10077769f260bb37b0e` |
| `archive-event-tag-heavy.json` | 32,756 `["x"]` tags, then `["x",""]`, then `["t","é"]`; 65,256 ASCII content bytes; 65,536 whole-envelope lexical nodes | `d3f4116349d7166d4e380bd015a59a46f8887c0f30a51599a443daaccfb8acb9` |

The relay accepts only these frozen hashes for `frozen-signed-event`. It retains
the original event field order and sends 262,215-byte frames: 262,144 signed bytes,
61 bytes of EVENT/subscription framing and 10 WebSocket header bytes. The original
128 KiB text-frame default stays unchanged; the new mode explicitly selects a
262,205-byte text payload bound. No shared small-frame encoder changes.

After external publication custody, this mode invokes the existing independent
C-API census with `--row-bytes 1048576`. The bound is exclusive and includes
scientific metadata; a 262,144-byte signed event cannot fit the old 262,144-byte
census row. Its engine memory, counts and other settings stay unchanged. The
existing independent CBOR decoder receives explicit 524,288 encoded bytes,
65,536 entries per collection and 131,072 total nodes, retaining depth 16.
Old callers retain their byte/count defaults and the same acceptance domain.
Both observation bounds are test-only, separate from production admission.

`write_archive_event_response.py --case CASE --check` reconstructs and verifies
the selected frozen file using the existing python-secp256k1 signer and separate
BIP340 equation checker. Omit `--check` to author a new boundary file exclusively;
`--output` selects an isolated authoring path. Its original no-argument recipe
generation remains byte-identical. Signing dependencies are required only for
explicit offline author/check work, never for setup or measurement.

`python3 -B experiments/export-execution/test_memory_oracle.py --boundaries`
adds offline positive and overbound controls to the existing test entrypoint.
It rechecks independent signatures, Node canonical IDs/byte counts, complete
relay frames, recursive envelope node counts and decoder/census boundaries.
The byte+1 control is validly signed and stays below the node limit; node+1
is validly signed and keeps normalized bytes at 262,144. Their rejection is by
the independent fixture constraints, **not** an executed product-parser result.

These inputs reach the installed Sync normalized-byte and lexical-node limits.
They do not reach a 1 MiB scientific native row: ordinary Sync still rejects an
accepted `ingest.max_event_bytes` above 256 KiB, while export retains its full 1 MiB
row allowance. Source `7641338` now has separate real publication and five-job
export captures for both inputs, with independent signed-file/SQL checks and
fresh 256 MiB/no-swap cgroups. See `supported_boundaries_20261020` in the recovery
report for qualified scope and exact identities. The full 1 MiB native-row gate
remains **NOT_RUN**.
