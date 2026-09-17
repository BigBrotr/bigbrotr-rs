# Security, recovery and practical validation

The selected [operational contract](../specification/operations-contract.md)
consolidates cross-store outcomes, owner/process placement, resource completion
reserves, configuration authority, compatibility and command-order recovery. Its
reference model is design evidence; real engine/process/release acceptance below
remains required.

## Trust boundaries

Remote relay frames, DNS results, redirects, NIP-11 JSON, seed directories, import
files and third-party assertions are hostile inputs. Verify framing, limits and
signed-event identity before durable admission. Verified authorship does not imply
truthfulness of content, timestamps, relay lists, trust claims or monitor reports.
A Nostr client library's cache and expiry policy do not define archive correctness.

One guarded transport path resolves and vets every dial, pins the permitted IP
while retaining correct TLS SNI/hostname validation, and rechecks redirects and
reconnections. Explicit private-network routes are narrowly scoped. A proxy must
not bypass private-address restrictions or silently leak onion hostnames to
public DNS. Failed TLS is a recorded result, not permission to downgrade globally.
Auth-scoped observations and endpoint URLs containing sensitive query data have
visibility labels; public exports, logs and publishers enforce those labels.

Public HTTP, websockets and DVM share typed query semantics, authorization and
resource budgets but have their own connection/rate/stream limits. SQL strings
from users are not passed to storage. Generated public query parameters are
allowlisted and parameterized. Untrusted content is escaped in the dashboard;
links, images, markdown and remote embeds cannot execute scripts or cause
unrequested private-network fetches. Frontend visibility alone is not access
control. Private administration uses a distinct authenticated surface and CSRF
protection; it cannot be exposed by enabling the public read API.

Signing keys are scoped by function, algorithm and viewpoint where required.
Support local protected key files and NIP-46 initially; hardware/KMS adapters
must explicitly support Nostr's Schnorr signing requirements before advertised.
A generic KMS ECDSA key is not automatically usable. Signed publication payloads
are durable before send, retries preserve bytes, and acknowledgement is recorded
per destination. Relay acceptance is not proof of global dissemination. Avoid
private keys in config dumps, environment diagnostics, logs and fixture artifacts.

## Failure matrix and operator response

| Failure | Required behavior | Recovery evidence |
|---|---|---|
| Process dies before durable admission | No successful durable receipt; producer retries | Replayed input restores missing record without false progress |
| Dies after admission before producer records receipt | Stable producer request returns prior outcome | Identity, body and pending record remain coherent |
| Archive commit succeeds but ACK is lost | Check transactional archive receipt before retry | Exactly one logical batch under exclusive publisher |
| Two would-be archive owners | Refuse unfenced failover; do not rely on receipt uniqueness | Pause/resume old owner and network partition test |
| Indexer inserts some tables then fails | Old published frontier stays readable; retry missing work | No phantom complete batch or duplicate answers after window expiry |
| PG/control unavailable | Pinned reads may continue only until their short-lived policy authorization expires; no unproven ownership/publication changes | Restart uses persisted receipts and valid ownership |
| CH unavailable | Intake/archive continue within bounded spool; read capability reports unavailable/stale | Replay from pending/archive catches up with stable answers |
| Archive storage unavailable | Intake within configured pending reserve, then throttle/stop safely | No premature spool reclamation; resume with verified files |
| Disk reserve exhausted / fsync error | Reject new durable acknowledgements, preserve recoverable state | Fault injection, restart and logical digest comparison |
| Corrupt pending/archive file | Quarantine affected unit, report exact scope; no silent skip | Restore known-good copy; explicit audited loss acknowledgement only if unrecoverable |
| Slow or malicious peer | Bound allocation/time/work, disconnect and record scope gap | Other endpoints retain fair service and bounded latency |
| Rank/derivation worker dies | Previous complete generation remains active | Partial run never becomes public or publishable |
| Key unavailable / destination rejects publish | Keep durable pending intent and visible per-target failure | Retry does not manufacture a new signature/event each attempt |
| Upgrade partially applies | Versioned migration checkpoint; block incompatible writers | Rehearsed rollback or forward repair with documented data-format limits |
| Backup succeeds but is unusable | Backup verification fails until isolated restore works | Full user-visible restore journey, not object existence only |

Each possible commit has committed/not-committed/outcome-unknown resolution in
the [effect table](../specification/operations-contract.md#cross-store-effect-and-recovery-table).
In particular a new catalog read showing no receipt is insufficient while the
old transaction might still commit; exclude/resolve it before retry. PG recovering
before CH, Rocks or object credentials changes availability, not their authority.

The [backup/restore procedure](../specification/operations-contract.md#backup-and-restore)
requires causal producer-checkpoint/owner-GC closure, all catalog-referenced objects,
required pins/jobs/exact outbox bytes and policy/config/version state. Restore uses
isolated roots, blocked production egress and positive old-owner exclusion before
activation. Initial local-synced receipts have no host-loss guarantee. Hourly
backup attempts, verified age <=2 h and <=4 h fixture restore are engineering
objectives pending measured release-envelope drills, not current SLAs.

The [compatibility procedure](../specification/operations-contract.md#compatibility-migration-and-rollback)
specifies adjacent mixed-release support only inside declared schema/RPC ranges,
reader-first additive upgrade, writer-floor activation and last safe binary
rollback point. After an incompatible write, forward repair or fenced backup
restore is required. Inspect/plan/doctor commands remain read-only and cannot
take ownership merely because the browser or a health endpoint is unavailable.

NIP-09 deletion requests remain archived with their targets. Read policy handles
logical deletion/current state. Exceptional operator-directed physical removal
is an explicit separate workflow that documents consequences for snapshots,
backups and already exported files; do not promise revocation of independently
copied exports. Retention defaults do not remove archival events by age or score.
Disk exhaustion is never solved by undocumented automatic data loss.

## Test programme with independent oracles

Pure tests cover protocol filter semantics, policy, identity normalization,
state-machine transitions, incremental reducers and graph math. Property/fuzz
tests generate hostile frames, tags, timestamps, URLs, filters, import manifests
and cancellation schedules. Their oracles come from specifications, reference
implementations, slow exact reducers or cross-engine result comparison—not merely
from repeating the implementation's expression in the expected value.

Integration tests use the actual pinned PG, CH, RocksDB binding and DuckLake
extension. Protocol conformance talks over real websocket sockets to controlled
relay implementations, including strfry and deliberately capped/nonconforming
fixtures. Deterministic simulations control time and failures; separate dated
real-network observations assess behavior that local fixtures cannot reproduce.
Public relay availability must not make deterministic release tests flaky.

Every service has practical acceptance scenarios in its contract. Cross-service
release scenarios must start from the shipped CLI/Compose entry point, not just
construct library objects in tests:

1. Empty volumes → validate → start → collect valid signed fixtures from two
   relays → observe provenance → query HTTP and archive relay → export → stop.
2. Repeat with provenance off: full bodies remain, no historical edge state or
   hidden P-sized work, provenance APIs return unavailable rather than zeros.
3. Modify service settings through documented interfaces; verify actual behavior,
   reload/drain rules and redacted diagnostics. Start each role independently
   against existing data and stop each producer/consumer in turn.
4. Kill processes at admission/archive/index/publication boundaries. Restart,
   replay beyond engine dedup retention, compare logical counts and payloads with
   an independent exact corpus manifest. Include deletion/replacement ties.
5. Run live acquisition, large historical backlog, import, mixed queries, graph
   work, export and compaction together under a fixed host resource envelope.
   Verify budgets, fair monitoring age, bounded memory and recovery after overload.
6. Back up during activity using the documented consistent procedure. Restore
   into isolated fresh volumes and exercise reads, resumption, signing receipts,
   provenance origin and graph generation selection. Verify full and projected
   exports through standalone DuckDB without the live catalog.
7. Upgrade from the previous supported release and import legacy full/incomplete
   fixtures. Refuse unsupported downgrade/false full-fidelity claims; document
   gaps and explicit repair paths.
8. Exercise dashboard and private operations in a real browser: empty/loading,
   stale/partial/disabled capabilities, keyboard use, narrow screens, malicious
   content, expired authorization and reconnects. Follow website installation and
   tutorial commands from a clean machine/release bundle.

Security testing includes SSRF rebinding, redirect chains, decompression bombs,
query amplification, slow consumers, auth-scope leakage, log injection, key
redaction, schema migration privileges and untrusted CI input. Use Miri or
sanitizers where unsafe/FFI boundaries warrant them; use concurrency model tests
for coordination logic and external process fault tests for real locks/storage.

## Review of every coherent submitted change

Before a coherent change is committed, submitted or merged, read its final diff,
affected contracts/call chains, comments, documentation and actual validation
results. Review behavior, failure/recovery, data meaning, authority boundaries,
resource costs, idiomatic code and user-visible consequences. Record unresolved
findings honestly; a blocking correctness or security finding prevents acceptance
of the affected change. No fixed reviewer count or repeated-clean-round quota is
required. A checkbox or green unit suite is not the substantive review itself.

Select practical evidence by impact: storage/coordination changes exercise the
actual binding and crash/replay path; protocol/HTTP changes use independent real
clients; browser changes execute the affected journey against the backend;
packaging/documentation changes run the delivered commands from a clean bundle.
Pure logic also uses meaningful specification-based unit/property/oracle tests.
Reuse prior evidence only when the relevant artifact, input, schema, dependency
and operating assumptions remain compatible; explain the scope of reuse.

At release, execute every advertised required journey and fault scenario in the
declared environment. The implemented harness distinguishes PASS, FAIL, SKIP and
NOT_RUN; missing execution cannot become PASS. This is the shared obligation for
P01–P10, including website tutorials, rather than a test-category quota on each edit.

## Current experimental evidence

Completed now: isolated SDK receive-handler reproduction, its relay parser tests,
DuckLake PostgreSQL commit/rollback and process-death/concurrent-writer probe,
RocksDB synchronized batch/process-death probe, plus the prior component lab.
Not completed: future BigBrotr executable, production Rust RocksDB adapter,
power-loss/ENOSPC/fencing drills, end-to-end real-relay capacity, browser journeys,
website build and billion-row serving layout. These are explicit implementation
acceptance work, not silently satisfied by planning documents.

## Derived visibility is an input contract

Every fact/graph/rank generation binds a visibility domain, input-policy revision
and eligible source set. Public generations consume public-eligible inputs only;
filtering rows after computing over private follows or private relay observations
would still leak their influence through counts, aggregates or scores. Private
analysis uses separately authorized generations. Publishers verify both output
visibility and the source-generation scope. Scope changes invalidate/recompute
affected generations; they are not cosmetic API filters. Statistical disclosure
through small cohorts needs explicit resource policy even for otherwise readable
aggregate results.

Policy restrictions use the management activation barrier, bounded authorization
leases and fail-closed expiry. Data snapshots may remain pinned for consistency,
but an old snapshot is not permission to bypass a newer security restriction.

## Additional acceptance recovered from historical task documents

Exercise shipped alert routing and its local receiver, not only rule expressions.
Build from a clean checkout and from a reused build cache; neither artifacts nor
images may contain a development `.env`, credentials or untracked lab material.
Inspect effective private listener binds under bridge and explicitly supported
host-network modes. Loopback inside a container and loopback on its host are
different reachability contracts; doctor and installation instructions must agree
with the actual port mapping.

Provenance-off replay/restore verifies event-level first admission/receive metadata
without allocating an edge inventory. Test timestamps at zero and u64 maximum
through storage and filter conversion, with the distinct NIP-77 maximum exclusion.
Exercise full text and server reconciliation through actual read entry points as
specified in [their contract](../architecture/search-and-reconciliation.md).

Required-suite results distinguish discovered/collected, executed, skipped, failed
and NOT RUN scenarios. A required engine/client suite with no executed required
scenario cannot report success. Missing tools or environment explain NOT RUN and
fail the required release job; optional suites report their scope honestly. This
is an execution check, not an arbitrary test-count quota or source-file census.

The low-volume path is a distinct integration case: one admitted event smaller
than every size threshold must reach archive publication and online queryability
within their independent configured age/flush deadlines. Do not let steady bulk
traffic conceal a timer that never flushes. Control pure time with a fake clock;
use bounded monotonic progress waits for actual processes, and verify receipts,
query results and artifact membership rather than replace readiness with sleep.

For the clean-install journey, extract the actual release bundle into a fresh
directory outside the checkout. Resolve every referenced Compose mount, config,
seed and startup asset there before launching. Then execute the documented
installation, useful fixture collection/read and shutdown. A developer checkout
or cached image must not supply an undeclared missing release file.

## Scenarios recovered from actual Go call chains

The [coordinating code review](../reviews/08-go-code-walkthrough.md) links the
full-file walkthroughs and source manifests. Add these discriminating real-path
fixtures to their owning implementation stages:

* An endless backfill competes with unvisited endpoints; inspect bounded
  preemption, receipt-backed resumption and retained live/control capacity.
* A known event is redelivered from a new endpoint with invalid well-shaped
  signature; no witness is committed. Delay verification before EOSE and deliver
  dropped/out-of-scope frames; no completion overtakes required validation.
* Stall the exact store while pending marks/lookahead/driver buffers grow; verify
  all byte reservations, backpressure and recovery, including one very large unit.
* Corrupt a large retained artifact and interrupt repair; preserve source bytes
  and bounded forensic work without manufacturing a complete recovered frontier.
* Disable a derived target and make dirty-only progress without new events;
  inspect actual storage queries, touched partitions and committed output cuts.
  Replace a list with a newer empty winner, then import an older list in another
  month; neither entries nor graph membership may be reconstructed incorrectly.
* Follow actual cursors through mixed/all-withheld candidates and losslessly
  encoded tag values; distinguish absent deletion state from a zero timestamp.
  Continue across API restart and worker changes, rotate/revoke cursor keys and
  verify private scan positions remain opaque while policy revocation still acts.
* Disable the cockpit, repeat stuck dependency probes, reuse a readable process
  identity and hold an uncooperative shutdown; writer exclusion and bounded work
  remain effective at their actual owning boundaries.
* Publish more subjects than one batch, interrupt between pages and fail one
  destination; resume the suffix and exact pending bytes without forgetting the
  failed target or recasting an unwired subject helper as a working feature.

These are source-derived test requirements, not tests executed in this review.
The source reports disclose static-inference limits and do not certify all Go
behavior or the future Rust implementation.

Local signing credentials are opened with supported OS facilities that prevent
a named pipe/device or disallowed symlink from blocking ordinary secret loading.
Validate the opened descriptor's regular-file identity, ownership and restrictive
permissions, then enforce the byte limit while reading; a pathname-only check
before an ordinary blocking open is vulnerable to substitution. Define supported
secret-mount behavior explicitly, keep errors redacted, and reject unsupported
file types/ownership. Test these cases in isolation and independently enforce
the typed signer's purpose/kind/subject allowlist. A generic key's ability to
sign any event is not the application signing authority.
