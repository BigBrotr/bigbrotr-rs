# Phase 07 — Signing, durable delivery and NIP-66/NIP-85 publication

This is a future implementation plan. No product files, network publication or
successful runtime tests are implied. The selected contracts are
[derived/publication services](../architecture/derived-read-publication.md),
[protocol evolution](../architecture/protocol-evolution.md),
[security/recovery](../engineering/security-recovery-and-validation.md),
[component boundaries](../specification/component-contracts.md) and
[DVM profile](../specification/dvm-profile.md). The
[Go analytics walkthrough](../research/go-analytics-code-walkthrough.md)
provides concrete failure examples, not a protocol authority or a complete audit.

Every source/schema/config/test path below is a proposed implementation output.
One application signing boundary and outbox serve NIP-66, NIP-85 and DVM; there
is no direct-send escape path. Separate purpose grants and keys preserve those
identities. A process capable of public reads does not thereby gain signing
authority. Ordinary publication cannot mutate or prune the scientific archive.

## Shared execution and evidence

T01–T04 depend on P01/P02 foundation/control only and can execute before P06 DVM
tasks. Fact/rank publishers additionally depend on the named P05 producers; they
must not create a phase-wide cycle with P06. P01-T03 verifies actual signed bytes,
T04 config/secrets, T05 budgets/lifecycle, T06 action authority, T07 internal
transport and T08 real-process fixtures. P02-T01/T07 supply fenced PG state/pins.

For each task the future packaged case is:

```sh
python3 tests/acceptance/run.py p07-tNN --binary dist/bigbrotr --compose packaging/compose/acceptance.yaml --evidence .research/implementation/p07-tNN
```

Replace `NN` with the task's two-digit number. Cases live in
`tests/acceptance/cases/`; they launch the actual CLI/services, PG and controlled
real WebSocket relay/signer clients. They must inspect independently verified
wire events, persisted outbox rows, per-destination obligations, source manifests,
process exits and resource counters. Record exact commands, versions, fixture
and release digests and fault timings. Missing infrastructure/scenarios are
`NOT_RUN`, never assumed successful. All keys and destinations are isolated test
identities; production relay or upstream publication is not part of this plan's
verification commands.

Initial common limits: signing timeout 30 s, two in-flight operations per key;
delivery concurrency four globally and one per destination/coordinate;
retry full jitter 5–3600 s; pending outbox byte budget 256 MiB; seven-day pending
age parks obligations without deleting them. Source freshness is checked before
constructing an intent and again before releasing an unsigned desired update.
Already signed retries preserve their exact bytes, while current policy can
cancel undispatched obligations. All work uses the shared deployment resource
budget and five-second authority lease; notifications alone do not prove a
restriction is applied.

## P07-T01 — Typed signing purposes and protected local keys

**Dependencies:** P01-T02–T06/T08; P02-T01.

**Proposed files:** `crates/bigbrotr-model/src/publication.rs`;
`apps/bigbrotr/src/publication/signer/{mod,policy,local_file,verification}.rs`;
`contracts/publication/v1/sign-intent.schema.json`;
`config/examples/signing.toml`; `docs/user/publication/signing.md`;
`apps/bigbrotr/tests/signer_boundary.rs`;
`tests/fixtures/publication/signer.json`;
`tests/acceptance/cases/signer_boundary.py`.

**Boundary:** validated purpose + canonical unsigned event + caller grant +
authorized key reference produces a full signed event or typed denied/locked/
timeout/invalid-key failure. Purpose binds kind, subject/coordinate, expected
public key, producer/job identity, content/tag bounds and algorithm/viewpoint.
No public arbitrary-message signing endpoint exists.

- [ ] **Step 1:** define purpose variants for NIP-85 algorithm/viewpoint, NIP-66
  observer/vantage, DVM accepted job, NIP-42 challenge and optional active probe.
  Use distinct key/config grants; a NIP-46 transport-client key is not the
  application event signer. Reject confused-deputy requests before key access.
  Provider metadata/preferences/withdrawals have their own explicit owner,
  profile, kind and coordinate variants; they do not bypass the purpose boundary
  merely because they lack an accepted DVM job. The DVM handler announcement
  variant permits only its configured kind-31990 profile/coordinate.
  Bind active probes to P04-T10's exact `monitor-write-v1`: kind 1, content
  `BigBrotr monitor probe v1.`, tag `["bigbrotr-probe",attempt_uuid]`, dedicated
  probe key and complete event below 1 KiB. Optional NIP-13 nonce work is capped
  at 20 requested bits and one second; failure to reach the configured target
  does not authorize a different event/profile or unbounded mining.
- [ ] **Step 2:** implement protected file key loading with bounded nonblocking
  open, symlink policy, descriptor-based type/owner/mode checks, bounded bytes and
  cryptographic scalar validation. Initial ordinary files require correct UID
  and 0400/0600 permissions; explicitly supported secret mounts document their
  alternative ownership checks. Never read an unbounded FIFO before checking it.
- [ ] **Step 3:** load secrets through configured protected references, not plain
  environment/CLI value strings or generated fallback keys. Redact nested error
  causes and never include private key material in debug serialization.
- [ ] **Step 4:** independently recompute returned event ID and validate signature,
  public key and unchanged unsigned fields. Treat SDK cached-ID acceptance as
  untrusted; a successful adapter return is not sufficient evidence.
- [ ] **Step 5:** enforce bounded in-flight operations, deadline/cancellation and
  current purpose authority. Rotation disables new old-key intents while
  preserving already signed delivery/lineage under explicit policy.
- [ ] **Step 6:** run packaged signing through the real service with regular file,
  symlink, FIFO, oversized file, wrong UID/mode, invalid scalar and wrong-purpose
  requests. Verify each successful result with a separate BIP-340 implementation.

**Oracle and negative control:** known independent signature vectors and immutable
unsigned-field fixtures define the result. A signer that returns a valid signature
for altered content or another public key fails. A FIFO fixture must finish
within its deadline without hanging a runtime worker. Invalid configuration must
not silently activate an ephemeral signer or expose the secret in stderr.

## P07-T02 — Bounded NIP-46 remote signing adapter

**Dependencies:** P07-T01; P01-T03/T05–T08.

**Proposed files:** `apps/bigbrotr/src/publication/signer/{nip46,session}.rs`;
`crates/bigbrotr-protocol/src/nip46.rs`;
`config/examples/signing.toml`; `docs/user/publication/remote-signer.md`;
`apps/bigbrotr/tests/remote_signer.rs`;
`tests/fixtures/publication/nip46.json`;
`tests/acceptance/cases/remote_signer.py`.

**Boundary:** an operator-configured remote signer connection maps one validated
purpose to an expected event public key through a separate transport identity.
Session request IDs, encryption, response ownership and cancellation are bounded.
Remote authentication waiting is a state, not a successful signature.

- [ ] **Step 1:** implement the pinned NIP-46 connection/handshake and method
  subset needed for get-public-key, sign-event and explicitly granted DVM
  encryption/decryption. Bind response sender, request ID and session generation.
- [ ] **Step 2:** verify advertised/returned event public key against configuration;
  never confuse bunker transport pubkey, client pubkey and user/event key.
  Apply T01's exact returned-event validation after remote signing.
- [ ] **Step 3:** handle user authorization/auth URLs as safe private operator
  status, with no automatic browser fetch, payment, permission widening or secret
  logging. Unsupported method or declined authorization fails that operation.
- [ ] **Step 4:** implement bounded reconnect/backoff, same-request recovery,
  timeout and cancelled-response cleanup. A stale response cannot complete a
  newer operation with a reused logical coordinate.
- [ ] **Step 5:** test an independently implemented remote signer through actual
  encrypted Nostr transport: approve, deny, delay, reorder, disconnect, rotate
  key and return altered event. Compare valid output with local signing semantics.

**Oracle and negative control:** independent client/signature checks verify the
expected signer and fields; a valid response signed by the transport key must
fail. Cancellation followed by a delayed reply must not resurrect an outbox
intent. KMS/HSM support remains outside the baseline unless a real adapter proves
the required BIP-340 behavior; generic ECDSA support is not interchangeable.

## P07-T03 — Semantic publication intents and durable exact-byte outbox

**Dependencies:** P07-T01; P01-T05/T06; P02-T01/T07.

**Proposed files:** `apps/bigbrotr/src/publication/{intent,revision,outbox,commit}.rs`;
`migrations/postgres/0703_publication_outbox.sql`;
`contracts/publication/v1/{intent,outbox}.schema.json`;
`config/examples/publication.toml`; `docs/user/publication/durability.md`;
`apps/bigbrotr/tests/publication_outbox.rs`;
`tests/fixtures/publication/outbox.json`;
`tests/acceptance/cases/publication_outbox.py`.

**Boundary:** a target manifest or accepted DVM job plus publication profile
produces an immutable desired artifact and semantic digest, then exact signed
bytes with durable destination obligations. Fenced ownership serializes each
signer/coordinate. A lease lost during signing prevents stale commit/send.

- [ ] **Step 1:** define semantic digest over emitted subject/content/tags/profile/
  algorithm/withdrawal state, excluding job IDs and delivery timestamps. Unchanged
  facts must not be re-signed because a scheduler tick or destination changed.
  Here excluded job IDs are internal execution IDs: protocol request IDs and
  requester/provider bindings in DVM output tags remain part of the digest.
  Coordinate timestamp ordering applies to replaceable/addressable outputs;
  ordinary DVM result identity remains the accepted job and exact result bytes.
- [ ] **Step 2:** persist desired artifact, source lineage, policy and key identity
  before signing. Atomically publish exact signed bytes and destination rows
  before networking. Failure may leave recoverable prepared work, never an
  unrecorded successful send.
- [ ] **Step 3:** implement durable per-coordinate `created_at` ordering. Coalesce
  unsigned changes in the same second or wait for legal next time; do not emit
  unbounded future timestamps. Concurrent workers cannot choose the same new
  replacement time for different bodies.
- [ ] **Step 4:** reuse an unchanged already signed event for newly added
  destinations. Preserve old key/algorithm identities through rotation rather
  than editing pending bytes or claiming a new algorithm under the same identity.
- [ ] **Step 5:** implement stop, crash recovery, policy withdrawal and byte-bound
  admission. Protect source pins for unfinished preparation/delivery where needed;
  completed publication keeps compact lineage/signed originals without pinning
  every old full graph/vector/catalog snapshot forever.
- [ ] **Step 6:** run two real workers with crash injection before/after desired
  commit, sign response, signed-byte commit and first socket write. Restart and
  compare immutable event IDs, coordinate clocks and PG obligations.

**Oracle and negative control:** a static semantic revision table predicts when
an event ID changes. Identical facts after a new generation and a new destination
must reuse bytes. A transport log showing a send before durable signed state is
a failure. A stale worker's signed response cannot commit even when its signature
is valid. Replacing output with a fresh timestamp on every retry fails the fixture.

## P07-T04 — Per-destination delivery, ACK ownership and parking

**Dependencies:** P07-T03; P01-T03/T05–T08.

**Proposed files:** `apps/bigbrotr/src/publication/{delivery,transport,ack,retry}.rs`;
`migrations/postgres/0704_publication_delivery.sql`;
`contracts/publication/v1/delivery.schema.json`;
`config/examples/publication.toml`; `docs/user/publication/delivery.md`;
`apps/bigbrotr/tests/publication_delivery.rs`;
`tests/fixtures/publication/delivery.json`;
`tests/acceptance/cases/publication_delivery.py`.

**Boundary:** `(signed_event_id,destination)` is a durable obligation; transport
attempt identity additionally includes connection/session epoch. Outcomes are
pending, accepted, retryable/unknown, rejected, cancelled or parked. One relay's
acceptance does not complete other destinations or prove future remote retention.

- [ ] **Step 1:** implement allowlisted destination resolution/egress and bounded
  real WebSocket transport. Default at least one accepted target can satisfy the
  service's minimum delivery condition while remaining target obligations continue.
- [ ] **Step 2:** correlate OK responses by event ID and connection epoch;
  coalesce same-event waiters or explicitly reject duplicate ownership. A second
  waiter cannot overwrite the first, and stale cleanup cannot delete a new waiter.
- [ ] **Step 3:** retry exact bytes for unknown outcomes with full jitter 5–3600 s,
  four global deliveries and one per destination/coordinate. Distinguish permanent
  rejection, authentication-required, oversized and transient failures. Never
  redirect automatically to unapproved relays.
- [ ] **Step 4:** enforce aggregate queue bytes and seven-day park state. Parking
  retains recoverable payload/obligation; detail retention is separate from
  correctness tombstones. Optional readback is explicitly configured and bounded.
  DVM obligations have the stricter profile deadline (300 seconds from durable
  result_ready, capped by job expiry); terminal DVM cancellation/refund stops
  undispatched retries. Seven-day generic parking must not revive these jobs.
- [ ] **Step 5:** recheck current authority before dispatch, drain on stop and
  resume outstanding destinations after restart. An already transmitted event
  cannot be recalled, even when later policy cancels its remaining obligations.
- [ ] **Step 6:** run three controlled real relays: acceptance, temporary failure
  then acceptance, and rejection. Drop ACKs, duplicate sends, reconnect and delay
  stale replies; inspect each persisted destination state and recorded bytes.

**Oracle and negative control:** independent relay logs determine which exact
event each target received. A success on target A must not delete B's obligation.
Two concurrent same-ID publications and a stale connection ACK must not report
another target/session as accepted. Exceeding the queue cap must reject new work
without losing admitted signed bytes.

## P07-T05 — NIP-85 assertions from real fact and rank generations

**Dependencies:** P07-T01–T04; P05-T01/T03–T05/T11; P02-T05/T07.

**Proposed files:** `crates/bigbrotr-protocol/src/nip85.rs`;
`apps/bigbrotr/src/publication/nip85/{source,bindings,publisher,cursor}.rs`;
`migrations/postgres/0705_nip85_publication.sql`;
`contracts/publication/v1/nip85-bindings.schema.json`;
`config/examples/nip85.toml`; `docs/user/publication/nip85.md`;
`apps/bigbrotr/tests/nip85_publisher.rs`;
`tests/fixtures/publication/nip85.json`;
`tests/acceptance/cases/nip85_publisher.py`.

**Boundary:** compatible `TargetManifest`/fact records and optional rank
generation produce standard subject kinds 30382 pubkey, 30383 event, 30384
addressable coordinate and 30385 NIP-73 identifier. Every emitted field has a
registered producer, units, scope and absent/zero rule. A rank is optional;
facts-only publication must work when ranking is disabled or withheld.

- [ ] **Step 1:** define exact field bindings for followers, authored/reply/
  reaction/report/zap/activity/topic facts and event/addressable/external
  engagement. Use `d` subject encoding and required NIP-73 context without
  inventing a relay-kind allocation or universal event rank.
- [ ] **Step 2:** wire every binding to an actual stored producer and compatible
  source generation. Validate scope, freshness (initial maximum input age
  172,800 s), units and policy before creating an intent. Missing/unsupported
  facts are omitted or explicitly withhold the subject, never fake zeros.
- [ ] **Step 3:** preserve exact msat facts locally; emit integer sat amounts only
  when the selected aggregate is exactly representable. Bind first post to
  earliest eligible signed authored time, not local receive time. Publish report
  counts as claims and distinct actors only under correctly named fields.
  Bind the exact 30-calendar-day zap numerator/denominator and the inspectable
  30-day UTC active-hours heuristic from P05-T03/T05; fractional sats/day and
  insufficient activity samples omit those optional fields without omitting
  other valid facts for the subject.
- [ ] **Step 4:** implement durable subject keyset pagination (initial page 500,
  minimum eligible events one, topic projection five). Page size is not a total
  population limit. Dirty subjects drive updates; a complete initial/backfill
  sweep has a persisted cursor and can be interrupted/resumed.
- [ ] **Step 5:** enforce one service key per distinct algorithm/viewpoint,
  including personalized configuration. Semantic changes create new desired
  revisions; unchanged subjects reuse signed bytes. Check zero, absent,
  suppressed and withdrawn outcomes separately.
- [ ] **Step 6:** run actual fixture ingestion→derivation/ranking→publisher→relay
  for all four families, including more than 500 subjects, no-ranker mode,
  fractional sats, same-time update and private-source restrictions.

**Oracle and negative control:** independently aggregate fixture facts and decode
captured signed assertions; compare exact subject/tag values to the registry.
An unconnected fixture helper cannot satisfy this test. Plant a changed subject
beyond the first page to detect historical population truncation. Two viewpoints
must never overwrite the same signer/kind/d coordinate. An unavailable rank must
not suppress valid engagement facts or be emitted as rank zero.

## P07-T06 — Provider metadata, preferences, withdrawals and key recovery

**Dependencies:** P07-T03–T05; P01-T06; P08 backup/private management contracts.

**Proposed files:** `apps/bigbrotr/src/publication/{provider_metadata,preferences,withdrawal,rotation}.rs`;
`contracts/publication/v1/provider-profile.schema.json`;
`config/examples/nip85.toml`; `docs/user/publication/provider-identity.md`;
`apps/bigbrotr/tests/provider_identity.rs`;
`tests/fixtures/publication/provider_identity.json`;
`tests/acceptance/cases/provider_identity.py`.

**Boundary:** an operator-authorized identity profile produces kind 0/relay-list
metadata and, only under an explicit own-user preference grant, kind 10040.
Provider capabilities and a user's selected providers are different statements.
Withdrawals affect current local publication policy and authorized signed
deletion requests; remote erasure is not guaranteed.

- [ ] **Step 1:** bind each configured key to operator-authored metadata describing
  algorithm version, viewpoint and source scope. Keep profile publication disabled
  until meaningful content is configured; use the same durable outbox.
  Implement the explicit optional DVM NIP-89 handler announcement from
  `dvm-profile.md`: kind 31990, persisted random d, k=5999, exact profile marker
  and operator-authored metadata stating experimental interoperability. It remains
  disabled by default and is not a kind allocation or user recommendation.
- [ ] **Step 2:** implement optional own-identity kind-10040 preference publication,
  default disabled, with exact kind:tag/provider/relay tuples and explicit private
  NIP-44 option. Never generate endorsements on behalf of other users.
- [ ] **Step 3:** define disappearance/withheld/withdrawn transitions. Stop new
  assertions and optionally issue matching-author kind-5 requests when configured;
  do not replace missing facts with zero or promise remote deletion.
- [ ] **Step 4:** implement controlled key/algorithm rotation and durable coordinate
  clock preservation. Restore starts signing/publication disabled until external
  identity/clock and positive old-owner exclusion are reconciled. A stale backup
  cannot safely infer the newest already published coordinate timestamp.
- [ ] **Step 5:** retain signed originals and compact publication lineage while
  releasing completed physical generation pins according to the canonical
  current-plus-two retention policy. Expose when exact recomputation inputs
  expired instead of claiming permanent reproducibility from a digest alone.
- [ ] **Step 6:** run metadata, private preference, withdrawal and rotated-key
  flows against real relays; restore an older control backup in an isolated
  environment and verify activation remains gated until reconciliation.

**Oracle and negative control:** independently verified signatures identify who
actually endorsed whom. A generated kind-10040 provider capability advertisement
must fail the intended preference fixture. A signed deletion request cannot be
used as proof that the target vanished from an uncooperative relay. Restoring
an old counter and immediately signing must fail the recovery test.

## P07-T07 — NIP-66 monitoring discovery and announcements

**Dependencies:** P07-T01–T04; P05-T06; acquisition actual measurements/documents;
P01-T03/T06.

**Proposed files:** `crates/bigbrotr-protocol/src/nip66.rs`;
`apps/bigbrotr/src/publication/nip66/{source,discovery,announcement}.rs`;
`migrations/postgres/0707_nip66_publication.sql`;
`contracts/publication/v1/nip66-bindings.schema.json`;
`config/examples/nip66.toml`; `docs/user/publication/nip66.md`;
`apps/bigbrotr/tests/nip66_publisher.rs`;
`tests/fixtures/publication/nip66.json`;
`tests/acceptance/cases/nip66_publisher.py`.

**Boundary:** attributed local eligible monitoring measurements and configuration
produce kind 30166 discovery and kind 10166 monitor announcements. Imported
claims cannot be republished as local witnesses. Each published vantage/profile
has a distinct NIP-66 signing identity so replaceable coordinates do not overwrite
incompatible observations.

- [ ] **Step 1:** implement current standard URL and public-key subject forms
  without destructive endpoint normalization. Preserve scheme/path/query and
  non-root trailing slash; non-dialable pubkey-only subjects remain discovery
  evidence. Apply private URL/source policy before intent construction.
- [ ] **Step 2:** select per-field eligible measurements with their actual age,
  method/auth/network scope and document basis. Initial max measurement age is
  28,800 s; successful RTT values convert explicitly to milliseconds. Failed,
  unsupported and not-run checks do not become zero or advertised success.
  For integer-ms wire tags, round the retained integer microseconds to nearest
  millisecond with half-up ties using checked quotient/remainder arithmetic.
  Keep exact microseconds and the conversion rule in API/context; a successfully
  measured sub-millisecond sample may round to zero, a failed check may not.
- [ ] **Step 3:** encode the pinned standard field/timeout semantics and preserve
  recognized legacy decoder dialect as attributed input. The initial encoder
  uses the prose timeout order `["timeout","5000","open"]`; do not emit both
  conflicting orders. An unmerged proposal is not an encoder update.
- [ ] **Step 4:** implement default disabled publisher; discovery cadence 14,400 s,
  announcement 86,400 s. Default public fields include RTT/capabilities/network
  and sanitized NIP-11; geo/DNS/TLS/HTTP detail is opt-in. Geo precision four
  applies only when enabled and valid measurement data exists.
- [ ] **Step 5:** derive announcement check/frequency/timeout claims from actual
  configured/performed capability, with units and intended cadence clear.
  Publish optional kind-0/relay-list metadata through the same exact-byte outbox;
  metadata transport failure cannot lose obligations.
- [ ] **Step 6:** run local controlled probes→measurement archive→fact selection→
  signed 30166/10166 against independent relay/client decoders, then test stale
  samples, imported-only evidence, two vantages, private endpoint and mixed-age
  fields. Inspect the source record supporting every emitted measurement.

**Oracle and negative control:** static standard fixtures plus actual controlled
probe outcomes determine tags. Imported success and local timeout must not
produce a locally witnessed successful RTT. Two observer configurations with one
signing key must be rejected or explicitly combined under a single coherent
profile before publication, never silently overwrite. Independent decoding must
distinguish the selected timeout order from the conflicting legacy/proposal form.

## P07-T08 — Opt-in assertion context and compatible richer protocol drafts

**Dependencies:** P07-T05/T07; P05-T01; P06-T01/T05 for public context retrieval.

**Proposed files:** `apps/bigbrotr/src/publication/context.rs`;
`apps/bigbrotr/src/interfaces/http/assertion_context.rs`;
`contracts/publication/v1/assertion-context.schema.json`;
`config/examples/publication.toml`;
`docs/user/publication/assertion-context.md`;
`docs/proposals/{nip66-timeout-erratum,nip66-measurement-context,nip85-context}.md`;
`apps/bigbrotr/tests/assertion_context.rs`;
`tests/fixtures/publication/context.json`;
`tests/acceptance/cases/assertion_context.py`.

**Boundary:** optional `bigbrotr-context/1` metadata extends explanation while
standard 30166/30382–30385 remains independently valid. The context object binds
field occurrence, definition, units, source/observer/method, window/freshness,
exactness/coverage and public evidence or explicitly restricted attestation.
It cannot authenticate hidden evidence merely by displaying its digest.

- [ ] **Step 1:** implement deterministic bounded context serialization and
  content-addressed publication before signing its reference tag:
  `["bigbrotr:meta","1",sha256,"https://.../<digest>.json"]`. Do not include the
  enclosing event ID in the pre-signing metadata and create a circular hash.
- [ ] **Step 2:** validate public-field privacy before storing the object. Exclude
  internal paths, credential/auth subjects, host IDs and source details outside
  the grant; preserve exact field occurrence and honest retained-input status.
- [ ] **Step 3:** implement immutable context retrieval with bounded cache, policy
  checks and optional origin-constrained client fetch. Default decoders do not
  auto-fetch untrusted URLs; enabled fetch rechecks DNS/IP/redirects, byte/time
  limits and digest before parsing. Bad context cannot alter signed core facts.
- [ ] **Step 4:** implement only the explicitly selected categorical NIP-32
  bindings where useful. Do not use NIP-78 kind 30078 as a generic public extension
  container or adopt conflicting kind/field claims from issue 2195.
- [ ] **Step 5:** prepare the three reviewable upstream documents with pinned
  primary spec references, exact proposed changes, backward compatibility,
  competing interpretations and independent interoperability fixtures. Separate
  the timeout editorial correction from richer measurement context. Do not
  publish an issue/PR or change standard encoding automatically.
- [ ] **Step 6:** run a legacy decoder that ignores the extension and a new client
  that validates it; exercise missing/tampered/private metadata, repeated field
  tags, redirects, DNS rebinding and an unmerged dialect. Preserve ordinary
  standard interpretation when the extension is unavailable.

**Oracle and negative control:** independently hash the exact context bytes and
map each declared field occurrence to the signed event. A tampered object must
fail its context check while leaving the valid event signature recognizable.
A context object containing its own enclosing event ID cannot satisfy the staged
hash/sign flow and fails. A permissive client that fetches loopback from arbitrary
metadata URLs must fail the egress fixture.

## P07-T09 — Complete publisher crash, restore and mixed-load acceptance

**Dependencies:** P07-T01–T08; P05-T13; P06-T09–T11 for the DVM integration case.

**Proposed files:** `tests/acceptance/cases/publication_system.py`;
`tests/acceptance/oracles/publication.py`;
`tests/fixtures/publication/system.json`;
`contracts/acceptance/publication-evidence.schema.json`;
`config/recipes/publication-lab.toml`;
`docs/user/operations/publication-recovery.md`;
`apps/bigbrotr/tests/publication_lifecycle.rs`.

**Boundary:** the packaged system turns real admitted measurements/events and
registered analytical outputs into externally verified publication while
maintaining independent acquisition/read availability. Evidence covers the
signer, semantic revision, durable bytes and every destination state.

- [ ] **Step 1:** freeze expected NIP-66, all four NIP-85 families, metadata,
  withdrawal and DVM result fixtures with independent signatures/semantic
  expectations. Include one subject changed beyond a full publisher page.
- [ ] **Step 2:** run actual CLI services and real PG/CH plus three independent
  relay behaviors and local/remote signers. Record every signed byte string and
  compare externally decoded values to source fact manifests.
- [ ] **Step 3:** kill the process at each persisted boundary, drop signer and
  relay responses, rotate keys, make disk full, expire ownership and partition
  PG. Restart and require exact-byte retries or explicit fenced refusal without
  lost target obligations or stale-policy disclosure.
- [ ] **Step 4:** restore a stale control backup with an advanced external relay
  state and keep publication disabled until reconciliation/positive fencing.
  Check completed generation cleanup releases large artifacts while pending
  publication, compact lineage, signed originals and financial ambiguity survive.
- [ ] **Step 5:** run foreground acquisition/read traffic with a slow signer and
  failing destinations. Measure global queue bytes, tasks, retry rate, memory,
  source pins and workload latency. Enforce backpressure/parking and independent
  service stop/catch-up; do not silently drop admitted output to meet throughput.
- [ ] **Step 6:** produce the acceptance manifest with actual commands, versions,
  fixture/source digests, expected/actual events, per-target states, failure
  traces, recovery outcomes and measured scale. Record any missing optional
  adapter/regtest scenario as NOT_RUN and distinguish it from baseline completion.

**Oracle and negative control:** external signature checks and relay/node logs
are authoritative for the exercised network outcomes; local `sent=true` flags
are insufficient. Deliberately collapse destinations to one boolean, refresh a
timestamp on retry and omit a real fact producer in separate negative controls;
each must be detected by the fixture. An unchanged end-to-end publisher loop
cannot count as a successful test without expected nonempty signed outputs.

## Design review record

These are five substantive planning reviews and corrections, not executed tests.
The central review ledger records cross-phase review and remaining release gates.

| Round | Concrete finding and disposition |
|---|---|
| 1 — Producer/protocol coverage | Wired all four NIP-85 subjects and NIP-66 to actual fact sources; retained facts without rank and exact per-destination retry rather than any-ACK completion (T04/T05/T07). |
| 2 — Identity/data semantics | Bound separate algorithm/viewpoint/vantage keys, precise sat/day/activity fields and compact completed lineage; completed outputs do not pin every old graph forever (T03/T05/T06). |
| 3 — Crash/authority | Required bytes before send, fenced coordinate clocks, stale-backup activation refusal, exact active-probe profile and explicit metadata/DVM-announcement purposes (T01/T03/T06). |
| 4 — Work/state cost | Bounded signer/transport concurrency, queue bytes, destination obligations and source pin lifetime; DVM's short terminal deadline overrides generic seven-day parking (T03/T04). |
| 5 — External proof | Required independent local/remote signature checks, real producer→relay tags beyond the first page, adverse ACK ownership, old/new context clients and complete packaged fault evidence (T01/T02/T04/T05/T08/T09). |
