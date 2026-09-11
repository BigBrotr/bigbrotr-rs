# Review 10: independent acquisition audit

Date: 2026-09-11. Audited baseline: coordinator-supplied revision `b543555`.
This is a planning audit, not a product execution report. All seven assigned
documents were read completely: **2,362 lines / 308,164 bytes**. Their before/after
SHA-256 values agree. This review identifies two high-priority integration gaps,
three medium-priority contract issues and one lower-priority ownership ambiguity.
It does not establish that every repository document was read or that every
choice is optimal.

## Scope and evidence

| Fully read document | Complete lines | Bytes | Role in this audit |
|---|---:|---:|---|
| [Acquisition architecture](../architecture/acquisition-services.md) | 1–499 | 75,351 | Selected acquisition contracts and defaults |
| [Phase 03](../implementation/03-acquisition.md) | 1–401 | 52,640 | Executable-work decomposition and distinguishing acceptance |
| [Phase 04](../implementation/04-relay-lifecycle.md) | 1–400 | 57,473 | Discovery, validation, monitoring and resource contracts |
| [Import formats](../specification/import-formats.md) | 1–106 | 7,181 | Normative format and fidelity boundary |
| [Acquisition research](../research/acquisition-findings.md) | 1–112 | 20,154 | Historical findings and design rationale |
| [Go acquisition walkthrough](../research/go-acquisition-code-walkthrough.md) | 1–449 | 35,206 | Previously inspected code behavior, not new runtime proof |
| [Phase 0–4 recheck](07-phase-0-4-recheck.md) | 1–395 | 60,159 | Historical requirement dispositions and prior review limits |

The [read manifest](../research/evidence/10-acquisition-read-manifest.json)
records full read ranges, bytes, hashes, and separately identified partial
cross-reference reads. Each assigned document was read in bounded line chunks,
including tables, historical corrections, examples and acceptance prose. Searches
located adjacent contracts but were not counted as full reading. A truncated
combined auxiliary search/read response was replaced by complete bounded reads
of the relevant P04/P08 passages; no omitted auxiliary text is claimed as read.

No Git command was used. Consequently `b543555` is the coordinator's frozen
revision identifier, not an independently resolved Git object. The seven actual
working-file byte hashes were checked before and after reading and remained
unchanged. Auxiliary documents have end-of-read metadata, not an independently
proven association with that Git object. Subsequent coordinator repairs are
outside these frozen findings unless recorded in a later disposition.

Severity here is planning priority: **P1** means a required cross-service input
or data-preservation contract is incomplete; **P2** means implementation can
reasonably choose materially different behavior; **P3** means an ownership or
complexity clarification can avoid redundant work. These are not measured
production incidents or exploit claims.

## Findings

### ACQ-10-01 — P1: imported source controls have no selected wire contract

**Competing text.** `docs/architecture/acquisition-services.md:338` requires
source tombstones to exclude removed source bodies before native round-trip
admission; line 354 selects `import.source_tombstones=respect_within_source`.
`docs/implementation/03-acquisition.md:232–234` requires a complete control
snapshot or a first pass staging all source controls, and tests a tombstone after
its body in file order. Its native decoder also names source controls at line
252. In contrast, `docs/specification/import-formats.md:10–22` defines complete
event JSONL, typed native event/evidence Parquet and Pensieve protobuf without
defining a source-control/tombstone family, identity, target semantics or
authority. `docs/specification/data-contracts.md:77–82,317–324` separates portable
scientific lineage from deployment control and calls replay tombstones
operational retention state. `docs/implementation/08-operations.md:113,129–137`
likewise separates research exports from backups containing deletion/policy state.

**Impact.** A native importer cannot implement the mandated exclusion consistently.
An implementer might conflate a signed kind-5 deletion request with an
administrative scrub/replay tombstone, omit useful immutable originals, or invent
deployment-policy import that the archive format never promised. A generic
whole-input control pass also adds an unnecessary second read/staging dependency
to JSONL/Pensieve, which do not carry such administrative records. The text does
not prove that kind 5 is intended to be the tombstone; that unresolved distinction
is the finding.

**Recommended disposition: change/simplify.** Ordinary signed-history imports
retain eligible complete originals and signed deletion requests under the
destination's current admission policy. They do not transfer source deployment
control. Keep administrative replay/scrub state in the explicit restore contract.
If a portable source-exclusion feature remains useful, first define its separate
versioned record family, source namespace, target and authority rules, complete
snapshot proof, and operator-selected import mode. Require a control prepass only
for that declared format/mode. A source statement must never become a global
destination ban. Remove unsupported tombstone promises from JSONL/Pensieve
round-trip acceptance.

**Distinguishing future verification.** Import a signed target plus its signed
kind-5 request in both orders and retain both originals; normal serving policy
may suppress the target. Separately restore an explicit administrative scrub
tombstone and show that it prevents forbidden replay under the restore contract.
A pure JSONL job must start bounded body admission without inventing a full
administrative scan. An unknown control schema must fail its declared mode
before admission, not be treated as an event or silently ignored.

### ACQ-10-02 — P1: zap issuer evidence has no acquisition producer

**Competing text.** `docs/implementation/05-analytics.md:290–298,313–315` requires
retained issuer evidence, prohibits resolving mutable URLs inside analytics,
and refers to an explicit acquisition task. The completely read P03/P04 task
inventories contain no LNURL issuer lookup producer, retained input schema or
historical binding policy. The listed monitor families in
`docs/architecture/acquisition-services.md:195–205,222` and
`docs/specification/data-contracts.md:206–216` do not supply this input.

**Impact.** Signature and invoice parsing can run, but the normal deployment has
no selected path for producing provider-key evidence used by accepted zap facts.
It either remains unresolved indefinitely or tempts an implementation to make
unbounded, SSRF-sensitive HTTP calls from an analytical query. A lookup performed
today cannot establish a provider's historical key association.

The pinned [NIP-57 protocol flow and Appendix F](https://raw.githubusercontent.com/nostr-protocol/nips/a2494f4f81d46684e5814a9bf35e2b1df978f955/57.md)
bind receipt validation to the recipient provider's `nostrPubkey` obtained from
its LNURL response. The specification also limits what a receipt proves about
payment. It does not supply BigBrotr with an authenticated historical directory;
the need for timestamped, non-backdated acquisition evidence is this audit's
design inference.

**Recommended disposition: add one bounded producer, not a new service.** Add a
P04 guarded HTTP task using the existing route, permits, byte caps, source-document
and receipt APIs. Its inputs bind the recipient and source profile/event or
explicit dataset assertion, selected LNURL endpoint and resolution-method version.
Its outputs record actual fetch time, endpoint/redirect context, status, response
digest and declared provider key. Share/coalesce identical eligible endpoint
lookups, cap concurrent and due work, preserve negative/stale results, and permit
network-free imported historical evidence with an explicit evidence class.
Do not invoke the invoice callback, generate a zap request, pay, or backdate a
current lookup. Select the evidence eligibility interval in the central zap
contract and add the concrete task dependency to P05-T05.

**Distinguishing future verification.** Use an actual guarded local HTTPS fixture
whose provider key changes between captures, a backdated receipt, a redirect to
a denied address, and many recipients sharing one provider. The independent
oracle expects immutable captures at their real times, bounded/coalesced fetches,
no invoice callback or payment, and an unresolved historical receipt when no
eligible historical evidence exists. Analytics reruns without networking.

### ACQ-10-03 — P2: pooled sockets and fresh transport measurements need one rule

**Competing text.** `docs/architecture/acquisition-services.md:79,95` selects
multiplexed work and one WebSocket per endpoint/auth scope. Lines 195–199 and
210–211 select five-minute liveness including WebSocket-open timing and slower
catalog checks. `docs/implementation/04-relay-lifecycle.md:219–220` requires phase
durations; line 238 allows TLS fact reuse for matching hostname, port, selected
IP and handshake policy. Neither selected task defines when a pooled open/TLS
sample remains an old reference, when an actual new handshake is required, or
how that handshake obtains the occupied single connection slot.

**Impact.** Plausible implementations either mislabel pooled reuse as a fresh
open/TLS sample, open an extra connection in violation of the selected cap, or
disrupt a live subscription on every monitor tick. The existing rule that only
measured fields refresh is correct, but does not settle scheduling or sample
identity for reused connections.

**Recommended disposition: clarify and reuse.** A probe on an existing socket
measures current request/read behavior only. Open/TLS evidence references the
actual connection/handshake observation and keeps its original timestamp and
source identity; reuse is not an independent fresh sample. Explicitly scheduled
fresh-connect work acquires the same endpoint permit, waits or coordinates a
bounded rotation, and records any displaced live interval. Cache reuse must bind
the actual observation/connection generation and freshness eligibility, not just
matching hostname/IP/policy. No second transport stack or per-tick mandatory
reconnect is necessary.

**Distinguishing future verification.** Keep one live fixture socket open over
two liveness ticks, change the certificate offered to new connections, and inspect
actual accepted sockets. Request timings may advance; old open/TLS timestamps
and bytes must not. A separately due fresh-connect check must respect the one-slot
cap and produce the new handshake evidence only after it occurs.

### ACQ-10-04 — P2: hostname-only fairness permits alias amplification

**Competing text.** `docs/architecture/acquisition-services.md:79` distinguishes
hostname, selected-IP and registered-domain budget keys, but the actual defaults
at lines 94–97 enforce only global, endpoint and hostname limits. P04-T02 at
`docs/implementation/04-relay-lifecycle.md:110–116` bounds metadata and hostname/
route state, while the public-candidate promotion in P04-T01/P04-T06/P04-T12 can
turn many source-discovered public names into future work. The shared allocator
at `docs/engineering/configuration-and-runtime.md:54–61` likewise names per-host
tokens without specifying an aggregate selected-address bound.

**Impact.** Many distinct hostnames resolving to one permitted public address can
consume multiple hostname buckets, concentrating up to the global allowance on
one destination and reducing unrelated fleet progress. DNS pinning correctly
prevents a denied-address connection; it does not provide this fairness property.
Bounded token-map memory also does not prevent aggregate alias traffic. This is
a derivable workload counterexample, not an observed attack.

**Recommended disposition: choose a simple aggregate bound.** Add a selected-IP
concurrency/start-rate bucket after guarded resolution, shared across names and
roles, with explicit defaults and hot-limit drain behavior. Endpoint identity
and TLS validation remain hostname-specific. Document how shared public hosts
can receive scoped operator overrides and how remote-resolved overlays receive
route limits when their final IP is unavailable. A registered-domain ownership
inference or extra domain-classification engine is unnecessary for the initial
fix. If the product deliberately accepts global-only alias exposure, remove the
implied additional fairness promise and state that limit explicitly.

**Distinguishing future verification.** Map hundreds of fixture hostnames to one
allowed address, plus an unrelated endpoint, and capture actual starts/concurrent
sockets. The aggregate address bound must hold while the unrelated endpoint
makes progress. Changing DNS to a denied address must still fail the independent
route check. Do not merge the distinct endpoint observations to achieve fairness.

### ACQ-10-05 — P2: an explicitly removed archival arrival series remains selectable in prose

**Competing text.** `docs/architecture/acquisition-services.md:43` allows a
per-session arrival series when explicitly enabled. P03 at
`docs/implementation/03-acquisition.md:69–74` says no archival delivery-time-series
feature is selected; lines 397–398 specifically record removal of the optional
proposal. The authoritative distinction in
`docs/specification/data-contracts.md:16–23,98–116` retains first local receive and
earlier corrections, not permanent ordinary duplicate-arrival state.

**Impact.** Reintroducing this sentence as a configuration feature creates
delivery-rate-sized storage, privacy and replay obligations without a schema,
budget or task. It weakens the explicit U/P-versus-delivery distinction at the
large target scale.

**Recommended disposition: remove the obsolete optional feature sentence.** Keep
first/earlier observation facts and bounded operational diagnostics. An eventual
deliberate measurement experiment needs its own scientific question, selection
method and budget rather than a hidden ordinary-delivery archive.

**Distinguishing future verification.** Existing P03/P04 duplicate-flood acceptance
should compare permanent body/witness/first-receive cardinalities with a static
oracle while showing only bounded telemetry growth. No additional feature or
new test family is needed to resolve this documentation mismatch.

### ACQ-10-06 — P3: separate monitor projection and analytical resolution ownership explicitly

**Competing text.** P04-T11 at
`docs/implementation/04-relay-lifecycle.md:288–309` proposes monitor-owned
projection/aggregate/publication-view modules and chooses latest field values
plus aggregates. P05-T06 at `docs/implementation/05-analytics.md:327–367`
proposes separate monitor-fact/claim-resolution modules with per-field selection,
freshness, local preference and a public projection. The responsibilities can
coexist, but no task explicitly names the shared reducer or separates the two
output identities and their consumers.

**Impact.** Independently implementing both descriptions can produce divergent
latest-value/denominator rules, two authoritative current views and unnecessary
rewrites. Conversely, making all NIP-66 local publication wait for an optional
cross-observer analytical target would weaken independent operation.

**Recommended disposition: clarify, not add another abstraction.** P04 defines
the local raw-measurement view and its exact local aggregates through the shared
indexing owner; P05 owns cross-observer imported-claim resolution and analytical
generations. Reuse pure field/eligibility reducers where semantics are identical.
Give the local and resolved outputs separate named contracts and state which the
NIP-66 publisher and public API read. Imported resolution must not become a
dependency for truthful local observation publication.

**Distinguishing future verification.** A local timeout plus several imported
success claims must retain the local publication result and independently expose
the resolved-claim view. A single local fixture reduced through both consumers
must agree on any intentionally shared fields and source IDs. Disabling the
cross-observer target must not disable local measurement capture.

## Complexity challenge and choices worth retaining

These are tradeoffs and verification obligations, not additional alleged defects.

| Topic | Audit conclusion and practical limit |
|---|---|
| 100,000-relay liveness | A five-minute target requests about 333.3 endpoint sequences/s. With 32 simultaneous sequences, sustaining that target requires average occupied sequence time below 96 ms before catalog/other work. If each selected sequence occupies a full 15-second timeout, 100,000 such sequences require about 13.0 hours of service time at concurrency 32. Backoff and real latency distributions change the achieved schedule; these calculations are sensitivity bounds, not runtime measurements. |
| Fleet defaults | The architecture already calls cadence a due target rather than an SLA; P04-T12 line 327 explicitly distinguishes real sockets from a 100,000-schedule simulation. Retain that honesty. Add a startup/operator capacity estimate with assumptions and observed backlog, and reduce collection frequency/feature scope deliberately when needed. Raising concurrency alone moves costs into sockets, verification, admission and permanent measurement writes. |
| Full catalog feature breadth | The default check set is broad. Keep simple liveness independent; make each expensive protocol/catalog check earn its cadence through measured utility and cost. An empty-corpus search probe can only report its tested behavior. There is no benefit in a costly repeated compliance claim that the evidence cannot support. |
| NEG scope and memory | Retain endpoint/auth local witnesses rather than global body membership when collecting provenance; imported assertions never satisfy that witness. Provenance-off may use the body-collection objective explicitly. P03-T06 bounds local sets, encoding, results and retries together, and exposes dense-second gaps. A frame-size limit alone would not provide this memory bound. |
| Exact admission and verification | Retain canonical-ID/signature verification before witness admission, including changed invalid signatures on a known body, and receipt-backed progress after ordered verification. The Go walkthrough provides a discriminating historical failure path; it is not a newly run reproduction. |
| Provenance off and import claims | Retain the `enabled-families/require-all/none` policy and signed originals, while skipping prohibited interpreted event→relay claims and event-availability pair state. Prefer streaming body-only JSONL/Pensieve where no relational join is needed; only selected native sidecar joins need external sorting/staging. Avoid making every import pay the cost of the richest artifact format. |
| Permanent measurement evidence | Retaining actual observations, their source documents and uncertainty is useful scientific history. Keep queued/deferred work, ordinary duplicate counters and retry noise operational. Measure intent/terminal/correction writes, dictionaries, object references and both serving/archive costs; P04-T12 already asks for this and for an empirical comparison of compressed wide rows against object normalization. Deduplication is not automatically a storage win. |
| Coverage evidence | Retain the distinction between traversal, witnessed bodies, remote set claims and unresolved gaps. Same-second caps, no body novelty, old signed live events, denied requested IDs, ordered EOSE admission and unserved NEG IDs all have useful proposed negative controls. There is no proof of globally complete mutable relay history. |
| Identity and guarded transport | Preserve scheme/path/query distinctions, scoped auth and actual destination pinning. These are justified by concrete different-endpoint behavior and SSRF boundaries. A public mention is discovery evidence; enrollment and route authority remain separate decisions. |

## Historical text and rejected candidate findings

`docs/reviews/07-phase-0-4-recheck.md:217,296–300` still describes the earlier
365-day raw-monitor default as addressed. The current A05/A06 contract and
P04-T11 select permanent measurement evidence. This historical review must not
drive implementation over the later normative contract. A superseded-status
annotation or historical-decision pointer would prevent confusion; this audit
does not count the old default as a second current retention policy.

The generic envelope sentence at acquisition architecture line 28 is also broader
than the field-level science/control separation. The authoritative data contract
already narrows permanent lineage and excludes transient worker/request detail;
point the summary to that contract instead of implementing a second physical
envelope. This is a summary cleanup, not evidence that current event rows contain
all operational metadata.

The active-write contract already requires a dedicated exact kind-1 profile,
durable signed bytes, scoped authority, matched OK and exact verified retrieval.
It should name the concrete P07 adapter dependency during integration, but this
audit did not establish that active probes are incorrectly required to use the
long-lived publication outbox: bounded measurement intent and session-bound
protocol work can legitimately have different delivery lifetimes. No speculative
second-outbox defect is reported.

The coordinator separately reported a producer retry-retirement/watermark repair.
That new storage contract was not fully reread here and is not certified by this
audit. Integrating its canonical request identity into P03 remains coordinator
work, separate from the frozen findings above.

## Completion boundary

Performed: complete reading of the seven assigned documents, targeted adjacent
contract reading, before/after source hashing, a pinned primary NIP-57 check,
call-contract comparison, resource arithmetic and this evidence inventory.
The practical cases above are proposed implementation verification, not executed
tests. No product or legacy builds/tests, actual relay probes, database mutations,
capacity experiments, Git commands or existing-document edits were performed.

The findings remain open for coordinator integration at this frozen baseline.
A later changed document needs an explicit resolution reference; an agreement
message alone is not evidence that a contract or implementation was corrected.
