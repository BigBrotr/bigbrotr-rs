# Go acquisition code walkthrough

Date: 2026-09-11. This is a bounded, full-file implementation reading for the
fresh Rust design. It is not a Go port, a whole-repository code audit, a newly
executed exploit reproduction, or evidence that any product test passes.

The reading followed actual function calls within scheduling, engine execution,
verification/admission, cursor durability, linear/live/negentropy and shared
websocket/guarded transport. It covered **25 complete production files and five
complete test files: 7,331 lines / 273,958 bytes**. The useful patterns are
permit-before-dial admission, one ordered EVENT/EOSE stream, explicit durable
barriers, scoped local-set loading, id-correlated acknowledgements and guarded
proxy routing. The most consequential lessons are where those boundaries fail to
carry enough information: body novelty is not request progress, a seen event ID
is not a signature cache entry, a chosen fallback is not capability evidence,
and a counted dropped frame is not a clean completed page.

## Frozen source and read coverage

Readonly source root:
`/Users/vincenzo/Documents/GitHub/BigBrotr/bigbrotr/`.
Local snapshot root: `.research/code-snapshots/acquisition/`, preserving each
repository-relative path below. Each selected file was captured with exclusive
creation, then read completely from the frozen copy in bounded chunks. A SHA-256
of the original before and after capture and after the complete reading matched
its snapshot in every case: **no concurrent source difference was observed**.
No legacy test/build/formatter/git command was run and no original was written.

The [machine-readable source manifest](evidence/go-acquisition-code-sources.json)
contains every absolute original path, local snapshot path, byte/line count,
hash at each comparison, full-read range and explicit scope exclusions. Ignored
snapshots are code paths rather than published Markdown links. Function/line
references in this document refer to these frozen bytes, not a claim that the
legacy working tree will always retain those line numbers.

| Source path relative to Go root | Full lines read | SHA-256 |
|---|---:|---|
| `internal/synchronizer/service.go` | 1–936 | `82838fcc16ccb4f94a41f5555febd3dfc6fe367a452f203a3ea8613e77ea9f44` |
| `internal/synchronizer/engine.go` | 1–643 | `43d0db49704e999989f6bf639c511e808cb6ed0893a043c64524827c2ad8dc4b` |
| `internal/synchronizer/supervisor.go` | 1–242 | `7110606ec53dc058b528a5833b1a38e6f8e8b597cb8d8511380ad713e493674a` |
| `internal/synchronizer/budget.go` | 1–94 | `749e397053b104fc5f248cac0133d8da719b59062fa9131cf052644135672e85` |
| `internal/synchronizer/sink.go` | 1–364 | `d9e252ec2ddf3e99df5eb945ad1c826dd5667413a2e29598c78e7464ec9e55b0` |
| `internal/synchronizer/barrier.go` | 1–347 | `633c3a48366f41b320836a0a629bb217cddad40b6b38beda5ee9ff6af3194727` |
| `internal/synchronizer/layers_livetail.go` | 1–61 | `5fe83ce7b818b4110fc8d41d55caac7fe6bec6abfbb5f2f520399c7fd5bdea4b` |
| `internal/synchronizer/layers_linear.go` | 1–332 | `dfa4cb1118b12694cfd9ff3ff42a2360bc4452967424ffb73272f8560d5a4830` |
| `internal/synchronizer/layers_negentropy.go` | 1–211 | `c1ef7cd975d0294e8bcda1d218f1a491e33f34354692256e2df44e33bda94f83` |
| `internal/synchronizer/layers_safetail.go` | 1–61 | `ce1738b56f0089868078ff3a9f4f79992729e837585a150d99cb45805fc687b6` |
| `internal/synchronizer/rscoped_loader.go` | 1–59 | `a129258f6ca54f8b6001ad1a9c9b2f6333ebf9411dfa86349613e581d13b32d2` |
| `internal/synchronizer/options.go` | 1–152 | `8c7b1dfa5a8c6f752fd3a3ce75bb93f984f8b143b1d99de60ba36323559cce87` |
| `internal/synchronizer/ledger.go` | 1–22 | `78734512ece21b07613adc7e865ae034dfdf398c9161366dee6ae015dc17311b` |
| `internal/synchronizer/blocklist.go` | 1–121 | `c0d46ea3299249d3c17cfd37495e0a93b02afaea70b44e45819adfeb3b8344be` |
| `internal/synchronizer/eligibility.go` | 1–142 | `dc13fcda6d501be30ebf1e771b17ed7f499acee596018e6b1bd56b80377eaf93` |
| `internal/synchronizer/rotation.go` | 1–27 | `faf9f226774ad55a602424ed36a2fafee302ffd477c07de369b8db9eae1993b3` |
| `pkg/relaystream/relaystream.go` | 1–1,034 | `4ad03ac4ea6ff627b54374bb0156e6db227b3be10974aecf9a1e1cada45c90c4` |
| `pkg/relaystream/session.go` | 1–251 | `2625b9095b0a798513a3f9d1289f7dc6b4c6549ab52b869abc4deab5fd94b494` |
| `pkg/relaystream/caps.go` | 1–170 | `700f215461a8f80d91fbe998b45960977bef3665b41599615c562e084fb9318f` |
| `pkg/relaystream/errors.go` | 1–52 | `636dc53b093b35349d37f88620b20265fcbdb8c52812a7b5f60a5a9cbc88aea5` |
| `pkg/transport/dialer.go` | 1–213 | `0de96114eb9d36ac8ca53e0adb5ef1aa0aeab0c6d0462f15f81c673fd72b036e` |
| `pkg/transport/guard.go` | 1–109 | `80615902600edd3061c7be8bd8d46dec18f55138f04edc1b3c3b1fdad1b23964` |
| `pkg/negentropy/negentropy.go` | 1–322 | `62857e6f8a7f2a5a54eb98be3c73e15225ef74ea1daae4b2e8111f5d382b75c8` |
| `internal/synchronizer/sink_verify_order_test.go` | 1–218 | `4ee60c6d0b9cd3183f928566a78a46990b8cf76ceb3fd353f93737ba109fbca7` |
| `internal/synchronizer/engine_frontier_test.go` | 1–90 | `27dc8d9680592b18f9b01cd09c2af9ffa4df96e89b30c7bd9d01a8d1a313e6e6` |
| `internal/synchronizer/layers_negentropy_bounds_test.go` | 1–241 | `2a4144fac012b75b78cd2214267a0ebd82e55ff2fb44690aeac0c2bb0099b5e1` |
| `pkg/relaystream/streamorder_test.go` | 1–236 | `eff12106df208d28fb43bdb4610369cbe79f2c1bcc7748525474f968b5a57cc8` |
| `pkg/transport/dialer_proxy_guard_test.go` | 1–284 | `91aacc58b007833b41f7958fd53eacd715a55c6dd819afa027bf25ff45411d60` |
| `internal/eventverify/eventverify.go` | 1–54 | `823a8b13cfa0a66bbe061e3ec2d4aff8e54d1d9cd1e529cb253d270f661ee2fd` |
| `internal/synchronizer/probe.go` | 1–243 | `80f5ec7692e27baa7b111b082d0446557a2382ded3bb872fc55b4c1540a65e8a` |

Selection followed the call chain rather than a file-count target. The service's
injected `EngineStarter` is a deliberate boundary: the CLI composition root,
real PG lease/cursor implementation, dedupe/journal engine and indexer are owned
by other walkthroughs. This reading does not prove their production wiring from
a constructor comment. The engine's historical re-verification helper, metrics
wrappers, all test helpers and the third-party Go Nostr parser/cryptography
implementation were not fully audited here. `probe.go` and `eventverify.go` were
added and read fully when the first call-chain pass required their semantics.

Current Rust comparison used the relevant sections of
[acquisition services](../architecture/acquisition-services.md),
[data and durability](../architecture/data-and-durability.md),
[storage services](../architecture/storage-services.md),
[configuration/runtime](../engineering/configuration-and-runtime.md) and
[practical validation](../engineering/security-recovery-and-validation.md).
This task changes none of those contracts; concrete refinements were sent to the
coordinator. Existing design guarantees are identified below rather than presented
as newly invented requirements.

## Actual execution chain and its limits

`SynchronizerService.RunOnce` (`service.go:445–537`) registers filters, lists and
orders relay candidates, checks eligibility, recomposes a dial URL, and tries to
seat one resident engine for each endpoint/filter pair. `startIfAbsent`
(`602–712`) reserves a shared host/global budget before invoking `EngineStarter`.
Its second check under the lock prevents duplicate seats after the nonblocking
permit attempt. The service passes a resident context, so completion of one
reconciliation tick is not intended to cancel its engines. The supplied starter
owns the already-dialed connection passed to `RunEngine`; this reading stops at
that composition seam rather than asserting how every CLI recipe supplies it.

`RunEngine` (`engine.go:73–187`) acquires an optional pair lease, starts a
heartbeat, preloads explicit blocked IDs, calls `ProbeCapability`, starts live
acquisition concurrently, and executes historical/recent work. Historical
`backfillTo` (`403–508`) loads bounded time ranges, selects negentropy or linear,
checks its layer result and calls `commitOwned`. `LiveTail` has no cursor handle.
`SafeTailSweep` calls the same linear implementation; the engine separately
chooses when to propose an updated frontier. Historical re-verification is called
from the safe-tail loop, but its helper internals are outside this bounded read.

Every layer calls `Sink.Admit` (`sink.go:167–281`). In this Go implementation its
success is **buffered acceptance**, not a synchronized durable event receipt:
`JournalWriter` explicitly says sealing/fsync happens at the barrier.
`CommitSegment` (`barrier.go:330–347`) obtains a `PairCommit`, writes only its
settled `Advance` to the cursor store and calls `ConfirmAdvance` after that write
succeeds. The barrier serializes seal/checkpoint/ack work across producers and
retains sealed-but-unrecorded identities for retry. The Rust plan deliberately
changes this interface to synchronized atomic admission receipts; it must not
rename the Go boolean result and accidentally preserve its weaker meaning.

The websocket side is `wsDialer.Dial → readLoop → readMessage → route →
deliverEvent/deliverEOSE` (`relaystream.go:188, 679, 762, 888, 902`,
`caps.go:98`). Event and EOSE delivery uses one ordered stream. NIP-77 sessions
share the same read pump and route by subscription ID. `wsDialer` injects the
shared transport's `DialContext` into the HTTP transport. That dialer resolves
ordinary hosts once, vets candidate IPs and hands a pinned numeric address either
to the TCP dialer or the SOCKS proxy (`transport/dialer.go:82–139`). Overlay
hostnames use their configured remote-resolving route without local DNS.

## Scheduling, ownership and operational outcomes

The fleet reserves scarce seats before networking; a waiting pair holds no
socket and failed peers back off independently. `budgetRelease` uses `sync.Once`
(`budget.go:60–69`), allowing explicit and deferred cleanup without double release.
`Shutdown` joins resident owners through terminal attempt recording
(`service.go:715–736`), and `recordAttempt` uses a fresh bounded context detached
from cancellation (`894–925`). These are useful patterns for Rust cancellation,
permit ownership and bounded shutdown reporting.

Several limits matter more than their explanatory comments:

- `rotate` (`service.go:748–776`) yields only a seat whose engine reached
  safe-tail. A continuously backfilling engine cannot be yielded by that method.
  This is an intentional local rule, not a proof that a larger eligible fleet is
  served fairly. The Rust baseline already makes historical work preemptible at
  bounded request/segment boundaries. Test a relay whose history never finishes
  alongside newly eligible small relays; reserve live/control capacity separately.
- `ceilingLocked` (`403–415`) lowers the admission ceiling when analytical replay
  lags. It does not itself reduce already occupied seats. Rust needs explicit
  semantics for stopping new work, draining active work and retaining live
  coverage under pressure. Queue bytes/age and durable spool reserve are stronger
  control inputs than conflating CH lag with all acquisition availability.
- The host semaphore map is retained in `Budget.hosts` (`budget.go:10–40`); the
  file contains no removal path. This reading makes no measured memory claim.
  The lesson is to account for scheduler/backoff/host metadata that follows
  lifetime distinct endpoints as well as payload queues and active sockets.
- `startIfAbsent` treats cancellation and `ErrLeaseHeld` as clean for backoff
  (`service.go:679–704`), but `recordAttempt` marks every nonnil engine error as
  failed (`911–914`). Some engine cancellation is normalized to nil upstream;
  a lease-held error still has a concrete contrary mapping. Rust's shared
  terminal outcomes must survive the entire caller-to-run-envelope path.
- `commitOwned` checks an in-memory lease-loss signal immediately before invoking
  the barrier (`engine.go:392–397`). `pgCursor.Advance` performs load/merge/upsert
  without an epoch in that interface (`supervisor.go:141–160`). These files do
  not establish an atomic ownership check at the database commit. The current
  Rust fencing/positive-exclusion protocol is a deliberate stronger contract.
- `stopIneligible` cancels reported rejected candidates but does not delete their
  active slot before its owner returns (`service.go:784–806`), preserving the
  no-second-owner condition during drain. Policy-revision enforcement still
  belongs at the Rust admission/commit boundary, beyond a periodic scheduler.

Configuration conversion is useful but not proof of effective behavior.
`OptionsFromConfig` validates and deep-copies filters (`options.go:74–132`), while
its comments acknowledge alternate composition paths. The completion-discipline
string also states a rule not referenced by the linear implementation. Preserve
one effective typed schema in Rust and test actual network/storage behavior of a
changed setting; do not equate a changed intermediate field or matching printed
string with working configuration.

## Verification, admission and provenance

`eventverify.CheckIDAndSignatureShape` (`eventverify.go:31–43`) recomputes the ID
and checks that the signature is 128 hex characters. `VerifySignature` performs
the Schnorr check (`46–54`). `sink.Admit` calls the former unconditionally, then
calls the latter **only when `HasEvent` is false** (`sink.go:168–194`). On a seen
ID it may proceed to a new endpoint edge without checking the received signature
cryptographically. Body identity and the received signature are different cache
inputs: changing only a well-shaped signature leaves the canonical ID unchanged.

This is an inspected local branch, not a newly executed network reproduction.
The selected `relaystream.route → deliverEvent` code contains no explicit call to
`eventverify`, although its interface/comments say events are verified. The
third-party parser was not independently audited here, so the comment alone is
not used to certify the caller boundary. The direct sink contract itself must
be safe for every producer, including import or a future adapter.

The Rust baseline already requires exact verified-payload/signature-aware caching
in [admission](../architecture/data-and-durability.md#admission-transaction).
Add the discriminating case to its acceptance: retain a valid event from A, then
receive the identical signed fields and ID with a different invalid well-shaped
signature from B. It must not create a verified B witness. Follow with the valid
original signature and check that B's witness can still be admitted. This differs
from the existing Go test that sends a forged signature only for an unseen ID.

Useful Go details remain: serialize the event body lazily at most once per admit
and reuse the bytes for size accounting and journaling (`sink.go:219–263`);
write before publishing an ownership mark; emit a new relay edge even when the
body already exists (`286–313`). Concurrent unchecked reads can intentionally
buffer duplicate bodies in this Go path, and deferred marks rely on the separate
journal/dedupe mechanism read elsewhere. Rust instead makes exact identity,
recoverable pending record, producer receipt and sequence atomic under the
single admission owner. Keep that changed concurrency contract explicit.

The Go sink's `(bool,error)` merges several materially different outcomes:
new buffered body, already-held body, new provenance with old body, blocked event,
oversized event and out-of-range kind. Its `emitFirstObservation` returns early
for any seen edge and has no earlier-receive correction path in this file.
Rust's immutable earlier corrections and independent event-level first receipt
are intentional improvements, not inferred behavior of these old functions.
Typed outcomes should name durable new/already-held body, new/existing witness,
explicit policy exclusion, invalid input and retryable failure, with an
appropriate receipt rather than a “new body” flag standing in for all evidence.

## Linear, live and negentropy evidence

The full linear code is more informative than its completion-disclaimer string.
`linearSubPage` (`layers_linear.go:265–332`) counts returned in-time events and
increments `newCount` only when the global sink says the body was newly buffered.
`LinearBackfill` (`31–95`) tracks the largest observed page and permits a page
with `newCount == 0 && page < observedCap` to finish the range. It also uses
all-seen pages while deciding whether a same-second bucket is saturated.
Consequently its notion of page progress depends on bodies previously obtained
from other endpoints and on count comparisons. This is the actual control flow;
this review does not present it as a newly measured missing-event incident.

The Rust [linear contract](../architecture/acquisition-services.md#layer-algorithms)
already rejects body novelty and short page count as completion proof. Preserve
an independent request-walk ID set, qualified EOSE/NIP-67 evidence and durable
per-endpoint witnesses. A controlled fixture should prepopulate the global body
archive from A, then make B return capped pages with a smaller later page and
older eligible data. B's traversal/witnesses must remain correct. The result
remains qualified traversal evidence rather than guaranteed full Nostr history.

The Go overflow routine enumerates 256 initial kind buckets for an unconstrained
kind filter, recursively splits saturated kind sets, and splits authors only if
all supplied authors are exact 64-character lowercase hex (`99–263`). It records
an unresolved second and can continue to older time. Retain this useful finite
partition strategy only within explicit total request/time/byte budgets. A finite
kind universe can still generate costly work per hostile saturated second; time
width alone is not a CPU/network bound. Exact author-prefix enumerability is a
selector property, not something inferred from whatever authors happened to be
returned by a capped relay.

`linearSubPage` checks timestamp membership, while `requestByIDs` admits returned
events before adding their decoded IDs to its `fetched` map
(`layers_negentropy.go:141–197`). Neither local function proves all requested
selector dimensions by itself. Rust must check the full normalized selector and
requested ID set before using a response to resolve that work. Valid but
unsolicited events can be separately accounted according to collection policy;
they must not expand a need-list, discharge another ID or widen a claimed scope.

`LiveTail` (`layers_livetail.go:25–61`) correctly serializes explicit `limit:0`,
ignores EOSE as an event and has no cursor dependency. It also overwrites Since
with the connection's current second. The Rust plan intentionally does not add
that time restriction: a newly delivered event can have an old signed timestamp.
Keep a real socket fixture with such an event after EOSE. Safe-tail uses the
linear walker (`layers_safetail.go:46–61`); its `AdaptiveS` uses twice observed
signed-age bounded by a floor/ceiling. `latenessObservingSink`
(`engine.go:549–562`) only samples newly admitted bodies. Collector order and
already-held bodies therefore affect the metric in addition to signed timestamp
bias. The Rust fixed-window/re-verification choice remains justified; adaptive
policy needs an independent comparison and does not become truth from a local
formula.

The negentropy chain is `NegentropyBackfill → WalkSegments →
ReconcileSegment → fetchNeedList → requestByIDs`. It preserves half-open local
bounds, converts the wire upper bound by subtracting one, and requires algorithm
convergence rather than treating a received NEG-CLOSE as success. Need-list
fetching uses bounded batches, retries only still-missing IDs, then records
advertised-but-unserved (`layers_negentropy.go:96–137`). Preserve the separation
between advertised set membership, successful body delivery and acquired witness.
A generic successful sink result must not silently convert a policy suppression
into a possessed verified event.

`NegentropyBackfill` first materializes the whole supplied interval (`29–69`).
`ReconcileSegment` builds another vector using hex IDs and concurrently drains
both SDK result channels to avoid a bounded-channel deadlock
(`pkg/negentropy/negentropy.go:93–211`). Concurrent drains and cancellation joins
are useful, but a duration/window/frame cap does not bound the cardinality of the
local vector or all result/temporary representations. Rust must budget local
items, encoded/decoded frames, result IDs, retry maps and in-flight body buffers
in the same envelope. Use bounded ordered iteration/spill or visible subdivision;
a one-second high-density interval can still require a gap rather than a larger
allocation.

`WalkSegments` retains a forced smaller width for the rest of its walk and checks
signed-addition overflow (`235–283`). Its `blockedReason` treats every machine
prefix `blocked` as a result-size cap (`300–303`). Preserve bounded adaptive width,
but do not generalize one relay implementation's refusal into a universal NIP-77
cause. Rust's typed interval/AfterMax model and bounded reason-aware subdivision
are already more conservative than these int64-specific choices.

## Capability and authorization classification

`ProbeCapability` caches by relay, attempts AUTH before a new probe and persists
its result (`probe.go:151–195`). `classifyProbe` returns `CapabilityLinear` after
all sent attempts time out (`85–109`); `resultForCapability` converts that fallback
to persisted `unsupported` (`238–243`). Thus the phrase “timeout is never a
demotion” does not survive this actual conversion path. Store **observed probe
result** separately from **algorithm selected for this attempt** in Rust. A
transport-timeout ladder may choose temporary linear work while leaving feature
support inconclusive. Cache keys must include relevant endpoint/auth/filter/
method semantics instead of reusing a capability observed under another access
context indiscriminately.

`Conn.Probe` distinguishes not-sent, uncertain write/session failure, correlated
result and response deadline (`relaystream.go:402–498`). A connection-wide NOTICE
is kept indeterminate rather than unconditionally demoting NIP-77; useful typed
causes survive wrappers. Correlated refusals still need reason interpretation:
an age/resource/auth restriction does not establish universal unsupported status.
NIP-67 extension bytes likewise need retention before an SDK discards them; the
Go `StreamItem` has only Event/EOSE, so copying that enum cannot meet the current
Rust evidence contract.

`Auth` waits a bounded 500ms grace and a challenge without signer returns
`ErrAuthRequired` (`relaystream.go:507–591`), which the engine can treat as a clean
skip. Challenge receipt is real evidence, but not proof that public reads are
forbidden. The Rust baseline deliberately keeps challenge, actual request refusal,
successful identity binding and permitted unauthenticated reads distinct. Test
challenge-plus-public-EOSE and request-scoped refusal separately. Signing remains
explicit and scope-bound.

## Ordered delivery, loss accounting and cancellation

The single `StreamItem` channel preserves EVENT/EOSE wire ordering
(`relaystream.go:97–114, 878–912`). `handOff` bounds a stalled consumer and reports
an error on that subscription while the shared pump can resume serving others
(`835–887`); a zero idle bound permits unbounded handoff. Rust should separate
network idle, consumer-backpressure and finite request deadlines, while keeping
all finite in the supported configuration. A local saturated verification queue
must not be mislabeled remote relay downtime.

Wire order is only the first ordering requirement once Rust runs signature checks
in a bounded parallel pool. Assign a monotonically increasing per-request receive
position. An EOSE outcome can be finalized only after every preceding candidate
has a classified validation/admission outcome and every required admission has
its durable receipt. A bounded reorder/join structure must use the same global
byte permits. Test a deliberately slow verification immediately before EOSE and
faster later work; no checkpoint or completed-page result may overtake it.

`readMessage` buffers only cap-plus-one bytes, drains excess up to a 16MiB
measurement ceiling, and returns size metadata (`caps.go:69–126`).
`droppedOverCap/readLoop` count fully consumed over-cap frames below that ceiling
and continue reading (`relaystream.go:662–702`). A later EOSE can therefore reach
a subscription without a corresponding failure in its event stream. The drop
callback names the connection, not a durable request loss record. Parser/router
losses are also counted separately (`762–801`). These are inspected behaviors;
no end-to-end missing-event reproduction was run here.

Rust may continue safe collection after malformed/oversized input, but must not
launder omitted required evidence into a clean page result. Classify known
invalid records and explicit bounded-policy exclusions; if discarded bytes cannot
be attributed safely, degrade the affected in-flight coverage rather than invent
an empty response. Preserve a bounded connection/request loss summary and test
oversized EVENT followed by valid EOSE. This does not require retaining hostile
payload bytes or calling an invalid object an eligible Nostr event.

`subscription.Close` checks handle ownership under the write lock before sending
CLOSE (`relaystream.go:291–317`). This avoids an old local handle closing its
replacement. In contrast `wsSession.Close` identity-checks deregistration but
always sends NEG-CLOSE (`session.go:101–122`). Inspecting only the registry cannot
prove the replacement survives at the relay. Prefer unique request/session IDs
within a connection epoch; any supported replacement needs matching ownership
checks for both registry mutation and wire close. Test delayed old-handle close
against a controlled relay that records actual outgoing frames. Similarly,
`awaitOK` is keyed by event ID with one waiter; simultaneous identical publish
attempts replace the waiter (`relaystream.go:957–969`). A Rust shared connection
should coalesce identical in-flight payloads or reject/serialize duplicate owners,
with an explicit acknowledgement contract rather than rely on identity-safe
cleanup alone.

## Guarded dialing and useful transport tests

The transport resolves ordinary hostnames once and calls `Unmap` before policy
checks. Denied answers are skipped; an allowed answer's real dial failure is
returned as reachability, not a misleading all-addresses-denied result
(`dialer.go:82–139`). Proxied ordinary hosts still become pinned numeric SOCKS
targets; overlay names remain names. Distinct configured overlay routes and a
failure when no route exists avoid leaking hidden-service names to local DNS.
This is worth preserving independently of the old scheme-less relay identity.

The preflight `CheckEligibility` rejects a hostname if **any** resolved answer is
denied (`eligibility.go:95–133`), while the actual dialer can use its permitted
answers. The new Rust policy correctly has one authoritative guarded-dial result;
preflight hints must not contradict that route's selection semantics or replace
fresh resolution checks. Test mixed A/AAAA permitted/denied responses, actual
socket destination, SNI/hostname validation and proxy target representation.

`Guard.Allow` strips IPv6 zones and unmapps mapped addresses before prefix checks
(`guard.go:88–109`); the denylist includes translation/tunnel prefixes, including
Teredo's specific /32 (`46–73`). Preserve normalization and explicit routing
policy without blindly copying a historical prefix list or the localhost-only
XOR mode. The Rust public/private scoped route contract is already a deliberate
change. `dialViaProxyURL` passes nil SOCKS authentication (`dialer.go:189–207`);
URL parsing alone does not demonstrate credential support. An explicitly
configured credential-bearing adapter must consume supported credentials or fail
validation, never merely accept the string and ignore it.

## What the selected complete tests actually establish

These are readings of test code, **not executions**. Referenced helper files were
not all reread; no claim of complete fixture or suite validation follows.

| Complete test file | Discriminating intent visible in its body | Practical limit and Rust consequence |
|---|---|---|
| `sink_verify_order_test.go` | Twenty duplicate admits should incur one signature check; a new canonical ID with forged signature must be rejected; victim-ID/body mismatch must never mark that victim; valid policy blocks and caps still work. | Duplicate optimization is tested for identical input, not changed signature on an already held ID. Add that cross-endpoint witness case; keep explicit policy exclusion separate from possession. |
| `engine_frontier_test.go` | Inject a layer returning a short frontier without error, assert the injection was reached and no excessive advance; positive control permits valid advance. | Strong caller-contract test, but does not prove the real network layer's completeness claim or PG commit fencing. Rust needs both caller checks and independent known-set socket/engine tests. |
| `layers_negentropy_bounds_test.go` | Configured need-list batch 2 actually changes outgoing ID counts; negative/zero direct-call settings; half-open bounds; one-ID and 200/201/400-ID request boundaries with retries. | Useful observable configuration and off-by-one cases. They do not establish peak local-set memory, signature outcome taxonomy, unordered unsolicited responses or correctness under real relay restriction/closure. |
| `streamorder_test.go` | One ordered accessor, buffered events before EOSE, duplicate EOSE reported rather than delivered twice. | Preserve the ordering behavior; do not import a fixed 200-run ritual or reflection shape as sufficient proof. Add parallel verification completion and dropped-frame evidence cases. Timing and helper claims are not new measurements here. |
| `dialer_proxy_guard_test.go` | Denied non-overlay destinations must not contact a proxy; ordinary DNS is resolved once and proxy target is numeric; overlay target stays a name without local resolution. | Valuable real local-listener/SOCKS harness design. It was not run here and does not independently establish production proxy auth, redirects, TLS SNI, operating-system routes or actual Tor/I2P behavior. |

## Concrete Rust refinements and disposition

These are bounded refinements to existing architecture, not a request to port all
Go modules or preserve its inheritance constraints.

| Refinement | Existing contract and proposed acceptance |
|---|---|
| ACQ-C01: scarce-work fairness and owned permits | [Shared allocator](../engineering/configuration-and-runtime.md#shared-budget-allocator) and [sync scheduling](../architecture/acquisition-services.md#scope-and-scheduling) already bound work. Test never-ending historical work, a new small endpoint, falling byte budgets and clean preemption; include host/backoff metadata accounting. |
| ACQ-C02: typed record outcomes and signature-bound caching | [Durable admission](../architecture/acquisition-services.md#durable-ingest-contract), [exact identity](../architecture/data-and-durability.md#admission-transaction). Carry verified-body/witness/exclusion/error outcome plus durable receipt; test a changed invalid signature on a known body from a new endpoint. |
| ACQ-C03: request progress independent of global novelty | [Linear/live algorithms](../architecture/acquisition-services.md#layer-algorithms) already reject old completion assumptions. Test already-global-known capped pages, full selector mismatch, unsolicited exact-ID response and a backdated live delivery. |
| ACQ-C04: EOSE joins completed admission, with loss evidence | [Durable ingest and queues](../architecture/acquisition-services.md#durable-ingest-contract). Preserve receive positions and bounded verification joins; slow one pre-EOSE admission, then test dropped oversized input followed by EOSE. A callback count alone cannot authorize coverage. |
| ACQ-C05: observations remain distinct from selected fallback | [Capability records](../architecture/acquisition-services.md#shared-records-and-invariants) and [algorithm fallback](../architecture/acquisition-services.md#layer-algorithms). All timed-out probes may temporarily select linear without persisting unsupported. Test AUTH challenge with successful public reads and operation-scoped refusal. |
| ACQ-C06: memory budgets cover the whole reconciliation | [Synchronization limits](../architecture/acquisition-services.md#synchronizer-configuration), [host allocator](../engineering/configuration-and-runtime.md#shared-budget-allocator). Measure a dense one-second local set, ID vector/encoding, result IDs and retries under a fixed total byte envelope; return bounded gap/deferred work when subdivision cannot help. |
| ACQ-C07: lifecycle/acknowledgement identity extends onto the wire | [Endpoint transport](../architecture/acquisition-services.md#guarded-dialing-and-budgets). Unique IDs per connection epoch; inspect old-handle CLOSE/NEG-CLOSE and simultaneous duplicate-ID OK waits. Cleanup of a map entry is not an acknowledgement/cancellation protocol. |
| ACQ-C08: one route policy, explicit adapter capability | [Guarded dialing](../architecture/acquisition-services.md#guarded-dialing-and-budgets). Match mixed-address preflight/dial behavior, preserve ordinary SOCKS pinned IP and overlay remote DNS, and reject ignored authentication/ratio settings. Full destination/SNI/proxy-frame inspection remains implementation acceptance. |
| ACQ-C09: durable cut, ownership and terminal run state stay separate | [Admission transaction](../architecture/data-and-durability.md#admission-transaction), [runtime outcomes](../engineering/configuration-and-runtime.md#shared-lifecycle-vocabulary-and-once-semantics). A refused cursor write can retry its prior durable receipt; a lease-held/cancelled run cannot become failed merely at envelope conversion. Exercise old-owner resumption with the actual control/storage boundary. |

## Verification boundary

Performed: source enumeration and sizing; exclusive snapshot capture; full reading
of every manifest-listed file; SHA-256 comparison before/after capture and after
reading; call-chain reasoning; targeted comparison with Rust contracts; written
findings and proposed acceptance. No source difference was observed during this
reading. Local file/hash checks validate evidence accounting, not product logic.

Not performed: Go or Rust product tests/builds, relay interaction, database fault
injection, proxy connections, capacity measurements, fuzzing, runtime verification
of historical comments, SDK parser/cryptography audit, or complete production
entry-point/storage/re-verification audit. Prior staging numbers and historical
PASS statements found in comments were not promoted into new measured results.
