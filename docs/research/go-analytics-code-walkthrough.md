# Go analytics and NIP-85 publication: bounded code walkthrough

Date: 2026-09-11. This is design evidence for the fresh Rust implementation,
not a proposal to repair the Go tree and not a certification of that tree.

The useful original product behavior is identifiable in the actual call chains:
derive current state and measured facts from archived events, rank pubkeys from a
curated trust viewpoint, assess relay operations from observations, and publish
attributed assertions only from completed work. The code also shows why a Rust
implementation should not inherit whole-population memory bounds, coupled service
completion, mutable source reads, or publication state that loses destination
failures. Rich assertion builders in a package are not evidence that a running
service produces those assertions.

## Evidence and complete-file coverage

The source root was `/Users/vincenzo/Documents/GitHub/BigBrotr/bigbrotr`, used
strictly read only. Selected files were copied, without overwriting any previous
snapshot, into `.research/code-snapshots/analytics/` in the Rust repository. Each
file was hashed before capture, immediately after capture, and after the full
walkthrough. All 51 files were unchanged across those checks. This establishes
the identity of these captured files, not an atomic snapshot of the whole moving
repository.

The [machine-readable inventory](evidence/go-analytics-code-sources.json) records
every original and captured path, full SHA-256 values, byte and logical line
counts, full-read coverage, and the opening line of every top-level Go function
declaration. Totals are **44 production files and 7 complete test files; 599,900
bytes and 13,702 logical lines**. These are coverage measurements, not quality
gates. All selected files were read in full, in bounded chunks where necessary;
truncated output was reread before marking coverage complete.

References such as `internal/ranker/runner.go:73` mean the line in the captured
version of that original source path. They are deliberately not Markdown links
into ignored snapshots. The exact bytes remain identified by the inventory even
if the working Go tree subsequently changes. No fresh Git revision was queried.
No Go build, test, formatter, migration, database query, relay operation, or
benchmark was run. Source assertions about earlier measurements are treated as
historical claims; reading a test is not running it.

“Observed” below means directly present in the fully read code. “Inference” means
a consequence of that code under the stated conditions, without an executed
reproduction. The CLI composition root, underlying PostgreSQL run/publication
stores, transport ACK implementation, external signing library, migrations, and
unlisted analytical targets were not fully audited here. In particular,
availability, relay clustering, claim-state internals and retention dispatch were
seen as dependencies, not counted as complete-file code coverage.

## Deriver: from journal progress to a completed generation

The call chain is `DeriverService.RunOnce` → `WindowPlanner.Plan` →
`RunRequestFromPlan` → `Runner.Run` → expected target manifest → run start →
target execution → completion verdict → terminal registry record → retention.
See `internal/deriver/service.go:389`, `internal/deriver/runs/window.go:242`,
`internal/deriver/runner.go:98`, and `internal/deriver/runs/completion.go:148`.

Observed useful behavior:

* Planning uses the indexed journal position and the created-at months touched by
  new segments, unioned with dirty months from repairs below the watermark. Late
  arrivals therefore have an explicit route back into derivation.
* A re-derivation gets a new generation and run ID. Starting an already COMPLETE
  run is refused. A failed or partial attempt can be superseded; completed output
  is not deliberately edited under its old identifier.
* Completion is evaluated against an expected set selected **before** target
  execution. Written-empty, suppressed, failed and missing target answers have
  separate meanings. A duplicate identical acknowledgement is allowed; a
  conflicting answer is refused. Completion is not inferred from nonzero rows.
* Consumers check service ownership, profile, run ID, lifecycle agreement and the
  stored completion document. Missing data and an unavailable registry are kept
  distinct. The registry facade delegates persistence rather than inventing an
  unrelated progress authority.

The following limitations are visible in the composed production path.

| Finding and exact source | Observed behavior | Rust disposition and discriminating check |
| --- | --- | --- |
| Dirty-only repair, `runs/window.go:270`, `service.go:183` | Planner deliberately continues when the new segment range is empty, but request construction rejects an empty range even if dirty periods exist. | **Change.** A correction generation can retain its ingestion cut while consuming a new dirty frontier. Test a caught-up deployment repaired below its cursor. |
| Empty progress, `runs/window.go:289` | A segment range with no eligible event periods returns idle without advancing the watermark. | **Change.** Commit an explicit no-output progress result. With a segment cap, put several control-only or below-floor segments before a useful event and verify eventual progress without new input. |
| Selected targets, `runs/completion.go:153`, `runner.go:98` | Target selection narrows the completion obligations; Runner still calls the analytical targets. | **Change.** Select executable dependency closure as well as required outputs. Disable an expensive target and prove its queries are never issued. |
| Broad time span, `runs/window.go:405`, `targets/facts.go:52`, `targets/propagation.go:86` | Current-state targets use the period set; global/social facts and propagation still bind `created_at > Start AND created_at <= End` to its broad span. | **Change.** Pass exact touched partitions and one explicitly typed time convention. Test two nonadjacent months and events exactly at both month boundaries. |
| Source identity, `targets/registry.go:42`, current-state/fact SQL | The run stamp identifies output, but these statements have no source-record frontier or projection-incarnation predicate. `ExecPhase` does not add one. | **Change.** Every target reads a compatible immutable source cut. Inject an arrival between two target reads and require either the same cut or a withheld generation, never mixed membership. |
| Cost refusal, `runner.go:98`, `runs/completion.go:356` | A required clustering target that cannot complete can leave the whole run PARTIAL and prevent its watermark advancing, even though cheaper outputs were computed. | **Change.** Independent target generations and dependency-specific readiness. A refused overlap job must not freeze exact event facts or unrelated current state. |

The period mismatch is concrete: `PeriodSpan` spans the first month's beginning
through the beginning of the month after the last. Its `(Start, End]` fact
predicate excludes the first midnight and includes the next month's midnight;
`toYYYYMM(created_at) IN Periods` does the opposite and skips intervening months.
The genesis-composition test below checks the planned period set, so it does not
by itself establish that all downstream SQL honors that set.

There is a further boundary worth tracing during implementation, not a confirmed
database defect: `runner.go:589` supplies `DirtyMarksThrough` in the start record;
`runner.go:635` does not supply it in the terminal record. Whether the underlying
store preserves or overwrites that starting value was outside this walkthrough.
Rust's progress transaction must state exactly which cursor fields it retains
and compare-and-swaps, independently of struct defaults.

These conclusions refine the selected contracts in
[analytics detail](../architecture/analytics-detail.md#execution-refinements-from-the-go-code-walkthrough),
[data and durability](../architecture/data-and-durability.md), and
[storage services](../architecture/storage-services.md). They do not justify
retaining the old single run as the scheduling unit for every target.

## Current-state and analytical target bodies

`targets/current_replaceable.go:236` reads touched event months, collapses event
duplicates, unions the previous snapshot, and selects the greatest `created_at`
with the lowest event ID as the tie-break. Its first-seen selection includes a
third tie-break component so duplicate winner rows do not arbitrarily select a
later sighting. Both explicit aggregation and FINAL source strategies exist;
the choice is tied to source partition/version semantics rather than a universal
ban on a SQL keyword. Retain the deterministic winner rule and tests over
unmerged duplicates. **Do not inherit the claim that all source fields agree
merely because IDs match**: the fresh admission boundary independently verifies
the complete event body before any projection uses that assumption.

Each new snapshot unions and rewrites previous winners, including untouched
authors. The inference is work proportional to touched source months **plus the
whole previous current-state population**, not merely the changed authors. At
billions of events, the Rust design should use changed-key updates and immutable
partition sharing, while measuring both full rebuild and high-churn paths.

`targets/addressable.go:78` recovers `d` with `min(tag_value)` because the old tag
index discarded order. A missing tag becomes the empty value under the join
setting. This is an explicit workaround for an impoverished index, not a reason
to keep lexical `d` selection when complete ordered tags are retained. Use the
selected protocol parser over verified original tags, and prove that body and
tag expansion come from a compatible source cut.

`targets/relaylist.go:370` correctly replaces a pubkey's **whole** relay list:
it arbitrates winning events before retaining their entries. It reads the winner
body to recover markers lost from the old single-value tag index; filtering to
winner bodies limits downstream working state even when it does not reduce rows
read. This is a useful example of distinguishing scan cost from aggregation
memory.

However, the carried state is only `current_relay_list_entries`. An empty winning
list contributes no entry and therefore leaves no independent winner identity
in that target. **Inference:** after an empty winner, an older list arriving in
a later repair month can be selected against no carried winner and reappear.
The separately derived current-replaceable table does not supply the relay-list
arbitration shown here. A Rust target must persist a winning list header even
when it has zero members. The distinguishing fixture is old list → newer empty
list → still older backfill in a different touched month. Malformed winner data
also needs a failure/invalid interpretation outcome, not accidental equivalence
to a valid empty list.

The selected analytical targets add the following lessons.

| Source | What is actually computed | Retain, change or remove |
| --- | --- | --- |
| `targets/facts.go:95`, `:242` | Distinct event IDs globally/per kind and per author for fixed kinds `{0,1,3,6,7}`, over the run's event-time span. Values are converted to Float64. | **Retain** exact scoped counts; **change** to typed checked integers and registered all-kind/relation facts. Do not label a touched-window count an all-history total, or imply Float64 preserves every integer beyond 2^53. |
| `targets/nip85facts.go:227` | Copies global, pubkey and relay fact families into prefixed generic keys; probes conflicting values before/after projection. | **Retain** explicit fact identities and conflict refusal. **Change** to the canonical assertion registry, typed values, source cuts and immutable target manifests. A prefix alone is not a definition or provenance contract. |
| `targets/propagation.go:158` | Deduplicates `(event_id, relay_url)` to minimum first-seen, reduces each event to observed relay count and max-minus-min sighting spread, then counts and computes exact p50/p95/max by relay-count bucket. Singleton buckets remain. | **Retain** the observed statistic and singleton coverage. **Change** to observer/access-scope-aware evidence, immutable corrections, named time units and explicit population/cut. Never call the spread transport latency or a bound on true propagation. |
| `targets/coverage.go:116`, `:303` | Per-relay distinct carried events and oldest/newest event dates; combines clustering output into an adjusted endpoint count. Summing per-relay coverage counts event-relay associations, not distinct events. | **Retain** those distinct quantities and visible unavailable-cluster state. **Change** “upper bound” language to a heuristic adjusted count with basis; false-positive clustering prevents a census guarantee. |
| `targets/coverage.go:203` | When deduction is at least the nonzero observed count, `DistinctRelayCount` returns 1. | **Remove** silent saturation of contradictory input into a plausible answer. Validate the cluster membership/count invariant and withhold the derived estimate on inconsistency. |

The propagation SQL's comment at `targets/propagation.go:42` reasons from a few
output buckets to negligible exact-quantile cost. Its actual query feeds every
per-event spread into exact aggregate state. Output cardinality does not bound
that state. Streaming the inner event grouping helps one stage, but does not
eliminate the outer exact quantile and distinct-ID working sets. The Rust
verification must account for full input state, external sorting/spill,
temporary disk and correction replacement, not only returned rows.

The [analytical contracts](../architecture/analytics-detail.md) distinguish
signed event facts, local witness facts, imported claims and derived opinions;
those distinctions are stronger than the old generic Float64 fact table. They
also preserve the product's useful history without inventing network completeness
or archive deletion driven by scores.

## Ranker: real inputs, arithmetic, output and completion

`RankerService.RunOnce` (`internal/ranker/service.go:136`) resolves the latest
COMPLETE Deriver run and derives its own ID using `RunIDForDeriverRun` (`:124`).
It skips work if that run already has the latest complete Ranker result, subject
to an in-process retry floor. The ID depends on the Deriver run, not on seed file
contents, numeric parameters, a separate monitor-observation cut or visibility
policy. **Inference:** changing those inputs alone does not necessarily schedule
a new ranking. Rust generation identity and invalidation must include the full
dependency/configuration fingerprint.

`internal/ranker/runner.go:73` verifies the Deriver record, loads generic input
facts, loads the social graph, loads seeds, computes social emission, reads relay
inputs, computes relay quality, validates one combined score batch against a
round-tripped run pin, writes ClickHouse scores, and then records the terminal
PostgreSQL pin. The loaded generic facts are only counted in the returned
`InputFactsRead`; neither algorithm consumes their values in this runner.
Remove that compulsory read unless an enabled algorithm actually depends on it.

The two scorers are pure enough to test independently, but their **service
execution is not independent**: graph, seed or unused-fact failures return
before relay scoring. Even an intentionally empty seed file is discovered only
after loading the graph. The relay observation window is derived from the
Deriver's event-created time bounds, not an independently selected measurement
interval. Retain empty-seed no-emission semantics; change the scheduling to
dependency-specific social and operational generations.

### Social graph and seeded viewpoint

`internal/ranker/clickhouse.go:265` defines the p-tag expansion: run-pinned
kind-3 and kind-10000 winners are joined by event membership to tag-index rows,
deduplicated, sorted, and bounded with a limit-plus-one sentinel. A minimum
winner creation date offers partition pruning, but one ancient winner can make
that floor ineffective. The source comments themselves qualify the cost. Stable
event identity makes tag expansion immutable only when admission and compatible
index visibility actually uphold that premise.

`internal/ranker/graph.go:339` rejects truncated edge reads and conflicting
current winners. The conflict probe can only see divergent versions before a
Replacing merge discards one; the production comment in
`internal/ranker/clickhouse.go:329` explicitly states that limit. Retain the
refusal, but prevent immutable-generation conflicts rather than relying on a
probe after possible data loss.

The graph builder folds valid p-tag keys, deduplicates edges, separates follow
and mute planes, drops self edges, and sorts nodes/neighbors. Target-only nodes
exist. An author with an empty list contributes no expanded p-tag row, so the
edge-based loader alone does not establish that author as an isolated vertex.
The Rust graph definition must explicitly choose whether eligible empty-list
authors are vertices and preserve their headers if they are. It must not equate
“absent from this graph” with “not a real account,” and a pubkey is not a person.

The exact Go arithmetic is in `internal/ranker/trustrank.go:163`:

* Present configured seeds receive uniform teleport mass; missing seeds are
  counted. No present seed causes a declared no-emission outcome rather than
  unseeded global PageRank. The seed parser accepts comments/blank lines,
  deduplicates valid keys and refuses malformed entries (`seeds.go:66`).
* Starting at the seed vector, 100 iterations by default use alpha 0.85. If
  `d` is dangling rank mass and `t` is the seed vector, each step is
  `r_next = alpha * follow_transition(r) + (alpha*d + 1-alpha)*t`.
  Dangling mass returns to seeds, so it does not manufacture rank for an
  unreachable follow ring. The final L1 change is reported, not used to end early.
* After diffusion, each muting source distributes its trust uniformly over its
  mute targets. `adjusted[v] = max(0, trust[v] - mute_weight*penalty[v])`, with
  default mute weight 0.15. Scores are `adjusted / max(adjusted)` on `[0,1]`.
* The optional incoming-follow-degree gate is applied **after** normalization
  (`gate.go:62`); surviving values do not change. If it withholds everyone, the
  emission path returns an invalid-configuration error (`emission.go:114`).

Rust's selected same-source follow suppression and eligible-population percentile
mapping in [analytics detail](../architecture/analytics-detail.md#seed-availability-optional-eligibility-and-curation)
are deliberate algorithm changes. They need distinct definition/version identity
and fixtures; they cannot be described as a faithful port of this Go formula.
Likewise, a policy-empty result should have an explicit withheld outcome instead
of being confused with seed absence or malformed configuration.

The Go iteration allocates two vectors once and scans them in a stable order
(`trustrank.go:272`). That is useful kernel discipline. It follows a more expensive
construction path: raw edge slices, map-of-map deduplication, sorted graph slices,
copied adjacency accessors, and ordinal lists. Each target becomes an int32
ordinal through binary search (`:325`, `:389`), adding `O(E log V)` mapping work;
a deterministic dictionary or sort-merge mapping need not make accumulation
order nondeterministic. Declaring no map in the arithmetic file is not a cost or
correctness proof.

The scale model must distinguish provenance `P` from current social graph `E`,
vertices `V`, historical list revisions and rank-vector storage. Two float64
vectors at 100 million vertices already occupy 1.6 GB before graph construction,
teleport, penalty, result nodes, score rows and ordinals. A 100-billion-edge
provenance archive is not a demand to diffuse over all provenance edges.
Conversely, an allocation-free warm loop does not establish that a complete
graph build fits the configured budget. Exact diffusion can affect a broad
reachable graph after a small list edit; local edge maintenance is not a promise
of exact `O(delta E)` scores.

### Relay operations and their observational limits

`internal/ranker/relayquality_inputs.go:120` first collapses each old natural key
`(relay_url, observed_at, monitor_pubkey)`. Conflicting variants are excluded and
counted, rather than averaged. Integer counters cross the SQL boundary; every
term has its own sample denominator. NULL liveness means not measured, not down.
`sources.go:125` reads monitor counters and then coverage sequentially, stopping
on failure. Output limits bound rows received by Go; they do not bound the SQL
scan or aggregation working state.

`internal/ranker/relayquality.go:249` computes one vote per monitor for each
measured term and combines votes in sorted monitor order with an incremental
mean. This avoids weighting a monitor by its polling count; it does **not**
establish independence or resistance to many identities operated by one party.
Rust must use explicit trusted-observer/viewpoint policy and retain access scope,
vantage and cadence rather than treating a monitor pubkey as an unbiased sensor.

The Go default base weights are uptime 0.5, RTT 0.2 and capability 0.3. Uptime is
the fraction of measured liveness samples up. RTT uses an arithmetic mean and
`max(0, 1 - mean_ms/2000)`. Capability uses the maximum advertised NIP count
relative to 20, capped at 1. Missing terms are renormalized away; an observed
relay with no positively weighted measured term is not scored. Coverage adds
at most 0.1 of the scale, relative to the maximum among scored relays, and cannot
lower the base. The result is clamped to 100 and rounded to six decimal places
(`relayquality.go:528`). Each relay carries a scored/absent basis ledger.

Retain that basis ledger, missing/zero distinction, contradiction handling,
duplicate invariance and explicit units. **Change** the selected formula under a
new identity: the fresh design's measurement intervals, freshness, minimum
evidence, latency distribution and coverage exclusion are not already implemented
here. The Go counter path measures observed sample success, not elapsed-time
availability; advertised NIP breadth is a relay claim, not verified protocol
behavior. Those qualifications belong in API and assertion definitions.

### Score persistence is a two-store handoff

`internal/ranker/pin.go:394` checks every batch row's run, algorithm/version,
subject type and score key against a closed pin, and verifies counts in both
directions. A no-emission member has an explicit reason. `clickhouse.go:164`
validates rows before opening the batch and aborts an unsent batch on failure.
These are useful admission-to-output guards.

The runner writes scores before `SaveRunPin` (`pin.go:480`). A crash between
stores therefore has an uncommitted-output state. It is correct that consumers
require a complete pin, but the selected reader collapses replacing versions
instead of verifying an immutable artifact digest. A retry obtains a new wall
clock `computed_at` while reusing the same Deriver-derived Ranker ID; monitor
inputs and local seed/configuration can also change before the retry. Do not
inherit the assertion in reader comments that same-run duplicates must therefore
be identical. Rust should freeze all inputs/configuration, publish a verified
immutable output manifest, and make recovery resolve staged versus committed
attempts explicitly. Per-identity row counts alone cannot certify row contents
or uniqueness.

## NIP-85: what actually reaches the signing boundary

The wired cycle is `Service.RunOnce` (`internal/nip85publisher/service.go:178`)
→ latest Ranker pin → `PinnedScores` (`scores.go:136`) → `Publisher.Publish`
(`publisher.go:158`) → named Deriver verification and latest Ranker re-resolution
→ score-origin checks → `BuildUserRankAssertions` (`payload.go:35`) → kind filter
→ change decisions → `Emitter.Publish` (`emit.go:87`) → ledger write after ACK.
Ranker owns numerical output; publisher owns protocol translation; Emitter owns
the event assembly/signing/transport call; the signer owns local key access.

Re-resolving latest prevents the caller from inventing authority, but if a new
run completes between the service's score read and the publication gate, the
batch is rejected. This is fail closed, not a consistent snapshot. Rust can
verify one named immutable completed generation under current authorization;
latest selection and generation verification should be distinct operations.

Observed live versus latent capability:

| Surface | Actual selected code | Product disposition |
| --- | --- | --- |
| Pubkey rank | The service calls `BuildUserRankAssertions`, which requires a pubkey subject. Its constructed `SubjectRow` has no facts. | **Retain** attributed pubkey viewpoints, with explicit algorithm/input lineage and scale. |
| Optional fact tags | `facts.go:171` documents `AssembleSubjects` as having no production caller. Its sole fact binding is authored kind-7 count to `reactions_cnt`, with direction acknowledged as an interpretation. | **Change.** Build the useful exact facts from specified parsers, then bind only unambiguous defined measurements. Do not claim that those facts were already published by this cycle. |
| Event/addressable/external assertions | `kinds.go` has dispatch and validation arms, but the production entry point is pubkey-only and these three types have no producer in the inspected chain. | **Retain** subject distinctions and useful engagement facts, not speculative algorithms just to exercise builders. The baseline has no universal event truth score. |
| Relay ranking | Ranker produces subject type `relay`; this publisher rejects it. | **Change deliberately** through the selected standard-compatible or opt-in protocol mapping, not by guessing a NIP-85 kind from a URL. |
| Provider profile | Builder exists; service comments record configuration refusing activation because provider metadata is not wired. | Treat as unsupported in this path, not a shipped named profile capability. Explicit configured metadata is needed if selected. |
| Provider declaration | Kind 10040 uses kinds served, including unchanged assertions, and its own change detection. Result tags come from static bindings. | **Retain** independently retryable publication intent. **Change** the wire interpretation: kind 10040 expresses its signing user's provider preferences, not a provider directory. Advertise capabilities only through the selected appropriate surface and activated/populated bindings; the old table advertises `reactions_cnt` although this cycle provides no facts. |

The generic external validator in `kinds.go:172` assumes `<scheme>:<value>` and
derives `k` from the prefix before the first colon. That is not sufficient for
all NIP-73 identifier namespaces: URLs use `k=web`, hashtags need no colon, and
podcast kinds can contain multiple colon-separated components. This targeted
compatibility check was verified against the primary specification on the report
date. [NIP-73](https://github.com/nostr-protocol/nips/blob/master/73.md).
The addressable validator parses any unsigned 32-bit kind without validating
the selected addressable-kind semantics. These are latent builder limitations,
not observed bad network publications. Use the pinned current definitions in
[protocol evolution](../architecture/protocol-evolution.md), with per-namespace
validators and fixtures; the August schema-name table is not independent proof
of present protocol compatibility.

### Selection, semantic change and delivery retry

`scores.go:136` reads the whole selected algorithm population with a sentinel.
If it exceeds `selection.batch_size`, the service refuses and tells the operator
to raise that value. `publisher.go:317` also requires the batch count to equal the
pin's whole emitted population. These guards prevent silently publishing a
prefix, but the setting is a population ceiling, not a resumable batch size.
There is no durable page cursor in this path. At large populations, retain the
completeness requirement and **replace the mechanism** with immutable generation
membership, bounded durable pages, per-subject decisions and an explicit
generation completion frontier.

`payload.go:335` hashes kind, signing pubkey, ordered tags, content and subject,
excluding `created_at`. That correctly distinguishes semantic change from event
identity and suppresses timestamp-only churn. Rank scaling requires a declared
positive domain, rejects nonfinite/out-of-domain input, and emits integer 0–100.
Keep those boundaries. The digest does not encode a separate algorithm version,
seed/viewpoint digest, destination policy or freshness obligation when they do
not appear in payload facts. Rust must distinguish semantic publication identity,
lineage change, periodic freshness policy and destination retry.

`createdat.go:76` returns the Ranker completion Unix second. It is stable for a
recorded run; there is no per-coordinate monotonic allocator here. The comment
assumes strictly increasing run times, which is not established by the inspected
code. Two changed runs in one second or a clock reversal need a real policy;
equal-time addressable replacement is governed by event-ID tie-breaking, not by
the author's desired revision order. The primary specification's equal-time
rule was checked on the report date.
[NIP-01](https://github.com/nostr-protocol/nips/blob/master/01.md).
The fresh design uses a durable coordinate
timestamp policy and explicit clock-backward/same-second behavior.

`Emitter.Publish` signs once per call, attempts all configured endpoints
sequentially, and returns success if **any one** accepts. The interface contract
requires the transport to wait for an OK; transport internals were not audited.
Zero acceptances preserve individual failure causes. When any acceptance
succeeds, failures at other endpoints are not persisted in this publisher's
result or ledger row. The unchanged payload can then skip the whole set forever.
Adding a destination also does not enter this semantic hash. Rust needs one
delivery obligation per destination, with accepted/refused/unknown/expired
states, while keeping a separate policy for how much delivery constitutes the
operator's publication objective.

The ledger is written **after** transport success, and stores IDs, hash, run
references, event time and wall-clock acceptance time. The inspected path does
not durably store the exact signed event before transmission. Rebuilding under
the same run reconstructs the same unsigned body; byte-identical signatures are
an external signer/library property not demonstrated by this walkthrough.
After an ACK followed by a ledger failure, retry is expected; after a newer run
arrives, old exact-byte retry is not represented as an independent durable job.
Retain at-least-once semantics, but persist the exact signed bytes and per-target
outbox state before networking. Cleanup of eligibility checkpoints is not proof
that previously published assertions have disappeared from remote relays.

### Local signer: concrete boundary, not merely an interface name

`internal/signer/signer.go:29` exposes `PublicKey()` and unrestricted
`Sign(*nostr.Event)`. `file0600.go:43` is the concrete local-key implementation:
open once, inspect that descriptor, refuse a nonregular file or permissive mode,
read and normalize a raw hex key, derive its pubkey, retain the key in a redacting
wrapper, and delegate event signing to go-nostr. Reading once avoids changing
identity mid-run. Signing failures omit the library's potentially secret-bearing
error (`file0600.go:130`). The Emitter checks claimed pubkey against the signer
and copies tag rows before signing (`emit.go:157`, `:196`). Retain these concrete
protections.

The interface itself provides no process isolation, purpose/kind allowlist,
request deadline, publication authorization or independent signed-body check.
The local implementation checks mode but does not check owner UID; it calls
`os.Open` before checking regular-file type, so opening a FIFO can block before
the intended refusal; `io.ReadAll` is unbounded. These are static boundary facts,
not reproduced exploits. The new signer contract should bound credential reads,
validate descriptor ownership/type without a blocking special-file open, and
enforce authorized purpose, kind, key and canonical body at the signing boundary.
It should neither log secrets nor silently use an SDK's cached event ID as a
substitute for recomputing the committed body.

The selected [publication and signing contract](../architecture/derived-read-publication.md)
and [security/recovery checks](../engineering/security-recovery-and-validation.md)
remain the design authority. This walkthrough supplies concrete reasons for
those boundaries rather than a requirement to reproduce Go packaging.

## Complete tests read, and what they do not prove

These seven complete test files were read but **not executed**. Helpers and
fixtures referenced from other test files were not automatically included in
coverage.

| Complete test source | Discriminating behavior present in the file | Limit of this evidence |
| --- | --- | --- |
| `internal/deriver/genesis_composition_test.go` | Composes genesis resolution with a planner over a fake journal; checks oldest-month inclusion, explicit genesis and epoch behavior. | Checks a period set, not all target SQL; resolver implementation and other helper files were not fully read here. |
| `internal/deriver/rerunpremise_test.go` | Exercises COMPLETE refusal against RUNNING/PARTIAL/FAILED controls, new-generation IDs and lifecycle failure reporting. | Fake lifecycle/registry answers do not prove concurrent PostgreSQL fencing or immutable source reads. |
| `internal/deriver/targets/propagationclaim_test.go` | Two arithmetic histories put observed spread below and above actual arrival spread; a textual guard checks a correction in `model.go`. | Correctly refutes a universal latency bound. The textual guard does not inspect every comment or establish physical network behavior. |
| `internal/ranker/relayquality_notmeasured_test.go` | Measured-sample denominators; absent liveness; clean-but-unmeasured relay; zero-weight evidence; impossible sample counts; measured-relay controls. | Pure scorer fixtures establish intended distinctions, not observed polling bias, SQL collapse correctness or runtime performance. |
| `internal/ranker/sources_sequence_test.go` | A first-read failure prevents the second relay-quality read. | A selector fake checks sequencing, not snapshot compatibility or network failure handling. |
| `internal/ranker/graphreach_test.go` | Enumerates declared test-only graph accessors and compares against textual production selector calls. It explicitly describes its overapproximation. | A reachability inventory is neither a graph-correctness test nor a complete typechecked caller audit. Do not port this as a substitute for behavior tests. |
| `internal/nip85publisher/declarationretry_test.go` | A stateful fake ledger distinguishes already accepted assertions from failed declarations; restart-shaped service reconstruction retries a declaration after sink or ledger failure; empty and nonempty controls differ. | Does not exercise real signatures, per-relay ACKs, PostgreSQL persistence, process crashes or the new outbox. |

The strongest reusable testing idea is composition with a negative control: a
test must fail for the plausible wrong interpretation, not just observe an
in-range number. A large inventory of source-shape checks and historical green
tests cannot replace implementation-specific verification of Rust behavior.

## Practical acceptance work for the Rust implementation

These are required future checks tied to the selected contracts, not results
obtained in this design task:

1. **Source progress and immutable cuts.** Dirty-only repair, no-output segments,
   missing projection chunks, parallel arrivals, incompatible incarnation and
   two writers racing generation activation. Assert membership and cursor
   ownership, not just identical aggregate totals.
2. **Current-state replacement under churn.** Ordered repeated `d` tags, same-time
   ID ties, many revisions per author, empty-list headers, older late arrivals,
   deletion-aware eligibility and full-history retention. Compare incremental
   output against a rebuild at the same cut.
3. **Facts and provenance corrections.** Duplicate deliveries do not increase
   logical counts; earlier sightings retract/replace affected contributions;
   access domains and observers do not collapse accidentally. Verify integer
   boundaries above 2^53 and explicit overflow behavior.
4. **Large analytical state.** Many unmerged source parts, sparse touched months,
   one ancient current list, wide-event fanout, skewed relay buckets, exact
   quantile spill and high revision churn. Measure full source scans, temporary
   bytes, memory and manifest/partition amplification. A small returned vector
   is not the memory bound.
5. **Seeded viewpoint and full ranking cost.** Empty, absent and partially present
   seeds; target-only/isolated vertices under the declared policy; unreachable
   rings; dangling mass; mute interpretation; gate-empty outcomes; configuration
   changes without new events. Compare the reference result and identity, and
   measure graph load/mapping/normalization/artifact writes in addition to the
   iterative kernel. Do not promise local exact updates where effects are global.
6. **Relay evidence independence.** Equal and unequal polling cadences, unavailable
   versus down, contradictory observations, multiple identities at one vantage,
   imported claims versus local witnesses, expired evidence and a broken social
   graph. Operational output must retain its basis and proceed when its own
   dependencies are healthy.
7. **Actual assertion production.** Follow data from the registered parser through
   committed facts to signed tags. Test every enabled subject family, namespace,
   amount unit and direction; verify declarations match supported populated
   bindings. Do not infer production reachability from exported builder tests.
8. **Paged and signed delivery recovery.** More subjects than one page, page-boundary
   crash, one-of-many ACK, timeout after possible acceptance, new destination,
   same-second revision, backward clock, signer/ledger outage and restart after
   signing. Exact bytes remain recoverable; per-destination retries survive
   unchanged semantic values and new analytical generations.

The resulting product choice stays narrow and useful: immutable verified history,
incrementally maintained exact facts/current state, explicit seeded pubkey
viewpoints, operational relay measurements, and clearly attributed compatible
assertions. Event and external-subject facts do not require inventing a universal
truth score. Optional recommendation algorithms must earn their own definition,
input dependencies, threat model, cost budget and verification.

## Full-read source inventory

The compact table below lists every complete file read. Full hashes and function
locations are in the linked JSON inventory; hash prefixes here aid comparison
only.

| Original source path | Logical lines | SHA-256 prefix |
| --- | ---: | --- |
| `internal/deriver/genesis_composition_test.go` | 195 | `a9d8c26bdd9e6075` |
| `internal/deriver/readpin.go` | 66 | `ecdd1170bb1ce3d7` |
| `internal/deriver/rerunpremise_test.go` | 198 | `bd75844cec608963` |
| `internal/deriver/runner.go` | 807 | `1e9224af956e508a` |
| `internal/deriver/runs/checkpoint.go` | 131 | `ddc21e9c245554fd` |
| `internal/deriver/runs/completion.go` | 417 | `7834254c9ae33a48` |
| `internal/deriver/runs/registry.go` | 327 | `f358b759cb3d1f22` |
| `internal/deriver/runs/request.go` | 135 | `08102fc7eca72705` |
| `internal/deriver/runs/window.go` | 417 | `4107e2d1aa9efe2b` |
| `internal/deriver/service.go` | 446 | `d0b2b91cc6a6f402` |
| `internal/deriver/targets/addressable.go` | 313 | `c83cc858469f2c73` |
| `internal/deriver/targets/coverage.go` | 379 | `8659633d8a0d9354` |
| `internal/deriver/targets/current_replaceable.go` | 306 | `6e88c6f10c656935` |
| `internal/deriver/targets/facts.go` | 280 | `210425e175cdc830` |
| `internal/deriver/targets/nip85facts.go` | 307 | `5852f8f78f99d95a` |
| `internal/deriver/targets/propagation.go` | 282 | `ea429295e4172cb7` |
| `internal/deriver/targets/propagationclaim_test.go` | 73 | `b58fdad135e394e0` |
| `internal/deriver/targets/registry.go` | 518 | `00edda7e9f3620a8` |
| `internal/deriver/targets/relaylist.go` | 461 | `a294d3fdc6d841d5` |
| `internal/nip85publisher/createdat.go` | 114 | `d73c97f3d1523887` |
| `internal/nip85publisher/declarationretry_test.go` | 260 | `759f465de13cfad9` |
| `internal/nip85publisher/emit.go` | 265 | `3c2cc0ad45f81430` |
| `internal/nip85publisher/facts.go` | 293 | `50bbb128d1c0aecd` |
| `internal/nip85publisher/kinds.go` | 180 | `66c38c6aa19441b7` |
| `internal/nip85publisher/payload.go` | 348 | `90bb5bfedbd39c14` |
| `internal/nip85publisher/provider.go` | 190 | `c83d14073c5e6cad` |
| `internal/nip85publisher/publisher.go` | 371 | `60b9a510df9763c9` |
| `internal/nip85publisher/runpin.go` | 86 | `32917ee9622ea6a6` |
| `internal/nip85publisher/schema_pin.go` | 216 | `235f8426d7eaf863` |
| `internal/nip85publisher/scores.go` | 191 | `aee1f869b90394a0` |
| `internal/nip85publisher/service.go` | 366 | `9c6d705784238d71` |
| `internal/nip85publisher/state.go` | 160 | `8ba6e1a3e327ac9c` |
| `internal/ranker/clickhouse.go` | 393 | `33b711722ead1d61` |
| `internal/ranker/emission.go` | 188 | `350017b28c6d8747` |
| `internal/ranker/gate.go` | 87 | `3ca13d0e1215d4e6` |
| `internal/ranker/graph.go` | 428 | `618bd10cc2a7b383` |
| `internal/ranker/graphreach_test.go` | 217 | `610fbf1340594f6b` |
| `internal/ranker/inputs.go` | 46 | `ac23083456ebeca2` |
| `internal/ranker/pin.go` | 588 | `fb00805a199318fd` |
| `internal/ranker/relayquality.go` | 670 | `1d510b40d6121d0c` |
| `internal/ranker/relayquality_inputs.go` | 237 | `3daa4611270e25d3` |
| `internal/ranker/relayquality_notmeasured_test.go` | 224 | `7a0a98a1217b1ae1` |
| `internal/ranker/runner.go` | 297 | `68d61ac0fb433cca` |
| `internal/ranker/seeds.go` | 152 | `6df372958f460228` |
| `internal/ranker/service.go` | 243 | `a9db30d5b377be8e` |
| `internal/ranker/sources.go` | 142 | `9f4cffdd249ef9b1` |
| `internal/ranker/sources_sequence_test.go` | 42 | `9846acc7f2cc6bfc` |
| `internal/ranker/trustrank.go` | 425 | `d5b09d7a1215c7b9` |
| `internal/signer/file0600.go` | 148 | `5cb2e3c73ba7c546` |
| `internal/signer/pubkey.go` | 33 | `c2983e88568124de` |
| `internal/signer/signer.go` | 44 | `67a84a9f8aececa2` |
