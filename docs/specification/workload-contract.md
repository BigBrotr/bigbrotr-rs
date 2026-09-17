# Mixed-workload acceptance and independent fixture contract

Status: selected S07 paper contract, 2026-09-17. Workload rates, target latencies
and completion ratios below are **provisional engineering inputs/objectives**, not
measurements or advertised capacity. The existing
[capacity dossier](../engineering/capacity-and-benchmarks.md) retains historical
measurements and their limits. This contract fixes what future candidate/release
comparisons must attempt and report; it does not assert that Tux supports 100,000
busy relays or that the future BigBrotr executable has run.

## Manifest, counts and host envelope

Every run binds a versioned workload manifest with fixture/seed digests, workload
variant, recipe/configuration digest, engine/binding/build versions, topology,
visibility/provenance modes, source/serving cuts, algorithm definitions and the
release-bundle manifest. Record planned and actual start/end, warmup, observation
period and failure schedule. Give each phase a separate result; a successful
small phase cannot mark an unexecuted larger one PASS.

Use the tuple
`(R_registered,R_active,R_due,D,U,P,C,S,M,V,E,H,K,Q,age,maintenance,faults)`:

| Symbol | Count and independent accounting |
|---|---|
| R | Registered eligible endpoints, active sockets, due measurement obligations, attempted/finished/deferred endpoints are separate values. An idle socket is not a synchronized endpoint. |
| D | Valid in-scope verified deliveries/s offered to admission/certification; also count raw frames/bytes, malformed/invalid/oversize/excluded inputs and cancellations before verification. |
| U | First retained bodies/s; body dedup is independent of scope eligibility and signature variants. |
| P | Newly retained distinct scoped event/endpoint witness edges/s, including the first edge of a new body. Repeat delivery is not permanent P. |
| C / S | Earlier receive/witness corrections and new scope eligibility effects; count actual immutable source facts separately from U/P. |
| M | Measurement executions with actual start/terminal evidence, plus distinct intent/interrupted counts; registry queue entries are not completed measurements. Report liveness, catalog, availability and active probe families separately. |
| V/E/H/K | Social graph vertices/current edges/historical input tag occurrences/concurrent viewpoints; none is provenance P. |
| Q | Offered queries/jobs per class, attempted/admitted/completed/refused/cancelled/restarted counts, bytes, service time and end-to-end queue age. |

Nominal workload `mixed-v1` below has duration 60 minutes after a 10 minute warmup,
followed by a 15 minute recovery window. At least two observed engine maintenance
cycles are required for a sustainable claim; if they take longer, extend the
registered observation window and report the actual duration. A brief run is a
burst/vertical slice. Aged-store setup is outside timing and its own cost is
reported. All four scales use the same shape/mix and offered-work accounting.

| Track | Registered R / live socket target | D/s | U/s | P/s | Liveness obligations/s at 300 s cadence | NIP-11 obligations/s at 3600 s cadence | Q/s | Graph V / E / H / K |
|---|---:|---:|---:|---:|---:|---:|---:|---|
| `fleet-5k` | 5,000 / 2,500 | 2,500 | 250 | 1,250 | 16.667 | 1.389 | 20 | 100k / 1M / 5M / 1 |
| `fleet-10k` | 10,000 / 5,000 | 5,000 | 500 | 2,500 | 33.333 | 2.778 | 40 | 200k / 2M / 10M / 2 |
| `fleet-50k` | 50,000 / 25,000 | 25,000 | 2,500 | 12,500 | 166.667 | 13.889 | 100 | 1M / 10M / 50M / 4 |
| `fleet-100k` | 100,000 / 50,000 | 50,000 | 5,000 | 25,000 | 333.333 | 27.778 | 200 | 2M / 20M / 100M / 8 |

Cadence rates are exact R/300 and R/3600; decimals are displays. All endpoints
participate over the schedule even though only half have live sessions at once;
bounded rotation records actual connection generations/gaps. `C=0.001D` and
`S=0.0005D` are offered correction/new-scope rates, using quotas across the whole
window so fractional per-second rates are not rounded away. In each complete
window, 10% of D introduces a new body, 40% adds a new edge to a known body, 50%
has an existing edge. New-scope S is selected from the 40% known-body/new-edge
population and is already included in P: a new visibility scope cannot already
have a scoped witness or receive minimum. Earlier-correction C is selected from
the 50% existing-edge population; C and S are disjoint for one delivery.
At provenance off, P/witness-C workloads are replayed as repeated
bodies without storing edge state; new scopes/event receive facts remain real
effects. This does not call body-only processing equivalent to provenance-on cost.

Each 5k/10k/50k/100k track also has a required **all-live-controlled-reachable**
variant with `R_active=R_registered` and 100% controlled reachable endpoints.
Hold aggregate D/U/P/C/S/Q and event distribution at the table values first,
halving per-socket delivery rate relative to the half-active mix; this isolates
socket memory, scheduling, polling and control overhead. Then run the same 2x
five-minute and 5x ten-second bursts. Observe real open/serviced sockets and
per-endpoint progress, not merely registered tasks. This variant deliberately
differs from the nominal 90%-reachable measurement population below and does not
claim real public relays are all reachable. At 50k/100k, supply a separately
budgeted multi-host/load-generator topology or report NOT_RUN; no promise that
all parallel sessions fit the reference host. Controlled routes/IP aliases and
any explicit test-only destination quota override are recorded; silently bypassing
the production per-address limits cannot establish their fairness.

Measurement denominators are scheduled obligations. Nominal controlled peers are
90% reachable; 5% consistently refuse connection and 5% time out at bounded
deadlines. Local budget deferral remains a different outcome and cannot inflate
remote failure/uptime denominators. Active signed probes are disabled in the
ordinary mix; their explicit acceptance variant tests 1% of endpoints/hour with
bounded allowed test keys/relays. Event availability jobs select 100 existing
events across 20 endpoints every 10 minutes on the 5k track, scaling selections
by R/5000; they have separate P-sized evidence and explicit provenance requirement.

The default synthetic distribution uses seed `0xB16B20260917`, a manifest-pinned
generator implementation and deterministic per-ordinal SHA-256 selection. Exact
fixture artifacts/digests, not seed alone, establish reproducibility across changed
generators. Assign finite-window categorical quotas by largest remainder with
lexical category tie-break; order choices by seeded hashes. Record actual U/P/C/S
and successful offered bytes instead of assuming the target generator achieved
them. Cardinality impossibility or generator saturation fails setup, not storage.

The 5k track is a candidate on the declared single-host recipe, 10k a separate
capacity test, and 50k/100k require a justified multi-host/load-generator placement
or an explicit virtual scheduling-only result. Do not label a virtual endpoint
list as 100k network peers. Initial Tux reference hardware is the existing
16-core/32-thread, approximately 64 GiB/NVMe/1 Gbit host; the research grant may be
much smaller and must be recorded. Test host OS/kernel/filesystem/device,
cgroup memory/CPU/I/O, network path/RTT/bandwidth, disk free/reserve and other active
workloads at start and throughout. Client/simulator/signature generator grants
are separately counted; loopback shares CPU/cache and does not establish WAN
capacity. Shared-host pressure that violates the declared grant makes performance
comparison inconclusive; correctness faults still count. No global cache drop,
root-volume fill, unrelated process stop or host reboot is a test prerequisite.

## Content, topology, age and competing work

| Dimension | Required controlled distribution and variant |
|---|---|
| Complete event envelope | Nominal quotas: 80% 1 KiB, 15% 4 KiB, 4% 32 KiB, 1% 256 KiB complete normalized JSON; exact emitted sizes may vary by fixed fields and must be reported. Conservative mean target 5.24 KiB gives about 12.79 MiB/s payload at D=2500 and 255.86 MiB/s at D=50000 before framing/TLS. The latter exceeds one 1 Gbit link. Include a separate exact maximum-eligible event and over-limit rejection corpus. |
| Tags | 70% 0–4, 25% 5–32, 4% 33–256, 1% near permitted element/byte cap; duplicate keys/values, empty elements, ordered extra fields, Unicode and high fan-out. Whole-event E/node/arena caps win; impossible category combinations are rejected by fixture setup, not silently changed. |
| Text/search | 45% English, 15% Latin multilingual, 15% CJK, 10% Arabic/Cyrillic, 10% emoji/URLs/code, 5% opaque/nonlinguistic text. Controlled popular/rare tokens and phrases plus empty content. Exact term/phrase membership has literal expected results; human-language relevance is assessed separately with a declared relevance set. Hash-rich synthetic padding is a storage stress variant, not natural compression or relevance evidence. |
| Remote behavior | 50% correct NIP-77+REQ, 25% REQ-only complete windows, 10% capped linear pages/same-second density, 5% advertise-unserved IDs, 5% slow/refused, 5% unreachable/timeout. Pin each controlled peer behavior; the measurement reachability distribution above is independently realized and reported. |
| Visibility/order | 80% public-only inputs, 10% private-only, 10% same event across both in both orders. Alternate valid signature variants retain scoped representatives. Imported full bodies, attributed claims and actual network witnesses remain distinguishable. |
| Duplication/skew | Uniform plus top 1% endpoints carrying 50% of D, remaining 99% sharing 50%. Event/endpoint fan-out independent of body novelty: sparse, dense and hot-event variants. Test D fixed with U/P independently varied, including 99.9% unchanged deliveries and novel-edge-heavy traffic. |
| Time/late data | 90% event times in last hour, 9% previous 1–48 h, 1% 2–365 d; delivery delay independent of author time. Inject backdated arrivals in every configured revisit band; invalid/future/boundary u64 events have separate oracle categories. Correction arrival cuts are later even when receive time becomes earlier. |
| Hostile input | Additional raw frames equal to 2% of D invalid signatures/IDs, 0.1% over limits, 5% out of acquisition scope; compressed expansion, truncation, slow fragments and bad EOSE hints have bounded dedicated scenarios. These are counted in total wire/CPU cost and never admitted as valid U/P. |
| Aged storage | `fresh`; `aged-20m` =20M bodies/100M distinct scoped witnesses/10M completed measurement records across 365 daily partitions; and `cache-exceeding` sized until measured active representation exceeds the assigned engine cache by >=2x. Required rebuild/pins/compaction reserves still fit. No assertion that a fixed row count exceeds cache. |
| Parts/fragmentation | Add randomized arrivals and small low-traffic batches, late corrections into older partitions, then ordinary maintenance for >=2 cycles. Compare fixed part/fragment counts and pending debt. Fresh bulk-sorted loading alone cannot stand for aged service. |

The remote behavior and reachability percentages deliberately describe different
axes; their exact cross-product assignments are in the manifest. Useful traffic
never depends on public relay uptime in deterministic release acceptance. Small
dated permitted real signed samples assess parser/content variation separately;
source/license/consent limits and sample selection are recorded. Never stress
public relays to reach the offered rates above.

The query mix is fixed per complete observation window: 20% event ID lookup,
15% author+kind+time, 10% tag→bodies, 10% time-ordered list, 10% search, 10% forward
provenance, 10% reverse provenance, 5% exact bounded counts, 5% current-state/profile,
5% named graph/score/explanation. ID requests include 10% known missing IDs. Each
scan family is 50% selective (<=100 eligible results), 40% moderate (101–10,000),
10% broad (above 10,000); broad work follows cursor/job/budget rules. Query outputs
carry declared scope/generation/source cut and expected results. Provenance-off
keeps the requests and expects typed unsupported responses but reports their
count separately; it does not inflate useful completion by calling them empty.

In addition to Q, start one projected export of 100,000 eligible rows every
15 minutes, one resumable full corpus verification sample every 30 minutes,
hourly coordinated backup attempt (start one during the timed hour), archive/CH/
Rocks normal maintenance, and one complete graph generation/rank job at minute
10. K viewpoints contend under the host heavy-job grant; waiting is measured.
One continuous backfill offers D/2 additional historical deliveries/s at the same
shape, and one import offers U/2 new bodies/s for minutes 20–30. These are **in
addition to** the live D/U/P table and are separately reported; offered total
during import is not D. Backfill is 95% repeated body/edge, 5% new witness on known
body, so no extra U; import has no invented local witness. Archive-only omits Q/
graph but keeps scheduled exports/offline jobs under the same reserve rules.

## Usefulness, overload and target selection

Correctness is nonnegotiable at any offered load: no acknowledged fact lost inside
the declared failure class, no invented coverage/witness, no scope leak/private
influence, no phantom COMPLETE, no unbounded queue or automatic scientific-data
deletion. Failed/cancelled/refused work must be visible and cannot count as a
successful empty response. Performance targets apply within an explicitly
registered admitted-work envelope; overload is separately tested at 2x offered
live D/Q for five minutes and a 5x ten-second burst every five minutes.

| Class | Nominal provisional objective | Overload/recovery observation |
|---|---|---|
| Control/health/ACK/cancel | p99 <=1 s response; <=5 s to observe cancellation request, followed by its bounded safe checkpoint/termination outcome | Reserved lane keeps p99 <=2 s while dependencies are healthy. Report stuck actual drivers; timeout alone does not free grants. |
| Valid admission/certification | p95 <=100 ms, p99 <=500 ms for admitted operations; >=99% of valid nominal offered D obtains a resolved durable/certified outcome within 5 s | Refusals and unknowns reported by cause; accepted-to-safe-boundary progress continues on reserved capacity. Do not drop observations yet claim EOSE. |
| Online source projection | p95 admitted-to-queryable <=2 s, p99 <=10 s | After 5 minute CH pause, old qualified reads or explicit unavailable; restore replays >=1.5x steady admitted source-fact rate and catches up within 15 minutes while intake continues at registered nominal rate. |
| Archive publication | Quiet single eligible event reaches committed archive within 60 s when healthy; p99 admitted-to-archive <=120 s under nominal mix | After 5 minute archive pause, accepted pending preserved and intake halts at high water. Resume honors unknown-commit resolution and clears lag within 15 minutes at sufficient measured completion capacity. |
| Monitor/fairness | >=99% reachable due endpoints actually start by due+60 s; every eligible endpoint either starts by due+300 s or carries an explicit local/remote deferral reason | No endpoint has repeated due-cycle deferral while younger eligible equal-cost work in its class always proceeds. Report worst/per-decile age and observations, including permanently cold endpoints. Unreachable remote completion is not required. |
| Ordinary finite reads | ID/current lookup p95 <=200 ms/p99 <=1 s; selective list/tag/search/provenance p95 <=1 s/p99 <=3 s; >=99% admitted ordinary requests complete under nominal mix | Report offered admission >=95% separately; bounded refusals are valid safety but failing this usefulness objective prevents a nominal pass. Broad queries have explicit budget/refusal or export path. |
| Default chronological traversal | >=95% of 10-page controlled ordinary traversals finish within the configured cursor lifetime at 2 s client delay/page; no repeated event IDs, monotone scanned position and current eligibility on each delivered page | Ordinary replacement/deletion/expiry does not repeatedly restart the selected traversal. Verify fixed candidate cut, explicit changing-membership behavior and authorized current winners; report cursor expiry/policy/incarnation restarts and abandoned traversals. |
| Explicit strict snapshot | Exact pinned membership/cut while valid, with invalidation at its stated policy/temporal deadline | Test separately from traversal. Count every justified `temporal_snapshot_expired`, restart and incomplete client journey; do not suppress required invalidation to improve completion or attribute its churn to the default traversal. |
| Derivation/expiry | Cheap incremental targets p99 input-to-active <=30 s; due temporal invalidation never serves stale membership after its declared deadline | Dirty-only progress tested with no new ingestion. Stopped/failed compute leaves older labeled results only while their contract permits. |
| Heavy graph/export/backup | One 100k-row export completes <=10 min under 5k nominal fixture; complete graph+one viewpoint <=15 min for the 5k graph; hourly backup latest verified age <=2 h | These initial 5k targets require measurement; larger K/graph/corpus tracks use preregistered calibrated job deadlines below. Count waiting/cancelled jobs and completed bytes, not only worker time. |
| Memory/disk/protection | No process exceeds accepted configured grant except measured accounting tolerance recorded before run; disk stays above completion reserve; no unresolved ownership breach | An OOM/ENOSPC may demonstrate safe failure handling but cannot pass useful nominal performance. No false host-loss protection claim. |

These thresholds are intentionally explicit starting objectives, not claims
derived from historical component timings. A release can advertise a smaller
qualified envelope if the nominal scale fails; retain the failed run and revised
scope. Do not choose only surviving low-cost queries for percentiles: publish
offered outcomes, queue/service/end-to-end distributions, per-class throughput,
oldest age and total refused/cancelled/restarted work. Report sample count and
uncertainty; short runs with too few tail samples cannot establish p99 confidence.

For large graph/backup/restore, cold broad-query and new algorithm workloads
without a justified duration, perform one calibration using the simple qualifying
baseline and all required results. Before candidate comparison, record offered
fixture size/host grant, completed cost and a target acceptance artifact with:
useful result deadline, maximum waiting age, minimum admitted fraction, recovery
headroom and resource reserve. Initial relative headroom rule is steady offered
work <=2/3 of measured safe mixed completion rate, with a replay target >=1.5x
the registered incoming fact rate. A candidate cannot turn refusal into throughput.
Targets are locked across candidates; revise only in a new named experiment with
reason, original failure retained and changed advertised envelope. If the baseline
cannot finish the calibrated fixture correctly, reduce the declared envelope or
change the algorithm/topology before comparing speed. P02-T09/P05 acceptance owns
calibration; P08-T12 owns the published release envelope. An unavailable engine
guarantee can reject the architecture, not merely adjust a target.

Fair scheduling uses bounded class quanta with minimum control/live/monitor and
archive-completion shares, oldest-due work within a class and endpoint/hostname/IP
quotas. One enormous backfill/graph unit must partition/yield or refuse before
holding all capacity. Idle shares may be borrowed with bounded revocation at work
boundaries; borrowed work cannot indefinitely occupy a reserved class. Within each
cadence window, record due set minus actually attempted set and reasons, not only
percentiles of attempted endpoints. This catches a scheduler serving 99% quickly
while the same 1% never receives an observation. Existing configured age-band
revisit policy remains a scientific choice; cheaper traffic does not silently
promise identical late-arrival detection.

## Independent cross-component fixture

Use the small valid signed corpus in
[signed-fixture.json](../../experiments/identity-contract/signed-fixture.json),
its literal expected visibility/current/provenance/export outcomes and
[identity model](../../experiments/identity-contract/README.md). The finite fixture
contains event IDs and all signed fields; symbolic labels in documentation are
aliases for those exact IDs, not mutable bodies sharing an ID. Test keys are
public fixture material and must never be configured as real publishing identities.
The separate [volume generator](../../experiments/storage-qualification/fixture/src/main.rs)
provides signed stress events; its mostly regular kind-1/30023 distribution does
not replace this semantic fixture or establish natural search/compression quality.

Replay the same records under controlled acquisition, import, pending/archive,
CH, derivation, HTTP/relay/gateway, export and isolated restore. Assertions bind
source cut and observation time separately. At minimum the oracle fixes:

| Fixture action | Required observation / wrong interpretation excluded |
|---|---|
| Private event E then same E public; reverse ordering; provenance on/off | One body identity, separate scoped eligibility and signature representative. Public time/metadata comes from public eligibility, not the earlier private admission. Missing provenance does not erase eligibility. |
| Ordinary repeated verified E; invalid signature variant; later new relay witness | Repeat has no new scientific record; invalid variant has no witness; new valid endpoint has exactly its attributable edge. CertifyExisting cannot turn a new edge/scope into a no-effect read. |
| Earlier public receive correction at later source cut | Old pinned cut preserves former minimum; new cut has corrected minimum. Replay/export retains both immutable fact identities rather than overwriting history. |
| Newer private replaceable value and private deletion of older public value | Public winner remains the eligible public event. A private source cannot suppress or influence public counts/graph by winning before authorization. |
| Newer public empty follow list, followed by import of the older nonempty list | Current A adjacency is empty; older import never resurrects B/C edges. Historical source event retains all ordered/duplicate tags. |
| Deletion arrives before target; unrelated-author deletion | Resolve only matching-author target once known; unknown target state is not a fabricated timestamp or blanket deletion. |
| Public replacement expires with no new ingestion | At deadline current coordinate becomes absent; older value does not resurrect. Invalidation/dirty-only work must run even with collection stopped. |
| Equal-time replacement tie | Winner follows the fixed event-ID tie rule in the actual literal fixture; input order and engine merge order cannot select it. |
| Interrupted measurement | Durable intent without proven execution gives no invented start/result; proven start without terminal creates explicit unknown/interrupted evidence. No local queue deferral becomes a remote failure. |
| Archive/export/backup | Full signed fields verify; required public scope facts/dictionaries/documents close references, private source envelope excluded. Portable export is not assumed to contain deployment checkpoints. Native backup preserves all required causal ledgers and exact outbox bytes. |

The signed fixture's final public graph has exactly B→C, follow indegrees
A=0/B=0/C=1 and its declared normalized score basis; A's newer empty list removes
both former edges. Its source cuts include required bootstrap dictionary bounds
and remain different with provenance on/off. The fixture file owns exact IDs,
cut vectors and scope representatives; do not hand-copy another contradictory
oracle into each engine adapter.

The separate [measurement schedule](../../experiments/workload-contract/measurement-fixture.json)
fixes four attempt identities and exact times: intent-only crash, proven start
without terminal, successful NIP-11 with lost commit response, and local budget
deferral. Expected totals are two actual starts, one successful terminal, one
unknown interruption, zero remote-failure measurements and one referenced raw
document object. Full export must include that exact document; duplicate terminal
resolution cannot add another result. This is fixture input/expected-output DSL,
not executed remote evidence or a second production wire format.

The semantic fixture's graph edges are an independent expected set, not extracted
from the production reducer. A separate three-node graph oracle uses edges A→B
and B→C with C dangling, teleport to A and alpha=0.85; its stationary vector is
`(400,340,289)/1029`. The simple exact equations A=(1-alpha)+alpha*C,
B=alpha*A, C=alpha*B establish the oracle. This qualifies a profile defined by
that stationary equation only; the existing fixed-iteration profile must compare
to its own declared finite-iteration oracle and cannot be relabeled. Empty seeds,
isolated vertices, duplicate tags and empty replacements have literal distinct
outcomes. Ranking/output percentiles follow the selected version's scope/ties;
the kernel vector alone is not a full score-resource oracle.

## Real-boundary evidence and discriminating failures

| Comparison/claim | Fixture and injected schedule | Independent evidence / owner |
|---|---|---|
| Ingest correctness and duplicate efficiency | Signed semantic fixture plus U/P/C/S matrix; response loss/epoch close/earlier correction; known body with invalid variant | Exact signatures/source facts/producer checkpoints, no hidden per-repeat writes in CertifyExisting, real synced Rocks binding and PG causal restore; P01-T02/T07, P02-T02/T06–T08 |
| Archive authority/layout | Same full logical family set wide vs object/reference, quiet rows and aged random corrections; process death around transaction, lost receipt and inaccessible file | Independent DuckDB logical corpus/digests/document closure, actual catalog/object/receipt state, peak bytes incl backups; P02-T04/T08/T09 |
| CH serving/access paths/search | Same semantic resources, many parts, cold/warm mixed readers with writer/merge activity; delayed family and dedup-window expiry | Real endpoint queries vs fixture ID/tag/scope/current oracle, CH content and PG frontier/incarnation; p95/p99 and refusal/restart fractions; P02-T05/T09, P06 |
| Network/session fairness | Controlled heterogeneous signed peers; aliases share IP, slow remote, top-1% firehose, endless backfill, 2x pressure, closure/EOSE races | Independent socket observer + due-set/service ledger, byte/sig work, no fabricated handshake/witness/coverage; P01-T05, P03/P04 acceptance |
| Derived/graph usefulness | Empty replacement, unknown target, expiry without ingest; hub/churn graphs, K viewpoints, equality classes/near-boundary overlap | Literal small outputs and independent complete reducer, full load/build/publish cost, exact logical pair answers and output size; P05 tasks |
| Signed jobs/outbox | Same input generation, target failure, lost accept/send/OK, policy expiry, cancelled job, old signer restore | Actual signed bytes/destination ledger and independent no-send observer; P07/P08-T03/T05 |
| Capacity under maintenance | Full declared mixed tuple on aged store with export/backup/pins, CH/archive outages and bounded ENOSPC | Actual OS/cgroup/engine metrics, progress and all offered outcomes, safe recovery vs useful nominal pass separated; P02-T09, P08-T08/T12 |
| Released user journey | Extracted bundle, correct fixture data, browser/API/relay/gateway/export, config change, backup/restore, prior-release upgrade | Independent clients and signed corpus, matching artifact digests, no missing suite reported PASS; P08/P09/P10 |

Fault timing is event/receipt/checkpoint based, not a sleep assumed to hit a commit.
The [operations effect table](operations-contract.md#cross-store-effect-and-recovery-table)
defines the allowable state after each cut; adapters expose bounded test hooks
without replacing real commits. Dataset oracle mismatches invalidate candidate
performance. A no-engine reference simulation is labeled MODEL; missing tools or
ineligible environment gives NOT_RUN, and an intentionally optional variant may
be SKIPPED with reason. Required scenarios with zero executions cannot pass.

The small [workload scheduler probe](../../experiments/workload-contract/README.md)
tests arithmetic, weighted/oldest-due scheduling on a bounded virtual fixture,
tail accounting and independent negative controls. It establishes no socket,
engine, power-loss or throughput property. The actual results remain separately
recorded in [workload model evidence](../research/evidence/workload-contract.json)
and linked by P00-T09/P00-T10; all product acceptance boxes remain open
until their declared paths execute.
