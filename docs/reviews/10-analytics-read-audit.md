# Analytics, read and publication coherence audit

Date: 2026-09-11. Baseline: `b54355541ac9be006e2464600c486d1d4ae60d02`.
This is an independent complete-file reread of the assigned domain. It finds
remaining design repairs; it does not establish that every choice is optimal,
that product code exists, or that future acceptance scenarios have passed.

## Coverage and evidence

All 13 assigned Markdown files were read in full: **6,304 logical lines and
465,938 bytes**. The [manifest](../research/evidence/10-analytics-read-manifest.json)
records every path, complete SHA-256 before/after, byte/line counts, revision,
read ranges and completion time. All assigned source hashes were unchanged after
the reread. Numbered contiguous chunks were used; truncated output was discarded
as coverage and reread. Searches were used only for additional crosschecks.

| Complete file | Lines | Bytes | Role in this audit |
|---|---:|---:|---|
| [analytics-detail](../architecture/analytics-detail.md) | 575 | 42,157 | Selected definitions, formulas, evidence and cost |
| [derived-read-publication](../architecture/derived-read-publication.md) | 1,272 | 91,901 | Selected service, authorization, query and delivery contracts |
| [protocol-evolution](../architecture/protocol-evolution.md) | 750 | 62,464 | Selected standard bindings versus explicitly unsubmitted proposals |
| [search-and-reconciliation](../architecture/search-and-reconciliation.md) | 281 | 19,106 | Selected search and NEG semantics and engine alternatives |
| [P05](../implementation/05-analytics.md) | 708 | 45,267 | Future analytics tasks and verification |
| [P06](../implementation/06-read-surfaces.md) | 671 | 44,101 | Future public query tasks and DVM integration |
| [P07](../implementation/07-publication.md) | 508 | 32,666 | Future signer, outbox and publisher tasks |
| [DVM profile](../specification/dvm-profile.md) | 366 | 24,735 | Selected experimental application/payment contract |
| [Earlier research](../research/derived-read-publication-findings.md) | 335 | 24,379 | Historical intent, rejected behavior and earlier questions |
| [Go analytics walkthrough](../research/go-analytics-code-walkthrough.md) | 574 | 42,933 | Attributed prior source observations and static inferences |
| [Phase-5 recheck](07-phase-5-recheck.md) | 210 | 32,843 | Historical requirement recovery and planning closure |
| [Contracts review](02-contracts-and-simplification.md) | 26 | 1,583 | Earlier revision record |
| [Failures review](04-failures-and-growth.md) | 28 | 1,803 | Earlier revision record |

Primary NIP-01/09/40/45/57/66/77/85 text was retrieved at the repository's pinned
NIP revision for disputed or niche semantics. The manifest identifies those
sources and the scope of this supplementary check. Auxiliary searches of central
and acquisition documents are not counted as additional complete-file reading.
The coordinating acquisition reviewer independently confirmed the producer gap
in AR-06 after a complete acquisition-domain reread.

No Go/Rust tests, product builds, database experiments, live publication or Git
commands were executed. In particular, rereading the Go walkthrough did not
repeat its underlying code audit or execute its seven historical tests. Engine,
crypto-library and payment-node compatibility remain implementation evidence
obligations, not results supplied by this review.

## Findings requiring reconciliation

Severity describes planning impact: **P1** can produce a false advertised
capability or incorrect externally visible contract; **P2** is a material ambiguity,
qualification gap or avoidable scope coupling. These are baseline findings, not
observed runtime failures. Subsequent coordinating corrections should cite this
baseline and record their disposition without rewriting its evidence.

### AR-01 — P1: NEG membership lacks explicit relay wire-size eligibility

**Competing contracts:** P06 lines 393–400 require an explicit refusal when one
complete escaped EVENT frame exceeds the relay's 64-KiB bound. P06 lines 427–448
and search-and-reconciliation lines 175–181 define the negotiated set as visible,
retrievable full bodies, but the construction/exclusion rules do not bind this
set to the effective output encoding/size policy. Those rules name missing and
restricted bodies; possession of a full body is not sufficient for this transport.

A retained 100-KiB event can satisfy ordinary source/body membership while every
ID REQ on that listener refuses to deliver it. The larger HTTP response allowance
does not make it retrievable through the negotiated relay. This differs from the
already documented race in which policy changes between negotiation and REQ.
NIP-77 exchanges identifiers and anticipates separate retrieval; BigBrotr's own
stronger retrievable-set promise creates this obligation.
[Primary NIP-77](https://github.com/nostr-protocol/nips/blob/a2494f4f81d46684e5814a9bf35e2b1df978f955/77.md)

**Recommended disposition:** define one relay-retrievability predicate shared by
NEG construction and ordinary relay delivery, including the complete encoded
envelope and a documented allowance for a supported subscription ID. Bind the
serving-policy/limit revision into the reconciliation generation and cache key;
a limit reduction cannot leave an incompatible set advertised. Retain excluded
full events in the archive and allow another explicitly capable API/export path.
A conservative maximum valid subscription-envelope allowance avoids making the
shared set depend on the client's chosen subscription-string length.

**Discriminating acceptance:** archive small, oversized and escaping-heavy events;
negotiate their IDs and fetch the differences on the same real listener. Every
advertised member must fit the declared relay encoding absent a documented later
policy/availability change. Changing the output bound must rebuild/invalidate the
appropriate set. A body-present-only predicate is the negative control.

### AR-02 — P2: an optional paid interface is an unconditional core gate dependency

**Competing contracts:** dvm-profile lines 267–274 and P06 lines 572–576 make
payments explicitly optional and default-off. Nevertheless P06-T12 depends on
T01–T11 without qualification (line 618), and P07-T09 lists P06-T09–T11 for DVM
integration (line 452). P07 lines 485–488 then say missing optional/regtest evidence
must be distinguished from baseline completion. These instructions leave two
incompatible interpretations of what closes the ordinary read/publication phase.

Paid queries can fund a service and preserve an intended DVM use case. Their
cost is substantial: a node dependency, receiving secrets, invoice ambiguity,
settlement polling, durable financial obligations, operator refunds and regtest
maintenance. Being default-off eliminates neither implementation nor release cost.
The docs do not justify making those dependencies necessary to verify free HTTP,
ArchiveRelay or the free explicit DVM profile.

**Recommended disposition:** retain the fully specified paid capability and its
own mandatory real-LND closure when advertised. Separate the core conformance
gate from the paid-capability gate in the dependency graph and release evidence.
Do the same for optional subscription/context adapters where applicable. Record
value and total maintenance cost in the product decision; do not delete paid
support solely because it is complex, or silently count unrun paid tests as passed.

**Discriminating acceptance:** a clean free deployment with no LND artifacts must
complete the core read/publication suite and advertise paid support unavailable.
A release advertising `lnd-prepaid-v1` must fail its capability gate without real
settlement/recovery evidence. Removing LND while still advertising paid support
is the negative control.

### AR-03 — P2: subscription controls lack an explicit admission class

**Competing contracts:** dvm-profile lines 120–131 require same-requester renewal
and cancellation of an active subscription. Lines 239–248 and P06 lines 502–505
impose one active job per requester plus global execution/queue bounds; P06
lines 534–549 add a separate subscription capacity. Neither specifies whether an
active subscription consumes the finite-job slot, nor how a control message
passes a saturated data-job admission boundary.

If it consumes the slot, a subscription can prevent its own renewal. If it does
not, the implementation still needs a precise ownership/accounting rule. Sharing
an exhausted query queue can also make cancellation wait behind the work it is
supposed to stop. This is an ambiguity, not a demonstrated implementation defect.

**Recommended disposition:** define finite execution slots, waiting-payment slots,
subscription slots and a small separately bounded authenticated control allowance.
Renewal/cancellation must not acquire a new query pin or repeat the source plan.
Keep signature, provider/requester, replay and control-rate checks; bypassing a
query slot must not bypass identity or allow unbounded feedback. Specify the
response when the control allowance itself is exhausted.

**Discriminating acceptance:** fill data-job slots and the normal queue, then renew
and cancel the caller's known subscription. Its permitted control must finish
through the reserved path; another caller must still fail. Counting renewal as a
new conflicting active data job is the negative control.

### AR-04 — P2: exact overlap has an unqualified useful-scale envelope

**Evidence:** analytics-detail lines 238–310 and 432–443 select exact eligible-scope
overlap by default with optional approximate discovery disabled. P05 lines
465–496 specify size pruning and exact candidate intersections. Common initial
bounds are 512 MiB resident, 8 GiB spill and 3,600 seconds (P05 lines 41–49).
The documents correctly admit quadratic work and explicit refusal; this finding
is not an accusation that they promise O(delta) exact clustering.

For R equal-size eligible relays, the size bound can eliminate no pair. At
R=10,000 there are 49,995,000 possible pairs; at R=100,000 there are
4,999,950,000. If the sets are identical mirrors, every pair also qualifies:
even writing the requested complete edge relation is quadratic. A daily new
window can change many memberships without large new unique-event volume.
Persisting candidate pairs, qualifying edges and components is not a bounded-size
result merely because the component list is short. Honest repeated refusal would
preserve correctness while delivering little useful fleet-scale overlap output.
These are combinatorial calculations, not measured workload forecasts.

**Recommended disposition:** define and measure the exact profile's practical
fleet/fanout envelope before calling its default useful at the target scale. Try
sound set-similarity join pruning and group identical set representations where
they preserve the promised evidence. If compressing identical-set cliques, specify
lazy/bounded edge explanation instead of pretending every pair was materialized.
Retain a named optional candidate-limited/approximate profile for broader fleets,
with explicit recall limitations; do not relabel it exhaustive. Avoid adding a
new sketch service merely to implement that alternative.

**Discriminating acceptance:** equal-size unrelated sets, large identical-set
cliques, one high-fanout event, sliding-window expiry and bridge deletion must
measure candidate work, complete edge/state bytes and update/rebuild cost. Use
an independent exact small graph for correctness and separate measured limits
for large refusal/throughput. Favorable heterogeneous set sizes alone are not
sufficient qualification.

### AR-05 — P2: baseline search correctness is specified more strongly than default usability

**Evidence:** search-and-reconciliation lines 62–82 and P06 lines 301–365 select
application Unicode tokenization, retained ordered tokens, exact phrase checking
and complete-candidate relevance ordering before top-K. Shared limits remain
256 MiB scanned/15 seconds (P06 lines 49–56). The baseline explicitly refuses an
over-broad plan, which is preferable to silently reranking a chronological prefix.

A frequent token across a large public corpus can require extensive candidate
verification and ranking even for ten results. The prescribed planted-old-result
fixture proves ordering correctness; it does not show that an ordinary broad
search is usable under the shipped bounds. Ordered tokens, distinct token arrays,
postings and rebuild overlap also have a real storage cost alongside full bodies.
The comparison with Tantivy/OpenSearch is appropriately conditional, but no
measured evidence establishes the selected profile as the best relevance/cost
tradeoff for expected user queries.

**Recommended disposition:** keep CH as the coherent first experiment, with the
single shared analyzer and exact oracle. Add an explicit user-query workload and
publish the measured accepted/refused scope for common terms, phrases, languages
and narrow structured filters. Inspect complete candidate work and token/posting
footprint. Evaluate safe top-K pruning or a Tantivy backend only if that evidence
shows a material advantage at equal semantics and resources. Do not silently
narrow the user's date range; the UI can explain a refusal and offer an explicit
narrower query.

**Discriminating acceptance:** frequent/rare terms, a common phrase with many
false token candidates, CJK/emoji, a high-relevance old result and protected
content must run through actual browser/API/relay paths. Report the fraction and
kind of the declared workload refused without inventing a universal pass quota;
compare result truth and complete work before latency. A fast latest-prefix
reranker remains the negative control.

### AR-06 — P1: validated zap facts lack an owned issuer-evidence producer

**Competing contracts:** P05 lines 290–298 require retained historical issuer
verification; lines 313–315 prohibit arbitrary URL resolution inside analytical
jobs and refer to an unspecified acquisition task. P07 lines 260–275 promise real
zap-field bindings. The coordinating acquisition reviewer confirmed that the
fully read acquisition phase has no concrete LNURL issuer-evidence producer.
This review's supplementary search also found no LNURL/issuer producer in the
central data/acquisition pages searched; that search alone was not treated as
proof of complete acquisition coverage.

The conservative historical policy is sound: a fetch today does not prove which
key a recipient's provider authorized years ago. NIP-57 validates a receipt against
the recipient provider's published `nostrPubkey`; a receipt signature by itself
does not establish that relationship or independently prove settlement.
[Primary NIP-57, Appendix F and receipt limitations](https://github.com/nostr-protocol/nips/blob/a2494f4f81d46684e5814a9bf35e2b1df978f955/57.md)
Without a producer, genuinely useful accepted zap facts can remain unresolved
indefinitely while the named producer/publisher tasks look complete.

**Recommended disposition:** give P04 an explicit bounded acquisition path for
recipient/profile/event attribution and LNURL issuer metadata, guarded by the
existing egress, DNS, redirects, time, bytes and concurrency policy. Preserve
retrieval time, source document/digest, recipient binding, validation status and
coverage. Never backdate current metadata to historical receipts. Permit an
attributed imported historical issuer-evidence source with a documented trust
class, and leave unsupported history honestly unresolved. Analytics only consumes
admitted evidence and performs no opportunistic network requests.

**Discriminating acceptance:** a controlled provider changes its key between two
captured observations; imported old receipts lacking historical authority remain
unresolved, while correctly time/scoped evidence enables the expected classified
facts. An SSRF endpoint must cause no forbidden request. A current-fetch-as-
historical-proof implementation is the negative control. Exercise actual
acquisition→stored evidence→zap facts→published fields.

## Choices that survived this reread

These decisions are coherent and have concrete reasons; the absence of another
finding is not proof of universal optimality.

| Decision | Assessment and remaining evidence |
|---|---|
| Full signed archive with deletion-aware normal reads | Coherent separation of storage and publication. NIP-09 permits stopping publication; ordinary and historical counts intentionally differ. Owner-scoped unresolved deletions and no resurrection are carried into P05/P06. |
| Distinct source cuts, target generations and dirty-only work | Useful correction of the historical run coupling. P05 explicitly requires cut-before-reduction, no-output progress, independent target failure and immutable publication. Actual PG/CH durability is outside this prose audit. |
| Exact current headers, counts and contribution retraction | Empty/invalid winners and actor/event units are preserved. Quantile/refcount state is charged by input cardinality. Exactness is never inferred from a small output table. |
| Seeded trust as a named viewpoint | The fixed 100-iteration formula, dangling redistribution, mute interpretation and percentile mapping are explicit. They differ deliberately from Go. No-seed withholding and no universal event-truth score remove misleading products. Reference correctness, numerical tolerance and attack sensitivity still require implementation evidence. |
| Relay quality separate from social trust | Local evidence, unknown time and component basis survive; broken social graphs cannot block the relay target. Weights and the five-sample eligibility rule are heuristics, not empirically established optimal choices. |
| One signer/outbox with distinct purpose and viewpoint keys | Preserves actual shared behavior without erasing identity. Exact signed bytes, destination obligations and bounded policy authority are explicit. Already transmitted packets are correctly outside recall guarantees. |
| Optional metadata context | Standard fields remain independently interpretable; signed digest binding avoids circular event-ID hashing. Protocol-evolution lines 551–557 expressly prevent context recomputation from forcing unchanged-score republishing. This potential contradiction was checked and found addressed. |
| Experimental directed DVM profile | Explicit kinds/profile/encryption avoid the historical 5050 misuse and unearned generic compatibility. Free finite reads have a clear role; subscription/payment modes need the independent capability and admission repairs above. |
| Historical findings separated from current selection | Go sample-uptime/linear-RTT/max-normalization formulas and lossy deployment axes are not current requirements. Earlier research's unresolved product questions do not override the later selected DVM or full-archive contracts. |

The primary check confirmed the NIP-66 timeout inconsistency, optional RTT units,
NIP-85 subject kinds/key separation/integer-sat fields, and the NIP-45 pubkey-based
HLL construction. Active-hour end 24 is permitted by the pinned NIP-85 field
range; it is not a wire bug. Exact-only sat omission is a deliberately conservative
product choice, not something the NIP requires: if real data show it routinely
hides useful amounts, a separately specified rounded aggregate binding can be
compared while retaining exact msats locally. No per-receipt rounding should be
silently introduced.
[Primary NIP-66](https://github.com/nostr-protocol/nips/blob/a2494f4f81d46684e5814a9bf35e2b1df978f955/66.md),
[primary NIP-85](https://github.com/nostr-protocol/nips/blob/a2494f4f81d46684e5814a9bf35e2b1df978f955/85.md),
[primary NIP-45](https://github.com/nostr-protocol/nips/blob/a2494f4f81d46684e5814a9bf35e2b1df978f955/45.md).

## Closure boundary

The complete assigned reread is finished. AR-01 and AR-06 require concrete
contract/producer repair before advertising the affected capability. AR-02/03
need explicit dependency/admission reconciliation. AR-04/05 need practical
qualification and, if it fails, a deliberate alternative or a narrower advertised
scope. This audit changes no selected architecture or implementation task file;
the coordinating review owns those corrections and their acceptance traceability.

A complete reread supports a defensible coherence review, not proof that every
technology, heuristic, storage layout and product feature is the best possible
choice. The relevant next evidence is executable source-to-user behavior and
measured total cost, including failed/refused workloads and recovery.
