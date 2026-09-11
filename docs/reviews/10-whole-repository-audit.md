# Complete-document audit and decision qualification

Date: 2026-09-11. Baseline: `b54355541ac9be006e2464600c486d1d4ae60d02`.
Planning/research only. The owner asked whether every document had really been
reread and every decision validated as the best possible choice. The candid
starting answer was no: the preceding pass read changed/new documents and major
connections, but had not performed another complete repository sweep after all
corrections. No amount of prose review establishes universal optimality.

This pass completes a fresh full Markdown sweep, identifies further defects,
repairs the selected contracts and runs one new bounded real-engine experiment.
The [decision-confidence register](../planning/decision-confidence.md) now states
the justification, alternatives, cost and empirical limits of the major choices.

## What was actually reread

All **64 Markdown files / 15,571 logical lines / 1,426,157 bytes** present at the
baseline were fully read. The coordinator read 32 files; three independent domain
reviewers read 12, 13 and seven assigned files respectively. The coordinator then
read their complete reports, checked disputed connections and reviewed the
integrated changes. This is a distributed full review, not a claim that one
reviewer personally reread every file twice. Hashing and search hits were not
counted as reading; truncated reads were repeated in bounded chunks.

The [combined manifest](../research/evidence/10-whole-repository-read-manifest.json)
maps every baseline path to its reader, SHA-256, bytes and full range. The
coordinator independently compared all 64 hashes to their exact Git blobs at
the named revision. Domain reports retain baseline line references and are
historical findings; the disposition table below records the later corrections.

* [Acquisition audit](10-acquisition-audit.md): complete P03/P04, acquisition and
  import contracts plus assigned historical research/reviews.
* [Analytics/read/publication audit](10-analytics-read-audit.md): complete P05/P06/
  P07, algorithms, read/protocol/search/DVM contracts and assigned source reviews.
* [Product audit](10-product-audit.md): complete product/P08/P09/P10 and assigned
  research/reviews, plus an explicitly disclosed supplemental format reread.

JSON evidence was mechanically validated and selected experiment/source hashes
and results reconciled. This is not a claim to have semantically reread every raw
value in all source-capture manifests. Earlier full Go/vision/phase readings
remain their dated evidence; this pass does not repeat or recertify those source
audits. The legacy repository and Tux services were not changed or tested.

## Findings and integrated dispositions

Priority describes a planning defect or qualification gap, not a deployed bug.
"Repaired contract" means the selected behavior and owning tasks are written;
the corresponding future practical acceptance is still unexecuted.

| Finding / baseline references | Consequence | Disposition and owner |
|---|---|---|
| High: PROD-03 availability pages lacked a producer | First witnesses cannot establish later negative availability | Repaired contract: P04-T13 finite guarded availability jobs → A05 → P05-T08 → P09; selection/budgets/unknown/provenance-off are explicit |
| High: AR-06 / ACQ-10-02 zap issuer input lacked acquisition | Analytics could remain unresolved or perform unplanned unsafe/current-time HTTP | Repaired contract: P04-T14 coalesced bounded metadata producer → A05/A06/A03 → P05-T05. Current issuer match cannot backdate historical authority; unresolved coverage remains visible |
| High: AR-01 NEG set omitted transport-size eligibility | Advertised ID could be impossible to retrieve through that listener | Repaired contract: common RelayRetrievable predicate, worst supported subscription-envelope allowance and encoding/limit generation identity in P06-T07/T08 |
| High: PROD-02 export publication had no authority transition | A completed private artifact had no valid path to the promised public dataset | Repaired contract: P08-T04 artifact publish/withdraw CLI/private routes, explicit grants, immutable digest/catalog/pins, policy revalidation and actual download checks; PG migration 0804 |
| High: ACQ-10-01 undefined source tombstones | Import could invent foreign policy transfer or omit signed archival history | Simplified: ordinary research formats do not import administrative controls; kind-5 originals and targets remain; deployment controls use explicit restore. Removed unused knob/prepass |
| High: PROD-01 unnamed analytical-import product | A second dataset catalog/query/security system appeared without specification | Removed that product branch. Slim exports remain standalone DuckDB artifacts and canonical import rejects them |
| Medium: PROD-04 attempts versus distinct IDs | Retries could cross the availability eligibility threshold | Repaired P05-T08 wording and fixture: 19 distinct IDs with many attempts remain insufficient |
| Medium: PROD-05 missing job discovery | New browser session could not list durable work without local memory | Repaired P08-T03 typed jobs list, bounded filters/cursor/visibility and role/registry summary routes |
| Medium: AR-02 optional payments coupled to core gates | LND could become a prerequisite for free read/publication verification | Repaired P06/P07 capability-specific gates. Paid support remains fully selected optional scope and requires actual regtest before advertisement |
| Medium: AR-03 subscription controls shared exhausted job admission | A subscription might block its own renewal/cancellation | Repaired DVM slot ownership and separately bounded authenticated control queue; saturation/foreign-author fixtures |
| Medium: ACQ-10-03 pooled socket timing | Reuse might refresh an old handshake or create excess connections | Repaired P04 transport-generation references and coordinated fresh-connect scheduling in the same endpoint slot |
| Medium: ACQ-10-04 hostname alias fairness | Many names could concentrate connections on one address | Repaired host-local selected-IP concurrency/start-rate grants, scoped overrides and remote-overlay limitations; aliases + unrelated-peer fixture |
| Medium: ACQ-10-05 obsolete arrival-series option | Delivery-sized permanent data could return despite deliberate exclusion | Removed obsolete option sentence; first/earlier observations and bounded diagnostics remain |
| Low: ACQ-10-06 overlapping monitor projections | Two implementations could claim authority for the same current values | Named LocalMonitorView and ResolvedRelayEvidence, shared pure rules where equal, independent NIP-66 local inputs |
| Medium: root storage target treated as hard cap | Memory planning could rely on a false bound | Repaired P02/storage semantics; actual 64k-target counterexample below. Native/application limits are distinct |
| High: root receipt "correctness horizon" lacked a reclamation protocol | Unbounded detail or late retry after GC could undermine idempotency | Repaired registered producer slot/epoch, bounded outstanding window, contiguous ACK and durable admission closure. Independent follow-up found and corrected incremental-progress and causal-backup obligations |
| Medium: root archive settings/protection transition were implicit | Engine default inlining or pending GC could silently change storage/protection assumptions | Pin disabled inlining to match the recorded probe, inspect effective overrides/quiet path, account for small files; preserve the prior receipt class across archive reclamation |
| Qualification: AR-04 exact overlap usefulness at large R | Equal-size candidate sets and qualifying-edge output can be quadratic even with correct refusal | Still empirical, P05-T09/T13: mirrors, hubs, identical-set grouping alternatives, expiry/bridge changes and full edge/state bytes; no achieved 100k envelope |
| Qualification: AR-05 common-term search usefulness | Correct exhaustive ranking may refuse ordinary broad queries | Still empirical, P06-T06/T12: explicit user query mix, accepted/refused scope, total postings/candidate work and equivalent-semantic alternative comparison |

The producer-retirement follow-up was a separate targeted static critique of the
new root contract. It found that ACK must follow an incremental durable outcome
handoff rather than waiting for EOSE; the owner must close admission before final
retirement resolution; and a backup containing post-GC owner state must include
the producer checkpoint that authorized GC. It also narrowed digest comparison
to the period when receipt detail exists and publication delay to a batch timer,
not a guaranteed commit SLA. Those corrections are in storage/P02/P01/component
contracts, with distinguishing future scenarios. This is useful counter-review,
not a second universal quality certificate.

A separate follow-up of the new measurement/NEG/DVM/product connections corrected
the issuer binding to require recipient-signed profile evidence rather than the
payer's asserted LNURL; bounded queued metadata demand and private-source egress;
reserved verification capacity for subscription controls; and accounted for
multibyte subscription IDs in the actual relay envelope. The final coordinating
reread also made host-budget recovery explicit in P01 and reconciled artifact
pagination ordering with its management route. These are static contract repairs,
not executed service results.

## New real experiment

[Parquet target probe](../../experiments/parquet-targets/README.md), local DuckDB
1.4.5, one thread, 128 MB engine memory setting, 200,000 integer rows. Requested
row-group target: 64,000. Actual groups: **65,536; 65,536; 65,536; 3,392**.
Independent count/min/max/arithmetic-sum readback matches the source sequence;
the false hard-cap interpretation fails. The output is 238,370 bytes and the
[evidence](../research/evidence/parquet-targets.json) records program/file hashes.

This proves a specific engine-target counterexample, not DuckLake integration,
RSS containment, Rust correctness, compression optimality or power-loss safety.
[DuckDB documentation](https://duckdb.org/docs/current/data/parquet/tips) explains
the row-group target/minimum and vector granularity. No network or other user's
service was used. The previous experiments were reread/reconciled, not relabeled
as newly executed capacity tests.

## Documentation complexity and decision authority

The volume of this repository creates its own maintenance risk: the same default
or invariant appears in a service contract, task, overview and historical review.
The discovered arrival-series, sample-denominator and target-size contradictions
are concrete examples. Adding more paragraphs is not automatically improvement.

Use field/component/specialized service specifications for selected semantics;
implementation tasks consume those contracts and own the executable proof.
PLAN/coverage are navigation and traceability. Reviews/research retain dated
evidence and superseded choices, not competing configuration defaults. A conflict
between current contracts is a defect to reconcile, not something to hide behind
precedence. The future generated config/schema/API reference must eliminate manual
copies; do not create another documentation or analytical-data framework now.

This pass adds two missing producer tasks, removes two unsupported import
branches, clarifies existing ownership rather than adding daemons, and makes
unknown performance claims explicit. Current selected implementation scope is
**116 tasks / 716 numbered steps**, including five planning tasks and 111 future
implementation tasks. The older review counts describe their actual prior cuts.
Every future product checkbox remains unchecked.

## Evidence boundary and handoff

Performed: complete baseline Markdown review, independent domain critiques,
targeted critique of new contracts, exact baseline Git-hash comparison, integrated
document repairs, one small actual engine probe and artifact verification.
Changed sections and new documents received a final coordinating reread; this
does not pretend to be another full sweep of every unchanged historical page.

Final mechanical checks passed over 70 current Markdown files, 40 JSON files,
four JSON examples and 281 local heading links. All 116 task IDs are unique;
their 716 numbered steps are sequential within each task, and no future product
checkbox is completed. The new experiment program/output hashes match the
recorded evidence. `git diff --check` passed. These checks detect broken artifacts
and inventory drift; they do not validate algorithms, usability or capacity.

Not performed: new full legacy-code audit, product implementation, real fleet
ingestion, billion-row queries, native Rust engine/fencing/ENOSPC/power-loss drills,
paid regtest, browser/website/release installation or end-to-end capacity testing.
Those cannot be certified by the written plan or by the number of review passes.

The architecture remains a reasoned selection with explicit costs and alternatives.
Its target-scale fitness is not fully validated, and "best possible in every
respect" is not an evidence-backed conclusion. Start future implementation with
the risk-bearing vertical slice and update these decisions when real results
warrant it. This planning audit itself does not authorize or begin product work.
