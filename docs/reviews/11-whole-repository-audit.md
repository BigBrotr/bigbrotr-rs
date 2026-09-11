# Review 11: complete reread, explicit catalogs and practical counterexamples

Date: 2026-09-11. Scope: planning/research only in the private `bigbrotr-rs`
repository. Neither the Go repository nor Tux was modified or exercised in this
round. No product binary, Rust adapter, HTTP handler or browser application exists
yet; future task checkboxes remain implementation obligations.

## Exact reading scope

The frozen baseline was `8c81472e75da118b04809636a73f3231157227b5`:
**70 Markdown files, 17,222 lines, 1,551,151 bytes**. All were read from beginning
to end across a coordinator and three bounded domain reviewers. The coordinator
read 37 files; acquisition 8; analytics/read/publication 12; product/operations 13.
Truncated tool output was reread in smaller ranges. The
[combined manifest](../research/evidence/11-whole-repository-read-manifest.json)
independently compares every recorded source hash/length against its exact Git
blob, with no uncovered or multiply assigned baseline file.

Reading reports are [acquisition](11-acquisition-audit.md),
[analytics/read/publication](11-analytics-audit.md) and
[product/operations](11-product-audit.md). Their findings concern the frozen
baseline; this report records their disposition. New/modified contracts were
subsequently reread and challenged separately. Hashes establish source identity,
not semantic correctness. JSON evidence receives structural/hash checks and
targeted inspection; this is not a claim to have reread every scalar in every
historical raw result. Earlier full historical Markdown and selected Go call-chain
reports were reread, but their moving source was not recaptured in this round.

## Findings and decisions

| Finding | Adopted repair and owning contract |
|---|---|
| ACQ-11-01: issuer HTTP lookup had no valid relay-only measurement subject | Closed tagged measurement subjects for relay versus recipient/profile-bound HTTP resource; A04/A05/A06 and P04-T14; no fabricated relay enrollment |
| ACQ-11-02: omitted IDs in an EOSE-terminated batch could become false negative observations | Omission stays unknown; a clean singleton exact-ID confirmation is required for scoped not-served; persistent cumulative request/deadline budgets; P04-T13 |
| ACQ-11-03: new producer controls/jobs lacked one runnable contract | Named hot gates and captured finite profiles, actual CLI/shared job routes, bounded issuer queue/cache/cooldown with durable retry obligations; P04/architecture |
| ACQ-11-04: stale admission/control vocabulary invited parallel contracts | Canonical Admit/AdmitEvidence/AdmissionResult/Receipt and source/request identities; imported scientific evidence separated from administrative restore |
| AA11-01: relay weights did not define a reproducible estimator | Selected `general-read-relay/1`, explicit time integration, strata, unknowns, cadence horizon, lower-median latency, minimum evidence and missing-component policy; capabilities remain a separate vector |
| AA11-02: source snapshots omitted passage-of-time eligibility | ReadViewStamp includes interpretation time/temporal generation/deadline; one list/count/search/body/NEG basis, explicit ordinary cursor invalidation and indexed expiry work |
| AA11-03: local NIP-66 depended on optional remote claim resolution | P07-T07 consumes P04-T11 local monitoring; resolver-off/corrupt-foreign-claim acceptance added |
| AA11-04: subject families did not specify actual producer-to-wire values | Closed Subject union, relation parser, exact received/authored/root/parent/actor/payment distinctions, concrete field bindings, family-specific support and explicit withheld profiles |
| AA11-05: mixed phrase matching had ambiguous relevance inputs | Decode searchable literals before ranking; ignored extension/quote syntax excluded; AST/ranking/analyzer/view identity and independent fixtures |
| AA11-06: graph cost notation changed H's meaning | Distinguish unique corpus U, historical relationship input H, graph V/E and provenance P; charge vertex mapping, remapping and global work |
| PROD11-01/05: a newcomer had no clear vocabulary/table map | Start-here guide, logical storage catalog, row units/owners/retention, names and concrete public-resource catalog |
| PROD11-02/03: promised administration workflows lacked precise routes/lifecycle | Account enrollment/reset/deactivation, recent reauthentication, safe token/secret handles, paused-job resume and configured-role start/stop semantics; P08/P09 |
| PROD11-04: publication catalog did not specify download behavior | Manifest and authorized GET/HEAD file routes, exact-byte range/ETag semantics, bounded streaming revocation and context hosting through the same artifact authority |

The [newcomer guide](../start-here.md) is the entry point. Normative details are in
[naming](../specification/naming.md), [storage](../specification/storage-catalog.md),
[interfaces](../specification/interface-catalog.md),
[data fields](../specification/data-contracts.md),
[read views](../specification/read-view-contract.md) and
[assertion bindings](../specification/assertion-bindings.md). Existing phase/task
IDs remain the implementation order; the catalogs do not create another plan.

### Corrections found while reviewing the corrections

The first table draft omitted the separate Indexer-owned RocksDB directory for
incremental state and reconciliation. The corrected catalog names coalesced dirty
keys, current headers/members, source contributions, exact distinct refcounts,
unresolved reverse references, deletion effects, rolling/expiry inputs and shared
NEG sets. These are typed domains in one derived owner, not a database per metric.
They have atomic state/checkpoint/output identity, CH materialization and a
separate fenced PG activation step; no cross-engine atomic transaction is claimed.
The existing Indexer exposes typed local/Index RPC access, not writable mounts
shared by independent processes.

Current empty/invalid list winners require headers independent of membership.
Normalized child rows do not acquire independent scientific sampling identities.
Archive commit receipts are technical transaction metadata: they cannot recursively
become a new source fact requiring yet another archive commit.

Shared jobs and retention claims are supplied by early P02 control migrations;
P08 management and P06 cursor migrations extend those authorities. P05's first
graph consumer creates common artifact/file inventory; P08 adds publication rather
than inventing a second owner for the same bytes. DVM result rows reference exact
signed-outbox identities. Qualified scope ordinals prevent imported document
namespace collisions. Wire formatting outside signed strings is distinguished
from authenticated content/tag whitespace.

A final temporal counter-review extended the stamp to delayed DVM signing/
delivery, paid-job eligibility, time-sensitive derived manifests and browser
history/cache. Stopping Deriver across expiry cannot relabel an old target as
current. Account challenge consumption now checks credential/security revision
and account state; deactivation/reissue invalidates outstanding challenges.
Paused jobs stay paused after restart and resume uses a monotonic job revision.
Index RPC checks action/resource/policy authority in addition to peer identity.

Historical zap-issuer authority is not inferred from a current HTTP lookup.
The binding specification makes its optional attributed input/authority explicit;
without eligible historical binding evidence, affected wire fields remain
unavailable/withheld rather than turning incomplete knowledge into zero payments.
Raw receipts and useful current issuer observations remain archived and queryable.
No universal note recommendation score is selected merely because engagement
counters exist. User trust, event engagement and relay service quality answer
different questions.

## Executed document-layout experiment

The [probe and reproduction](../../experiments/document-layout/README.md) use
local DuckDB 1.4.5 with eight chronology-controlled cases of 16,384 observations:
low/high document churn, compressible/high-entropy 1 KiB bodies and one/16 files.
Both layouts preserve identical observation metadata, qualified document
identity, bytes and scope. Split size includes object and reference files.

* Reused documents across 16 files: split objects saved **20.04%** for compressible
  bodies and **60.10%** for the high-entropy fixture.
* The same low-churn fixtures in one file: wide rows were slightly smaller.
* All four high-churn cases: wide rows were smaller; split cost ranged from
  **3.06%** to **75.63%** more bytes.

The first run ordered objects by namespace/digest, changing compression locality;
its high-entropy/high-churn one-file split was 17,438,720 bytes. Preserving
first-observation order reduced that to 9,033,835. Both runs and original source
were retained instead of discarding the confound. Their total local output is
85,667,881 bytes. Exact complete row/body reconstruction and cross-namespace/
corrupted-byte negative controls passed; overwrite refusal was actually exercised.
Evidence retains hashes, settings and actual row groups/codecs.

This verifies conditional compressed-file size and logical reconstruction using
one Parquet decoder. It measures no RSS, throughput, latency, lookup-index cost,
privacy-separated physical layout, recovery, backup or production NIP-11 corpus.
The selected semantic document/object distinction therefore remains independent
of the final physical wide-versus-split choice in P02-T09.

## Executed relay-profile arithmetic check

The [independent minute-enumeration probe](../../experiments/relay-profile/README.md)
reconstructs the hand fixture from anchors, not from its written duration totals.
It confirms S/F/U = 40/15/1385 minutes, A=8/11, coverage=11/288 and lower p50
200000 us; Q is approximately 0.76156949. A method boundary at minute 60 yields
30/15/1395 and Q approximately 0.72369070. Probe-count availability, averaging
middle latencies and ignoring the unknown anchor produce differing results;
all-failure inputs preserve A=0 while withholding L/Q. This checks the arithmetic
of a minute-aligned fixture only, not product clocks, correction/window engines,
statistical sufficiency or the usefulness of the selected weights.

## Review and practical verification obligations

Every coherent submitted change receives a substantive final-diff and contract/
call-chain review with impact-appropriate actual entrypoint evidence. The
[engineering discipline](../engineering/security-recovery-and-validation.md)
requires real storage/protocol/fault/browser/tutorial paths where affected,
meaningful independent oracles, explicit evidence reuse limits and honest NOT_RUN
results. Unit tests remain useful; neither their count nor another clean-review
quota certifies the product. Future release acceptance must cover every advertised
required journey, including offline export/restore and combined load.

The plan still has **116 tasks / 716 numbered steps**. These counts describe its
current work breakdown, not a quality threshold. Planning completion does not
check off future product tests. New source code in this round is a bounded
research probes only.

## Final verification performed

The [mechanical report](../research/evidence/artifact-checks.json) records checks
of the resulting 82 Markdown files, 46 JSON files, four JSON examples, Python
syntax, local links/fences/whitespace and selected historical evidence hashes.
An additional check resolved 297 local heading links, all concrete task IDs,
116 unique tasks/716 steps and zero checked future-implementation steps.
Independent source/output hashing and actual Parquet metadata checks covered all
348 files from both layout runs; relay-profile source/section/result artifacts
matched. Final `git diff --check` was clean at verification.

The first direct redirection of checker output briefly emptied the report being
checked and produced a self-report JSON error. Capturing output before replacing
the report fixed that workflow; the complete subsequent check returned no errors.
These mechanical results are distinct from the semantic review and do not claim
that a future product test ran.

## Confidence and remaining alternatives

The selected stack remains Rust, PostgreSQL control/separate DuckLake metadata,
synchronized Rocks admission, DuckLake Parquet history, CH online serving, and
DuckDB archival/offline jobs. The second full event body in CH is an explicit
space/latency cost. Archive-only and tiered modes expose their differing read
capabilities. Go remains a viable alternative; socket memory alone does not
settle simultaneous busy-relay capacity.

Physical CH layout, document placement, common-query search backend, NEG index
value, isolated/native DuckDB integration and multi-host durability still require
their scoped experiments. Relay-score weights are an explainable versioned
selection, not measured universal utility. Heavy expiry may make strict ordinary
snapshot continuation inconvenient; its live-traversal alternative is named and
must change client semantics explicitly if selected.

No claim is made that every choice is globally optimal, all future private
function names can be predetermined, no defect remains, or 100,000 busy relays
and billion-row serving fit one machine. The handoff now states the newly found
decisions, costs, alternatives, owners and discriminating evidence in writing.
