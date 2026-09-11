# Independent product, operations and interface audit

Date: 2026-09-11. Source cut assigned by the coordinator: `b543555`.
This audit fully reread the twelve assigned Markdown files, rather than only
their headings or earlier review summaries. It found concrete specification
handoffs to repair. It does **not** establish that every choice is optimal, that
the complete repository has been independently reread by this reviewer, or that
the product works.

The most consequential findings are an unspecified analytical-import branch,
missing export-publication control, and an incomplete acquisition-to-availability
handoff. These are planning defects or ambiguities, not reproduced runtime bugs.
The coordinator accepted standalone DuckDB analysis as the disposition for the
first finding; canonical document repairs remain a separate coordinated change.

## Exact read coverage and evidence boundary

The [machine-readable read manifest](../research/evidence/10-product-read-manifest.json)
contains each complete SHA-256, bytes, lines, contiguous fully read ranges and
before/after comparison. All twelve assigned files were unchanged between the
initial hash inventory and completion of their full reading. Their total is
**2,536 lines / 336,891 bytes**. No assigned full-file output was truncated.

| Assigned file, read completely | Lines | Bytes |
|---|---:|---:|
| `docs/product/dashboard-and-website.md` | 619 | 54,215 |
| `docs/product/private-administration.md` | 147 | 10,420 |
| `docs/product/data-and-feature-decisions.md` | 164 | 16,017 |
| `docs/product/requirements-and-history.md` | 123 | 8,788 |
| `docs/implementation/08-operations.md` | 274 | 57,579 |
| `docs/implementation/09-product-interfaces.md` | 198 | 39,163 |
| `docs/implementation/10-website.md` | 169 | 35,594 |
| `docs/research/product-surfaces-findings.md` | 233 | 25,147 |
| `docs/research/go-read-runtime-code-walkthrough.md` | 234 | 39,076 |
| `docs/reviews/07-phase-6-9-recheck.md` | 323 | 47,708 |
| `docs/reviews/05-complete-product.md` | 29 | 1,814 |
| `docs/reviews/01-vision-recovery.md` | 23 | 1,370 |

Also read completely: `docs/specification/import-formats.md`, 106 lines / 7,181
bytes, with its full SHA-256 recorded separately. Its digest was first recorded
after the full read; this report does not invent a pre-read comparison for it.

Targeted supplementary reading comprised P05-T08 at
`docs/implementation/05-analytics.md:414–453` and
`docs/architecture/analytics-detail.md:139–163,202–231`. Searches checked
availability inputs, import interpretation, and management/publication route
ownership in the current implementation/specification/architecture documents.
The manifest distinguishes these partial reads and search hits from full coverage.
Earlier authorship of acquisition plans does not count as rereading their full
current bytes during this independent product audit.

`b543555` is the coordinator-designated revision. No Git action or independent
comparison with Git objects was performed; exact inspected worktree hashes are
the evidence identity. Line references below identify those inspected bytes and
can move when the coordinator integrates repairs.

No product/Go tests, builds, running services, browser/device checks or deployment
actions were performed. No new legacy source files were read or changed. No new
library/protocol guarantee was adopted from memory or web research. The historical
Go walkthrough's source analysis and test descriptions remain attributed evidence;
reading that report does not independently recertify its 43 original source files.

## Findings

Severity here concerns implementation readiness: **high** means a selected
user journey lacks a necessary contract or conflicts with its source policy;
**medium** means an observable result or bounded adapter contract needs a specific
correction. These labels do not assert a deployed vulnerability.

### PROD-01 — High: analytical projections acquire an unspecified import product

**Competing text.**
[Dashboard/product](../product/dashboard-and-website.md), lines 226–231, offers a
separate analytical-projection import for body-incomplete legacy material.
[P08-T04](../implementation/08-operations.md), lines 111–119, sends projection
imports to a named analytical dataset path; P08-T06 line 150 repeats that path.
[Import formats](../specification/import-formats.md), lines 10–19, lists
`bigbrotr-analysis/1`, then says adapters yield signed-event candidates. By
contrast, `docs/architecture/acquisition-services.md:330` rejects such data from
canonical admission and offers separate analysis tooling.

**Impact.** Full analytical exports are useful, but this wording quietly adds a
second ingestion/data-catalog/query product. The selected tasks do not define its
retained schemas, namespace, source identity, query resources, visibility,
retention, indexing or migration. Treating it as an ordinary signed-event adapter
would also contradict the explicit full-event verification invariant. A future
implementer must otherwise either invent this branch or ship a dead migration
instruction.

**Disposition accepted by the coordinator.** Keep projected artifacts as standalone
DuckDB-readable analytical outputs, with manifest/schema/fidelity labels. Do not
select an in-product analytical dataset importer. Remove or qualify the format
table entry as an **export/analysis** format, exclude it from the signed-adapter
sentence, and replace the two migration instructions with the actual standalone
analysis guide. Full original events, when available, still use canonical import;
best-effort refetch does not fabricate missing fields or history.

**Discriminating future journey.** Produce a valid projected export, open it with
standalone DuckDB after stopping BigBrotr, then present it to the actual canonical
import CLI. Expect a precise analytical-projection refusal and no admission or
hidden analytical table. The downloaded full-event artifact must still import and
verify. This preserves the user's useful slim-export request while removing
unjustified product complexity.

### PROD-02 — High: public datasets lack their explicit publication transition

**Competing text.** [P08-T04](../implementation/08-operations.md), line 117,
correctly says publishing an export is a separate grant/command, distinct from
creating it. [P09-T06](../implementation/09-product-interfaces.md), lines 106–112,
requires that actual transition before public listing/download and explicitly
tests private, unpublished and expired artifacts. The P08 route catalog at
lines 35–50 and its job/schema declarations supply no exact artifact-publication
command, permission, catalog transition or withdrawal operation. The cross-document
route search found promises of publication, not that missing control contract.

**Impact.** A completed export cannot safely become public just because its files
exist. The dashboard's reproducible-dataset journey has no specified way to reach
its required public state. Implementers could mistakenly use export permission,
Nostr signer administration or direct database edits as publication authority.
Public artifact publication does not require a Nostr signing identity.

**Proposed repair.** Give P08-T04 explicit ownership of a small publication catalog
and typed CLI/private API publish/withdraw commands over immutable export digests.
Define the action grant, expected revision/idempotency, current source visibility,
verified complete manifest requirement, artifact pins, listing state and authorized
download behavior. Withdrawal removes future listing/access under its declared
policy; it does not pretend to recall copies already delivered. Distinguish
committing an export manifest from exposing that artifact to readers. This needs
a bounded command/catalog extension, not another daemon or generic publication
framework.

**Discriminating future journey.** An operator can export an allowed fixture but
lacks publication permission; no public dataset appears. A separately granted
actor publishes the exact verified artifact, loses the command response, retries
and sees one publication. The public dashboard downloads the expected hashes.
Withdrawal and source-policy restriction stop subsequent authorized access as
specified while private retention/backup remains truthful. An incomplete artifact
and a changed referenced document cannot be published.

### PROD-03 — High handoff gap: availability pages have a reducer but no clear selected producer

**Competing text.** [P09-T04](../implementation/09-product-interfaces.md), lines
76–81, implements dated positive/negative event availability; P09-T05 lines
90–93 implements selective-availability comparisons. P05-T08, inspected at
`docs/implementation/05-analytics.md:426–445`, consumes finalized per-event,
endpoint, access, method and interval attempts. The data catalog can represent
availability experiments. However, the acquisition plan primarily promises first
witnesses and compact proof/gaps, while
`docs/implementation/04-relay-lifecycle.md:324` describes availability experiments
as “if later enabled.” The actual producer-to-retained-attempt transition required
by the reducer is not identified in those task handoffs.

**Impact.** First-seen edges cannot produce a later negative or repeated
availability history. Bounded operational retry/need-list data cannot silently
become permanent scientific evidence. A consumer supplied only synthetic S/N/U
rows can pass its reducer/browser fixture without proving any shipped acquisition
path creates those rows. This is a gap in the acquisition plan I previously
authored too; frontend capability hiding alone does not complete the feature.

**Proposed repair.** Choose and document one concrete existing-worker producer:
for example an explicitly enabled, bounded Monitor availability profile/finite
job over a declared eligible event/endpoint sample. Alternatively, identify the
precise already-executed acquisition outcomes that supply the required retained
facts and their interpretation limits. Bind sample/target selection, method,
request outcome/finality, timestamps, receipt, budget and current provenance
permission; keep ordinary delivery/retry telemetry outside that dataset. Preserve
unknown for timeout, auth limitation and incomplete execution. Provenance-off
must create no event/endpoint experiment facts. If the producer is deliberately
not selected, explicitly mark this group as deferred product functionality rather
than imply P09 alone completes it.

**Discriminating future journey.** Actual controlled socket requests produce
served, completed-empty and timeout outcomes; inspect their durable typed inputs,
run the real reducer, then view the history/ratio in the browser. Change the
sample scope and disable provenance. Independent request and archive observers
must detect absent or misclassified producer facts; preloaded aggregate rows
cannot replace this journey.

### PROD-04 — Medium: availability eligibility alternates between attempts and distinct IDs

**Exact conflict.** P05-T08 at
`docs/implementation/05-analytics.md:434–435` requires “at least 20 classified
attempts.” Its fixture at lines 443–445 uses 19 versus 20 classified targets.
`docs/architecture/analytics-detail.md:223–227` defines S/N/U as distinct-ID sets
and requires a classified denominator of at least 20. The same P05 task's lines
437–439 explicitly distinguish target and attempt counts.

**Impact.** Nineteen event IDs fetched repeatedly could wrongly cross the display
eligibility threshold. The interface would present a supported ratio from fewer
distinct targets than its method claims. This is not repaired by a correct
percentage calculation after choosing the wrong population.

**Proposed repair.** Use classified **distinct event IDs within the endpoint/
access/method/interval stratum** consistently. Keep raw attempt counts as a
separate descriptive quantity. The initial threshold is display eligibility,
not statistical confidence.

**Discriminating future fixture.** Nineteen classified IDs with many retries stay
insufficient; twenty classified IDs become eligible. Duplicate replay changes
neither count. An ID's later finalized outcome replaces its contribution within
the defined interval instead of adding another target.

### PROD-05 — Medium: the Jobs page has no specified bounded job-discovery endpoint

**Exact handoff.** The private route catalog in
[P08 operations](../implementation/08-operations.md), line 46, defines job create,
detail-by-ID and cancel. [P09-T09](../implementation/09-product-interfaces.md),
lines 142–154, requires a Jobs page, reconnection/reopening and finding a job after
a lost creation response. P08-T03 persists the registry, but no bounded list/search
route or corresponding CLI list contract is specified in its exact route list.

**Impact.** A new browser/device or reopened Jobs page cannot rely on the old tab
remembering every job ID. Persisting one idempotency key helps resolve a particular
ambiguous command; it is not a permission-filtered job history interface. A UI
could otherwise invent an unbounded registry dump or depend on local-only history.

**Proposed repair.** Add a typed, bounded `GET /management/v1/jobs` and `jobs list`
mapping with supported kind/state/time filters, stable pagination, caller/job-data
visibility, redacted metadata and terminal-detail expiry behavior. Keep exact
command-key retry for the lost-response case. While completing the route/action
crosswalk, explicitly name the sources of private role/relay overview data rather
than assuming each page creates a new endpoint.

**Discriminating future journey.** Start allowed and restricted jobs from separate
principals, lose one response, open a clean browser session and list multiple
pages. Expect one recovered logical job and only authorized rows/metadata, with
explicit expiry for old detail. No browser-local cache can supply the oracle.

## Choices that remain defensible, with practical limits

The review did not find a reason to replace every selected tool or add new
components. It did find a reason to remove the unnamed analytical-import branch.
The following choices have a stated user purpose; none is proven best merely by
being written down.

| Choice | Assessment and cost consequence | Evidence still needed |
|---|---|---|
| Separate public dashboard and private administration artifacts/origins | Retain. Different authority and deployment requirements justify separate surfaces; shared typed clients/components avoid duplicating domain logic. Separation complements server authorization and is not its proof. | Actual proxy/router isolation, grants/revocation and bundle/runtime-origin tests in P08/P09. |
| SvelteKit applications plus existing Astro/Starlight site | Reasonable continuity and deployment tradeoff, not a universal speed or maintenance optimum. Both alternatives and static-output limitations are explicitly recorded in the source findings. Avoid adding a query-cache or blog plugin before its concrete need. | Pinned dependency/license review, production deep links, measured browser size/interaction and version-aware static search. |
| Complete signed events with optional provenance | Retain the selected scientific fidelity. Compact projections can save serving/export cost but cannot replace a verifiable event. Imported interpreted edges need the same provenance-off discipline. | Independent full-event roundtrip and physical no-provenance inventory/byte checks. |
| Permanent useful executed Monitor facts | This is an explicit scientific retention choice with potentially substantial M-sized cost, not free operational logging. The product's 28.8M-attempt/day example is qualified arithmetic, not achieved capacity or measured compression. | Measure actual cadence, dispatch/terminal/correction writes, documents, archive/index/backup costs and useful query workload before claiming an affordable fleet size. |
| Content-addressed monitoring documents | Retain identity, replay and reference semantics; do not infer a physical compression win from object count alone. | Compare total wide-compressed versus object/reference representations and query/recovery costs on identical changing/unchanged document corpora. |
| Generated contracts and versioned docs from one source | Retain to reduce command/config drift. Metadata and historical snapshots do have maintenance cost; they should map to real release/guide ownership rather than an extra editorial database. | Execute shown commands from built pages; reject stale source/schema/artifact references and verify historical-topic redirects. |
| Extensive P08/P09/P10 qualification | Relevant for a complete release. Avoid turning the three whole-product journeys into automatic repetition of every expensive scenario for an unrelated editorial correction. Reuse explicitly version-bound evidence for unchanged components; run affected output/journey checks. | A documented change-impact/evidence-reuse rule that still binds each released component to actual compatible results. This is an efficiency recommendation, not a reason to waive an unexecuted required product case. |

The whole backend engine selection, large-corpus planner and native cancellation
performance are outside this review's full-read scope. Product claims about their
fitness require the coordinator's other audits and actual experiments. A prose
comparison cannot prove a global optimum across different operators, datasets,
team skills and future maintenance costs.

## Historical reconciliation and selected behavior

The assigned research/review files consistently distinguish historical findings
from current contracts. Their old `PASS`, counts and gate outcomes were not
reused as Rust acceptance. The current plans explicitly retain the important
counterexamples: actual returned cursors across withheld rows, zero timestamps,
winner-before-filter behavior, current policy on direct-ID reads, bounded engine
probes, real private/public sockets and jobs detached from browser requests only
after durable acceptance.

Friendly LilBrotr wording is not an accidental return of body-loss storage:
`requirements-and-history.md:65–74` retires the historical profile semantics,
while lines 102–109 and the newer product document allow a visibly expanded
full-archive/no-provenance recipe. That distinction should remain legible in
migration instructions. It is not reported here as a contradiction.

Similarly, `01-vision-recovery.md:8–10` is an earlier revision's reconstruction;
its broad permanent-data wording does not override the later explicit separation
of bounded operation state and scientific facts. This report recommends preserving
that historical chronology instead of silently rewriting old reviews to imply
all decisions were settled from the beginning.

## Completion and residual work

The twelve-file assigned reread and the supplemental format-spec read are
complete at the hashes in the manifest. Findings PROD-01–05 were sent to the
coordinator; PROD-01's simplification was accepted, and source document repairs
remain the coordinator's work. This reviewer changed only this report and its
read manifest. Document coverage is established; product acceptance, performance,
accessibility and universal optimality are not.
