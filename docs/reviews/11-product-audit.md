# Product, interface and operational vocabulary audit

Date: 2026-09-11. Baseline: `8c81472e75da118b04809636a73f3231157227b5`.
This is a planning review. It finds missing handoffs and proposes concrete
repairs; it does not establish product implementation, runtime correctness,
accessibility, production capacity or a universally optimal architecture.

The baseline is already substantial: it preserves the full product, separates
its three browser surfaces, explains Rust reuse and includes practical tests.
Its main weakness for a newcomer is that the reader must assemble exact names,
state owners and interface mappings from several long documents. The remaining
product gaps are account-management completion and published-artifact access.
Review 10's recommendation to choose tests by change impact also needs a current
engineering owner rather than remaining only in a historical report.

## Read scope and evidence

The [read manifest](../research/evidence/11-product-read-manifest.json) records
all thirteen assigned files, complete byte/line counts and SHA-256 values,
contiguous read ranges, baseline provenance and final Git-object verification.
The assigned baseline totals **3,191 lines and 383,671 bytes**.
Assigned reading used `git show` against the immutable baseline, rather than
assuming a concurrently edited worktree still matched it. The coordinator's
baseline inventory supplies the initial hashes; this reviewer independently
verifies the corresponding Git objects afterward. It is not described as an
independent pre-read worktree capture.

One initial combined display was truncated. Each of its four source documents
was subsequently reread in complete, smaller outputs. All other assigned reads
used full outputs or contiguous chunks; search hits were not counted as full
reads. Supplementary reading is separately recorded, including the current
entry path and selected shared/read/storage contracts. This review reads the
historical Go walkthrough as a report; it does not claim to reread its original
Go source inventory or rerun its tests.

Only this report and its read manifest are written by this reviewer. The
coordinator owns canonical repairs. No Go or historical repository was accessed
for new source material, modified, built or tested; no service, browser, Tux
workload, deployment, commit or push was run. No new external-library guarantee
is adopted from memory; existing research claims retain their original scope.

## Findings and concrete repairs

Severity describes planning readiness, not a deployed vulnerability.

### PROD11-01 — Medium: the entry path assumes the vocabulary it should teach

Baseline `README.md:10` sends readers directly to PLAN. PLAN's opening sends
them onward to tasks, field contracts and a coverage ledger before the service
and deployment terms have a short explanation. The separate
[Rust guide](../engineering/rust-and-repository.md) already distinguishes
module, library/binary crate, package, workspace, feature, release packaging
and extension correctly. The product documents explain the audiences of
Dashboard, Administration and Website. Those strengths need an earlier home.

Add one newcomer entry map before the execution plan. Explain one event's
journey from guarded acquisition through verification, durable admission,
archive/index publication, derived facts and authorized reads; then show how
an operator configures and recovers the same product. Link a single catalog
mapping each stable role name to purpose, invocation/lifecycle, module, durable
state, inputs/outputs, permissions and owning task. Explain that a reusable
Rust library, an application role, a running process and an image are distinct.
Keep the fifteen historical responsibilities traceable without requiring one
container or crate per responsibility. Add internal owners such as management,
ingest, archive publication and maintenance without pretending they are all
new mandatory daemons.

The proposed repair is an entry document and indexes over existing contracts,
not a second manually maintained architecture. Verify that a newcomer can
trace an event read, a paused role and a saved configuration to exact owners
and instructions without first reading historical review chronology.

### PROD11-02 — High: promised private workflows lack complete command mappings

[P08](../implementation/08-operations.md), lines 35–54, has exact enrollment,
session create/logout, principal listing/grant update and token issue/revoke
routes. [P09](../implementation/09-product-interfaces.md), lines 116–127 and
160–169, also promises user removal, reauthentication and secret-reference
submission. The P08 text says those mechanisms exist but does not give a
complete interface for creating another account, deactivating one, completing
a credential reset, reauthenticating or listing tokens. Its secret forms need
an explicit boundary between registering a protected reference and sending raw
secret material. Persisted saved-view routes exist later in P08, outside its
initial route index.

The following is a minimal proposed closure, sent to the coordinator. These
are proposed names, not claims that routes already exist:

| Private operation | Proposed mapping and behavior |
|---|---|
| Create an enrolled account | `POST /management/v1/principals`; administrator action, expected registry revision and idempotency; create a short-lived one-time setup credential for that principal through protected output, without sending email |
| Deactivate/reactivate/reset credentials | `POST /management/v1/principals/{principal_id}/commands`, typed `deactivate`, `reactivate`, `reset-credential`; preserve actor identity and audit history instead of deleting attributed records |
| Complete setup/reset | `POST /management/v1/credentials/reset`; only a short-lived, principal-bound, single-use setup/reset credential authorizes replacing that credential; this is not anonymous management authority |
| Reauthenticate | `POST /management/v1/sessions/current/reauthenticate`; produce an expiring session/principal/action-bound proof for sensitive commands; do not infer recent authentication merely from an unexpired session |
| Discover tokens | `GET /management/v1/tokens`; bounded authorized metadata with the existing listing rules, never raw credentials or password/token hashes |
| Register a secret reference | `PUT /management/v1/secret-references/{reference_id}`; accept a configured provider and allowed opaque handle/reference, with permission and revision checks; no raw secret-value upload, arbitrary filesystem browser or provider activation |
| Saved queries | Include the existing `GET|POST /management/v1/saved-views` and `PUT|DELETE /management/v1/saved-views/{view_id}` in the route catalog |
| Configure publishing, routes and exposure | Use the existing configuration preview/revision transaction and explicit signer/security grants; do not invent competing setters that bypass the activation barrier |

The default secret-value provisioning path can remain the local deployment/CLI
mechanism; the browser registers or selects safe references. If browser secret
upload is deliberately selected later, it needs a separate write-only contract,
size limits, protected provider transaction and redacted evidence. A reference
metadata/list route is justified only when a selected panel workflow needs it.

Deactivation and token/reference restrictions use the existing five-second
authorization lease and affected-role activation barrier. Preserve last-owner
recovery and the distinction between accepted revision and globally applied
restriction. Reactivation must not resurrect previously revoked credentials.
Resetting a credential does not change data grants or silently activate signing.

The existing hash-only token storage creates an additional important case:
after losing an issue response, retry must resolve the same token ID/receipt.
It cannot recover an original secret that was never stored recoverably. Report
that the one-time credential is unavailable and provide explicit revoke/reissue;
do not silently create another token or imply the old token was never issued.
Apply the same honest delivery distinction to one-time setup/reset credentials.

Practical acceptance must create an additional account, complete enrollment,
exercise viewer/operator/administrator and explicit grants directly through the
server, deactivate during a read/export with notifications lost, list tokens
from a clean browser, and recover a lost issuance response. The independent
oracle inspects grants, receipts, denied output after authority expires and
redacted audit references. A menu test or preloaded principal fixture does not
exercise the missing creation/reset transition.

### PROD11-03 — Medium: starting a role needs one precise lifecycle meaning

[Private administration](../product/private-administration.md), line 20,
promises Start. P08-T03 describes `pause|drain|resume`, then says start/stop
controls a selected in-process role. The exact route table does not settle
the additional command states. This can produce a Start button with no
contract or a command that accidentally becomes a deployment manager.

Extend the existing role command with explicit `start|pause|drain|resume|stop`
semantics if Start/Stop remain part of the selected panel. Start acts only on a
configured role supervised by the receiving BigBrotr process, after dependency,
budget, policy and ownership checks. It cannot install infrastructure, spawn a
remote container, enable an undeclared listener or provision secret roots.
Stop joins or positively excludes work before releasing ownership; pause retains
its documented resumable state; resume does not create an unconfigured process.
The durable command receipt and actual supervisor state must agree after a lost
reply or restart. A stopped role requiring process restart reports that outcome.

### PROD11-04 — High: published datasets have a catalog but no exact public read/download contract

Review 10's publication finding was repaired: P08-T04 now owns migration 0804,
private artifact discovery, publish/withdraw, immutable manifest verification,
pins and current source policy. That previous defect is not reported as still
open. The remaining handoff is public discovery/download. P06-T05 names event,
generic resource, count, meta and capability routes; P08 refers to an authorized
HTTP artifact adapter; P09-T06 requires a real dataset list/detail/download.
Neither an exact artifact resource name nor its file/manifest route is selected
in the inspected baseline.

Select a stable `published_datasets` resource for bounded catalog discovery and
detail by artifact ID, then name original-manifest and referenced-file routes,
for example `GET /v1/artifacts/{artifact_id}/manifest` and
`GET|HEAD /v1/artifacts/{artifact_id}/files/{file_id}`. File IDs resolve only
through the verified manifest/catalog; clients cannot submit arbitrary paths or
storage URLs. Define headers, original bytes/digests, size and supported range
behavior. Metadata HEAD requests obey the same authorization as GET. Publication,
withdrawal, expiry and policy restriction are checked at request and within the
existing bounded streaming interval. Independent downloads remain outside recall
guarantees. These reads belong to the API/artifact adapter; they do not create a
public job scheduler or permit arbitrary exports.

The real journey must export privately, publish the exact manifest with its
grant, discover it through the generated client, download and verify every
referenced file, then withdraw/restrict it during another download. The source
manifest and actual bytes are the oracle. Public catalog entries cannot outlive
the pins or authorization needed to serve their promised files.

### PROD11-05 — Medium: operational state has migration owners but not an exact table catalog

[P02-T01](../implementation/02-storage.md) describes typed control state and
privileges; the [component contract](../specification/component-contracts.md),
lines 161–186, reserves migrations. The assigned product and operations documents
name data responsibilities rather than stable physical table names. That was
adequate for broad architecture, but it does not answer this request for every
table, role and interface. A migration filename is not a table definition.

The coordinator should own a single logical/physical catalog with exact table
names, primary/natural identity, important foreign keys, state/revision fields,
writer role, authorized readers, retention/pins, recovery behavior, migration
owner and public-resource mapping. Keep tables out of reusable model APIs and
public HTTP naming. Separate application-control schema/database ownership from
DuckLake's own catalog; do not present extension-owned catalog internals as
BigBrotr-owned public tables.

## Operational PostgreSQL inventory for the catalog owner

These are confirmed **migration owners and required state families**. Exact
physical table names are intentionally left to the coordinator's central
catalog; the baseline has not selected a table with every name below.

| Reserved PG migration | Required state and key/recovery distinction |
|---|---|
| `0001_control.sql` | Dataset/topology, role-instance identity, desired configuration/policy revisions, owner epochs/leases and control versions; liveness expiry alone does not fence an old writer |
| `0002_ingest.sql` | Source ownership/range and archive/projection frontier coordination; frontier advancement requires its durable receipts, while exact local identity/pending data belongs to RocksDB |
| `0003_relay.sql` | Canonical endpoint enrollment and route/policy references; immutable endpoint ID is distinct from mutable backoff/health |
| `0004_acquisition.sql` | Scoped traversal, coverage, resumable work and ownership; no cursor advances beyond attributable durable evidence |
| `0005_import.sql` | Immutable source digest/unit identity and import progress; moving/retrying a file cannot create new source identity or local relay observations |
| `0006_relay_lifecycle.sql` | Scheduling, eligibility, backoff and attempt references; queued work does not become a permanent executed measurement |
| `0501` analytics control | Definition/input/visibility manifests, target jobs and complete-generation activation; an empty complete target differs from missing or partial |
| `0603` read pins | Authorized source/generation pins and expiry; snapshot retention is not an authorization exemption |
| `0609`, `0610`, `0611` DVM control | Request ledger, subscriptions and selected payment state; durable acceptance/replay identity and signed result obligations remain distinct |
| `0703`, `0704` publication | Immutable signed outbox identity and per-destination obligations/receipts; one ACK cannot satisfy every target |
| `0705`, `0707` publisher progress | NIP-85 and NIP-66 traversal/coordinate progress; pending bytes, completed traversal and compact retained lineage have different lifetimes |
| `0801_management_identity.sql` | Principals, local password records, bootstrap/setup/reset credentials, sessions, scoped tokens, grants, protected reference metadata and attributable audit; secrets do not enter public config history |
| `0802_management_commands.sql` | Actor/key/digest command receipts, desired/applied revisions, per-role application, durable jobs/progress/cancellation; accepted job and its source/authorization pins commit together |
| `0803_saved_views.sql` | Owner-scoped query plus label/revision; reopening reauthorizes instead of reusing historical permissions |
| `0804_artifact_publication.sql` | Immutable artifact/manifest/file identity, visibility basis, location reference, owner/revision/publication state and retention pin; complete export remains private until explicitly published |

The detailed job display/audit TTLs do not authorize reclaiming pending effects,
replay tombstones, signing-coordinate clocks, import identity or required source
metadata. References needed by active jobs, published artifacts, backup/restore
or complete generations protect their data independently of a log's expiry.

## Stable surface and resource inventory proposal

The deployment/API distinction is already sound and should be retained:

| Interface | Owner and intended consumer |
|---|---|
| BigBrotr CLI | The private Rust application dispatches the same typed domain operations used by management; it also supplies local setup/recovery |
| Internal authenticated RPC | Process-to-process owner/admission/control boundary; no shared writable embedded directory or public management proxy |
| Public/authorized HTTP | Shared admitted queries, sanitized metadata and explicitly published artifacts; dashboard, generated client and external language examples |
| ArchiveRelay | Nostr read/count/search/reconciliation plus committed history/live behavior under shared policy; no inbound archive writes |
| DVM | Explicit optional signed query-job profile with durable ledger and bounded delivery; not a generic public command runner |
| Private management | Authenticated commands, configuration, roles, jobs, grants, diagnostics and recovery plans; distinct listener/router |
| Dashboard / Admin | `apps/dashboard/` and `apps/admin/`, separate static applications with shared types/components; neither is a database role |
| Project site | `apps/site/`, static product/docs/blog/releases; independent artifact without archive credentials |
| Rust libraries | Candidate `bigbrotr-model`, `bigbrotr-protocol`, `bigbrotr-analytics`; introduced with useful real consumers, unpublished until their own API/reuse review |

The following names were proposed to the coordinator to make existing page
semantics addressable. They are a proposed registry, not new database tables or
an assertion that each resource has an implemented handler:

| Proposed stable resource IDs | Existing page/use and row meaning |
|---|---|
| `events`, `event_metadata`, `event_history` | Event search/detail and authorized history; ordinary complete event versus metadata/body state versus historical version |
| `event_provenance`, `event_availability` | Attributable event/endpoint observations versus dated executed availability outcomes; both retain their required provenance grants |
| `author_profiles`, `current_lists` | Author page, permitted parsed profile and current list header/entries, preserving empty/unknown/invalid distinctions |
| `relay_directory`, `relay_measurements`, `relay_documents` | Relay listing/detail, typed actual attempts and original evidence versions |
| `relay_coverage`, `relay_overlap`, `relay_availability` | Scoped traversal/coverage and comparable derived sets/availability strata |
| `network_statistics`, `event_statistics`, `social_statistics` | Network, archive and social panels with explicit denominator, cut, exactness and public-input basis |
| `scores`, `assertion_lineage`, `graph_subgraph` | Explained subject/type/algorithm/viewpoint/generation scores, source assertion basis and bounded graph expansion |
| `content_search` | Named indexed text semantics, coverage, ranking and compatible continuation |
| `published_datasets` | Verified publicly published artifact manifest metadata, independent of expired detailed job logs |

`/v1/events` and `/v1/events/{id}` can be documented aliases for the ordinary
`events` semantics. `/v1/meta` supplies safe instance/about/status description;
`/v1/capabilities` supplies caller-visible discovery. A distinct `/status`
dashboard page does not itself require another backend endpoint. Relay comparison
and selection reuse bounded resources under a common window and declared
incomparability; no automatic optimizer or extra daemon is implied. Final registry
selection must specify each row unit, filters/sort/count support, ready inputs,
authorization and schema owner rather than copying only this list of names.

## Review and test requirements

The baseline already requires meaningful pure/unit tests, property/fuzz tests,
real pinned engine and socket integration, process/crash/fencing drills,
mixed-workload resource measurements, real browser/assistive journeys and
executable clean-bundle documentation. Its expected values come from independent
signed corpora, slow reducers, source manifests, external socket/process observers
and standalone export readers. It explicitly refuses coverage percentages,
arbitrary line limits, empty required suites and fabricated universal benchmarks.
No missing blanket test category was found in the assigned full-read scope.

The missing process statement is that **every coherent submitted change receives
substantive review**, including planning/documentation changes. Review should
inspect the concrete problem, final diff, owning contracts and affected call
chains; data/security/failure/resource/API/UX/docs consequences; actual evidence;
and remaining unknowns. State blocking findings and the verification that would
resolve them. A second review is justified by a changed design, new finding or
unresolved concern, not by a required count of clean rounds.

Record a change-impact decision with the affected scenarios. Reuse evidence for
unchanged components only when the relevant component/input/schema/dependency/
artifact identities and execution conditions remain compatible. New browser
assets require their affected journey; control/storage changes require affected
real-engine and failure cases; edited commands require execution from the built
guide. Pure editorial changes do not automatically require unrelated full-host
restore and load reruns. A whole product release still owes every required
advertised role and journey, including actual clean-install/restore/browser
evidence. Reuse cannot turn a required NOT RUN case into a pass.

The private-account and artifact journeys above are concrete additions because
their missing routes could otherwise remain untested behind prepared fixtures.
Keep positive and negative controls that reveal a competing wrong result; do not
require pointless source mutation or add tests that merely repeat an expression.
Observe total work/memory/bytes and real cancellation under skew, not only a
returned row cap or a quick helper response.

## Alternatives and limits

SvelteKit applications with an Astro/Starlight site remain a reasonable selected
tradeoff. Existing research records React and Rust templates/htmx as viable
alternatives and explains static-client startup/deep-link costs. This review did
not benchmark those choices and finds no evidence of universal superiority.
Keep a small shared client and accessible components; add cache/framework/plugin
dependencies only for an actual measured need. Likewise, exact table naming
improves ownership and migration review but does not require one table per state
field or one crate/container per service.

Canonical adoption and final reconciliation belong to the coordinator. This
report establishes complete reading of its assigned baseline and identifies
specific planning changes. It supplies no new product test or capacity result.
