# Bounded countercheck of operational and implementation handoff

Date: 2026-09-17. Status: findings repaired and affected checks rerun; no product code or runtime
qualification. This AI reviewer authored the identity contract and signed fixture
in this planning session, but did not author the operational/workload contracts
or implementing-agent guide. The review is a cross-contract countercheck, not a
claim of independent authorship of every artifact reviewed.

Read in full: `operations-contract.md`, `workload-contract.md`,
`implementation/agent-handoff.md`, `planning/specification-readiness.md`.
Checked affected foundation/storage/analytics/operations tasks, dependency index,
coverage ledger and backup/data catalog paragraphs. No engine, Tux process,
network workload or legacy repository was changed. The initial review wrote only
this report. The root agent then authorized the minimal repairs recorded below;
findings describe the pre-repair state.
The final acquisition pass also read acquisition architecture and P03/P04 task
contracts against the adopted identity and operational boundaries.

## 1. P01-T04's new durable configuration work creates a task dependency cycle

**Priority: P1 — blocks the prescribed execution order.**

[P01-T04 step 6](../implementation/01-foundation.md#p01-t04--configuration-defaults-and-topology-resolution)
(line 134 at review) requires managed revision CAS, declarative activation,
atomic mode-switch and restore reconciliation. Those operations require the PG
control schema/transactions owned by
[P02-T01](../implementation/02-storage.md#p02-t01--typed-control-schemas-and-minimum-relay-registry)
(line 27), which currently depends on the whole P01-T04. P01-T05 and P01-T06
also depend on the whole P01-T04 (foundation lines 147/184). The existing
core-versus-storage-acceptance exception covers T06/T07, not this new T04 cycle.

An implementing agent following “select a task whose prerequisites exist” cannot
finish either side without inventing an undocumented split or moving migration
ownership. This is a dependency-contract issue, not missing product code.

**Minimal fix:** explicitly identify T04's static schema/normalization/recipe and
read-only CLI core. Let T05/T06 and P02-T01 depend on that core only. Finish T04's
PG-backed authority step and integrated acceptance after P02-T01. Record the
same edge in the phase index/coverage note; no new task or second schema owner is
needed.

## 2. The required signed fixture uses an unsupported initial shard topology

**Priority: P1 — its literal cut oracle cannot run on the declared initial recipe.**

[Operations, split recipe](../specification/operations-contract.md#process-placement-and-capabilities)
(line 113) says the initial dataset topology remains one shard. The
[signed fixture](../../experiments/identity-contract/signed-fixture.json), required
by S07, instead bootstraps dictionaries in shard 1 and event facts in shard 0.
Its generator assigns those dictionary source IDs at line 164 and constructs
two-shard cuts at line 240 of
[signed_fixture.py](../../experiments/identity-contract/signed_fixture.py).
This is real source identity, not two physical tables inside one logical stream.

**Minimal fix:** retain the selected one-shard topology and place bootstrap
dictionaries before event facts in the same shard/epoch. Offset event sequences
by the bootstrap count (18 without provenance, 24 with provenance). Final complete
cuts then end at sequence 63/86 respectively, rather than exposing two stream
bounds ending at 45+18 or 62+24. Regenerate the frozen fixture and evidence, and
update the short fixture explanations. Event IDs, signatures, scoped metadata,
graph and public/private export answers need not change.

## 3. Nominal new-scope quota is incompatible with the scoped edge quota

**Priority: P1 — the fixed nominal workload cannot realize its specified categories.**

[Workload tuple](../specification/workload-contract.md#manifest-counts-and-host-envelope)
(lines 50–55) partitions D into 10% new bodies, 40% new edges on known bodies
and 50% existing edges. It then selects new-scope S and corrections C from the
existing-edge population and permits them to overlap. Under the selected exact
key `(event,endpoint,observer,access_scope,visibility_scope)`, a newly admitted
visibility scope cannot already have an edge or receive minimum: those facts
would already imply that scope's eligibility. The new-scope delivery creates a
new P edge; it cannot be an existing scoped-edge delivery or an earlier correction
to that same scope's nonexistent minimum.

The generator must therefore violate either its S quota, its P count or its
claimed existing-edge percentage. “Report actual rates” does not repair a
mathematically incompatible nominal input, especially when setup must fail if
it cannot realize the registered cardinalities.

**Minimal fix:** choose S from the 40% known-body/new-edge bucket; P=0.5D already
includes those edges. Choose C from the existing-edge bucket. Treat S/C as
disjoint for an individual event delivery. State the provenance-off mapping
separately: S still creates eligibility/first receive while no witness is stored.
Keep all headline workload rates and their provisional status.

## 4. Resource contract promises a new document-chunk protocol without its contract

**Priority: P2 — blocks implementing the advertised oversized-document path.**

[Maximum units](../specification/operations-contract.md#maximum-units-reservations-and-progress)
(lines 149–154) says documents larger than F use bounded content chunks plus a
final reference manifest. A06 instead specifies one object row with original
bounded bytes, and the [storage catalog](../specification/storage-catalog.md#ducklake-scientific-and-archival-relations)
has no authoritative chunk/assembly manifest family. There is no selected chunk
identity, source-record schema, incomplete-upload reclamation or restore/export
closure rule for this proposed path. It cannot be treated as a private encoder
detail when it crosses independent admission records and adds a commit boundary.

**Minimal fix:** remove chunked document admission from the initial contract.
Require the maximum configured retained document, its envelope and required
admission metadata to fit F; reject an incompatible configuration/input before
admission. The default D=1 MiB/F=2 MiB already follows that direction. Keep large
Geo/download job artifacts on their existing independent artifact contract.
Selecting a future chunked scientific object format would require its own
explicit schema/recovery decision, which this closure does not need.

## 5. Receipt-only acquisition progress excludes the adopted certification path

**Priority: P1 — forces unnecessary mutation writes or blocks certified progress.**

[Acquisition admission](../architecture/acquisition-services.md#durable-ingest-contract)
(lines 47–60 before repair), P03's shared PageOutcome, T01 step 3, T02 steps 3–4,
T05 step 2 and T10 steps 3–4 require receipts/request identities for every event
basis and checkpoint. The adopted CertifyExisting operation explicitly allocates
neither. A page of valid known events must otherwise issue strict Admit calls and
their durable request outcomes solely to obtain the old task's required receipt,
or remain unable to checkpoint despite sufficient retained source dependencies.

**Applied repair:** `EventEffectBasis = Receipt | ExistingProof` is the acquisition
basis, without modifying AdmissionResult or strict Admit conflict/retirement rules.
The producer checkpoints bounded classified prefixes/pages with deduplicated
dependency closure, compacts only through retained complete source-cut manifests,
and ACKs only contiguous mutation requests. Proof-only processing creates no
request sequence, ingest ACK or synthetic receipt. New effects/NeedsAdmission and
separate sidecar/measurement effects retain their mutation paths. A lost proof
reply is read again; an unresolved Admit must resolve its original request first.
Updated the architecture, component/operations/storage checkpoint boundaries,
P01 model ownership, P02 admission handoff and P03/P04 practical requirements.

## 6. Acquisition witness and reconciliation keys omit target visibility

**Priority: P1 — private history can suppress required public acquisition.**

[Witness compaction](../architecture/acquisition-services.md#shared-records-and-invariants)
(line 43 before repair), historical Negentropy (lines 286–288), P03's acquisition
scope and T06 step 1 name endpoint/observer/access but omit visibility. A private
witness can therefore enter a public request's local set, removing E from its
need list before certification could demand public eligibility/first provenance.
Even public import eligibility cannot turn that private witness into a public
network observation. This conflicts with the canonical visibility-qualified key.

**Applied repair:** acquisition scope carries target visibility separately from
access scope. Witness minima and local-set membership use that exact dimension;
current authorized archive eligibility and scoped witnesses are selected before
set construction. Provenance-off body acquisition still requires target-scope
eligibility, without retaining an endpoint pair. Added P03/P04 acceptance cases
for both arrival orders, imported eligibility, restricted scopes and earlier cuts.

## Scope of the conclusion

The protection classes, provisional workload targets and distinction between
model evidence and real adapters are stated honestly. No throughput/RPO/RTO or
engine crash proof is being inferred from the small probes. The signed corpus
really supplies distinct valid signatures for one ID and explicit public graph,
expiry and scoped export answers; the topology finding does not invalidate those
cryptographic or logical checks.

The agent guide is useful and does not require arbitrary repeat-review counts or
premature scaffolding. The initial four findings and final two acquisition findings required small contract/task repairs
before the blanket “closed semantic specifications” disposition was final. They do not
justify changing the storage stack, adding a service, inventing a generic
framework, running a large benchmark, or postponing ordinary semantics until
implementation. Real backup catalog/file capture, positive fencing and reserved
progress remain required adapter qualifications already owned by P02/P08.

## Disposition and bounded verification

| Finding | Applied repair |
|---|---|
| Configuration dependency cycle | P01-T04 now defines its static steps 1–5 core. P01-T05/T06 and P02-T01 consume that core; durable T04 step 6 and persistence acceptance follow P02-T01. Phase index/coverage repeat the same edge. |
| Fixture topology | Bootstrap dictionary identities and subsequent event batches now share shard0/epoch1. Frozen cuts end at 63/86, including 18/24 bootstrap facts; all event IDs/signatures/read/graph/export answers are unchanged. |
| C/S workload categories | S comes from known-body/new-edge deliveries and is included in P; C comes from existing-edge deliveries. They are disjoint for one delivery; provenance-off behavior remains explicit. |
| Unspecified chunk protocol | Initial scientific document admission has no chunk/assembly path; D plus envelope/required metadata must fit F or be refused before admission. Large job artifacts keep their existing separate contract. |
| Receipt-only acquisition progress | Page/import event bases accept Receipt or ExistingProof with bounded retained closure. Only mutations allocate requests/ACKs; separate executed evidence still has receipts. |
| Visibility missing from local inventory | Acquisition scopes/witness minima include target visibility. Local-set construction selects authorized eligibility and exact-scope witnesses before suppressing retrieval. |

The backup follow-up found the invariant already specified: all catalog-referenced
objects must be in closure (security/recovery and P08-T05). Operations step 2 now
makes capture ordering explicit: a short archive-publication/catalog-maintenance
barrier, resolved outstanding transactions, one consistent catalog image and its
own exact pinned inventory, then resumed live publication while copying. A single
historical data-snapshot pin cannot justify restoring a newer catalog with only
the older file set. This clarifies the existing invariant; actual engine capture
remains an implementation qualification, not a new paper guarantee.

After the shard repair, generation and independent verification of the frozen
signed fixture passed: 15 valid variants/14 IDs, 30 public query/graph cases,
four signed export reconstruction checks and exact one-shard cut assertions.
The encoding/history probe still passes 14 golden vectors, 52 scenario runs and
2,018 independent history comparisons. The existing operations model passes its
120 admission orderings and seven wrong-state controls; it does not execute the
new catalog barrier against real engines. The workload owner adds the arithmetic
counterexample to the workload probe and records its own evidence (completed:
D=2000/U=200/P=1000/C=2/S=1, four negative controls). The final
acquisition probe adds 44 independent encounter-history inventory comparisons,
two logical eight-delivery proof-only checkpoints with zero new requests/facts,
and six wrong-path controls. They catch receipt-only blocking, synthetic duplicate
mutations, private-witness/payload suppression, import-eligibility laundering,
missing restored scope dependencies and revoked new-checkpoint authority. These
logical assertions execute no NIP-77 exchange, PG transaction, fsync or ACK adapter.
The unchanged real signed fixture was reverified successfully after these repairs.
The local artifact checker also passed (105 Markdown files, 58 JSON files,
four JSON examples, no errors), and `git diff --check` passed. The integrating
agent retains responsibility for final shared-tree/pre-commit verification.
