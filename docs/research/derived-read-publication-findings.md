# Derivation, reads, export, and publication: research findings

Status: design research, reviewed 2026-09-11. This document separates historical
intent, defects or ambiguities in that intent, current protocol requirements, and
new proposals. The implementable service contracts are in
[derived-read-publication.md](../architecture/derived-read-publication.md).
No product code, benchmark, compatibility test, or live publication was performed
for this work. Reading historical plans is not an audit of the Go implementation.

Subsequent owner steering resolves storage fidelity: retain every accepted
complete signed original and deletion request immutably, with logical deletion
filtering on ordinary relay reads and explicit historical API/export resources.
There is one BigBrotr product/image. A possible LilBrotr recipe can omit provenance
while still retaining complete events; it must not silently reuse the old lossy
meaning. Lossy field selection belongs only in labeled analytical projections,
which are not standalone verifiable Nostr events. Low ranking never deletes
original archive records. Historical references to missing heavy fields below
describe a rejected old capability axis, not a new deployment option.

## Source and reading coverage

The frozen source root is `.research/source-snapshots/go/`; it is deliberately
ignored research material. Paths below are relative to that root. All listed
service documents and phase READMEs were read in full, including the extensive
phase-5 correction and experiment history. Spec line ranges refer to the frozen
file, not a moving upstream copy.

| Source | Coverage | Useful evidence |
|---|---|---|
| `docs/services/deriver.md` | Full | Journal watermark, target execution, facts and snapshots |
| `docs/services/ranker.md` | Full | Seeded trust, relay quality, run identity, no-emission state |
| `docs/services/nip85-publisher.md` | Full | Present pubkey-only scope versus broader intended assertion families |
| `docs/services/nip66-publisher.md` | Full | Stored observation translation, two publication cadences, identity and metadata |
| `docs/services/exporter.md` | Full | Native and JSONL export, resumability, scrub, backup claims |
| `docs/services/api.md` | Full | Shared resource registry, routes, admission and exposure policy |
| `docs/services/dvm.md` | Full | Nostr adapter, remote job priming, result construction |
| `docs/services/archive-relay.md` | Full | Current/history distinction, hybrid query paths, COUNT, delivery limits |
| `docs/superpowers/plans/bigbrotr/phase-5-derivation/README.md` | Full, including all 4,550 lines of appendices/corrections | Snapshot carry-forward, dirty periods, provenance verdicts, clustering, time travel, query experiments, repeated failures of earlier contracts |
| `docs/superpowers/plans/bigbrotr/phase-6-read-side/README.md` | Full | Shared read core, pagination, blocklist, exact counts, read-only boundaries and protocol seams |
| `docs/superpowers/plans/bigbrotr/phase-7-publication/README.md` | Full | Publication retries, signer adapters, provider identity, payload change detection |
| `docs/spec/BIGBROTR_VISION_DECISIONS_2026-06-29.md` | Lines 502–612, 740–873, 1846–2133, 2539–2792 | Product areas E/F/V, complete service config surface, services II.9.9–15 and their acceptance intent |
| New repository `AGENTS.md`, `docs/planning/mandate.md` | Full | Fresh design, English repository content, independent services, evidence and implementation boundary |

No corresponding current Go source code was inspected in this bounded work.
Statements about implementation are attributed to its service documents or plan
history. A future implementation audit must inspect code and execute it before
claiming a behavior is present. Historical ClickHouse measurements are evidence
for the measured query, data layout, version, and host only; they do not select
the fresh Rust storage stack.

Primary protocol documents were retrieved on 2026-09-11 from the official
`nostr-protocol` repositories. These links track `master`; implementation must
record exact tested revisions and maintain fixtures when specifications change.
Draft or merged status is not a stability guarantee.

## Recovered product intent and disposition

| Product behavior | Disposition and reason |
|---|---|
| Preserve all acquired event history while offering usable current views | Retain. Standard reads use current/deletion-aware views; historical exposure is an explicit resource and policy decision. |
| Derive global, social, relay, NIP-85, overlap, coverage, propagation, selective-availability, and retention-related information | Retain every family, with evidence and uncertainty attached. A missing capability suppresses a dependent result; it does not create a zero. |
| Recompute old facts when old events arrive or indexing is repaired | Retain. Drive affected partitions from ingestion progress and repair notices, not only event time. |
| Serve only complete derived output | Retain completeness, replace monolithic run coupling with independently committed target generations and explicit input manifests. |
| Carry complete current snapshots forward even when a run touches a small interval | Retain the semantic result; prefer immutable manifests sharing unchanged data instead of rewriting the full snapshot each run. |
| Curated trust and relay usefulness scores | Retain as separate, named viewpoints. Explicit curators and algorithm identity are part of the product, not incidental metadata. |
| Assertions for people, events, addressable content, and external identifiers | Retain all four intended families. Historical pubkey-only code documentation is a delivery gap, not a reason to remove the other families. |
| Publish independently measured relay discovery information | Retain as a translator over stored observations; publishing never schedules fresh probes. |
| Native migration/backup and portable, deduplicated research corpora | Retain both with distinct manifests, fidelity statements, privacy rules, and restore verification. |
| One query meaning across API, Nostr job requests, and archive relay | Retain one read core and resource registry; expose transport capabilities honestly rather than force identical protocols. |
| Search, counts, history, statistics, and provenance for public dashboard and external clients | Retain as first-class resources; search cannot be left as an unspecified dashboard feature. |
| DVM one-shot and recurring archive requests | Retain one-shot purpose; specify recurring delivery as an explicit application contract because it is not supplied by generic NIP-90. |
| Independently stopped, restarted, or omitted services | Retain. Durable progress and bounded backlogs define the effect of each stop. |

## Derivation findings

The phase-5 history repeatedly corrected contracts that initially sounded
complete but were not. A time window is not an ingestion watermark: an old event
arriving today changes an old partition, and a repair below the last watermark
requires an explicit dirty-partition record. A set of affected partitions must
not become a huge min-to-max interval. Replaying a partition must replace its
contribution or deterministically rebuild it; adding its counts again is wrong.

Current views have whole-object replacement semantics. A newer relay-list event
replaces the author's entire previous list; unioning row-level relay entries
resurrects removed relays. Equal timestamps use the lowest event ID. The old
lexicographically smallest `d`-tag workaround followed a lossy tag representation;
the new archive must preserve tag positions and use a documented protocol parser.
Malformed tags remain recoverable in raw history, with an explicit interpretation
status. They must not silently inherit valid fields from a superseded event.

The 15 historical targets remain useful: `current_replaceable_events`,
`current_addressable_events`, `current_relay_list_entries`, `global_facts`,
`social_facts`, `relay_facts`, `nip85_input_facts`, `relay_clusters`,
`relay_dedup_candidates`, `relay_cluster_edges`, `relay_coverage`,
`relay_state_aggregates`, `event_propagation_stats`,
`relay_selective_availability`, and `relay_availability_frontier`.
Their dependencies differ. Optional, expensive clustering should not make fresh
current-event reads unavailable. The historical `targets` switch sometimes
changed the manifest without avoiding computation; a new target switch must stop
that target's actual work and report intentional suppression.

Provenance has at least three materially different states: intentionally not
collected, collected but absent/insufficient for the query, and present. A store
failure is a fourth operational state, never evidence of intentional absence.
Third-party monitoring events can still supply attributed claims when local
event-to-relay provenance is disabled. Counts of samples and counts of distinct
monitor keys answer different questions; multiple keys do not establish
independent operators. Remove duplicate observations before averages, quantiles,
and weighted scores, while preserving conflicting claims as conflicts.

Relay overlap is advisory. The historical threshold of Jaccard 0.99 with at least
10,000 shared events over 24 hours is a useful initial analysis profile, not a
proof of common ownership or substitutability. Candidate pruning and overlap
must use the same event set and reference interval. Incomplete traversal can make
an ordinary relay look like a partial mirror. Connected components also do not
imply every pair meets the edge threshold. Preserve pairwise evidence, component
membership, sample method, and coverage independently; do not stop collecting an
endpoint because its component has another member.

Acquisition first-seen times reflect observer scheduling and historical imports.
They do not establish actual publication time or causal network propagation.
Likewise, a peer possessing an event absent from our observations does not prove
the other relay refused it. Direct served/not-served/unknown evidence needs probe
scope, access/auth context, observation time, and failure cause. Retention
frontiers are estimates over that evidence; no event-time minimum proves when a
relay stopped serving anything. Evidence histories need their own retention
policy, independent of deleting old derivation runs.

The phase-5 ClickHouse work usefully demonstrates that fewer materialized rows can
still mean more scanned bytes, high part counts can reverse an optimization, and
large exact distinct sets require substantial memory. It does not justify
permanent SQL lint rules, mandatory full-snapshot rewrites, or fixed in-memory
graph/corpus limits in the new implementation. Select execution strategies by
measured CPU, memory, temporary disk, scan bytes, and elapsed time together.

## Ranking and assertion findings

The intended seeded TrustRank is not ordinary global PageRank. Empty seeds must
produce a healthy withheld state, without falling back to global popularity.
The historical bundled seed selection came from another project's environment
example and effectively concentrated the viewpoint in one seed. That is an
editorial decision requiring an identifiable curator, not a defensible neutral
default. Ordered, deterministic execution is useful, but no fixed iteration
count proves universal Sybil resistance. A compromised seed or an influential
follow can confer rank on an attack cluster. The product must state these limits.

The original specification mixed score data, algorithm configuration, and service
identity. New score records need the subject, authoring service key, source input
generation, algorithm family/version, parameter and seed-set digests, curator or
viewpoint, observation window, population and normalization, missing evidence,
and raw versus published values. Relay quality is a separate algorithm family;
path-dependent RTT, sparse local evidence, and absent provenance must not be
reported as objective universal quality or unexplained zero scores.

[NIP-85](https://raw.githubusercontent.com/nostr-protocol/nips/master/85.md)
requires different signing service keys for distinct algorithms, including
personalized viewpoints. An algorithm tag under one shared key does not satisfy
that identity requirement. It defines four assertion subject families and
integer normalized ranks, while kind 10040 records a user's provider preferences.
A provider cannot create trust by publishing other users' preferences. Its kind-0
profile should explain who controls the service and the algorithm it uses.

Publication must freeze all inputs needed to reproduce an assertion. Joining a
Ranker output to an independently selected latest Deriver run can combine facts
that never belonged to one result. The original larger statistics package remains
in scope, but each count needs a versioned definition: received versus sent zaps,
validity rules, replies versus posts, quote versus repost, reports, active hours,
topic extraction, and first activity. Missing historical coverage changes the
meaning of totals and first-seen claims. Old checkpoint deletion does not retract
a previously accepted remote assertion; withdrawal and stale eligibility need
an explicit publication ledger and user-visible freshness policy.

## Publication and signing findings

Both publishers need durable desired-output records and per-relay delivery state.
Record exact signed bytes before sending. A crash after remote acknowledgement
but before local persistence causes a retry of the same event, not construction
of a fresh event. Network publication is at least once; an `OK` is the relay's
acceptance claim, not proof of permanent storage. Changing output targets requires
delivery to new targets even when the semantic payload is unchanged. Backfill
recomputation must not publish an older replacement timestamp that relays ignore.

The signer boundary covers more than two publishers: DVM replies and feedback,
NIP-42 authentication when enabled, any explicitly enabled monitor write probe,
and NIP-46 client transport messages also sign events. Purpose, key, allowed kinds,
and permitted request shape must be bound together. Generic KMS ECDSA support is
not sufficient evidence of Nostr-compatible BIP-340 Schnorr support.
[NIP-46](https://raw.githubusercontent.com/nostr-protocol/nips/master/46.md)
distinguishes client, remote signer, and actual user keys; the user key must be
discovered and checked, not inferred from the bunker URI. Its remote protocol uses
NIP-44 transport encryption and explicit method permissions. Remote approval or
reauthorization creates a waiting state, not authority to use another key.

[NIP-66](https://raw.githubusercontent.com/nostr-protocol/nips/master/66.md)
supports URL discovery subjects and pubkey-only subjects that need not be dialable.
The discovery format includes optional measured RTT, capability and network
information; monitor announcements are optional. The current timeout-tag prose
and example disagree about element order. Follow the chosen pinned interpretation
and test real consumers rather than silently treating both layouts as equivalent.
Monitoring extensions require named, documented vocabularies. Publication must
not copy precise geolocation, private endpoint details, or stale successful
measurements by default merely because a monitor stored them. Full endpoint
identity and host-level measurement scope are separate concerns.

## Read and DVM findings

The useful common read pipeline is admission, resource resolution, exposure
policy, access-plan selection, snapshot/quality resolution, and bounded delivery.
It needs refinement: resolving small control metadata is not a forbidden data
scan, and an estimated access plan must still enforce runtime scan, memory, and
deadline budgets. A low result limit does not bound the scan. Silently clamping
requested time or kinds changes a query's meaning and must be replaced with an
explicit refusal or a documented continuation contract.

Current resolution must precede the final time-filter test; filtering historical
versions first can resurrect a superseded event. Hybrid current/raw/index paths
need a disclosed common source cut or deliberately different freshness contracts.
Opaque pagination tokens must pin ordering, filters, policy epoch, and source
generation. A count must share the same visible predicate and union deduplication
as the events query. Counting physical duplicates and setting `approximate=true`
does not provide a useful statistical estimate.

[NIP-01](https://raw.githubusercontent.com/nostr-protocol/nips/master/01.md)
defines AND within filters, OR across filters, inclusive time bounds, and lowest-ID
ties for current events. Its subscriptions are ongoing; in particular `limit=0`
requires EOSE followed by an active subscription for new matches. The historical
open-but-silent post-EOSE contract is therefore unsuitable as a conforming default.
A private NIP-11 field cannot make all clients understand altered behavior.
[NIP-11](https://raw.githubusercontent.com/nostr-protocol/nips/master/11.md)
is the place to advertise actual supported NIPs and limits, not unsupported
conformance claims. [NIP-09](https://raw.githubusercontent.com/nostr-protocol/nips/master/09.md)
requires ownership checks for deletion requests; local honoring does not guarantee
global deletion. The retained archive and public exposure must be distinct.

[NIP-45](https://raw.githubusercontent.com/nostr-protocol/nips/master/45.md)
allows an explicitly approximate count and defines a specialized HLL response.
Its pubkey-oriented HLL construction must not be confused with a generic event-ID
estimator. [NIP-50](https://raw.githubusercontent.com/nostr-protocol/nips/master/50.md)
expects relevance ordering for search, allows implementation-specific matching,
and says unsupported search extensions should be ignored. Search therefore needs
its own stable ordering and capability description. An HTTP API can reject
unknown structured options more strictly while the Nostr adapter follows the
advertised NIP behavior.

[NIP-90](https://raw.githubusercontent.com/nostr-protocol/nips/master/90.md)
is now marked unrecommended and advises use-case-specific microstandards. Its
request/result and feedback envelope does not supply durable recurring-job
cursors, result acknowledgement, or a complete payment workflow. Its encrypted
parameter convention still names NIP-04; silently substituting a newer encryption
scheme would create a different contract. The historical default kind 5050 is
[AI text generation](https://raw.githubusercontent.com/nostr-protocol/data-vending-machines/master/kinds/5050.md),
not a generic archive query. Keep the useful Nostr query service, but explicitly
identify and document its supported profile and optional compatibility mode.

The service page describes rebuilding job memory from at most 5,000 results per
relay, while the vision promises durable subscriptions. Remote relays are neither
a complete local job ledger nor an atomic cursor store. Jobs, pinned inputs,
results, cancellations, and recurring delivery progress belong in local durable
state. Untrusted job URLs, output relays, chained jobs, encrypted content,
authentication, payment feedback, and error responses all create distinct network
and privacy risks. Invalid-signature requests must not become signing or feedback
amplification triggers. No requester-supplied URL is blanket egress authorization.

## Export and operational findings

The historical native segments and JSONL served different promises. Native export retained raw
records and acquisition duplicates; JSONL provides a portable event corpus with
explicit event-ID deduplication and optional separately authorized provenance.
Filter scope, missing heavy fields, unknown record types, and excluded control
records must be stated in the manifest. A very large corpus needs external-memory
deduplication, not an arbitrary count ceiling that changes export completeness.

Shipping files and subsequently dumping control state does not establish a
consistent backup while ingestion continues. Capture an explicit durable source
cut, pin its files/journal, and prove the control snapshot can restore that cut.
Restore must reconcile leases and publication watermarks: old leases cannot be
revived, and a rollback must not resume a signer with a stale publication sequence.
Files verified by checksums alone do not demonstrate service restoration. A real
blank-instance restore, import, index, query, and publication-resume inspection is
the acceptance path. A weekly scrub can detect damage and coverage discrepancies;
sampled signature checks must never be described as exhaustive verification.

Integration with the selected fresh storage design changes the native artifact:
deployment backups contain a consistent DuckLake catalog snapshot and its data,
required control/pending state and stable identity dictionaries. Portable Parquet
corpora materialize a selected logical snapshot without requiring the live catalog;
JSONL retains complete signed fields. Exact event and compact edge admission
suppress ordinary repeated acquisitions, so a native backup does not implicitly
contain delivery history or every JSON wire serialization. Those require their
separate optional capture modes. Immutable observation corrections retain their
source record identity and are reduced only after the pinned source-cut predicate.

The cross-service review also required one archive-maintenance scrub executor,
explicit per-stream/epoch source-cut vectors and projection row identities,
public-input visibility domains for public graph/aggregate generations, and a
bounded authorization-lease activation barrier for restrictive configuration
changes. A saved configuration revision or cached generation is not authority to
continue exposing revoked data. An instance created without provenance allocates
no edge state; disabling provenance later preserves its historical evidence and
introduces an explicit new coverage epoch.

## Product questions to carry into owner discussion

These are product choices; the root planning work owns recording answers in
`docs/planning/questions.md`. They are not requests to choose storage libraries.

1. Which explicit historical API/export resources should be public, and which
   require researcher authorization, especially encrypted-message history?
   Immutable retention and deletion-aware normal relay serving are already
   selected. Recommended: current public views plus deliberately granted history
   and provenance access, with one exposure policy shared by all surfaces.
2. Who is the editorial owner of the default trust seed set, and should BigBrotr
   offer multiple named viewpoints? Recommended: no trust-score emission until a
   curator and seed set are configured; distinct algorithm/viewpoint identities.
3. Is the intended Nostr service a free one-shot archive query, a durable recurring
   delivery product, a paid query product, or all three? Recommended: preserve the
   design for all, introduce a documented archive profile, enable free one-shot
   first, and keep recurring delivery and settlement explicit independent modes.
4. Do standard archive-relay clients need ongoing new-event delivery after their
   historical result? Recommended: implement a bounded committed-index feed for
   conforming ongoing subscriptions; reject the silent-open historical behavior.
5. Which externally published monitoring details are valuable enough to expose:
   coarse relay location, DNS/TLS metadata, or precise location? Recommended:
   public measured RTT/capabilities, optional coarse location, and separate opt-ins
   for potentially identifying endpoint/monitor metadata.

## Release evidence still required

Before implementation release: pin and review protocol revisions; choose the
archive job profile; confirm all four NIP-85 families with independent consumers;
exercise NIP-66 timeout interoperability; test actual signer capabilities;
establish query/graph/export resource budgets on representative data; and execute
crash/recovery, independent stop/catch-up, policy revocation, and blank-instance
restore scenarios. These are outstanding validation obligations, not completed
checks or reasons to omit the corresponding product areas.
