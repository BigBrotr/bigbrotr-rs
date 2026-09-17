# Protocol evolution, evidence, and incremental assertions

Status: proposed implementation and standards-development contract, 2026-09-11.
The selected design preserves full historical signed-event retention with
deletion-aware ordinary reads, following critical review of the owner's context. The extension names and proposed upstream wording below are
BigBrotr design proposals; nothing here has been submitted upstream or implemented.
The owner invited richer NIP-66/NIP-85 behavior, not automatic adoption of every
existing proposal or unrestricted collection of additional metadata.

The [shared assertion registry](derived-read-publication.md#assertion-registry-and-incremental-computation)
owns definitions, dependencies, provenance and computation. This document owns
their wire bindings, compatibility boundaries, measurement semantics and potential
standards changes. [Acquisition](acquisition-services.md) owns actual probes and
coverage traversal; the [publication contract](derived-read-publication.md) owns
signers, outboxes, delivery and read resources. All service families remain in
scope. Implementation order follows these dependencies, not different quality
standards for different services.

## Decisions

1. Implement a pinned, independently tested current NIP-66/NIP-85 subset first.
   Preserve correct event kinds, subject identifiers, units and signing identity.
   Document omissions; an experimental tag is not a newly standardized field.
2. Keep rich observations and assertions in the shared typed API regardless of
   publication. Add a small, opt-in BigBrotr metadata binding to otherwise valid
   standard events. Public consumers can verify its digest and ignore it safely.
   A deployment need not publish extensions to use rich local analytics.
3. Separate measured availability, observation coverage, freshness and editorial
   quality. Neither a timeout nor absence from a partial corpus proves censorship,
   nonexistence or low trust. A service key identifies a signer, not independence.
4. Incremental computation is a family of algorithms, not one generic materialized
   view. Use durable idempotent contributions and invalidation; budget global graph
   and clustering work explicitly. Preserve full signed originals for replay.
5. Retain accepted complete signed history, including effective deletion requests
   and superseded versions. Ordinary REQ, search and counts use the defined current,
   deletion-aware exposure policy. Historical access is a separate authorized
   resource. NIP-09 never triggers automatic physical archive erasure.
6. Split potential upstream work into editorial repair, measurement context and
   assertion context. Do not couple useful corrections to a large new taxonomy or
   a universal relay trust formula.
7. Add optional finite MCP and ContextVM gateways through the selected
   [gateway contract](../specification/gateway-contracts.md). Native MCP pins
   2026-07-28; initial experimental ContextVM pins 2025-11-25 MCP carried in
   kind-25910 Nostr messages with CEP-4 encryption. They expose the same typed
   registry and authority, remain disabled in every supplied recipe, and require
   independent client/bounded-resource qualification before advertisement.
   ContextVM is not NIP-90 or BigBrotr's durable DVM application profile.

### Gateway evolution and compatibility boundary

The [2026-09-17 gateway evidence](../research/evidence/gateway-research.json)
records separate exact revisions for MCP specification, official rmcp, ContextVM
documentation, and Rust/TypeScript SDKs. ContextVM's base protocol and CEP-8/19
extensions have draft status; transport similarity is not a compatibility proof.
The selected finite subset excludes CEP-8 payment, CEP-22 oversized transfer and
CEP-41 streams. Existing DVM finite, subscription and paid capabilities retain
their separate contracts and qualification. No generic public RPC is introduced.

Changing a gateway version requires generated request/result schemas, explicit
capability negotiation, negative version tests and an independent client journey
through continuation/authorization/cancellation. In particular, modern MCP uses
per-request metadata and required result/cache fields rather than legacy
initialization sessions. Do not rely on an SDK's `LATEST` constant or carry a
legacy response into a newer binding without validation. Passing local schema
and in-memory SDK tests is evidence of those cases, not production maturity or
permission to advertise untested extensions.

## Source baseline and reading coverage

The protocol baseline is nostr-protocol/nips commit
[`a2494f4f81d46684e5814a9bf35e2b1df978f955`](https://github.com/nostr-protocol/nips/commit/a2494f4f81d46684e5814a9bf35e2b1df978f955),
committed 2026-09-09 and inspected 2026-09-11. These NIPs are evolving drafts;
release notes must name the tested revision instead of claiming timeless support.
Protocol documents are public domain under the repository
[license](https://github.com/nostr-protocol/nips/blob/a2494f4f81d46684e5814a9bf35e2b1df978f955/README.md#license).

| Primary source | Reading and relevance |
|---|---|
| [NIP-01](https://github.com/nostr-protocol/nips/blob/a2494f4f81d46684e5814a9bf35e2b1df978f955/01.md) | Full; event hashing/signature fields, replaceability, filter semantics, EOSE and zero-limit live subscriptions. |
| [NIP-09](https://github.com/nostr-protocol/nips/blob/a2494f4f81d46684e5814a9bf35e2b1df978f955/09.md) | Full; deletion ownership, address cutoffs and the distinction between deletion and stopping publication. |
| [NIP-66](https://github.com/nostr-protocol/nips/blob/a2494f4f81d46684e5814a9bf35e2b1df978f955/66.md) | Full; required subject, optional measurements, monitor metadata, contradictory timeout ordering. |
| [NIP-85](https://github.com/nostr-protocol/nips/blob/a2494f4f81d46684e5814a9bf35e2b1df978f955/85.md) | Full; four assertion families, integer units, provider preferences and required algorithm-specific keys. |
| [NIP-11](https://github.com/nostr-protocol/nips/blob/a2494f4f81d46684e5814a9bf35e2b1df978f955/11.md) | Relay information and limitation sections, supported-NIP declarations and examples; values are relay declarations. |
| [NIP-32](https://github.com/nostr-protocol/nips/blob/a2494f4f81d46684e5814a9bf35e2b1df978f955/32.md) | Full; namespace/label relationships, non-label values and self-label semantics. |
| [NIP-40](https://github.com/nostr-protocol/nips/blob/a2494f4f81d46684e5814a9bf35e2b1df978f955/40.md) | Full; expiration is useful for serving policy, not guaranteed erasure or universal withdrawal. |
| [NIP-45](https://github.com/nostr-protocol/nips/blob/a2494f4f81d46684e5814a9bf35e2b1df978f955/45.md) | Full; COUNT, approximate results and a particular pubkey-based HLL wire format. |
| [NIP-57](https://github.com/nostr-protocol/nips/blob/a2494f4f81d46684e5814a9bf35e2b1df978f955/57.md) | Full; millisatoshi inputs, receipt validation, wallet assertions and limits of payment evidence. |
| [NIP-67](https://github.com/nostr-protocol/nips/blob/a2494f4f81d46684e5814a9bf35e2b1df978f955/67.md) | Full; EOSE hints are scoped relay statements, including authentication-dependent completion. |
| [NIP-73](https://github.com/nostr-protocol/nips/blob/a2494f4f81d46684e5814a9bf35e2b1df978f955/73.md) | Full; normalized URL external subjects and the `web` category. |
| [NIP-77](https://github.com/nostr-protocol/nips/blob/a2494f4f81d46684e5814a9bf35e2b1df978f955/77.md) | Full; set reconciliation including protocol appendix; advertised IDs are not retrieved verified bodies. |
| [NIP-78](https://github.com/nostr-protocol/nips/blob/a2494f4f81d46684e5814a9bf35e2b1df978f955/78.md) | Full; application-specific owner data is explicitly unsuitable as a generic public interchange format. |
| [NIP-66 PR #2241](https://github.com/nostr-protocol/nips/pull/2241) | Open when inspected; full proposed NIP at [head `6409b5f`](https://github.com/VincenzoImp/nips/blob/6409b5f2a4274fb4a5869232b96419ca951cb460/66.md), plus PR metadata. Richer monitoring proposal, not current protocol. |
| [Trusted Relay Assertions issue #2195](https://github.com/nostr-protocol/nips/issues/2195) | Full issue body and current status; open issue, not an accepted NIP-85 revision. |
| [ClickHouse incremental views](https://clickhouse.com/docs/concepts/features/materialized-views/incremental-materialized-view), [Apache KLL](https://datasketches.apache.org/docs/KLL/KLLSketch.html), [Apache HLL](https://datasketches.apache.org/docs/HLL/HllSketches.html) | Relevant implementation semantics and algorithm documentation; these establish constraints, not a Rust library selection or measured BigBrotr performance. |

The architectural contracts and the historical findings in
[derived/read/publication research](../research/derived-read-publication-findings.md)
were cross-read. This document does not claim another full audit of the historical
Go implementation or all code linked from the protocol proposals.

## Current interoperable wire subset

### NIP-66 observations and announcements

| Wire element | BigBrotr contract |
|---|---|
| Event envelope | NIP-01 fields and signature verification; `sig`, integer `created_at`, string tag elements. Hash/sign the exact ordered tag representation. |
| Kind 30166 subject | One normalized `d` relay URL, or a supported pubkey-only subject retained as non-dialable. Preserve meaningful scheme, path, query and non-root trailing slash under acquisition's conservative normalizer. Never append a viewpoint suffix to a URL to create another address. |
| `rtt-open`, `rtt-read`, `rtt-write` | Milliseconds from successful, defined checks. No timeout-as-latency, missing-as-zero or conversion from seconds by guesswork. Record check boundaries and access context locally. |
| `n`, `N`, `R`, `T`, `k`, `t`, `g` | Only pinned meanings and repeated tags where required. Observation supports the assertion in its recorded scope; imported/self-declared facts stay attributable. Public geolocation is omitted by default. |
| Content | Optional bounded stringified NIP-11 document. Do not replace it with a new aggregate JSON schema. Preserve retrieved content separately; omitted or sanitized publication is recorded as a projection. |
| Kind 10166 | Configured checks and intended publication frequency in seconds; optional timeouts. It is an announcement, not proof that a scheduled sweep completed. |
| Monitor identity | Stable configured public identity, with optional kind 0 profile and kind 10002 relay list. A key with several physical vantage points needs an explicitly documented aggregation policy. |

The current timeout prose places milliseconds before check name; its example
reverses those fields. Emit `["timeout", "5000", "open"]` under the pinned
prose interpretation. Accept the reverse layout only when values unambiguously
identify the alternative dialect, retain the original event and parser result,
and report the dialect. Do not emit both layouts or silently change historical
meaning. An explicitly configured legacy destination adapter can select a tested
dialect, labeled in its compatibility record. Omitting an optional timeout is
preferable to an ambiguous value. Current examples also contain an incorrect
`signature` field and an incomplete/malformed 10166 object; fixtures must follow
NIP-01 rather than copy those errors. [NIP-66 baseline](https://github.com/nostr-protocol/nips/blob/a2494f4f81d46684e5814a9bf35e2b1df978f955/66.md)

A single 30166 address is `(30166, signer, relay subject)`. Consequently two
vantage points signing separate measurements under one key overwrite one another
at ordinary relays. BigBrotr's proposed default is one stable publication identity
per intentionally public vantage/method profile, with local observer UUIDs kept
independent from keys. An aggregate publisher is also valid if it labels its
aggregation and preserves each contribution; it cannot call imported claims local
measurements. Different keys alone do not demonstrate different operators.

Clients must remain able to connect without a monitoring event: NIP-66 says they
MUST NOT require 30166 discovery and its absence MUST NOT prevent a connection.
BigBrotr's dashboard therefore displays missing monitoring evidence without
turning it into an unreachable or forbidden endpoint. The NIP also recommends
using more than one monitor rather than trusting one report.
[NIP-66 client guidance](https://github.com/nostr-protocol/nips/blob/a2494f4f81d46684e5814a9bf35e2b1df978f955/66.md)

### NIP-85 assertions and identities

| Subject | Kind and standard binding | Important boundary |
|---|---|---|
| User | 30382, `d` pubkey; standard followers, rank, activity, post/reply/reaction, report and zap statistics | Define acquired-corpus scope, current versus historical counts and exact integer conversion. Earliest acquired activity is not proof of the user's first-ever post. |
| Event | 30383, `d` event ID; rank, comments, quotes, reposts, reactions, zaps | Count the registered relation and deduplication unit, not every similarly named tag. |
| Addressable subject | 30384, `d` coordinate; corresponding event-family statistics | State whether association is to one version or the stable coordinate, and avoid double attribution. |
| External subject | 30385, `d` NIP-73 identifier; rank, comments and reactions; appropriate `k` category | A new relay-specific `score` field is not the standard `rank` field. |
| User's provider preferences | Kind 10040, entries naming kind/tag, service pubkey and relay hint | This is the signing user's preference list, not a provider directory or endorsement from somebody else. |

NIP-85 explicitly states: “Service providers MUST use different service keys for
distinct algorithms, including a key per user when the algorithm is personalized
to that user's point of view or settings.” BigBrotr validates the mapping from
algorithm, semantic version and viewpoint to signing identity before enabling
publication. A different `algorithm` tag on the same key cannot satisfy that
requirement. Rotate identity for materially different algorithms; preserve the
mapping and explanation of superseded profiles. A rerun or bug fix that leaves
the algorithm definition unchanged is not automatically a new algorithm.
[NIP-85](https://github.com/nostr-protocol/nips/blob/a2494f4f81d46684e5814a9bf35e2b1df978f955/85.md)

NIP-85 defines integer `rank` on 0–100, not a universal probability of trust.
Keep raw score, normalization population and formula in the assertion record.
Missing, withheld, stale and withdrawn are distinct from rank zero. Do not average
ranks from incompatible viewpoints as though they were calibrated probabilities.
The standard advises publishing changed contents promptly while avoiding unchanged
republication; it does not define per-metric measurement age or coverage.
[NIP-85 assertion and update rules](https://github.com/nostr-protocol/nips/blob/a2494f4f81d46684e5814a9bf35e2b1df978f955/85.md)

Relay quality can potentially use the current external-subject kind: NIP-73
defines normalized URLs with `k=web`, so a preserved `wss://` URL is a plausible
external identifier. This is an interpretation to validate with independent
consumers, since NIP-73 does not give a relay-specific example. Do not rewrite
`wss://relay.example/path` to an HTTPS page or truncate its path to force acceptance.
Until interoperability is demonstrated, relay rankings remain available through
the typed API; local availability must not depend on an upstream amendment.
[NIP-73 URL identifiers](https://github.com/nostr-protocol/nips/blob/a2494f4f81d46684e5814a9bf35e2b1df978f955/73.md)

## Archive retention and ordinary serving

The owner selected full history plus deletion-aware ordinary reads. This preserves
evidence without claiming that every archived record belongs in normal Nostr
query results. It is also separate from optional provenance: turning provenance
off does not remove signed fields or weaken ownership checks.

| Normative source | Exact force and BigBrotr behavior |
|---|---|
| NIP-01 replaceable/addressable events | The latest event **MUST** be stored; older versions **MAY** be discarded. Relays **SHOULD** return only the latest version for matching REQ. Retaining older versions is allowed; BigBrotr keeps them in history and resolves the winner before ordinary filters. |
| NIP-09 relay response | Relays **SHOULD** “delete or stop publishing” referenced events with an identical pubkey. BigBrotr chooses stopping ordinary publication while retaining originals. There is no NIP-09 **MUST** to erase archival bytes. |
| NIP-09 ownership | Clients **MUST** validate that deletion and target authors match. Relays **MAY** validate ownership because a target may be unknown. BigBrotr deliberately applies strict author validation when the target becomes available; this is a product safeguard beyond the relay's minimum wording. |
| NIP-09 address deletion | For an `a` target, relays **SHOULD** delete all versions up to the deletion request's `created_at`. BigBrotr suppresses that author's matching coordinate versions at or before that cutoff. An `e` target names an exact event ID; the specification does not impose that address cutoff on an `e` deletion. |
| NIP-09 deletion history | Relays **SHOULD** continue publishing deletion requests indefinitely; a deletion request against a deletion request has no effect. Keep them available through ordinary permitted reads and retain them in history. |
| NIP-40 expiration | Relay text allows indefinite persistence and says expired events **SHOULD NOT** be sent to clients. BigBrotr keeps history while suppressing expired records from ordinary reads. Expiration is not secure erasure. |

Sources: [NIP-01 event kinds and REQ](https://github.com/nostr-protocol/nips/blob/a2494f4f81d46684e5814a9bf35e2b1df978f955/01.md),
[NIP-09](https://github.com/nostr-protocol/nips/blob/a2494f4f81d46684e5814a9bf35e2b1df978f955/09.md),
[NIP-40](https://github.com/nostr-protocol/nips/blob/a2494f4f81d46684e5814a9bf35e2b1df978f955/40.md).
These are separate normative recommendations, not invented combined MUSTs.

Persist deletion interpretation as a rebuildable, author-scoped index over signed
source events. An unknown `e` target remains pending; its later arrival resolves
ownership without exposing it briefly through an unfiltered index. An `a` target
must parse correctly and name the deletion author's coordinate. Conflicting,
malformed and unauthorized requests remain evidence without affecting others'
visibility. Applying the same request twice is idempotent.

Resolve the newest replaceable/addressable candidate, then apply deletion,
expiration and exposure. Do not fall back to an older profile/list merely because
the winning version is deleted, expired or malformed. A genuinely newer eligible
version can become current. Apply the same policy to direct ID lookup, search,
COUNT, streaming updates and ordinary export resources. Separate historical API
and export resources require explicit authorization and identify suppressed or
superseded records. They must not masquerade as the standard public relay view.
The archive relay is read-only; deletion requests arrive through the authorized
acquisition pipeline, not an undocumented inbound write exception.

For example, Alice's addressable article versions have timestamps 100, 200 and
400. Alice issues an `a` deletion at 300. History retains all versions and the
request; ordinary reads show version 400. Before version 400 arrives, ordinary
reads show no article, including a query restricted to timestamp 100. A later
exact `e` deletion naming version 400 suppresses it without resurrecting 200.
Bob's otherwise identical request does not affect Alice's article. An unknown
target arriving after its valid deletion is indexed directly as suppressed.

Archive counts of acquired signed records therefore differ intentionally from
current-visible event counts. Their registry definitions and dashboard labels
must say which view they count. Visibility changes invalidate public derivative
facts and cached responses according to their policy; they never decrement the
immutable archive acquisition ledger. Existing remote copies cannot be recalled.
Exceptional operator retention or takedown procedures, if later introduced, need
their own authority, audit and fidelity consequences; a low rank, deletion request
or unavailable relay is not such authorization.

NIP-01 describes ephemeral events as not expected to be stored, rather than using
a universal MUST NOT. Any deliberate ephemeral capture needs an explicit source
scope; ordinary historical REQ should not expose that capture as durable relay
history. Keep this choice distinct from mandatory full fidelity of records that
the configured archive has accepted.
[NIP-01 kinds](https://github.com/nostr-protocol/nips/blob/a2494f4f81d46684e5814a9bf35e2b1df978f955/01.md)

## Measurement context that must survive aggregation

### Several clocks and several identities

The registry's window and authorship fields project into these concrete concepts:

| Concept | Meaning and failure rule |
|---|---|
| Event `created_at` | The source author's signed event timestamp; it is neither local ingestion order nor proof of when the author actually acted. |
| Observation start/end | UTC wall-clock timestamps for a particular attempt, with clock quality; elapsed durations come from a monotonic clock. Preserve original times through import and republishing. |
| Local receipt/admission | When this instance received and durably admitted a source record. An imported monitor event's receipt is not its measurement time. |
| Source cut and generation | Instance-scoped committed frontier plus the exact derived dependencies used. A complete generation is complete for this declared cut and scope. |
| Computed at | When a particular result was computed from those inputs. Recomputing yesterday's measurements today does not make them today's measurements. |
| Published at | NIP-01 `created_at` of the signed assertion revision; distinct from original observed time and semantic data age. |
| Valid until / freshness policy | A definition-specific operational judgment about usability, not a proof that reality stopped changing. Source failure produces stale/unknown, not a new successful observation. |
| Observer, signer, viewpoint | Local observation identity, cryptographic publication identity and algorithm perspective remain distinct. Retain attributed imported authors and do not silently merge them into local evidence. |

Store a persistent latest signed timestamp per replaceable publication address.
Retries reuse identical signed bytes and event ID. Coalesce competing unsigned
revisions of the same address; wait for a valid later wall-clock second when
needed instead of continually future-dating events to manufacture order. Clock
regression pauses affected new revisions and raises an operator condition; it
does not mutate an existing signature. NIP-01 timestamp ties prefer the lowest
lexicographic ID, so the sender cannot assume later network arrival wins.
[NIP-01 replacement rule](https://github.com/nostr-protocol/nips/blob/a2494f4f81d46684e5814a9bf35e2b1df978f955/01.md)

### Coverage is not availability

A monitor schedules 12 read checks, attempts 10, receives 8 successful responses
and 2 timeouts. Report `scheduled=12`, `attempted=10`, `success=8`, `timeout=2` and
`unattempted=2`. The success fraction among attempts is 8/10. That sample is not
80% proven uptime; outages between samples, scheduling bias and time correlation
remain unknown. A separately named time-coverage estimator can use the registry's
carry-forward rule and disclose uncovered intervals. It must not silently reuse
the same number under another definition.

Likewise, a relay serves 80 of 100 specifically requested event IDs and 20 requests
time out. This is positive evidence for 80 IDs and unknown outcomes for 20, not
proof that the relay stores only 80 or deleted the rest. Distinguish an explicit
policy rejection, authentication requirement, completed empty matching response,
transport failure and an unfinished query. Repeated requests under different
credentials are different access contexts.

NIP-67 `finish` improves the description of what the relay says it returned under
the current authenticated scope. It does not attest global completeness or an
honest remote server. NIP-77 reconciliation learns differences between presented
ID sets; successful reconciliation alone does not establish that every body was
retrieved and verified. Track advertised IDs, requested bodies, verified bodies,
remaining work and failed fetches separately.
[NIP-67](https://github.com/nostr-protocol/nips/blob/a2494f4f81d46684e5814a9bf35e2b1df978f955/67.md),
[NIP-77](https://github.com/nostr-protocol/nips/blob/a2494f4f81d46684e5814a9bf35e2b1df978f955/77.md)

## Optional BigBrotr context profile

### Binding and compatibility

The first extension is a small metadata binding, not dozens of unregistered
short tags. Proposed opt-in profile `bigbrotr-context/1` adds:

```json
["bigbrotr:meta", "1", "<sha256-hex-of-exact-metadata-bytes>", "https://data.example/assertion-context/<digest>.json"]
```

This is an illustrative tag template, not a published event or actual digest.
The colon-prefixed project name is a BigBrotr-owned vocabulary identifier, not a
registered NIP namespace. There is no claim that relays index this multiletter tag:
NIP-01's expected tag indexing concerns single-letter tags. Fetch the ordinary
kind/author/`d` event and interpret this extension locally. A standard-only output
mode omits it entirely. Both modes must pass the same core event validation.
[NIP-01 tags](https://github.com/nostr-protocol/nips/blob/a2494f4f81d46684e5814a9bf35e2b1df978f955/01.md)

The metadata is a versioned immutable UTF-8 JSON artifact. Its exact bytes have a
SHA-256 digest; the enclosing event signature binds that digest. Construct and
persist metadata before signing. Metadata identifies the event address and the
wire fields it explains, but does not include the enclosing event ID, avoiding a
circular digest dependency. The implementation validator checks those bindings
against the event. The outbox pins both artifacts until their release retention
obligations end. Mirrors can serve the same bytes and digest.

Extension-aware consumers verify the Nostr event, retrieve metadata only from
allowed HTTPS origins with response/redirect/time/size limits, verify its digest,
validate the schema and check subject/field bindings. A bad or unavailable artifact
makes context unverified or unavailable; it does not modify the signed standard
fields. Browser fetches require appropriate CORS. No preview or background parser
automatically follows an arbitrary supplied URL, accesses private IPs, or reveals
user credentials to a metadata origin. The dashboard can serve verified cached
metadata from its own API origin. Rehosting content by digest avoids dependence
on a particular original host, but this profile still adds a retrieval dependency.

A signature proves who asserted the metadata, not that its source is complete or
honest. Hashes make changes detectable; they do not make private evidence publicly
reproducible. Mark evidence as public-retrievable, restricted-verifiable or
publisher-attested, and expose the limitation. The public projection excludes
secrets, credentials, private endpoints, local paths and individualized private
viewpoints. Hashing a small sensitive set does not make its members anonymous.

NIP-78 kind 30078 is deliberately not the public metadata container. The current
NIP limits it to application-specific data, recommends owner authentication for
accepting/serving it, and explicitly rejects generic public cross-application
interchange. A future dedicated kind requires its own accepted purpose and number;
BigBrotr will not invent a kind and advertise it as standardized.
[NIP-78](https://github.com/nostr-protocol/nips/blob/a2494f4f81d46684e5814a9bf35e2b1df978f955/78.md)

### Registry projection requirements

This is a projection of the canonical registry, not a second independent schema
of metric meanings. The extension version controls the envelope; each assertion
definition retains its own semantic version.

| Projection | Required contents |
|---|---|
| Address and field | Kind, signer, normalized `d`, exact wire tag occurrence or API-only metric ID. Reject a metadata document describing another subject or value. |
| Definition | Registry ID/version, claim class, method/configuration digest and public method documentation. Keep parser versions for social relations and protocol dialects. |
| Quantity | Value, unit, scalar type, exact/estimated status, numerator/denominator when applicable and quantization. Use decimal strings for integers beyond interoperable JSON number precision. |
| Evidence | Source record or manifest references, original claim authors, local/imported distinction, verification status, access/sampling context and retrieval visibility. |
| Time | Observation interval, event-time window where applicable, source cut, computed time, freshness policy and uncertainty. Preserve per-field times when inputs differ. |
| Viewpoint | Algorithm identity, curator/personalization identity or a public opaque identifier, normalization population and component interpretation. Private viewpoint publication requires separate explicit enablement and disclosure review. |
| Generation | Exact immutable source/dependency generations, scope/filter/exposure digests and the completed/withheld/failed state. A newer source cut does not silently re-label an older score. |
| Limitations | Coverage gaps, omitted values, unknown reasons, observer dependence, sample/estimator error and privacy omissions. Do not combine these into an unexplained confidence percentage. |

NIP-85 sats fields are integers, whereas NIP-57 invoice amounts use millisatoshis.
Retain exact validated millisatoshi totals in the registry. For the initial strict
wire binding, publish an exact sats amount only if the total is divisible by 1000;
otherwise omit that optional standard amount and expose the exact amount/unit
through the API/context. If a rounded binding becomes desirable, version it and
declare its rounding; never floor each receipt and then call the sum exact.
Validate receipts under NIP-57 and retain provider-verification evidence at the
relevant time. A wallet's signed receipt is not independent proof of settled
payment or audited revenue. Unverifiable historical provider attribution stays
explicitly unverified.
[NIP-57](https://github.com/nostr-protocol/nips/blob/a2494f4f81d46684e5814a9bf35e2b1df978f955/57.md),
[NIP-85 units](https://github.com/nostr-protocol/nips/blob/a2494f4f81d46684e5814a9bf35e2b1df978f955/85.md)

The similarly named `first_created_at` field must not silently receive local
receipt time. Keep `first_observed_post_created_at` as a scoped API assertion;
enable the standard field only with a documented interpretation of earliest
post in the stated acquired corpus and interoperability review of that meaning.
An empty corpus omits it. A late older post invalidates it even if no current
state changes. General-purpose clients may ignore the context, so ambiguous
fields should remain omitted rather than depend on a disclaimer they never see.

### Before and after examples

All following objects are unsigned input/metadata examples. They deliberately
omit `id`, `pubkey` and `sig`; they are not complete valid signed Nostr events.
Fixture generation during implementation supplies real keys, hashes and signatures.

Before: a tempting but misleading announcement republishes yesterday's timeout
as today's successful 5000 ms read measurement:

```json
{"kind":30166,"created_at":1789120800,"tags":[["d","wss://relay.example"],["rtt-read","5000"]],"content":""}
```

After: core output omits the failed measurement. An optional context artifact
records the actual outcome without inventing a core NIP-66 failure tag:

```json
{"kind":30166,"created_at":1789120800,"tags":[["d","wss://relay.example"]],"content":""}
```

```json
{
  "schema": "bigbrotr-context/1",
  "subject": {"kind": 30166, "d": "wss://relay.example"},
  "assertions": [{
    "definition": "bigbrotr.monitor.read-outcome/1",
    "binding": {"api_only": true},
    "claim_class": "measurement",
    "outcome": "timeout",
    "timeout_ms": "5000",
    "observed_start": 1789034400,
    "observed_end": 1789034405,
    "observer": "public-vantage-a",
    "access_context": "unauthenticated-clearnet",
    "freshness": "stale",
    "coverage": "single-scoped-attempt"
  }]
}
```

The metadata example shows only the fields involved in the correction; production
includes the signer/address binding and complete required registry projection.
Do not issue a new 30166 solely to give stale evidence a newer timestamp. The
after example is appropriate only if other current subject metadata justifies
publication; otherwise withhold the new event and retain the prior one with age.

Before: one key publishes Alice's personalized rank 80, then Bob's rank 20, both
for the same 30382 subject. Adding a `viewpoint` tag still leaves one NIP-01
address, so ordinary relays replace one with the other and the key arrangement
violates current NIP-85. After: Alice's and Bob's algorithms have different
registered service keys; each has a separate signed assertion and context.
The client follows the actual user's 10040 preferences rather than choosing the
latest assertion from an unrelated viewpoint.

Before: `{"reaction_cnt":"120"}` includes 20 physically replayed rows for 100
unique valid reaction events. After: `reaction_cnt=100` uses event-ID-keyed
contributions, and an independently named `unique_reactors=73` counts pubkeys.
Neither quantity is substituted for the other. The concrete values are illustrative,
not measured network statistics.

## Richer statistics: benefits, costs and exposure

These are proposed registry families, enabled by evidence and resource capability.
Names below describe definitions to be registered, not new standardized NIP tags.
“Public optional” means the operator explicitly selects publication after preview;
it does not mean every stored field may be exposed by default.

| Family | Useful additions and units | Cost, uncertainty and publication decision |
|---|---|---|
| Check outcomes | Attempt/success/timeout/rejection/auth-needed counts, last attempt and last success, per check and access context | Linear observation ingestion plus bounded buckets; publish scoped aggregates, not a universal online/offline verdict. |
| Check timing | Successful open/read/write p50/p95 RTT in ms; handshake versus first-valid-event versus write-OK/readback boundaries | Requires retained samples or sketches. Never mix unsuccessful timeouts into success quantiles; display attempt counts beside latency. Core NIP-66 individual RTT stays separate. |
| Scheduling and freshness | Scheduled/attempted/completed checks, oldest pending work, observation age and covered/unknown seconds | Mostly local counters. Public coverage summaries can explain blind spots; worker topology and private backlog detail remain operator-only. |
| Reconnection | Connection termination reasons, reconnect attempts, recovery interval observations | Path and client retry policy affect results. Do not call a reconnect delay a server outage duration. |
| NIP-11 claims | Declared NIPs, limitations, auth/payment/write requirements, document digest and fetched-at | Bounded fetch/parser reuse. Preserve declaration separately from test evidence; absence is not false. |
| Tested capabilities | Specific filter, auth, COUNT, EOSE-hint or reconciliation behavior with test version and result | Small passive/read tests can be reused. One passing example is not full conformance; expensive and write tests require explicit capability and budget. |
| Write evidence | Signed probe ID, OK acceptance, later readback outcome and elapsed intervals | Explicit dedicated probe identity and public-safe content; an OK does not guarantee indefinite retention. Keep write acceptance distinct from verified retrieval. |
| TLS | Validation result, negotiated version, not-after timestamp, certificate fingerprint | Reuse only for the same applicable endpoint/SNI/address context. Certificate chain size and issuer detail are optional; valid TLS is not moral or operational trust. |
| DNS/network | Resolver-relative RRset digest/change time, per-record TTL, observed address family and ASN attribution source/date | Cache within valid scope; TTL is not persistence proof. Full addresses/records, infrastructure mappings and precise locations need separate exposure policy. |
| Geography | Coarse relay/observer region, attribution method/date and uncertainty | GeoIP is an estimate, not exact physical coordinates. Omit by default; never infer operator nationality or trust from host location. |
| Read traversal | Filter/auth/window, EOSE hint, received unique IDs, verified body counts, unresolved fetches and explicit gaps | Reuse acquisition receipts. Public bounded summaries are useful; raw requested IDs or restricted subjects can reveal interests/access. |
| Selective availability | Served, explicit not-served and unknown outcomes for an eligible requested set | Requires direct matched evidence and the denominator's sampling rule. Avoid claims of intentional censorship or global retention from sampled absence. |
| Propagation | Local first-seen/acquisition intervals stratified by live/backfill/import path and observer clock quality | Per-event/per-endpoint state can be large. Publish aggregates when allowed; inferred network transmission order is not causal proof. |
| Relay overlap | Observed intersection/union/Jaccard, window, eligible set sizes, candidate method and edge evidence | Potential quadratic endpoint/fanout cost. Public clusters are advisory similarity; they never authorize merging identities or deleting archive data. |
| User/activity facts | Current followers/follows, unique interlocutors, posts/replies/topics, first observed activity, observed UTC activity histogram | Reuse versioned parsers and current state. Counts of keys are not counts of humans; fine activity patterns and social targeting have privacy costs. |
| Event interactions | Separate reaction events, unique reacting keys, comments, replies, quotes, reposts and evidence windows | Subject relation parsing must be explicit. Publish standard counters where semantics match, and new diversity measures through API/context. |
| Reports and mutes | Distinct signed reports, public mute edges, report categories and source diversity | Observed expressions are not adjudicated abuse. Resist report flooding; any negative score influence is a separate versioned policy. |
| Zaps | Validated/claimed receipt counts, exact msat totals, unique asserted senders/providers and validation status | Invoice/provider parsing and historical verification cost; no automatic network fetch on every read. Keep unverified receipt statistics separate. |
| Scores | Raw score, standard rank projection, components, viewpoint, source population, missing reasons and sensitivity notes | Graph and normalization may be global. All profiles need explanations; no universal weights or confidence-from-sample-count assumption. |

Capability declarations and test observations can conflict without one globally
overwriting the other. For example, `auth_required=false` in NIP-11 does not rule
out authentication for a particular operation; the NIP explicitly permits that.
Record “this unauthenticated query required auth” beside the declaration, not
“relay lied about all authentication.” A timeout is not a negative supported-NIP
test. Similar care applies to temporary rate limits and paid identities.
[NIP-11 limitations](https://github.com/nostr-protocol/nips/blob/a2494f4f81d46684e5814a9bf35e2b1df978f955/11.md)

Do not let a prolific monitor dominate aggregation merely by emitting more
records. Deduplicate exact observations, compute per-observer contributions,
retain known shared-operator relationships, and expose conflicting evidence.
Repeated probes are temporally correlated; sample count alone cannot justify a
statistical confidence level. A model-based interval needs its assumptions and
definition, separate from coverage and source-verification status.

## Incremental computation and honest complexity

Let `Δ` be newly admitted or invalidated contributions, `U` distinct retained
keys, `B` affected time buckets, `V/E` graph vertices/edges, `I` iterations, `R`
relay endpoints and `f` a particular event's observed relay fanout. The following
are planning cost classes, not benchmarks or fixed latency promises.

| Family | State and normal update | Retraction/late-data behavior and irreducible cost |
|---|---|---|
| Counters, sums, means | Deduplicated source contribution or replaceable partition subtotal; O(Δ) arithmetic after indexed source access. Means retain sum and count. | Removing a visible contribution subtracts exactly once. Archive totals do not retract for serving deletion. Rebuild affected partitions if contribution history is unavailable. Never average batch averages without weights. |
| Exact distinct | Per-group distinct key/refcount, or a sorted exact set; O(U) state. Indexed/hash update cost depends on engine and spill. | A removal decrements a refcount and changes distinct count only at zero. Window membership and multiple source events per actor require state, not just one scalar. |
| Approximate distinct | Versioned sketch per declared group/window; small mergeable state and generally cheap inserts. Record hashed identity, algorithm/seed and error model. | Ordinary HLL maxima have no general inverse. Retraction or correction rebuilds affected buckets from retained inputs. Merging cannot repair a sketch that counted the wrong identity domain. |
| Current state and current edges | Winner index by coordinate/author plus old contribution; process changed winners and the difference between old/new whole lists. | Late losing versions do not change current facts. Deleting a winner can require historical lookup for policy resolution; BigBrotr's no-resurrection rule still applies. Work scales with affected list/history size, not merely number of changed authors. |
| Rolling windows | Bucketed exact contributions/sketches plus expiry scheduling; merge the required current buckets. | Advancing time invalidates results even without arrivals. Late observations rebuild their true buckets. Exact partial boundary windows need finer/raw boundary data; otherwise label bucket-aligned approximation. |
| Quantiles | Mergeable KLL candidate for large series; exact sorted values for small or exact-required outputs. Retain count, configuration and rank-error interpretation. | Do not average medians/p95 values. Arbitrary deletions and changed samples rebuild touched bucket state; a compact sketch is not the original sample set. Merging many buckets adds work and estimator constraints. |
| Graph diffusion | Update adjacency from current edges; share parsed graph across viewpoints. Deterministic reference cost O(I × (V + E)). | A local edge/seed change can affect the entire graph and normalization. Warm starts/local residual methods are separate validated algorithm variants, not an O(Δ) guarantee. Personalized score vectors add per-viewpoint cost. |
| Relative rank normalization | Sorted/indexed population distribution derived from a pinned raw-score generation | A population or distribution change may alter many wire ranks. Recompute affected global order statistics; “unchanged subject inputs” does not imply unchanged percentile. |
| Relay pair sets | Inverted event-to-relay membership, candidate size/coverage pruning, exact comparison for admitted candidates | Naive all pairs is O(R²); updating all pairs for one event is O(f²). Approximate MinHash/LSH candidate generation can miss edges and must publish its method/coverage. No promise of globally exact clustering from sampled candidates. |
| Cluster components | Versioned qualifying edge set plus connected-component computation | Edge insertion can use union-find; expiry/deletion can split a component and requires a suitable dynamic algorithm or recomputation. Components imply connectivity, not all-pairs similarity. |

ClickHouse incremental materialized views react to inserted blocks, not every
change to all referenced data. Right-side join changes do not automatically update
old results. Therefore BigBrotr cannot delegate invalidation, replay deduplication
or graph propagation to a generic insert view. Use those views only where the
declared algebra and durable contribution scheme fit; store committed generation
pointers separately from transient inserts.
[ClickHouse incremental view semantics](https://clickhouse.com/docs/concepts/features/materialized-views/incremental-materialized-view)

KLL offers compact mergeable quantile state with a configurable accuracy/size
tradeoff; its normalized rank error is not a fixed millisecond error. HLL is useful
for unions and distinct estimates; it is not a suitable general intersection or
difference primitive. Choose exact sets or a validated set-operation sketch when
relay-overlap semantics require those operations. These are algorithm candidates;
Rust implementation, serialization portability and resource behavior still require
evaluation. [Apache KLL](https://datasketches.apache.org/docs/KLL/KLLSketch.html),
[Apache HLL](https://datasketches.apache.org/docs/HLL/HllSketches.html)

NIP-45's optional HLL representation specifically updates registers from event
**pubkeys** and a filter-derived offset. It therefore estimates participating keys,
not arbitrary event-ID cardinality. Its fixed wire representation is not an
interchange format for an arbitrary database sketch. Keep this adapter separate
from exact `reaction_cnt` and internal sketches; use supported filter shapes and
preserve approximate status. Do not sum relay counts to estimate a deduplicated
network total. [NIP-45 algorithm](https://github.com/nostr-protocol/nips/blob/a2494f4f81d46684e5814a9bf35e2b1df978f955/45.md)

### Contribution and invalidation contract

The unit of replay is a stable admitted source identity or committed replacement
partition, never a physical ClickHouse insertion. Atomic manifests identify both
the input cut and the output generation. A worker crash after writing staging
output but before commit must leave the previous generation visible. Replay
either reuses an already committed result or replaces the exact same partition;
it cannot add its subtotal a second time.

Keep a durable dirty ledger keyed by definition/version, scope and partition or
subject. Coalesce repeated invalidations without losing the greatest required cut
or an older repair range. A missing referenced event arriving later, a corrected
parser, a changed visibility policy, a current follow-list replacement and a
window expiring can all dirty results without a new event-time maximum. If the
ledger cannot retain every key within its budget, commit a broader rebuild marker
before compacting individual entries. The scheduler must not discard correctness
work to keep a queue superficially healthy.

For example, a day's partition counts 100 unique events. Ten new events arrive,
the worker computes 110, and it crashes after staging. Restart commits the same
110, never 120. A late older event rebuilds its historical day and advances that
day's generation. An effective deletion changes `visible_events` under the
selected serving policy but leaves `archived_signed_events` unchanged.

A follow list changing from `{B,C}` to `{C,D}` retracts `A→B`, adds `A→D` and
preserves `A→C`. It does not union both lists forever. That cheap adjacency update
can still require a broad Ranker run. An overlap edge expiring can split a cluster;
a permanent union-find structure cannot report the resulting components correctly.
These counterexamples belong in implementation acceptance, not only prose.

Backpressure and separate generations are necessary when global jobs are slower
than ingestion. Continue serving the previous complete graph/cluster generation
with its age and required source cut. Cheap counters can advance independently;
an assertion pins the explicit dependency vector it used. Track dirty age, scan
bytes, CPU time, spill, rows/keys examined, retained state and publication backlog.
Measure cost per definition and workload before changing cadence or precision.
Never publish a truncated global computation as complete.

## Publication cadence, validity and withdrawal

Computation, eligibility and transport are separate stages. Deriver/Ranker commit
facts; the publisher freezes a semantic projection and its context; the outbox
signs once and tracks delivery per destination. Repeated send attempts do not
rerun analytics. Missing destination delivery is retried even when the payload
has not changed. Replacing an unsigned desired revision is distinct from losing
an already signed delivery obligation.

For NIP-66, a new actual measurement can justify a newer event even if its RTT is
numerically unchanged. The configured cadence bounds publication volume. A new
timestamp with unchanged old measurements is not a new check. For NIP-85, compare
semantic assertions after stable normalization and avoid a publication every time
an execution ID changes. Context generation should not force identical scores to
be re-signed on each internal checkpoint; retain the prior publication's original
lineage and expose newer local recomputation separately.

NIP-40 expiration can provide optional finite validity to consumers implementing
it, but old clients/relays may ignore expiration. A validity renewal must be backed
by a successful fresh evaluation and have a documented renewal policy; it is not
permission to recycle stale evidence. Because NIP-85's changed-content guidance
and frequent validity renewal have different goals, keep expiry disabled in the
initial standard binding until its intended client behavior is validated. The
local API always reports evidence age and withdrawal state.
[NIP-40](https://github.com/nostr-protocol/nips/blob/a2494f4f81d46684e5814a9bf35e2b1df978f955/40.md),
[NIP-85 update guidance](https://github.com/nostr-protocol/nips/blob/a2494f4f81d46684e5814a9bf35e2b1df978f955/85.md)

Loss of eligibility withholds refresh and records withdrawal. A separately enabled
kind-5 request for the publisher's own assertion can request suppression remotely,
with best-effort delivery tracked; it cannot erase copies or guarantee consumers
stop using the result. Do not overload `rank=0`, `rtt=0` or an invented standard
status with that meaning. Private keys, write probes and public destinations
remain explicitly configured, as in the existing service contracts.

## Disposition of existing proposals

The open [PR #2241](https://github.com/nostr-protocol/nips/pull/2241) contains useful
editorial repairs and a larger monitoring vocabulary. Its head resolves timeout
ordering toward `[timeout, check, milliseconds]`, the reverse of current prose.
That is a compatibility choice requiring implementation evidence, not a harmless
rename. BigBrotr's present encoder remains pinned to current prose until a reviewed
version changes the contract. Do not present the unmerged head as current NIP-66.

Evaluate the proposal in separate units:

| Proposal area | Decision and reason |
|---|---|
| Valid NIP-01 examples and consistent timeout grammar | Pursue independently; small scope, direct interoperability benefit. Inventory existing dialects before recommending a migration. |
| Check names and measurement boundaries | Pursue a concise vocabulary with explicit success/failure context. A check's start/end condition is more valuable than another ambiguous number. |
| Structured NIP-11 and capability tags | Keep raw declaration plus typed local projection. Standardize selected commonly queried facts only when semantics and consumer need are clear. |
| NIP-32 labels | Suitable for stable categorical vocabularies after a namespace/target contract. Numeric latencies, exact timestamps and other nearly unique values remain quantities, not labels. |
| DNS/TLS/HTTP/network details | Useful for diagnostics; retain structured API context first. Public detail is optional, scope-specific and bounded. Avoid duplicated payloads and unbounded tag cardinality. |
| Precise geography and blanket probe precedence | Do not adopt as defaults. GeoIP is approximate and potentially sensitive; a particular probe only establishes its tested context, not universal truth over all self-declared policy. |

NIP-32 labels on a non-1985 event label that event itself; a profile must explicitly
explain how a label describes its relay subject. `#L` and `#l` filter conjunction
also does not prove that the matching label belongs to the requested namespace
when several namespaces occur in one event. Consumers must validate the `l` tag's
namespace association. Use categorical controlled vocabularies, not duplicate
every numeric value into both a custom tag and a label.
[NIP-32](https://github.com/nostr-protocol/nips/blob/a2494f4f81d46684e5814a9bf35e2b1df978f955/32.md)

The open [issue #2195](https://github.com/nostr-protocol/nips/issues/2195) proposes
relay-specific kind 30385, `score`, and a 10385 provider list. Current NIP-85 already
uses 30385 for external identifiers, `rank`, and 10040 user preferences. Adopt
neither conflicting wire identifiers nor the proposed universal score weights.
Its desire for explained relay assessments is useful; implement that through the
registry and test the current external-URL binding. Sample-count tiers are not
statistical confidence, and location-based trust is a viewpoint requiring explicit
justification. A possible upstream contribution should reconcile these concepts
with existing kinds instead of introducing parallel preferences.

## Draft upstream issue and PR texts

The following are reviewable drafts inside this repository only. Before any later
submission, reread current upstream text, inspect relevant implementation behavior
and coordinate overlapping proposals. No upstream action is part of this stage.

### Draft A: NIP-66 timeout grammar and valid examples

**Proposed issue title:** Clarify NIP-66 timeout tag order and repair event examples

**Draft body:**

NIP-66 currently describes timeout tags as `["timeout", milliseconds, check]`,
while the monitor example uses `["timeout", check, milliseconds]`. A producer
following one reading cannot know which a consumer implements. The example also
uses `signature` in place of NIP-01 `sig`, and the monitor object needs its kind
and valid JSON punctuation.

Please select one timeout grammar explicitly after checking deployed producers
and consumers. A minimal prose-preserving correction would specify:

> A timeout tag has the form `["timeout", "<milliseconds>"]`, optionally followed
> by a check name as its third element. Milliseconds are a non-negative decimal
> integer string. A check-specific timeout applies only to that named check.

Update examples to that order and valid NIP-01 event fields. If deployed evidence
instead supports the example order, document the compatibility consequence and
a migration rule rather than leave the two contradictory forms. This correction
should not depend on accepting additional monitoring fields.

**Draft PR scope and evidence:** change timeout prose/examples only after the
grammar decision; correct envelope/JSON errors; attach two independent consumer
parses and signed fixtures for chosen order, generic timeout and unsupported
check handling. Reject ambiguous values; a tolerant parser can preserve known
legacy order without advertising it as the selected encoder grammar.

### Draft B: NIP-66 observation context and freshness

**Proposed issue title:** Distinguish measurement time, check outcome and vantage
from NIP-66 publication time

**Draft body:**

A 30166 event can combine an old NIP-11 fetch with a recent connection check, then
receive a new event timestamp. Consumers cannot infer which values are fresh or
whether a missing RTT means failure, disabled checking or privacy omission.
Multiple vantage points using one signer and `d` also replace each other's values.

Please define observation boundaries and optional context without requiring
clients to depend on monitoring or reveal precise location. Suggested normative
direction for review:

> A published RTT describes a successfully completed named check. A timeout is
> a distinct outcome and is not a successful RTT value. Reusing an observation
> does not change its observation time. A monitor combining observations from
> different vantage points or times describes that aggregation explicitly.

The context should carry per-check observed time, method/access scope, outcome
and observer identity distinct from publication time. Coverage, attempted checks
and availability should remain separate quantities. An optional versioned context
binding can evolve before agreement on a large fixed field vocabulary. No context
field should require precise geolocation or change the existing relay `d` subject.

**Draft PR scope and evidence:** begin with definitions and examples; compare one
inline minimal context design with an immutable referenced context design using
actual client byte/fetch costs. Include stale republishing, auth-only reads,
multi-vantage replacement and mixed-time fields. Introduce any new wire tags only
after implementers agree on their exact grammar and legacy behavior.

### Draft C: NIP-85 assertion context and external relay identifiers

**Proposed issue title:** Document assertion scope, units and freshness while
preserving NIP-85 algorithm identities and existing subject kinds

**Draft body:**

NIP-85 correctly separates algorithm/personalized service keys, but counters and
ranks are difficult to compare without their corpus, window, units, viewpoint and
normalization. `created_at` records publication time; it does not by itself state
source age. Unchanged-value guidance also leaves the validity of old assertions
unclear after their provider stops refreshing them.

Please add optional, versioned context semantics for definition/version, source
window/cut, observed/computed time, quantity and exactness, algorithm/viewpoint,
coverage limitations and withdrawal/validity behavior. Keep missing values distinct
from zero and preserve the current key-separation rule. A context reference should
be bound by the event signature if clients rely on its integrity; unavailable
context should not make a client reinterpret ordinary standard fields.

Please also clarify whether normalized `ws`/`wss` relay URLs use the existing
NIP-73 external-identifier binding with `k=web`. A relay-ranking example could use
the current 30385 `rank` and 10040 preferences instead of a conflicting relay kind
or provider-list kind. A score formula remains the provider's documented viewpoint,
not a mandatory universal definition of relay trust.

**Draft PR scope and evidence:** first add clarified definitions and external-URL
examples if accepted; separately evaluate the context transport. Test all four
subject families, two personalized keys, exact/estimated counts, fractional-sat
source amounts, incomplete acquired history, unchanged recomputation, withdrawal
and clients that ignore extensions. Do not standardize a specific BigBrotr score
or force a public dump of private source evidence.

## Implementation and release acceptance

These are planned checks, not claims that a prototype or compatibility suite has
already run. Release order is dependency-based: registry and local evidence;
standard serializers and read policy; durable publication; opt-in context;
external interoperability; then optional broader standards work. Rich API metrics
and all planned service families remain part of the product plan throughout.

| Entry point and scenario | Required observable evidence |
|---|---|
| CLI configuration/preview | Standard-only and extension modes produce inspectable unsigned payloads with omission reasons. No signer is required for preview; unavailable dependencies cannot become empty or zero values. |
| Core publication | Generate real signed 30166/10166 and all four NIP-85 families; independent code verifies IDs/signatures, units, repeated tags and subject identity. Test standard and legacy timeout consumers explicitly. |
| Extension-aware and legacy clients | A legacy client reads unchanged core semantics; an aware client verifies context. Missing artifact, invalid hash, wrong subject, unknown version, excessive size and unsafe redirect produce visible bounded failures. |
| Personalized publication | Two viewpoints cannot share an algorithm service key. Both correct keys coexist for one subject; each user's 10040 selection retrieves the intended provider. |
| Clock and freshness | Replayed signed events keep IDs; clock regression cannot future-date a stream indefinitely. Stale successful RTT is not refreshed after a failed check; mixed-input ages remain distinct. |
| Monitor through real local relay fixtures | Successful read, explicit auth, timeout, disconnection, write-OK without later readback and NIP-11/test conflict produce distinct evidence through CLI/API/dashboard, not only parser tests. |
| Standard relay/read API | Valid/foreign/unknown-target deletion, `a` cutoff and exact `e` deletion, supersession, expiration and direct-ID queries share visibility. History retains and exports the complete signed originals under explicit access policy. |
| Live protocol | A NIP-01 `limit:0` subscription receives no historical events, receives EOSE and remains live for newly eligible matching events. Read-only ingress rejects writes with valid protocol responses. |
| Incremental counters/state | Replay, crash before/after commit, late data, deleted visible events, replaced follow lists and newly resolved references match an independently computed exact reference. Archive and visible totals intentionally differ. |
| Sketches | Merge/bucket expiry and replay behave as defined; quantiles expose rank-error semantics, distinct estimates name the counted identity, and a retraction triggers correct rebuild. Do not compare incompatible serialized sketches as interchangeable. |
| Graph and clusters | Edge/seed change, normalization population change and a removed bridge edge produce the correct dependency invalidations. Optimized variants are compared against a deterministic reference on adversarial as well as ordinary graphs. |
| Bounded operation and recovery | Use skewed realistic corpus/probe distributions, high-fanout events, many viewpoints, a stopped consumer, outbox outage and process restart. Observe finite state/backlogs, reproducible committed generations and explicit stale output. |
| Public surfaces | Dashboard explains measurement versus claim/score, source age, scope, unavailable context and privacy omission; exported manifests retain matching definitions and evidence. No UI implies a complete network census or guaranteed remote deletion. |

The live requirement follows current [NIP-01](https://github.com/nostr-protocol/nips/blob/a2494f4f81d46684e5814a9bf35e2b1df978f955/01.md):
with `limit=0` the relay MUST NOT return stored events for that filter, MUST send
EOSE after initial queries and MUST keep the subscription active. An archive
service cannot claim that behavior by leaving a silent socket open.

Record fixture source, protocol revision, independent implementation versions,
observed disagreements, dataset scope and actual resource measurements in release
evidence. Resolve semantic disagreements before claiming the relevant capability;
leave an unsupported optional binding explicitly unavailable. Changing the NIP
baseline or extension schema triggers targeted compatibility and migration checks,
not a silent parser reinterpretation of archived signed history.
