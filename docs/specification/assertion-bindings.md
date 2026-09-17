# Assertion bindings: source relations, facts and publication

Status: selected design after review; no producer, migration or runtime
qualification is claimed here. This document closes the relation-to-field
contract for P05/P07. [A08](data-contracts.md#a08--derived-current-state-facts-graphs-and-scores)
owns fact envelopes, the [storage catalog](storage-catalog.md) owns logical
relations, and [protocol evolution](../architecture/protocol-evolution.md)
owns publication identity, units and compatibility. These rules must become
registered schemas and executable fixtures before their bindings are enabled.

## Identity, scope and versions

The local tagged subject is exactly `Subject::Pubkey(PublicKey)`,
`Subject::Event(EventId)`, `Subject::Addressable{kind,pubkey,identifier}`,
`Subject::External{identifier}`, or `Subject::Relay{endpoint_id}`. Equal bytes in
different variants never join. Coordinates split on the first two colons;
the remaining identifier, including further colons and an empty value, is exact.
Only NIP-01 addressable kinds qualify for `Addressable`. A pubkey is a signing
identity, not proof of one person.

| Local subject | Binding ID | Wire subject |
|---|---|---|
| `Pubkey` | `nip85-pubkey-facts/1` | 30382; `d` = lowercase hex pubkey |
| `Event` | `nip85-event-facts/1` | 30383; `d` = lowercase hex ID of a verified regular event |
| `Addressable` | `nip85-addressable-facts/1` | 30384; `d` = canonical decimal kind, lowercase hex pubkey, exact identifier joined by colons |
| `External` | `nip85-external-facts/1` | 30385; `d` = validated NIP-73 identifier; one matching `k` |
| `Relay` | `nip85-relay-rank/1` | Disabled initially; opt-in 30385 URL/`k=web` mapping only after the independent consumer gate |

The four NIP-85 subject kinds and optional field names below come from the
[pinned NIP-85](https://github.com/nostr-protocol/nips/blob/a2494f4f81d46684e5814a9bf35e2b1df978f955/85.md).
The counter interpretations are BigBrotr's explicit provider choices.
`seeded-trust/2` identifies the default P05-T11 pubkey algorithm;
`seeded-trust/1` is its explicit fixed-pass reference profile;
`general-read-relay/1` identifies P05-T12's separate relay profile. Neither
creates another wire kind. No event/addressable/external recommendation-rank
profile is selected. Algorithm, binding version, population, parameters and
viewpoint are part of publisher identity validation; materially different
interpretations cannot share a service key merely by adding another tag.

Field activation uses the exact registry key `(binding_id, wire_tag_name)`;
for example `(nip85-pubkey-facts/1, zap_cnt_recd)`. This permits qualified facts
without enabling every field in the family. Unit/subject/parser compatibility
is checked per field and input generation before constructing the signed event.

All definitions below use the public normal-visible acquired corpus at the same
compatible SourceCut, interpretation time, policy and completed input generations.
They are exact within that declared corpus, not estimates of all Nostr activity.
Historical-acquired and authorized private facts remain separate definitions and
cannot feed these public bindings. Source originals survive visibility changes.
Except explicitly named 30-day summaries, counts cover the selected corpus with
no rolling age window. Source admission's signed-time and visibility policies
apply; calendar conversion is never an additional archival admission filter.

Every fact key includes subject, full definition ID/version, population/window
and generation. The prefix `bigbrotr.fact.` is implicit for fact IDs in the
tables below (algorithm IDs and existing `social.*` IDs retain their exact spelling);
for example `authored-posts/1` means `bigbrotr.fact.authored-posts/1`.
Integer counts use distinct eligible source event IDs, except explicitly named
distinct-pubkey or payment-identity counts. Repeated deliveries, repeated tags
and multiple paths to the same subject contribute once to that definition.
Self-interactions count as claims; graph self-follow exclusion is separate.
No report or reaction silently becomes a trust penalty or proof of endorsement.

## Parsed relation contract

P05-T04 owns the `nostr-relations/1` semantic matrix; P05-T02 implements its
shared pure parser prerequisite before T03 activity or T04 aggregates. Its output includes
source event ID/author, relation role, tagged target, source tag positions,
parser version and `resolved`, `unresolved`, `ambiguous`, `invalid` or
`unsupported` status. Aggregates do not infer roles from the first-value index.

| Source | Selected `nostr-relations/1` rule |
|---|---|
| Kind 1 replies | Any `root`/`reply` marked `e` selects marked mode exclusively. Collapse identical marked IDs; conflicting values for one role make that role ambiguous. A sole root is also parent; a reply without root supplies only parent. Otherwise apply deprecated first-root/last-parent positions to unmarked `e` only; one is both. Marked mentions never become replies; unknown markers remain unsupported. No reply candidate means non-reply. Kind-1 reply targets must resolve to kind 1. |
| Kind 1111 comments | Parse uppercase root and lowercase immediate parent independently, with their required kind/author context. `E/e`, `A/a` and `I/i` remain typed. A version ID paired with its matching coordinate is one logical target in that role. Conflicting IDs/coordinates or contradictory known kind/author context make the affected role ambiguous; do not choose whichever tag came first. Missing required context excludes the affected comment relation. |
| Quotes | Every valid `q` on an eligible source event supplies a quote claim to its event ID or coordinate. Collapse repeats. A quote may coexist with a reply. Inline text links and arbitrary `e` mentions are not this producer. |
| Reposts | Kind 6 targets a verified kind-1 event; kind 16 a verified other-kind event. Require one distinct target `e`; validate supplied kind/author/coordinate. Matching `a` is the same target. Verify any embedded full event matches; kind 16 without `a` requires that full content. Absent, malformed or contradictory targets remain unresolved/invalid. |
| Kind 7 reactions | The last `e` selects the event target. Validate supplied last `p`, `k` and `a` against a resolved target; a matching `a` also identifies its stable coordinate. Earlier `e`/`p` are context. Empty or `+` content is positive, `-` negative, all other content is other. All three classes count in the all-reaction counter; keep classes separately. |
| Kind 17 external reactions | Each distinct valid `i` whose derived namespace has a matching `k` supplies one external reaction claim. Associate by validated namespace, not positional zip or Cartesian product. Unsupported namespaces remain unsupported. Duplicate references do not multiply a source event's contribution to one external subject. |
| Kind 1984 reports | A recognized report-category third element on a `p` is a direct pubkey complaint. A categorized `e` is an event complaint only when a verified target author has a matching `p` in the source; a bare `p` in that event report is author context, not a second complaint. Categories remain attributable claims. `x` blob reports and unknown categories have separate unsupported classifications in this binding. |

These selections use the pinned [NIP-10](https://github.com/nostr-protocol/nips/blob/a2494f4f81d46684e5814a9bf35e2b1df978f955/10.md),
[NIP-22](https://github.com/nostr-protocol/nips/blob/a2494f4f81d46684e5814a9bf35e2b1df978f955/22.md),
[NIP-18](https://github.com/nostr-protocol/nips/blob/a2494f4f81d46684e5814a9bf35e2b1df978f955/18.md),
[NIP-25](https://github.com/nostr-protocol/nips/blob/a2494f4f81d46684e5814a9bf35e2b1df978f955/25.md)
and [NIP-56](https://github.com/nostr-protocol/nips/blob/a2494f4f81d46684e5814a9bf35e2b1df978f955/56.md).
Strict ambiguity handling is a local selection, not a claim that every client
rejects the same event. Unsupported input stays available as original evidence.
The parser distinguishes non-reply from present, ambiguous and unsupported
reply syntax; unresolved syntax never falls back to non-reply. T03 classifies
authored activity by syntactically supported source reply/comment claims, without
waiting for the referenced body. T04 separately resolves and validates targets
before attributing received engagement. This separates observable authored reply
intent from a successfully validated target relation.

A target body is required to attribute an event reference to its actual author,
verify its kind or map its version ID to a coordinate. A supplied `p` alone does
not establish authorship. Coordinate references can be parsed without a body,
but public 30384 selection additionally requires an eligible current coordinate.
A retained superseded version can resolve to that eligible coordinate; it never
becomes current again. Event-version and coordinate facts have different keys.
30383 does not publish an addressable version as a regular event.

Maintain source contributions and a reverse target-reference index. Late target
arrival, target interpretation changes and visibility retractions revisit its
indexed dependents with resumable bounded work. Per-subject/actor reference counts
support distinct actors without subtracting the actor when one of two inputs
disappears. A high-fanout target still costs its affected fanout; no whole-history
join per arriving target is the baseline. Unknown references are measured in
coverage/status output. They never produce invented target authors or zero-valued
unresolved subject facts.

`comment-events/1` counts the union of source events whose declared root **or**
direct parent is the subject. Thus a nested comment contributes once to its
thread root and once to its immediate parent when those subjects differ.
`direct-comment-events/1` counts only the parent relation and is API-only.
There is no recursive ancestry inference: an inaccurate signed root remains a
source claim. The alternative direct-only wire definition is coherent but needs
a new binding/profile; it must not silently replace this selected union.

External parser `nip73-identifiers/1` initially enables `web`, `isbn` and `#`.
For `web`, extend the conservative [URL normalizer](../architecture/acquisition-services.md)
to HTTP/HTTPS with their scheme-specific default ports; discard the fragment for
NIP-73 identity, retain the original spelling, preserve path/query distinctions
and reject credentials. WS/WSS use the endpoint normalizer but require the relay
interop gate for rank publication. For `isbn`, remove hyphens and accept only a
valid 10- or 13-digit checksum (terminal `X` allowed for ISBN-10); do not equate
ISBN-10 and ISBN-13. For `#`, initially accept nonempty lowercase ASCII letters,
digits and underscore; other spellings are unsupported, not silently folded.
Other NIP-73 namespaces await explicit normalization fixtures and a versioned
extension; original identifiers remain intact. This is a deliberately partial
implementation scope, not a claim that the
[pinned NIP-73](https://github.com/nostr-protocol/nips/blob/a2494f4f81d46684e5814a9bf35e2b1df978f955/73.md)
permits only these namespaces. Supporting all its namespaces requires more
canonicalization evidence than the earlier generic identifier placeholder.

## Fact-to-wire registry

The following are required logical fact definitions to materialize, not claims
that current migration files already contain these columns. T03 activity and
T04/T05 interaction facts feed `nip85_input_facts`; current graph facts come from
`social_facts`, and optional pubkey ranks come from T11. P05-T01 records exact parent target
names/keys and lineage; P07 consumes stored completed facts, never re-parses tags
inside the publisher or substitutes a fixture helper.

| 30382 field | Definition ID / producer | Exact selected value |
|---|---|---|
| `followers` | `social.current.followers/1`; T02/T10 | Distinct incoming neighbors in the eligible current follow graph; same-author mute removes that follow and self-follow is excluded. No seed/rank gate. |
| `post_cnt` | `authored-posts/1`; T03 + relation parser | Authored kind-1 events classified as having no reply relation. Ambiguous reply syntax is excluded, not reclassified as a post. Long-form articles are separate facts. |
| `reply_cnt` | `authored-replies/1`; T03 + relation parser | Authored kind-1 replies or kind-1111 comments with unambiguous supported reply/comment syntax, once per event; target-body availability is not an authored-activity prerequisite. |
| `reactions_cnt` | `reaction-events-received/1`; T04 | Distinct kind-7 reaction events targeting content whose verified author is this pubkey, across positive/negative/other classes. Coordinate/version aliases collapse per source/pubkey. |
| `reports_cnt_recd` | `report-events-received/1`; T04 | Union of distinct direct pubkey complaint events and supported event complaints against this author's content. One source report targeting both contributes once. |
| `reports_cnt_sent` | `report-events-authored/1`; T04 | Distinct authored report events having at least one supported complaint target; multiple targets/categories do not multiply it. |
| `zap_cnt_recd`, `zap_cnt_sent` | `zap-payments-received/1`, `zap-payments-requested/1`; T05 | Accepted payment identities attributed to recipient or signed request author respectively, under the qualification below. |
| `zap_amt_recd`, `zap_amt_sent` | `zap-msat-received/1`, `zap-msat-requested/1`; T05 | Corresponding exact msat sum divided by 1,000 only when divisible. |
| `zap_avg_amt_day_recd`, `zap_avg_amt_day_sent` | `zap-msat-received-30d/1`, `zap-msat-requested-30d/1`; T05 | Previous 30 completed UTC days' numerator divided by 30,000 only when divisible; denominator is 30 calendar days. |
| `t` | `authored-topic-events/1`; T03 | Top five literal nonempty signed `t` values among eligible authored posts/replies; one source contribution per value. Sort count descending, then UTF-8 bytes ascending. Emit repeated `t` tags. |
| `active_hours_start`, `active_hours_end` | `authored-active-hours-utc/1`; T03 | P05-T03's paired 30-day, at-least-20-sample, shortest non-wrapping 80% histogram interval, with earliest-start tie break and exclusive end. Same authored posts/replies as above. |
| `first_created_at` | `first-observed-post-created-at/1`; T03 | **Withheld on wire initially.** API `first_observed_post_created_at` is the minimum signed timestamp among eligible `authored-posts/1`, not local receipt time. Requires the separate acquired-corpus consumer interpretation gate. |
| `rank` | selected `seeded-trust/2` or explicit `/1`; T11 | Its completed compatible integer projection, with its own graph/seed/population manifest. No ranker or withheld rank leaves other fields eligible. |

`reaction-events-authored/1` is an additional T04 API fact counting authored
supported reactions; it is **not** the selected `reactions_cnt`. Distinct reaction
actors, positive/negative/other reactions, reporting actors/categories and
`social.current.following/1` remain separately named API facts. Their values cannot be
substituted into event-count fields. Changing received to authored would require
another binding version and publisher identity, because both interpretations are
plausible and the short standard field name does not choose between them.

| 30383/30384 field | Definition ID / producer | Subject rule |
|---|---|---|
| `comment_cnt` | `comment-events/1`; T04 | Root-or-parent union defined above; exact event or stable coordinate |
| `quote_cnt` | `quote-events/1`; T04 | Supported distinct quote source events |
| `repost_cnt` | `repost-events/1`; T04 | Supported distinct repost source events |
| `reaction_cnt` | `reaction-events/1`; T04 | Supported distinct kind-7 reaction source events, all content classes |
| `zap_cnt` | `zap-payments/1`; T05 | Accepted payment identities with validated target attribution |
| `zap_amount` | `zap-msat/1`; T05 | Exact corresponding msat sum, divisible-by-1,000 projection |
| `rank` | No selected producer | Withheld; engagement volume is not automatically a recommendation/truth rank |

For 30384, resolve references across all retained public versions to the stable
coordinate, count each source event/payment once, then apply current coordinate
eligibility. Do not add per-version distinct-actor totals. For 30385 external
subjects, `comment_cnt` uses supported kind-1111 root-or-parent relations and
`reaction_cnt` uses kind 17; both use the same event-count definitions with a
tagged external subject. `rank` is withheld without a separately declared profile.
There is no standard external quote/repost/zap field in this binding.

Relay quality remains `Subject::Relay` in `scores`, with profile/viewpoint,
measurement method, observer/access scope, component availability and generation.
`nip85-relay-rank/1`, if enabled after interoperability validation, maps the
canonical full endpoint URL to 30385 `d` with `k=web`, using only
`general-read-relay/1`'s registered integer rank. It does not borrow pubkey
trust, claim authorship by the relay, pool pubkey-only NIP-66 subjects into an
endpoint, or add undeclared capability weights. General external engagement and
relay operational rank remain different binding/profile identities even when
their external URL bytes coincide.

## Zap qualification and conflict handling

`zap-attestations/1` validates bounded receipt/request signatures, kind and tag
cardinality, the invoice signature/amount, and the hash of the **exact** embedded
description bytes. Request and receipt recipient/target fields must agree;
optional amount and sender tags must match their validated sources. A supplied
preimage must match the invoice payment hash. Missing preimage is permitted.
The signing request pubkey is an attributed request author, not independent
proof of who funded the payment. Neither a receipt nor its preimage establishes
independently observed settlement.
[Pinned NIP-57](https://github.com/nostr-protocol/nips/blob/a2494f4f81d46684e5814a9bf35e2b1df978f955/57.md)

Select issuer policy `zap-issuer-historical/1` and an optional concrete input
`IssuerAuthorityBindingV1`, an A03 typed claim with assertion kind
`bigbrotr.issuer-authority-binding/1`. Existing P03-T10 import submits it through
`AdmitEvidence`; it is not a new Nostr event kind, service or HTTP inference.
Required fields are recipient pubkey, retained recipient-signed profile event ID,
the profile-authorized normalized LNURL endpoint and normalization version,
receipt-signer pubkey, claimed half-open `[valid_from,valid_until)` in signed-time
units, retained evidence references, `authority_class=operator-attested`,
authority-policy ID/version and allowed interpretation/publication scope.
Validate the profile signature/recipient/URL binding and a nonempty interval;
profile content alone does not prove that interval. Source dataset, artifact
digest and source-record identity distinguish claims under A03; corrections or
withdrawals name the prior claim and take effect only after their admitted cut.

Enable this optional claim family through `import.claims=enabled-families` and
its registered capability/source grant, then activate the corresponding zap field
bindings by their registered IDs; they start disabled until the input capability and authority gate qualify.
No new HTTP-fetch switch silently activates historical authority.
Enabling consumption requires an authenticated operator's explicit grant for
that source/artifact and authority policy. An unsigned sidecar stays an unsigned
claim; a caller-supplied `authority_class` cannot self-authorize it. Import retains
its bytes/evidence in the existing claim/document relations and checkpoints only
after the normal admission receipt. Public bindings require public-eligible
binding/evidence inputs, export closure over their source objects/dictionaries,
and separately allowed public disclosure of the operator-attested scope with
an approved authority label; private principal IDs/configuration cannot become
purported public evidence.
This is an operator's historical eligibility assertion, not independently measured
issuer continuity. Its qualified interval must cover the receipt's claimed
payment time, and the matching claim/evidence/policy IDs enter the zap fact.
Incomparable contradictory active claims remain conflicts; arrival time does
not pick a winner. An authorized immutable correction must name what it
supersedes, preserve the earlier cut's interpretation and carry the same
required evidence/public-use checks. Private-only authority remains useful to
authorized analysis but cannot feed the standard public binding.

P03-T10 implements the exact claim parser/authority gate; P04-T14 contributes the
current-observation side of the admitted producer fixture. P05 qualifies both
before advertising the historical input capability; a generic imported-JSON
column does not qualify. If the optional producer or explicit grant is absent,
standard zap fields are disabled/withheld and receipts remain classified locally.
P04-T14's current fetch or cache freshness cannot supply the missing interval.
Missing evidence is `issuer-unresolved`; contradictory eligible evidence is
`issuer-conflict`.
There is no analytics network lookup. The less restrictive observed-key-match
alternative remains a differently named API classification and cannot feed this
wire profile. This deliberately leaves some historical zaps unpublishable.

Payment identity is `(invoice_network, payment_hash)`, not receipt event ID,
sender/recipient pair, day or target ID. Qualifying receipts for one identity
must agree on invoice payee, exact msats, request event ID, recipient, signed
request author, canonical optional target and claimed payment time
(`receipt.created_at`). Differences in transport/relay hints or receipt ID do
not create another payment. Identical agreements contribute once; differing
agreements are `payment-conflict` and contribute no accepted payment until an
explicit qualified resolution. Preserve every receipt/evidence reference.
An unqualified impostor cannot create a conflict against a qualified payment.
Later qualifying conflicts retract the old contribution before replacing the
generation; even daily-window attribution must agree rather than selecting an
arbitrary receipt timestamp. This is a conservative local conflict policy,
not a claim that all providers generate identical retry timestamps.

Recipient/request-author attribution and content-target attribution are separate
statuses. A profile zap needs no content target. An event target requires a
verified body and compatible optional coordinate. If the target declares no zap
split, its author must be the recipient; otherwise the recipient must be an
eligible positive-weight split recipient under the pinned split interpretation,
with that recipient's own authorized issuer binding. An invalid/unresolved
content target withholds target engagement; it does not rewrite an otherwise
qualified recipient payment as one to the content author. Two split invoices
with different payment hashes are two payments, not one payment per source post.

Local facts expose accepted, rejected, unresolved and conflicting populations
separately. Wire amounts/counts concern only the accepted attestation population;
they never include an unresolved receipt or claim complete payment coverage.
If the issuer-input capability or classified fact generation is unavailable,
omit affected zap fields. If relevant candidates exist but none can qualify
because their issuer/payment status is unresolved or conflicting, withhold those
fields rather than publishing a misleading zero. A complete empty eligible
population can produce zero. When accepted and unresolved candidates coexist,
publish the accepted-population value with that declared scope and expose the
excluded population in context/API; consumers unable to accept that scope must
disable this optional binding. P07 fixtures must distinguish these states.

Amounts stay exact checked integers locally. A 999-msat plus 1-msat pair sums to
one wire sat; 999 alone omits the amount while a valid count can remain. Daily
numerators use claimed payment time in `[UTC_day_start-30d, UTC_day_start)`;
late qualifications/conflicts update affected buckets. Numeric window eligibility
precedes calendar casts. A 30,000-msat numerator emits one sat/day even when all
activity occurred on one day. No per-receipt or active-day rounding is allowed.

## Subject selection and missing values

`selection.min_events` remains the configuration name, with default one. Its
registry entry declares the family-specific support unit below; it is never
applied as an implicit SQL `authored_event_count` predicate across all subjects.

| Family | Counted support for `selection.min_events` |
|---|---|
| Pubkey facts/rank | Distinct eligible authored event IDs, received interaction/report source IDs and current graph-list header IDs directly supporting this pubkey's vertex/incident relations, unioned. A graph-only subject can qualify without a kind-1 post. Global diffusion inputs are not counted as local support. |
| Regular event | Its eligible verified body plus distinct counted incoming interaction source IDs, unioned by event ID; the body must exist even if references alone exceed the threshold. |
| Addressable | Eligible coordinate required; distinct retained public version bodies and counted incoming interaction source IDs, unioned. A coordinate/version alias does not add a second event ID. |
| External | Distinct supported comment/reaction source event IDs targeting the identifier. An unsupported identifier or bare unclassified mention does not qualify. |
| Relay rank | Distinct eligible executed attempt IDs admitted by the selected relay profile in its window; child checks, imports of the same attempt and retries of delivery do not multiply support. Profile minimum evidence/freshness rules also apply. No authored-event requirement. |

These are publication admission thresholds, not denominators for the metric,
proof of statistical sufficiency or total-subject limits. Zap identities do not
turn into event IDs: qualifying receipt source IDs may enter the corresponding
subject's incoming-support union once each, but never its zap-count unit.
Increasing a threshold does not recompute graph percentiles over that smaller
publication population. A later withdrawal or below-threshold subject follows
P07-T06's explicit withdrawal lifecycle; silently skipping it leaves stale wire
assertions discoverable.

Each optional field is emitted once, except repeated topics; paired active-hour
fields are emitted together. Completed exact zero, unavailable, unsupported,
withheld, overflow and withdrawn are distinct outcomes. Omit an unsupported rank
without suppressing valid facts. Never construct an assertion containing only
`d`/`k` as a substitute for an explicit withdrawal. P07's age limit, visibility
revalidation, changed-content rule and durable complete keyset sweep still apply.
Support indexes, reverse dependencies, classifying excluded input and potentially
U-sized zero-engagement subject publication all count toward storage/work/outbox
budgets. A 500-subject page does not cap a generation or make that cost constant.

## Discriminating acceptance fixture and owners

Use fixed authors A/B/C and a pinned public cut. T03/T04 fixture originals contain:
A's top-level kind-1 event R; B's root-only reply S to R; C's nested reply T with
root R and parent S; A's addressable article versions V1/V2 at coordinate D;
one C-authored kind-1111 comment U naming D and matching V1 in the same role;
two B-authored reactions to R (`+` and `-`); one A-authored reaction to S;
a B-authored source Q with repeated `q` references to R, root R and parent S;
one C-authored categorized event report against R with A as author context and
a categorized direct complaint about A in that same source; one further
B-authored direct complaint about A; and an ISBN comment/reaction with matching namespaces.

The independent expected table must establish: A receives two reactions and
authors one; B receives one and authors two; the first two contribute two
reaction events but one reaction actor for R. R receives S/T in its comment
union, S receives T; including Q gives R three comment events and S two, while
R's direct-parent count remains one. Q's repeated quote counts once. U contributes
once to D,
does not make V1 a current body or a 30383 subject, and cannot be counted twice
through its coordinate/version pair. A receives two report events, not three;
C sends one report event despite its two complaint paths. A no-post graph
vertex and a relay with qualifying attempts but no authored events must pass
their family support threshold.

Zap fixtures add two qualified identities of 999 and 1 msat, a duplicate receipt
for the first identity, a later qualified conflicting amount for that identity,
an unqualified impostor conflict, an unresolved historical issuer, a split
recipient, and 30,000 msats on one of 30 completed UTC days. Check deduplication,
1-sat aggregate, conflict retraction, unchanged earlier pinned results,
impostor isolation, recipient versus content attribution, and one sat/day.
Delete one of B's two reactions: event count falls but distinct actors remains
one. Resolve a late target and compare bounded incremental results with a full
rebuild at the same cut. Include ambiguous marked replies, mixed external
namespaces and a public output whose only candidate input is private.

| Owner | Required completion evidence |
|---|---|
| P05-T01 | Register every definition/binding/subject/status/unit and concrete stored row key; reject incompatible parent cuts/policies and absent producers. These logical names must match engine schema introspection. |
| P05-T02/T10 | Materialize current follower/support contributions including empty headers; support facts-only publication without running diffusion. |
| P05-T02/T03/T04 | T02 implements the shared pure parser specified by T04 before either aggregate; T03 source-syntax classification does not await T04 target resolution. Materialize authored/received, root/parent, alias and actor-refcount facts plus reverse dependencies. |
| P05-T05 + P03-T10/P04-T14 | Implement and exercise `IssuerAuthorityBindingV1` through actual authorized import/admission, then classification, payment dedup/conflict reduction and per-domain exact amount facts. A missing/forged/private-only grant must withhold wire output. Measure unresolved coverage and dependency fanout; no successful live payment is required for fixture qualification. |
| P05-T11/T12 | Supply only their declared subject/profile rank and compatible manifests; independently qualify math and measurement sampling in their owning plans. |
| P07-T05 | Consume real stored producers through all four NIP-85 families, optional ranker and gated relay mapping; inspect signed output from a controlled relay with an independent decoder. Add T02/T10 dependency for followers, T12 only for enabled relay rank; T11 is conditional on rank publication. |
| P07-T05/T06 | Verify every selected field, omission/zero/withdrawal, family threshold, exact units, two service identities, >500-subject resume and changes beyond the first page. Retain the alternative interpretation as disabled until a versioned compatibility decision, never as an undocumented fallback. |

These are future implementation checks. No fixture execution, consumer
interoperability result, production throughput or universal fitness is claimed
by this design document.
