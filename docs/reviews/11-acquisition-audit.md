# Review 11: acquisition contracts and newcomer implementability

Date: 2026-09-11. Frozen baseline:
`8c81472e75da118b04809636a73f3231157227b5`.
This is a planning review. All eight assigned documents were read completely:
**2,848 lines / 343,383 bytes**. Working bytes matched the baseline Git objects
before and after reading. The [read manifest](../research/evidence/11-acquisition-read-manifest.json)
records every range, hash, size, supplemental excerpt and recovered truncation.
Subsequent coordinator changes require their own disposition; these findings
describe the frozen input, not a product implementation.

The older acquisition paths have useful, concrete failure cases and bounded
interfaces. The main remaining gaps concern the two newly added producers:
their subject schema, per-ID negative evidence and executable job/configuration
contract. These can be repaired within existing owners without another service.
P2 below means materially different plausible implementations; P3 is terminology
or ownership cleanup. Neither denotes an observed production incident.

## ACQ-11-01 — P2: issuer measurements have no valid canonical subject

P04-T14 sends recipient-bound LNURL HTTP observations through A05/A06.
However, [A05](../specification/data-contracts.md#a05--executed-measurements)
requires `endpoint_id` for an attempt, while the acquisition `EndpointIdentity`
contract accepts relay `ws`/`wss` URLs and non-dialable NIP-66 subjects. The LNURL
recipient, signed profile and HTTPS metadata resource do not identify a relay
endpoint. A06 occurrence metadata likewise names endpoint/scope without defining
the new subject. The new measurement-family row alone does not resolve this
required-reference mismatch.

**Minimal repair:** give A05 a closed, typed subject choice: `relay_endpoint`
with `endpoint_id`, or `recipient_metadata_http` with recipient public key,
binding profile EventId and guarded HTTP resource identity/URL descriptor.
Retain actual request/redirect/dial context separately from the subject. A06
references use that subject or the authoritative observation identity. Keep
credentials outside descriptors and preserve visibility-qualified object keys.
Do not invent relay enrollment, use a fabricated endpoint, or make every subject
field independently nullable. The coordinator owns the exact shared names.

**Discriminating acceptance:** capture metadata for a recipient having no relay
identity, export it, and decode it with an independent reader. Its profile,
recipient, HTTP resource, observation and original document must resolve, while
the relay registry gains zero enrollments. A relay TLS measurement still requires
its real endpoint. Reject a subject variant with another variant's fields and a
cross-visibility document reference. A self-roundtrip alone does not prove this
portable reference contract.

## ACQ-11-02 — P2: a partially returned batch is called completed empty per ID

P04-T13 steps 2–3 batch up to 20 exact IDs, then permit completed-empty results
for absent requested IDs after ordered EOSE. P05-T08 describes completed empty
exact-ID requests feeding the distinct-ID not-served denominator. A peer holding
all 20 IDs can return one event under a lower result cap and then EOSE. The
remaining 19 were omitted from a partially fulfilled batch, which is weaker than
19 individually completed empty lookups.

NIP-01 permits fewer initial results and defines EOSE as the stored/live boundary;
it does not provide a per-ID exhaustiveness certificate. This counterexample is
a design inference from the [pinned primary protocol](https://raw.githubusercontent.com/nostr-protocol/nips/a2494f4f81d46684e5814a9bf35e2b1df978f955/01.md),
not an executed relay observation.

**Minimal repair:** batching establishes positive served evidence. Follow omitted
IDs with bounded singleton exact-ID requests before classifying completed-not-served;
budget exhaustion leaves remaining IDs unknown. An explicit `more`/restriction,
invalid requested payload, dropped frame or incomplete ordering also stays
unknown. Even a completed singleton proves only not served in that request's
scope/time, never physical absence or intent. Record batch size and final request
profile so unlike measurement methods do not silently share a denominator.

**Alternative:** retain a separately named batch-not-returned outcome, exclude it
from the singleton not-served denominator, and show its actual request context.
Do not hide this choice inside one shared `completed_empty` label.

**Discriminating acceptance:** a controlled peer holds all 20 IDs, returns one
per batched request and EOSE, but serves each singleton. The final classified
set must be all served after allowed confirmations, or served plus unknown if
the job budget ends. Add a truly missing singleton, `more`, forged requested
payload and AUTH/rate refusal. Inspect independent socket logs and the actual
P05/P09 result: no lower-cap fixture may acquire 19 unsupported negative votes.

## ACQ-11-03 — P2: new producer jobs and controls lack one concrete entry contract

P04-T13/T14 name proposed schema files, numerical limits and future shipped jobs,
but do not select command/Manage submissions, exact configuration keys/reload
classes, or the selection-to-attempt identity relationship. P04's configuration
ownership table ends at T12 and says the architecture owns all defaults, while
the architecture delegates these two profiles back to their tasks. An implementer
must currently invent these user-visible details. T14 bounds pending demand but
does not state eviction/recovery of its separate cache and retry-cooldown state.

**Minimal repair:** extend the existing finite-job/Manage owner and P04 table.
Select typed submissions and working CLI adapters; no new daemon is needed.
One concrete proposal is:

| Entry | Typed input/result |
|---|---|
| `bigbrotr monitor availability run --selection FILE` | `AvailabilityJobRequest`: canonical submission identity, explicit pairs or pinned query selection, source cut, access/visibility, profile version and finite budget; returns job identity |
| `bigbrotr monitor availability status JOB` / `resume JOB` | Existing finite-job status/receipt recovery, including selected/attempted/served/not-served/unknown/unattempted counts |
| `bigbrotr issuer lookup --recipient PUBKEY --profile EVENT_ID` | `IssuerLookupRequest`: verified recipient/profile binding, source cut, access/visibility and profile; returns job identity or typed refusal |
| `bigbrotr issuer status JOB` / `resume JOB` | Same finite-job owner; cache-hit source identity/time, deferred, unavailable and current-capture outcomes remain distinct |

These names are proposals for coordinator selection. A concrete existing Manage
operation with named CLI/browser wiring is an equally sufficient alternative.
Submission retry uses the shared management idempotency contract and payload
digest, rather than creating a second job accidentally.

Keep configuration small. The following named gates implement the existing
eligibility choices; the default alone schedules no arbitrary work:

| Proposed key | Default and reload | Meaning |
|---|---|---|
| `monitor.availability.enabled` | `true`, H | Permit explicitly submitted finite jobs only; provenance and current grants remain required. No periodic producer is selected. False stops new dispatch and resolves outstanding receipts. |
| `issuer.enabled` | `true`, H | Permit guarded eligible public demands from enabled zap analysis or an explicit job. False yields unavailable issuer inputs. Private-input disclosure remains separately authorized. |

Keep numerical starting limits in named, versioned job/profile schemas as J
values, captured before execution: availability 100 IDs/endpoint, 20 IDs/request,
1,000 pairs/job, four requests and 10s; issuer four requests, 10s, 64KiB response,
two redirects, 24h cache freshness, 4,096 queued demands/64MiB, one new start/s
with burst four and one-hour unsuccessful retry spacing. Per-request limits
never exceed current hot shared grants. Expose every effective value and origin;
promote a profile field to a hot deployment key only when its operational use
justifies another knob. Add these profiles and ownership to the central inventory.

Freeze the following distinctions explicitly:

| Identity/state | Lifetime and required behavior |
|---|---|
| Job submission | Operational request and durable progress; replay resolves the same submission, not a new measurement |
| Scientific selection | Immutable exact selected pairs or reproducible bounded selection descriptor with input cut/method/seed; separate from mutable worker progress |
| Logical pair | Event, endpoint and access context; the same pair may have several genuine observations |
| Actual request attempt | New identity for a new external request; batch children link selected IDs to one executed request. A persistence retry preserves this identity. Singleton confirmations are distinct requests. |
| Terminal child fact | Stable fact identity and complete receipt before job progress; unattempted pairs remain unattempted and create no fabricated negative measurement |
| HTTP response cache | Bounded reconstructible lookup state referencing original A05/A06 evidence; eviction costs a later permitted fetch, never changes original observation time |
| Retry cooldown | Retain not-before state for its correctness horizon or defer new identities when capacity is full; eviction cannot bypass the one-hour rule |

Apply one memory grant to queue/cache/cooldown representations, and state their
cardinality, byte and reclamation policy. Sharing a fetch for the same eligible
URL/visibility/method can coexist with distinct recipient-binding facts; first
compare that simpler reuse with T14's recipient-specific coalescing key. Do not
infer a physical storage saving from identical content.

**Discriminating acceptance:** invoke the selected parser/Manage boundary through
the built binary; reject malformed/oversized/provenance-disabled selection before
traffic; deduplicate overlapping selected pairs; lose reply and restart at job
submission, dispatch and terminal receipt. Compare exact selected, executed and
unattempted sets. Disable each gate while a request is outstanding, resolve its
actual committed result, and issue no later unauthorized request. Drive many
distinct recipients through a tiny cache, inspect memory and real start times,
and verify cooldown cannot disappear with cache eviction. A private-input lookup
without disclosure authority emits no request. Analytics reruns without network.

## ACQ-11-04 — P3: remove summary vocabulary that invites parallel contracts

The architecture's durable-ingest summary still uses
`Admit(batch, producer, policy_revision, idempotency_key)` and `DurableReceipt`.
P03/P04 and the component contract select `ProducerRequestId`, `Receipt`, typed
outcomes and bounded retirement. P03-T09's input/output summary still says
event/claim/control records and its coverage table still says controls, although
the selected research formats now exclude administrative controls.

**Minimal repair:** link the single normative admission signature and use its
names consistently; do not create a second producer ID/receipt struct. Use
event/claim/scientific-evidence records for research decoding. Define producer
slot, producer epoch, local request sequence, archive owner epoch and connection
generation once with their distinct lifetime; these identities are not synonyms.
Keep `access_scope_id` versus older auth-scope spellings consistent in actual
schemas. The coordinator owns this central naming/table inventory.

The later producer retirement protocol is a useful bounded alternative to keeping
every request receipt forever. This acquisition review read its component
summary, not the complete storage/backup proof. Preserve P03's rule that a pending
request and immutable import range/digest survive display TTL and ambiguous
commit. Future acceptance must recover the same receipt before source progress,
and reject retired identities without recreating effects; the storage reviewer
owns the full acknowledgement, slot-reuse and restore cases.

## Current versus historical decisions

| Earlier item | Baseline disposition |
|---|---|
| Review 10 source administrative tombstones | Repaired in architecture, P03-T08/T09 and import-formats. Signed targets and kind-5 requests remain archival history; deployment control restoration belongs to P08. Only stale summary vocabulary remains above. |
| Review 10 missing zap issuer producer | P04-T14 now supplies a bounded producer and rejects payer-only endpoint authority. Subject/schema and entry-contract gaps are the new findings, not a claim that no producer exists. |
| Review 10 pooled handshake age | Repaired: original connection-generation evidence/time is preserved; a fresh handshake obtains the same permit and coordinated rotation. |
| Review 10 hostname alias fairness | Repaired with host-local selected-IP grants, shared Control ownership across processes, and live-holding reconciliation after owner loss. Split-host global IP coordination is explicitly not claimed. |
| Review 10 optional arrival series | Removed. Ordinary duplicates remain bounded operational telemetry; intentional availability observations are a separate provenance-dependent feature. |
| Review 10 local versus resolved projection | Repaired with LocalMonitorView and ResolvedRelayEvidence plus distinct consumers and shared pure reducers. |
| Review 07's 365-day raw retention and old research tombstones | Historical, superseded. Current A05/A06 evidence is permanent by default; explicit lossy retention and restoration controls have different contracts. Add a superseded-status pointer instead of treating old review prose as a second active policy. |
| Go walkthrough failure paths | Source-inspected lessons, not newly reproduced incidents. The current plan already covers seen-ID signature errors, body-novelty paging, EOSE barriers, capped sets, fallback labels, stale CLOSE and actual guarded sockets. |

## Complexity, evidence and completion boundary

Retain the six selectable acquisition capabilities in one application, shared
transport/admission, finite offline imports, exact first-witness semantics and
bounded uncertainty. Streaming body-only imports should not pay native sidecar
join costs. A stale conservative reconciliation set may increase downloads;
missing storage must not masquerade as an empty authoritative set. Imported
claims must not grant enrollment or substitute for local delivery.

Keep the proposed comparisons open: compressed wide documents versus A06
normalization; conservative archive reconstruction versus an extra P-sized
ordered index; fixed recent windows versus a measured adaptive policy. The
existing practical journeys already distinguish these choices. Repeated review
counts, extra wrappers and extra tables provide no missing empirical evidence.
No new performance promise follows from this review.

The stricter issuer policy deliberately cannot reconstruct unseen historical
authority from today's metadata. The [pinned NIP-57 flow](https://raw.githubusercontent.com/nostr-protocol/nips/a2494f4f81d46684e5814a9bf35e2b1df978f955/57.md)
provides current provider-key association, not a historical directory. Preserve
observed-only status, explicit historical trust/time scope and unresolved counts;
measure their impact on the declared zap corpus before promising useful accepted
historical coverage. Source inspection alone cannot settle that product utility.

Performed: complete assigned reading, independent baseline-byte comparison,
bounded adjacent contract excerpts, pinned primary protocol inspection and this
report. One truncated review-07 chunk was reread completely in smaller ranges.
No legacy repository was edited or exercised. No product implementation, build,
test, live relay request, database mutation, benchmark, commit or push occurred.
All acceptance cases above are future discriminating work, not reported passes.
