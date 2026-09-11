# BigBrotr query DVM application profile, version 1

Status: selected design, not a deployed protocol, allocated Nostr kind or promise
of generic DVM-client compatibility. The implementation tasks are
[P06](../implementation/06-read-surfaces.md) and
[P07](../implementation/07-publication.md). This profile uses the shared
[query, authority and outbox contracts](component-contracts.md).

## Standards basis and deliberate scope

The pinned [NIP-90 revision](https://github.com/nostr-protocol/nips/blob/a2494f4f81d46684e5814a9bf35e2b1df978f955/90.md)
labels the protocol unrecommended and recommends use-case-specific standards.
Its request/result/feedback envelope, result offset and requester cancellation
are useful, but do not define a general database query or subscription protocol.
Its encryption convention uses NIP-04. BigBrotr's private variant below explicitly
selects NIP-44 v2 and is not advertised as that legacy encryption convention.

The separate [kind registry](https://github.com/nostr-protocol/data-vending-machines/tree/d102c48472c739e0e94eb169c63206985a59da46/kinds)
was inspected at `d102c48472c739e0e94eb169c63206985a59da46` on 2026-09-11.
Kind [5302](https://github.com/nostr-protocol/data-vending-machines/blob/d102c48472c739e0e94eb169c63206985a59da46/kinds/5302.md)
describes content search returning event/address tags, while
[5400](https://github.com/nostr-protocol/data-vending-machines/blob/d102c48472c739e0e94eb169c63206985a59da46/kinds/5400.md)
describes counting with tag/relay inputs and inconsistent examples. Neither is
redefined as an arbitrary resource-query envelope. The registry repository is
[archived](https://github.com/nostr-protocol/data-vending-machines).

Selected application binding: request kind **5999**, result **6999**, feedback
**7000**, with mandatory exact `bigbrotr.query/1` profile and one directed provider.
5999 has no file in the inspected registry; this does **not** reserve the number,
exclude unpublished uses, or imply Nostr community adoption. The listener requires
both the numeric kind and exact profile marker. Other 5999 applications are
ignored without feedback or side effects. A future recognized incompatible
allocation disables this binding pending explicit migration; never automatically
reinterpret existing jobs. Generic kinds 5302/5400 and arbitrary other job kinds
are outside the initial implementation. HTTP and ArchiveRelay remain the standard
general read interfaces; this optional profile serves clients that opt into it.
Provider announcements are initially disabled. Explicitly enabled
[NIP-89](https://github.com/nostr-protocol/nips/blob/a2494f4f81d46684e5814a9bf35e2b1df978f955/89.md)
kind-31990 announcements use a persisted random `d`, `k=5999` and the profile
marker. Kind-0-style name/about/website metadata identifies the application
schema, supported operations, encryption variant and experimental status.
This announces a handler, not allocation of a kind or another user's endorsement.
The announcement uses the same purpose/outbox boundary and configured relays.

## Request wire contract

The DVM is disabled by default. When enabled, accept independently verified full
signed kind-5999 events no older than 3,600 s and no more than 60 s in the future.
Age is admission policy, separate from query time ranges. Event-ID replay never
refreshes acceptance time. Malformed signatures receive no feedback.

Required public tags, each occurring exactly once:

| Tag | Value and interpretation |
|---|---|
| `p` | Exact provider public key configured for this profile; one provider only |
| `param`, `profile` | `bigbrotr.query/1` |
| `output` | `application/vnd.bigbrotr.query+json;version=1` |

Plaintext requests additionally contain exactly one `i` tag with three fields:
`["i", "<UTF-8 JSON query object>", "text"]`, and empty `content`.
The JSON grammar below rejects duplicate object keys, unknown fields, trailing
content, invalid UTF-8 and limits violations. No URL fetching, event dereferencing,
job chaining, executable code or SQL is accepted. A referenced event can be a
filter value, never an instruction to fetch arbitrary remote input.

Optional public tags: one `bid` containing unsigned decimal millisatoshis, and
one `relays` with at most eight destination URLs. Requested destinations must be
a nonempty subset of the operator's allowlist; otherwise reject the request. If
absent, use the configured destination set. A requester cannot grant egress or
private-source authority. Unknown extra tags are retained in the signed original
but do not become parameters; unknown `param` keys or duplicate recognized tags
are rejected for this version.
An absent bid permits a payment-required quote, never authorizes a charge or
execution without settlement. A present bid below the configured quote is
rejected before invoice creation. Free mode charges exactly zero regardless of
a supplied higher bid.

The encrypted variant adds exactly `["encrypted"]` and
`["param","encryption","nip44-v2"]`, has no outer `i`, and places a NIP-44 v2
ciphertext in `content`. Its decrypted plaintext is a JSON array containing the
single `i` tag defined above. Both encryption markers are required. Requests
using NIP-04, ambiguous markers or plaintext/ciphertext mixtures are rejected;
there is no downgrade fallback. NIP-44 padding/encoding/maximum plaintext limits
are enforced before encryption/decryption in addition to the wire limits.
The fixed profile/output/provider tags and routing/timing remain public metadata.

## Exact query object and operations

The proposed schema is `contracts/dvm/v1/query.schema.json`. All operations have
`version: 1` and one `operation` string. Each schema uses
`additionalProperties: false`. Integers named below are JSON integers in their
specified small ranges; event timestamps and large fact integers use the shared
lossless resource schema. Filter encoding is exactly the read core's JSON filter
object/array, with no comma-split or DVM-specific SQL language.

| Operation | Required fields | Optional fields and defaults | Output |
|---|---|---|---|
| `list` | `resource`, `filters` | `page_size:100` (1–1000), `cursor`, `projection` | One `QueryPage` with an opaque next cursor or null |
| `count` | `resource`, `filters` | `count_mode:"exact"` (`exact` or `auto`) | Count, row unit, exactness/method and source identity |
| `subscribe` | `resource`, `filters` | `ttl_s:3600` (60–86400), `batch_interval_s:60` (60–3600), `page_size:100` (1–1000) | Initial bounded snapshot and subsequent numbered batches |
| `renew` | `subscription_id`, `ttl_s` (60–86400) | none | New expiry or typed rejection |

`resource` is an exact enabled read-registry name, not a table name. `projection`
is a list of enabled public resource fields, never SQL expressions. `filters`
have at most eight filters with at most 100 values per field. History resources
require an explicit principal grant. `list` and `count` support every advertised
DVM-readable resource; unavailable capabilities return a typed refusal. A
`list` request returns one bounded page, not an automatic whole-corpus export.
A cursor is encrypted, bound to this requester/provider/profile/resource/policy
and expires after 900 s. Every next page is a new signed request and an independently
budgeted operation; it preserves the pinned query cut.

Initial subscriptions support the **normal-visible events** resource only, with
event filters supported by the ArchiveRelay live feed. Search, counts, historical
views, global/derived aggregates, arbitrary sort and projected event bodies are
not subscription resources. They require finite queries. This narrower baseline
avoids pretending that every aggregate has an incremental change feed.

`subscription_id` is the original accepted subscribe request's event ID. Renewal
must come from the same requester to the same provider, before expiry, and carries
the original subscription ID. The new expiry is acceptance time plus `ttl_s`,
never an additive unlimited extension. Each renewal has its own request ID and
deduplication record. A signed matching-author kind-5 request with an `e` tag for
the original request cancels local pending execution and undelivered obligations.
It neither erases archived requests/results nor recalls remote deliveries. It
does not cancel someone else's job; address deletions are not a control operation.
Cancellation applies to a known accepted job. Unknown-job controls are ignored
without claiming cancellation; clients wait for processing or payment-required
feedback before using this control. Known-job cancellation is idempotent and
remains effective on restart. It does not require a fresh source query or a
global subscription to every network kind-5 event.

## Result and feedback schemas

The proposed `contracts/dvm/v1/result.schema.json` envelope contains:
`version`, `profile`, `request_id`, `operation`, `state`, `data`, `error`,
`source`, `next_cursor`, `subscription`, `payment`, `truncated`.
Absent optional information is null; unknown output fields require a new version.
`source` is the permitted read metadata with generation, cut identity, coverage,
visibility basis and exactness; it never exposes private shard/host details.
`truncated` is false for a completed finite page; a next cursor represents normal
paging. An oversized single row yields `result_too_large`, never a partial event.
Byte-bounded list pages may contain fewer complete rows than requested, with a
valid next cursor that has not consumed the first unreturned row. Report the
effective page size through QueryPage metadata; continuation keeps the same
normalized request contract. Do not mark ordinary paging as truncated or return
a cursor whose own effective-limit validation will reject it.

The envelope `state` is one of `processing`, `payment_required`, `result`,
`subscription_batch`, `renewed`, `error`. These are payload states, not a claim
that an as-yet-unsent event has been remotely delivered. `data` is the exact
registered QueryPage/count response for list/count, a list of complete signed
events for a subscription batch, and null for renewal/feedback/error. Successful
result fields have `error:null`; error envelopes have `data:null`.
`subscription` is null or `{id,batch_seq,phase,expires_at}`: ID is the original
subscribe event ID, sequence is a lossless decimal string starting at zero,
phase is `snapshot|live|closed`, and expiry is a decimal Unix-second string.
Renewal acknowledgements identify the existing subscription and its latest
sequence without generating a data batch. An empty live interval need not emit
a batch; persist its source progress so it does not rescan forever.
`payment` is null or `{quote_id,status,amount_msat,received_msat,expires_at,bolt11}`,
with amounts/deadline decimal strings, unavailable received amount and invoice
null, and status from the payment state machine below. Quote IDs are opaque
random identities; payment preimages are never response fields. `source` and
`next_cursor` are null for pure control/payment feedback. The same source-cut
and result-schema meanings apply inside private ciphertext.

Result kind 6999 uses `e=request_id`, `p=requester`, and the same profile/output
tags. Its content is the envelope JSON, or its NIP-44 v2 ciphertext with both
encryption markers. No `i` or full `request` echo is included. This is an explicit
application-profile choice that avoids copying input and consuming the wire
budget; clients correlate through the verified event ID.

Feedback kind 7000 uses `e`, `p`, profile and one standard status tag:
`processing`, `payment-required`, `success` or `error`. Detailed messages belong
in the same bounded JSON envelope; private feedback encrypts it. Errors do not
reveal query text, private resource existence, tokens or exception chains.
For public paid feedback an `amount` tag contains exact msats and the provider's
BOLT11. For private requests those values are only inside encrypted `payment`;
outer feedback remains a generic status. Clients must implement this named
variant, not assume NIP-90 legacy behavior.

`error` has `{code,retryable,message}`. Codes include `invalid_request`,
`unsupported_resource`, `forbidden`, `budget_refused`, `unavailable`,
`cursor_expired`, `cursor_incompatible`, `result_too_large`, `cancelled`,
`expired`, `resume_unavailable`, `payment_unavailable`, `payment_expired`,
`payment_mismatch` and `refund_due`. Forbidden point lookups follow HTTP's
non-disclosing not-found policy inside their resource result. Invalid or
misdirected wire events produce no response; one well-formed rejected job can
produce one bounded error response under the anti-amplification budget.

## Durable execution, delivery and limits

Identity is `(provider_pubkey, request_event_id, profile_version)`. Admission
atomically stores verified request identity, normalized parameters, requester
grant/policy context, input cut/pins, expiry, budget and destination obligations.
Replay of the same request resolves its existing state. An incompatible body with
the same claimed event ID fails independent verification before lookup.

Finite lifecycle: accepted → queued → running → result_ready → delivering →
complete, with failed/cancelled/expired branches. Optional paid mode inserts
waiting_payment before queued. Record signed result bytes before networking.
Restart resumes the same result/outbox rather than requerying a new source cut.
Expired pins before a result exists fail the job; they do not authorize a fresh
charge or invisible recomputation. A relay `OK` proves that relay's acceptance
response, not that the requester processed the result.

Finite job expiry is 900 s from durable acceptance; request age is a different
admission rule. Queue wait is at most 60 s from entering queued (after settlement
for a paid job), and actual read execution remains bounded to 15 s. Retries and
restarts do not extend these absolute deadlines. The result delivery deadline is
`min(result_ready_at + 300 s, job_expires_at)`, starting when the complete result
artifact becomes durable, including subsequent signing time. At least one
configured destination must accept before that deadline. Check authority again
before signing/dispatch; exact signed bytes and compact lineage allow source
read pins to be released once no other reader needs them.

At deadline, cancel all undispatched DVM obligations and stop retry scheduling.
A free job without acceptance becomes failed with its exact artifact inspectable;
a paid job without acceptance becomes failed plus refund_due. Parked administrative
artifacts are not live delivery obligations. A paid result is not automatically
sent after refund or timeout; any later resend is a distinct explicitly authorized
operation. If at least one target accepted, the finite job completes and remaining
targets can retry only through that same deadline. A packet already in flight
cannot be recalled; record late acceptance separately without reviving a terminal
job or silently cancelling a refund obligation.

Subscriptions are opt-in, at most 32 globally, and use snapshot+acquisition-feed
handoff with durable `batch_seq`, input position and outbox batch commit. Complete
the initial snapshot under its cumulative admitted bound or refuse subscription;
do not hide an unbounded history scan behind successive batches. Delivery is at
least once; clients deduplicate `(subscription_id,batch_seq,event_id)`. Include
backdated newly acquired events. Never advance a batch position before its exact
bytes and delivery obligations are durable. Feed exhaustion or expired generation
pins produces `resume_unavailable`, pauses the old subscription and requires an
explicit new request; a new snapshot is not a continuation of the old feed.

Limits: four running jobs, queue 64, one active job per requester, requester rate
one per 10 s with burst two; request wire 64 KiB; logical result content at most
192 KiB and total signed wire bytes at most the configured relay bound (initial
256 KiB), whichever is smaller. NIP-44's own plaintext limit can lower this for
private output. Apply bounds to actual escaped JSON and encoded ciphertext,
including framing and tags, before accepting the output. Shared query limits
remain 15 s, scan 256 MiB, memory 64 MiB, page maximum 1000 and finite response
16 MiB; the smaller DVM bounds win. Feedback, retries and subscriptions consume
shared network/output reservations. Initial subscription TTL is 3600 s, maximum
86400 s, with batches no faster than 60 s. No payment increases these limits.

Every query and delivery rechecks the five-second authorization lease. A
restrictive policy change invalidates prepared output and stops its undispatched
delivery; previously released data cannot be recalled. PG outage fails closed
after lease expiry. Stop drains bounded work and leaves recoverable jobs;
restarting does not accept fresh requests before authority and pins are valid.
Detailed terminal jobs retain 30 days by default; compact replay/cancellation and
financial tombstones survive for their correctness horizon.
Limit unresolved financial obligations to 64 globally and a configured
payment-ledger byte reservation. Creation-unknown, possibly payable cancelled/
expired and refund-due records all consume this allowance. A full ledger refuses
new paid jobs; it does not discard unresolved money state. Completed detail and
replay-key retention remain separate and cannot revive a request outside the
accepted age window. Cursor pages share one absolute 900-second query/pin expiry;
requesting a next page cannot keep a physical snapshot pinned forever.

## Concrete optional payments: LND invoice-only adapter

Default `dvm.payments.mode="disabled"`, `price_msat=0`. Paid mode is explicit
`lnd-prepaid-v1`, available only for finite `list`/`count`; subscriptions and
renewals remain unpaid and cannot be enabled with a paid-only resource policy.
The operator sets one fixed positive `price_msat` per advertised finite resource
operation. A quote buys one page or count under the normal quotas, never an
unlimited scan, broader grant or payout. Reject a bid below that exact quote.
No price discovery, automatic zap settlement, credit balance, currency conversion,
metered recurring billing or automatic refund execution is in this baseline.

The concrete adapter uses LND REST `POST /v1/invoices` and
`GET /v1/invoice/{payment_hash_hex}`. LND creates the actual BOLT11. The
[AddInvoice API](https://lightning.engineering/api-docs/api/lnd/lightning/add-invoice/)
accepts a preimage and rejects duplicate invoices; the
[LookupInvoice API](https://lightning.engineering/api-docs/api/lnd/lightning/lookup-invoice/)
resolves a 32-byte payment hash. The pinned
[invoice schema](https://github.com/lightningnetwork/lnd/blob/32b6e2dce485c0c0246a011ba392e6ffc7249b74/lnrpc/lightning.proto)
defines `value_msat`, `r_preimage`, `state`, `amt_paid_msat` and settlement time.
These are inspected API contracts, not results of a live invoice experiment.

Configure a fixed HTTPS LND origin, expected TLS trust/certificate, network,
protected macaroon reference, and encrypted-record key reference. Redirects,
unverified TLS and requester-selected nodes are forbidden. Provision a macaroon
with only `uri:/lnrpc.Lightning/AddInvoice` and
`uri:/lnrpc.Lightning/LookupInvoice`; LND documents
[method-specific permissions](https://github.com/lightningnetwork/lnd/blob/32b6e2dce485c0c0246a011ba392e6ffc7249b74/macaroons/README.md).
No admin macaroon, wallet seed, payment-sending, invoice-hold settlement/cancellation,
macaroon baking or on-chain spending capability belongs in BigBrotr. The external
LND node/operator controls the wallet. A dedicated invoice preimage is a protected
receiving secret, not a wallet spending key; do not claim the application handles
no secrets. Missing signer/payment secrets prevent activation, without fallback
keys or fabricated invoices.

The exact creation/recovery procedure is:

1. Admit the finite query's resource, policy, source pin and capacity before
   offering payment. Reserve a source pin for the 300-second quote plus bounded
   execution/delivery; release execution CPU while waiting. Cap waiting quotes
   at 64 globally and one per requester.
2. In a fenced PG transaction create one quote identity, immutable msat amount,
   requester/job/provider/resource binding, 300-second expiry and random 32-byte
   preimage. Store the preimage encrypted under the protected record key and its
   SHA-256 payment hash in a uniqueness-constrained row **before** AddInvoice.
   Record plaintext secrets in neither logs, fixtures with live keys nor public
   metadata. An opaque quote ID in the invoice memo suffices; no private query.
3. Call AddInvoice with that same base64 preimage, `value_msat` and 300-second
   expiry; never set both sat and msat values. Check the returned payment hash
   and independently decode/check the BOLT11 network, amount, hash and expiry.
   Persist the exact invoice before queuing payment-required feedback.
4. A lost response leaves `creation_unknown`. Look up the known hash, validate
   the invoice and recover it. If definitely absent, retry creation with the
   **same** preimage/quote, handling duplicate rejection by lookup. Transport
   errors are not absence. Never create a new random invoice to escape ambiguity.
5. Poll known pending hashes, initially every 5 s, at most two concurrent RPCs,
   four requests/s globally, timeout 10 s with bounded backoff. Lookup is the
   authority; requester claims, Nostr zap receipts, HTLC acceptance, HTTP 200
   alone and relay acknowledgements are not settlement.
   Five seconds is the minimum per-invoice polling interval, not a promise that
   all 64 pending invoices are checked every five seconds under the global cap.
   Coalesce due hashes in a fair bounded queue, expose lookup lag, and retain
   ambiguous state rather than substituting a missed poll for nonpayment.
6. Accept only the bound LND invoice with `state=SETTLED`, expected network/hash/
   amount, `amt_paid_msat >= price_msat`, and settlement within the recorded quote
   deadline. Use a unique settled-payment record so replay starts the job once.
   Overpayment is recorded separately as refund due, not automatically a tip.
   Revalidate authority/pins before execution; payment never bypasses revocation.
7. Expiry/cancellation/policy loss prevents execution. Continue bounded
   reconciliation of possibly payable/ambiguous invoices; do not forget them
   when job details expire. Late settlement or paid-but-undeliverable results
   records `refund_due` with the exact amount and reason. Operator refunds are
   performed outside BigBrotr; a separately authorized audit command may record
   `operator_reported_refunded` with a reference, without claiming automatic
   independent verification. There is no app endpoint that spends or pays a
   requester-provided invoice.

Payment states are quoted → creation_unknown/invoice_ready → waiting → settled,
with expired/cancelled/mismatch/refund_due and operator-reported resolution.
Job state and payment state are separate: cancelling a job cannot undo a settled
invoice. An acknowledged signed result is the sold delivery obligation's
acceptance basis, explicitly not proof of customer processing. All configured
destinations failing until the delivery deadline makes the paid job refund due;
the operator can inspect exact bytes and attempt history. Financial ambiguity and
refund obligations block their own retention cleanup, not unrelated archive work.

## Required implementation evidence

The packaged profile tests must use an independently implemented client and real
relay transports. Check directed/undirected/colliding 5999 traffic, NIP-44
cross-client decryption, unsupported legacy ciphertext, escaping-dependent size
limits, duplicate request across workers, cancellation by another author,
renew-after-expiry, snapshot/live backdating, feed exhaustion, policy revocation
and lost acknowledgement. Assert actual persisted query and signer/outbox paths.

Paid acceptance runs only on isolated Bitcoin/LND regtest nodes with generated
test funds. Use a real invoice-only macaroon and a separate payer; inspect the
node's independent invoice state. Drop the AddInvoice reply after node commit,
restart BigBrotr, and require recovery of the same payment hash/BOLT11 and one
job execution. Exercise unsettled/ACCEPTED versus SETTLED, wrong hash/amount,
late settlement, cancellation, duplicate settlement polling and refund due.
Attempt a spending RPC with the configured macaroon and require permission
denial. No test uses real funds or claims unexecuted regtest evidence as success.
