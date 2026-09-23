# Read views, time and search identity

Status: selected contract for P06-T01–T11 and their P05/P07/P09 consumers, before implementation. Source identity
and current-state reduction remain in the data contracts. This specification
closes two independent gaps: time can change eligibility without new ingestion,
and search syntax is not the text that should determine relevance.

## Select the read mode explicitly

`QueryRequest.mode` is `traversal` or `snapshot`; history access remains a separate
resource/permission, not a third way to bypass visibility. Ordinary chronological
event lists default to `traversal`. Point lookups evaluate current eligibility.
Search/relevance, NEG, exact context-bound counts, analytical joins and finite DVM
results use `snapshot`; reject unsupported mode/resource combinations at admission.
HTTP, MCP and ContextVM expose the same choice through the shared query registry.
A strict snapshot is still available for chronological lists when requested.

The shared response fields are `mode`, `candidate_cut` (SourceCut for traversal,
null for point/snapshot operations), `view_stamp` (the complete eligibility or
snapshot ReadViewStamp), and `eligibility_lag_us` (nonnegative lossless integer,
or null when the resource is not current-dependent). Traversal cursors bind their
candidate cut and mode; each page reports its fresh view_stamp. Snapshot cursors
bind the whole unchanged stamp. Count responses identify their own stamp even
when requested alongside traversal; adapters must not copy a preceding page's
stamp onto an independently evaluated count.

### Chronological traversal

Pin a fixed acquisition upper cut and immutable `(created_at descending, event_id
ascending)` ordering for candidates. The cursor binds the normalized filters,
principal/policy domain, acquisition cut, mode/version, maximum lifetime and last
**scanned** key. Never use offset pagination. Events admitted after that cut,
including backdated events, appear only after refresh. Returned event IDs cannot
repeat within one traversal. Skipped/ineligible candidates advance the scanned
position; an empty page may carry a continuation and an explicit scan-budget
reason, but must not claim exhaustion before reaching the end.

Evaluate current eligibility using the latest compatible applied visibility
state before each page/chunk is delivered. Authorize the input population before
winner/deletion reduction. A newly admitted authorized replacement can suppress
an old candidate even though the new event lies outside the traversal's candidate
cut; expiry and deletion can remove candidates too. Private replacements cannot
influence a public traversal. The page reports its eligibility evaluation stamp
and lag; unavailable or overdue eligibility processing is a typed refusal, never
permission to emit known expired or suppressed events. Cancellation and the
five-second authorization lease remain mandatory.

Ordinary expiry removes the affected event without invalidating the entire
traversal. Re-evaluate an undelivered page crossing its eligibility deadline;
bounded retries end in explicit `eligibility_busy` if it cannot be delivered
within its query budget. Do not buffer unbounded pages while eligibility changes.
Restrictive policy revisions still invalidate authority/cursors. Membership may
change after an earlier page; already delivered bytes cannot be recalled.
Therefore this mode does **not** promise exhaustive enumeration of a changing
population or equality between an earlier count and all later pages. Counts have
their own dated snapshot stamp. Refresh starts a new traversal. Exports requiring
reproducibility use authorized immutable historical snapshots.

Acceptance: an event expires between pages, another gets a public replacement,
a third gets only a private replacement, and a backdated new event arrives after
the fixed cut. Continue without duplicate IDs; omit the first two, retain the
third, exclude the fourth until refresh. Repeat with sparse filters, skipped-only
pages, policy revocation, an unavailable visibility reducer and concurrent reads.
Measure completed traversals and restart/refusal rates, not just query latency.

## A reproducible snapshot with an explicit time boundary

`ReadViewStamp` contains `source_cut`, input/target generation IDs,
`visibility_as_of_us`, `temporal_generation`, `expiry_deadline_us` and
`policy_epoch`. The expiry deadline can be absent only when no eligible expiry
can affect that view within its maximum lifetime. It is distinct from the pin's
absolute lifetime and the short authorization lease. All body lookups, lists, counts, search candidates/ranking and NEG membership
for one snapshot context use the same stamp. Traversal fixes its candidate cut
and reports a fresh eligibility stamp per page as specified above. Response metadata identifies it without leaking restricted IDs.

For ordinary-visibility `snapshot` reads, a pin is valid only until the earliest relevant scheduled
eligibility change, or an earlier conservative deadline established by the plan.
At that deadline, HTTP continuation fails with 409 `temporal_snapshot_expired`;
the caller starts a new query and gets a new stamp. Pin lifetime expiry/reclamation
still yields 410. The cursor's 30-second clock allowance does not extend event
visibility, grant lifetime or the temporal deadline. Existing restrictive policy
invalidation remains independently effective within its five-second bound.

The Indexer owns a rebuildable ordered expiry index and per-partition next-expiry
summaries (`visibility_deadlines` in the logical catalog). An admitted plan obtains
a covering deadline for the partitions it can read, then pins data and temporal
generation together. It may choose an earlier safe deadline; it cannot skip an
unknown partition or scan the whole archive merely to mint every cursor. Missing
or incomplete expiry state makes the affected current resource unavailable in
both modes; traversal does not bypass visibility maintenance.
No new source sequence is invented for the passage of time. New admission after
an expiry uses the same source cut if appropriate and a new temporal generation.

Time conversion checks the full unsigned Nostr seconds domain before arithmetic.
A far-future expiration beyond the representable operational horizon does not
wrap into an already expired value; compare in a wide checked domain and treat
it as outside the finite pin lifetime. Clock uncertainty and backward jumps
cannot extend an established monotonic deadline. Use a bounded conservative
deadline when the wall-clock mapping is uncertain; report unavailable if one
cannot be established safely.

Time-sensitive target manifests also declare their evaluation/window basis and
relevant temporal generation/deadline. Admission verifies compatibility with the
read stamp. A stopped Deriver's old COMPLETE current-membership/count target cannot
be relabeled current by attaching a fresh temporal stamp. Refuse the affected
current-dependent operation until compatible reduction is available; explicitly
dated stale analysis remains allowed only by its resource contract. Test expiry
with Deriver stopped while expiry scheduling and reads stay alive.

Recheck the temporal deadline before exposing a page/count or serializing/writing
each stream chunk, and propagate cancellation to engine work. A page under
construction that crosses its deadline is discarded with the typed expiry error.
Already delivered bytes cannot be recalled. Keep output buffering bounded; this
does not claim control of packets already handed to the operating system. Long
running historical relay output is closed explicitly on invalidation; a new REQ
can obtain a fresh view. Bounded live feeds evaluate each delivery under current
visibility and do not reuse an expired historical stamp.

Authorized immutable-history resources deliberately preserve original historical
events under their own declared evaluation semantics. Ordinary expiration does
not delete that archive. Policy/security changes still invalidate history access.
An authorized export pins its declared historical view and is a different
operation from a short ordinary cursor.

NEG session identity includes this stamp and ordered-set generation. Close an
expired/restricted session rather than continuing with a shrinking anonymous set.
A subsequent ordinary REQ has current eligibility; NIP-77 followed by REQ is not
an atomic exported snapshot and may legitimately find an event newly expired.

DVM finite results and each subscription batch also bind this stamp. Their
effective execution/signing/delivery deadline is the minimum of existing job/
delivery limits and the ordinary temporal deadline. Recheck before signing,
outbox dispatch and every retry; a completed result cannot be delivered after its
view becomes ineligible merely because it was signed earlier. Source pins may be
released after immutable result construction only if the stamp and all authority/
deadline validation state remain available. Expired work fails without implicit
fresh-query recomputation; undispatched obligations are cancelled and the existing
paid-job refund rules apply. A future subscription batch obtains a fresh admitted
view; it cannot replay an expired ordinary batch as newly eligible output.

Payment quoting admits only a view whose declared safe lifetime can cover the
quote/execution plan, otherwise refuses or uses an explicitly supported shorter
quote. Payment never extends temporal eligibility. Test expiration before signing,
between signing and sending, before a retry and during quote/settlement; inspect
the actual socket output and refund state rather than only the query result.

**Snapshot acceptance:** with no new ingestion, expire an event between page 1,
COUNT and page 2; old-context operations fail, fresh operations agree, and the
archive still verifies the original. Repeat with a point lookup, full-body
hydration, search, NEG, slow output and clock anomalies. A frozen input-cut-only
cache must fail this fixture. Compare a temporal rebuild with incremental expiry
at equal stamps. Measure completion/restart rates under dense expiry and broad
queries: a correct refusal is not a claim of useful availability.

The traversal default was selected after the architecture challenge: restarting
an entire human browsing session for one unrelated expiration is unnecessarily
costly. Snapshot semantics remain useful where reproducibility is part of the
operation. Neither mode is relabeled as the other for a performance comparison.

## Exact private historical selection

The private native `query-event` selects one event ID under a qualified scope,
ordered source prefixes and an exact retained snapshot. A successful result is
either the original verified signed event or scoped absence: no eligibility
matched that selection. Select eligibility before the shared body. Finding
eligibility but no payload within the same cut is inconsistent, never absence.
Duplicate references, corrupt matching facts, failed native work, interrupted
transactions and unreadable required objects cannot become an empty result.
This does not certify unrelated archive objects or authorize a principal. The
read consumer separately owns the complete certified cut, pin and action grant.

`bigbrotr-native-event-query/1` contains `snapshot_id`, `request_digest` and a
closed `result`: `{"status":"absent"}` or `{"status":"present","matched":...}`.
The match contains snapshot, signed event and its two immutable fact references.
Emit either result only after the read transaction commits. The request SHA-256
preimage is `bigbrotr/archive-event-query/1\0`, dataset bytes16, event ID bytes32,
scope dataset bytes16, kind u16, allocation domain bytes16, ordinal u64,
topology u64, snapshot u64, bound count u64, then ordered
`(shard u32, epoch u64, prefix u64)` tuples; integers are big-endian.
[The frozen absence vector](../../tests/fixtures/protocol/archive-event-absence.json)
uses dataset01, event02, scope domain03/ordinal1, topology0, snapshot2 and the
single explicit zero bound `(0,1,0)`. The parent checks correlation for both
outcomes and independently verifies every returned signed event and source bound.
It retains the allocation grant and owned reader custody even for absence, until
the last consumer releases the observation. Positive native termination precedes
construction of either observation. Output has a2KiB envelope allowance above the
configured event-row bound, covered by the existing conservative allocation floor.

This replaces the earlier unversioned private match response. Parent and worker
are installed as one exact artifact pair; mixed versions refuse decoding and
cannot reinterpret an old native error as absence. No persisted format, migration
or public API version changes. Scanned-byte admission and the actual authenticated
HTTP consumer remain P06-T01/T02 work; these private results alone do not qualify
the common64MiB profile.

## Visibility allocation before a private grant

A submitted `QualifiedRef` is a name, not evidence that the visibility mapping
exists. Before activating a scope-qualified history grant, the management consumer
resolves that exact mapping from the selected certified cut and retained native
snapshot. The class label, including `public`, grants no permission. No event body
is needed merely to establish the mapping, and no active grant may wait for a
future allocation to make an otherwise nonexistent reference usable.

The private native `read-visibility` boundary selects the complete qualified
reference inside explicit ordered source prefixes. It first requires exactly one
complete mapping value, allowing equal repetitions at distinct source IDs as the
certified cut does. It selects the lowest `(shard, epoch, sequence)` occurrence,
then hydrates by source ID alone in the same transaction. That final lookup
rejects multiple rows, including a different reference that reused the source ID.
Validate the full fact envelope/digest, schema and descriptor versions, visibility
class syntax and all inactive descriptor fields. The parent independently verifies
the canonical fact, complete reference, source membership, snapshot and request
binding before constructing its opaque allocated-scope observation. It retains
its memory reservation and actual catalog custody through the last consumer.
This is allocation evidence; certification of the complete cut, cross-family
source uniqueness, the live retention pin and principal/action authority remain
separate prerequisites checked by the management/read consumer.

The private wire is closed. `VisibilityRead` carries dataset, qualified scope,
topology generation, ordered `CutBound` entries and an exact native snapshot ID.
It shares the1024-stream structural ceiling; the complete credential-bearing
request must also fit the existing32KiB private input bound before launch. Zero
prefixes are retained and exclude every record. SQL clamps each native row to4097 characters; Rust then refuses a rendered
row above4096 UTF-8 bytes. The worker's wider engine/native envelope
still covers construction. The result contains at most512 canonical fact bytes
encoded as lowercase hex, with a1536-byte complete response bound. These fixed
limits cover a visibility descriptor's sole64-byte ASCII class, not arbitrary
dictionary variants. They are allocation limits, not a measured process RSS cap.

Response format `bigbrotr-native-visibility/1` binds `snapshot_id`, `fact_digest`,
`preimage_hex` and `request_digest`. The request SHA-256 preimage is
`bigbrotr/archive-visibility-read/1\0`, dataset bytes16, scope dataset bytes16,
kind u16, allocation domain bytes16, ordinal u64, topology u64, snapshot u64,
bound count u64, then each ordered `(shard u32, epoch u64, prefix u64)`;
all integers are big-endian. The independent vector is
[archive-visibility.json](../../tests/fixtures/protocol/archive-visibility.json).
The digest correlates a response; it authenticates neither a user nor a cut.

Missing/corrupt objects, ambiguous mappings, absent streams, unavailable engines
or expired/cancelled work yield no allocated-scope result. No fallback to the
latest snapshot or conversion of missing data into permission is allowed.
The current parent reports native failures as `ObservationUnknown`; public typed
absence/error outcomes and scanned-byte admission remain P06-T01/T02 work.
`LIMIT 2` bounds each result, not the scan. This private boundary does not qualify
the common64MiB read profile or advertise an HTTP endpoint.

## Search meaning after parsing

P06-T06 owns the selected `unicode16-word-v1` analyzer and NIP-50 grammar. Parsing
produces a matching AST and an ordered list of decoded searchable literals.
Unsupported ignorable extension tokens are removed before both matching and
ranking. Quotes and supported escape syntax are decoded; whitespace inside a
literal remains data. Unsupported structured API fields still fail validation.

Define `ranking_text` as the normalized literal strings joined by one U+0020
space, and `ranking_tokens` as their analyzed token sequences concatenated in
literal order. Do not normalize the raw syntax string as content. The relevance
tuple is: normalized whole-content equals `ranking_text`; its ordered tokens
contain the contiguous nonempty `ranking_tokens`; number of distinct ranking
tokens present; then authored time descending and event ID ascending. Matching
still follows the AST (including each required phrase), not this scoring tuple.
An empty analyzed literal is rejected rather than creating a vacuous phrase.

For `alpha "beta gamma" ignored:value`, matching requires alpha and the beta/gamma
phrase, while ranking uses `alpha beta gamma` and its three tokens. Adding an
ignored extension cannot alter relevance. Two phrases retain their internal
matching boundaries even though the ranking sequence concatenates their tokens.
Repeated terms remain in phrase order but count once in the distinct-term boost.
Internal normalized whitespace affects whole-string equality; token matching can
still be equal. The documented tuple is deterministic, not a measured optimum.

Canonical query/cursor/cache identity binds the matching AST, ranking values,
analyzer/Unicode-data version, grammar version, structured filters, resource and
view stamp. Equivalent ignored extensions do not change it. Do not declare two
syntactic forms equivalent unless their canonical matching and ranking meanings
are both equal. Run mixed terms/two phrases/escaped quotes/ignored extensions/
repeated terms/Unicode whitespace through HTTP and NIP-50; an independent literal
and tuple fixture catches ranking on raw syntax. Complete candidate coverage,
resource bounds and explicit refusal remain mandatory before top-K.
