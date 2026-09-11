# Read views, time and search identity

Status: selected contract for P06-T01–T11 and their P05/P07/P09 consumers, before implementation. Source identity
and current-state reduction remain in the data contracts. This specification
closes two independent gaps: time can change eligibility without new ingestion,
and search syntax is not the text that should determine relevance.

## A reproducible view with an explicit time boundary

`ReadViewStamp` contains `source_cut`, input/target generation IDs,
`visibility_as_of_us`, `temporal_generation`, `expiry_deadline_us` and
`policy_epoch`. The expiry deadline can be absent only when no eligible expiry
can affect that view within its maximum lifetime. It is distinct from the pin's
absolute lifetime and the short authorization lease. All body lookups, list,
count, search candidates/ranking and NEG membership for one admitted context use
the same stamp. Response metadata identifies it without leaking restricted IDs.

For ordinary reads, a pin is valid only until the earliest relevant scheduled
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
or incomplete expiry state makes the affected ordinary resource unavailable.
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

**Practical acceptance:** with no new ingestion, expire an event between page 1,
COUNT and page 2; old-context operations fail, fresh operations agree, and the
archive still verifies the original. Repeat with a point lookup, full-body
hydration, search, NEG, slow output and clock anomalies. A frozen input-cut-only
cache must fail this fixture. Compare a temporal rebuild with incremental expiry
at equal stamps. Measure completion/restart rates under dense expiry and broad
queries: a correct refusal is not a claim of useful availability.

**Alternative retained:** a separately versioned live traversal can accept changing
membership, but must abandon fixed-snapshot list/count equivalence and state its
pagination guarantees. It is not silently substituted into the baseline. If the
baseline repeatedly prevents useful ordinary reads, evaluate that alternative
with explicit client semantics before changing the contract.

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
