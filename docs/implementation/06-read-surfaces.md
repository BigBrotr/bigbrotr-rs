# Phase 06 — Query core, HTTP, ArchiveRelay, DVM and finite agent gateways

This plan defines the complete selected behavior. Partial evidence notes below
identify implemented and executed subsets; unchecked steps remain open.
Follow the [component contract](../specification/component-contracts.md),
[read design](../architecture/derived-read-publication.md),
[search/reconciliation design](../architecture/search-and-reconciliation.md),
[private administration](../product/private-administration.md) and
[data contract](../specification/data-contracts.md). The selected, complete
[DVM application profile](../specification/dvm-profile.md) governs T09–T11;
generic NIP-90 compatibility must not be substituted for that profile.
The selected [gateway contract](../specification/gateway-contracts.md) governs
optional MCP/ContextVM tasks T13/T14. ContextVM is a separate experimental MCP
transport; its bounded finite capability does not replace DVM delivery/payment
semantics. Both new adapters are disabled by default and require qualification.
The [Go read walkthrough](../research/go-read-runtime-code-walkthrough.md)
identifies implementation pitfalls without certifying future Rust behavior.

All named implementation files and commands are proposed outputs. The application
owns query admission/planning and engine adapters; pure wire codecs remain in
`bigbrotr-protocol`. No public endpoint accepts SQL, engine credentials, arbitrary
paths or executable expressions. API, relay, DVM, MCP and ContextVM use one admitted query
contract and deployment resource budget. Stopping acquisition or derivation
does not stop reads of retained compatible generations.
Exporter/importer user workflows and standalone artifacts are owned by P08-T04;
this phase supplies their authorized pinned reads and full-body contract. Export
jobs do not become unbounded public HTTP or DVM queries.

## Common dependencies and practical verification

P01-T02/T03 supply types and independent protocol verification, T04 config,
T05 budgets/lifecycle, T06 authority, T07 internal RPC and T08 the real-process
harness. P02-T05/T06/T07 provide immutable serving, frontiers and snapshot pins.
P05 supplies optional derived resources. P07-T01–T04 signing/outbox primitives
depend only on foundation/storage and may run before DVM tasks; this is a task
dependency, not a phase-wide P06↔P07 cycle.

For every numbered task run the future case:

```sh
python3 tests/acceptance/run.py p06-tNN --binary dist/bigbrotr --compose packaging/compose/acceptance.yaml --evidence .research/implementation/p06-tNN
```

Replace `NN` with its task number. Cases live in `tests/acceptance/cases/` and
must launch the actual packaged listener/CLI and real PG/CH plus controlled
external HTTP/WebSocket clients. The driver records commands, fixture and release
digests, engine versions, captures, database frontiers, policy epochs, process
exits, resource measurements and the independent expected result. Listener roles
reject `--once`; finite jobs use their own bounded job entrypoints. Setup is
outside measured intervals. Missing infrastructure or an unimplemented case is
`NOT_RUN`, never a green empty response. Each negative control runs a named wrong
interpretation against the oracle without mutating production source.

Common initial read bounds: page 100/max 1000; eight filters, 100 values per field;
request 64 KiB; deadline 15 s; scan 256 MiB; memory 64 MiB; concurrency 16/queue 64;
finite output 16 MiB; cursor 900 s plus 30 s clock allowance; public rate 5/s with
burst ten. Per-principal and process reservations compose with global budgets,
including CH execution and cancellation. Five-second authorization leases are
separate from a 60-second metadata cache. Restrictive changes invalidate old
authority through the desired/applied activation barrier. PG outage cannot
extend a lease. Per-task protocol bounds can be smaller.

The first cold private historical reader is an explicit native profile, not
qualification of the common64MiB memory envelope. Its complete512MiB worker grant,
parent decode/output and credential/transport overlap must fit the configured
per-read allowance. The common default refuses capacity; the tiny qualification
fixture deliberately uses1GiB. See [the initial profile](../user/api/history.md).
Conservative physical-input admission counts two complete file passes, including
metadata-versus-actual length checks; aged/mixed workload and OS amplification
qualification remain required. No task checkbox is closed by this subset.

## P06-T01 — Resource registry, typed admission and shared authority

**Dependencies:** P01-T02/T04–T08; P02-T05/T06/T07; P05-T01 for derived manifests.

**Proposed files:** `crates/bigbrotr-model/src/read.rs`;
`apps/bigbrotr/src/read/{registry,admission,policy,budget}.rs`;
`contracts/read/v1/{resource,request,response}.schema.json`;
`config/examples/read.toml`; `docs/user/api/resources.md`;
`apps/bigbrotr/tests/read_admission.rs`;
`tests/fixtures/read/resources.json`; `tests/acceptance/cases/read_admission.py`.

**Boundary:** `QueryRequest + Principal` produces one owned admitted context with
`VisibilityScope`, `PolicyEpoch`, immutable source/target pins, resource version,
typed filters, exactness mode, reservation and cancellation. No adapter can
construct it by copying a claimed principal from user JSON. Resource entries
declare row unit, fields, filters/sorts, history policy, counts/search support,
freshness, cursor compatibility and unavailable states.

Canonical resource IDs/routes and row units are selected in the
[interface catalog](../specification/interface-catalog.md). Implement that
registry rather than exposing internal table names. Every admitted context carries
the [read-view stamp](../specification/read-view-contract.md), including time-based
eligibility, not just source and target generations.

- [ ] **Step 1:** register events/current/history, provenance, profiles/lists,
  event/social statistics, measurements/claims, relay facts/coverage/availability/
  overlap, graph scores and assertion lineage. Missing provenance/ranker/search
  capability is explicit; resources do not disappear ambiguously or return zero.
- [ ] **Step 2:** resolve principal grants and visibility before planning, then
  intersect typed constraints. Represent impossible intersections as `MatchNone`;
  never drop an emptied author constraint and accidentally broaden a query.
- [ ] **Step 3:** acquire compatible source manifests and budget as one admitted
  context. Bind public aggregate resources to public-input generations; filtering
  final rows cannot remove private data's earlier influence on a graph or score.
- [ ] **Step 4:** propagate deadlines, cancellation and authorization expiry
  through RPC, engine query and output delivery. Enforce restrictive policy at
  each new output chunk and retire a noncooperative worker under bounded drain.
- [ ] **Step 5:** generate capability/config documentation from the actual
  registry, including stale-but-usable versus unavailable/corrupt states.
- [ ] **Step 6:** run packaged concurrent HTTP/WS requests under two principals,
  revoke one grant with notifications dropped, partition PG and inspect the
  requested scans and bytes delivered after authority expiry.

**Oracle and negative control:** a static resource/permission matrix defines
permitted results. Restricted graph input must affect neither public scores nor
public corpus statistics. Compare a private-data-added run with the same public
generation; row-filter-only authorization must fail. Requests without a grant
cannot become admissible merely by changing transport.

## P06-T02 — Exact event queries, historical policy and full bodies

**Dependencies:** P06-T01; P02-T03/T05/T06/T07; P05-T02.

**Proposed files:** `crates/bigbrotr-protocol/src/filter.rs`;
`apps/bigbrotr/src/read/{events,planner,body,current_state}.rs`;
`contracts/read/v1/filter.schema.json`; `config/examples/read.toml`;
`docs/user/api/event-queries.md`; `apps/bigbrotr/tests/event_queries.rs`;
`tests/fixtures/read/event_queries.json`; `tests/acceptance/cases/event_queries.py`.

**Boundary:** one admitted event filter plan yields complete original signed
events plus mode/cut metadata, or an explicit typed refusal. Snapshot mode uses
one compatible data/eligibility stamp. Traversal pins candidate IDs/tags/bodies
at its acquisition cut and applies the separately reported newer current
eligibility cut, as the read-view contract specifies; do not relabel it a snapshot.
Normal visibility includes deletion/expiry rules; authorized immutable history
is an explicit resource. An ID lookup is not an archive-policy bypass.

- [ ] **Step 1:** implement AND across fields, OR within a field and distinct
  union across filters, exact case-sensitive tags/IDs and inclusive Nostr time
  bounds over the full accepted timestamp domain. Preserve absent versus zero
  limit and define strict supported filter shapes at the adapter boundary.
- [ ] **Step 2:** authorize the source population before winner/deletion
  reduction, then apply ordinary expiry/query predicates and deterministic `created_at DESC,event_id ASC` keysets and limits.
  Consult indexed logical deletion/expiry state, not a global tombstone scan
  reloaded for every page. Expiry changes visibility without a new ingestion cut.
  Build the ordered expiry state/partition summaries and deadline checks specified
  by the read-view contract; no global per-query expiry scan is permitted.
- [ ] **Step 3:** implement projection selection and full-body hydration with
  independent canonical hash/signature validation where crossing an untrusted
  boundary. Never reconstruct a signed event from lossy indexed fields.
  Build byte-bounded pages from whole rows. If a later row would exceed the
  transport's finite-output allowance, return the complete preceding rows and a
  continuation at the correct scan position; do not consume the unreturned row.
  If the first eligible row cannot fit, refuse explicitly. Report effective page
  size without changing the normalized query/cursor contract mid-pagination.
  The [private archive hydration prerequisite](../reference/archive-format.md#exact-historical-signed-event-hydration)
  reads an exact payload/eligibility pair with complete digest and signature
  verification. [Native qualification](evidence/p02-archive-event-native-20260920.json)
  at `3f29952` passes 20 connected cases and seven native cases; 50 parent tests
  pass, and the default skipped native-holder case is explicitly executed and
  passes against the same packaged worker. It supplies neither source admission,
  current-view reduction, installed pins nor public/history authority. This step
  and the first-event milestone remain incomplete.
  The private consumer selects an exact retained native snapshot and refuses
  a missing/expired selection without latest-view fallback. [Native qualification](evidence/p02-archive-snapshot-native-20260920.json)
  at `e1852b8` passes31 connected cases and nine native tests, including actual
  latest4/exact3 reads before and after PG crash. A numeric selector is not a
  retention or authorization lease.
  The private event-ID lookup now resolves eligibility in one exact qualified
  scope and both facts inside explicit source bounds at the retained native
  snapshot, with parent membership/signature checks. [Native qualification](evidence/p02-archive-event-query-native-20260920.json)
  at `eceab88` passes 41 connected cases, 11 native tests, 56 parent tests and the
  explicitly executed native-holder test; Mac 11 native/2 protocol/1 parent tests
  and Clippy pass. It does not certify the caller's topology or supply live action/scope
  authorization, scanned-byte admission or public missing/unavailable semantics;
  those remain required by steps2/4/5 and P06-T01.
  [Completed scoped absence and owned query custody](evidence/p06-event-query-absence-native-20260923.json)
  at882654e pass51 practical cases,11 native/2 protocol/2 parent tests and scoped
  Clippy. The result is correlated to the exact selection and keeps local custody
  for either outcome; required eligibility/payload object failures and a blocked
  empty query refuse. Three PG crashes/restarts preserve compared metadata and27
  original Parquet hashes. This closes the private typed-absence prerequisite,
  not the public error API, scanned-byte budget or authenticated history delivery.
- [ ] **Step 4:** distinguish temporarily unavailable, corrupt, absent and
  policy-withheld bodies. Enforce bounded cold lookup and scan cost. Return
  resource unavailability or the documented policy result, not a partial body
  or silent raw-history fallback.
- [ ] **Step 5:** expose explicit history grants and snapshots; retain originals
  after kind-5 deletion while normal reads stop publishing matching-author
  targets. A prior authorized response is not permanent access authority.
- [ ] **Step 6:** run packaged multi-filter, duplicate-tag, extreme-time,
  replacement, empty-list, deletion and body-corruption fixtures through CH and
  archive fallback with Indexer stopped and then resumed.

**Oracle and negative control:** an independent full-fixture evaluator sorts
the authorized source population, selects winners, then applies ordinary
expiry/deletion/query predicates. Private inputs cannot affect a public winner. A newer
nonmatching replacement must prevent a matching obsolete event from resurfacing.
A reverified archive export contains both, while the normal relay response
contains neither. Corrupt one content byte with an unchanged cached ID and require
verification failure rather than trusting a known event ID.

## P06-T03 — Durable encrypted keyset cursors and generation pins

**Dependencies:** P06-T01/T02; P01-T04/T06; P02-T07.

Step 1's protected shared keyring/AEAD foundation may start with P08-T03's first
operational job-list consumer after the P01 configuration/authority foundations.
Scientific scan positions, generation pins and full acceptance still require the
P06/P02 prerequisites above. Reuse the same key ownership and envelope, without
inventing scientific pins for redacted operational metadata.

Current partial implementation prepares/inspects protected local key bytes through
`access cursor-keyring` and the optional deployment root `read.cursor.keyring_ref`.
It has independent frozen keyfile fixtures, bounded descriptor reads, cooperative
locking, identical retry and first-publication no-replace. The
[local report](evidence/p06-cursor-keyring-local-20260925.json) records50selected
Mac consumer tests, the collision RED/GREEN and review boundaries. The
[native report](evidence/p06-cursor-keyring-native-20260925.json) qualifies15local
keyfile scenarios at7acfd32 with unchanged1f251db ELF, including sync failures,
four SIGKILL windows, late collision and blocked stdout. The original observer
failure remains INCOMPLETE in its separate report. The bounded product envelope
now binds version/key ID, dataset, stable keyring and resource. The
[local envelope report](evidence/p06-cursor-envelope-local-20260925.json) records
exact libsodium fixtures,4081byte mutations, retained16KiB capacity and scoped
tests/Clippy. Control lifecycle,
cross-worker consumers and the rest of Step1 remain open; local bytes and a codec
do not activate issuance or complete this step.

Initial Control registration has [local checks](evidence/p06-cursor-registration-local-20260927.json): additive0601, an
existing-only synchronized key snapshot and the actual `access cursor-keyring
register` consumer. It records an immutable complete key set and installation
audit; it cannot activate issuance. The [native registration report](evidence/p06-cursor-registration-native-20260927.json)
qualifies atomicity, concurrent same/conflicting retries, COMMIT-loss resolution,
sync refusal before SQL, blocked output and engine restart at3ba3ceb. Current
lifecycle/activation/rotation and all remaining Step1 acceptance stay open.
The next local addition command, `prepare-key --key-id`, now passes23selected
Mac keyring/registration/codec/CLI tests and Clippy. A caller-stable public ID
makes a lost-response retry preserve the same material; existing keys and ring
identity remain unchanged. The [native addition report](evidence/p06-cursor-key-addition-native-20260927.json)
qualifies24Rust tests and13actual CLI cases at e684915, including EIO/ENOSPC,
three SIGKILL windows, forced locking and a full output pipe. Control enrollment/
readiness/activation/retirement remain required work.
The next bounded prerequisite is explicit additional-key enrollment with an
actual CLI, additive0602, permanent ID/material bindings and original audit retry.
The [local enrollment report](evidence/p06-cursor-enrollment-local-20260927.json)
records48selected tests/Clippy after schema and actual CLI RED. The
[native enrollment report](evidence/p06-cursor-enrollment-native-20260927.json)
qualifies155checks across12PostgreSQL databases atc6595c8 with unchanged9b02ELF:
upgrade/fresh registration, material/audit closure, stable receipts, file changes,
conflicting material, privilege rollback, both COMMIT-loss sides, actual dataset
lock contention, killed pre-COMMIT client, blocked output/fsync refusal and restart.
The [original network setup refusal](evidence/p06-cursor-enrollment-native-incomplete-20260927.json)
and [fixture spacing failure after106checks](evidence/p06-cursor-enrollment-fixture-incomplete-20260927.json)
retain their separate INCOMPLETE outcomes. SQL0602 has been applied and is
immutable. Actual runtime readiness/activation/retirement/revocation and the
remaining Step1/3 consumer acceptance stay open; enrollment grants no issuance.

The actual `serve` consumer now loads an explicitly configured keyring, retains
its shared32KiB reservation through driver join, and checks immutable0602material
bindings under the current Control fence with post-wait lease validation. No DDL
or issuing authority is added. The
[local runtime report](evidence/p06-cursor-runtime-local-20260927.json) records
27selected Rust tests/Clippy and22wire-helper tests. The
[native runtime report](evidence/p06-cursor-runtime-native-20260927.json) records
54Rust tests/Clippy and117checks across9actual PostgreSQL databases at6474cd3:
startup/restart/additional/subset/reordered/16key snapshots, material/ring/schema/
privilege refusals, absent-root compatibility, actual dataset lock contention,
server-observed lease expiry, cancellation/deadline and same-byte recovery.
A retained prior executable is the negative control for mismatched material.
Runtime loading does not close activation, retirement/revocation or full readiness.



**Proposed files:** `apps/bigbrotr/src/read/{cursor,keyring,pins}.rs`;
`migrations/postgres/0603_read_pins.sql`;
`contracts/read/v1/cursor-envelope.schema.json`; `config/examples/read.toml`;
`docs/user/api/pagination.md`; `apps/bigbrotr/tests/read_cursors.rs`;
`tests/fixtures/read/cursors.json`; `tests/acceptance/cases/read_cursors.py`.

**Boundary:** an admitted context and true scan position produce an opaque
authenticated/encrypted cursor. It binds deployment, resource version, principal,
normalized query/projection/sort, policy, source/target generations, issuance and
expiry. It must not reveal withheld IDs or query text through base64 decoding.

Bind mode, acquisition cut and `ReadViewStamp` in the encrypted envelope and PG
pin. In `snapshot` mode the earlier relevant temporal deadline yields 409
`temporal_snapshot_expired`, independently of the 900-second absolute lifetime.
Ordinary chronological `traversal` instead rechecks current eligibility at each
page/chunk, preserves its candidate cut and advances the true scanned key; it
promises no duplicate IDs, not immutable membership or list/count equivalence.
P06-T03 acceptance covers both modes, expiry/replacement/private-input cases,
slow clients and scan-only pages. Record useful completion/refusal/restart rates.

- [ ] **Step 1:** implement a persistent shared keyring with an AEAD profile,
  key ID/version and associated-data binding. Initial selected primitive is
  XChaCha20-Poly1305 with fresh OS-generated 192-bit nonces; no deterministic
  nonce, plaintext MAC-only payload or per-worker ephemeral key.
- [ ] **Step 2:** encode the true scan position, including privately withheld
  candidates, and pin the exact generation. A page with no returned rows can
  still advance safely; the last delivered event is not always the scan cursor.
- [ ] **Step 3:** implement 900-second expiry, 30-second allowance, cross-worker
  lookup and normal key rotation that decrypts old cursors until their lifetime
  ends. Emergency rotation explicitly invalidates affected cursors.
  Pages share one query pin and its original absolute expiry; fetching another
  page cannot slide the pin indefinitely. Admit at most 4096 active cursor query
  pins initially, plus the deployment's retained-artifact byte allowance, before
  issuing a token. Unused expired pins are reclaimed under P02-T07 ownership.
- [ ] **Step 4:** implement bad-token, expired/reclaimed and incompatible-policy/
  resource errors without silently switching to latest data. Recheck current
  restrictions even when the encrypted cursor was once authorized.
- [ ] **Step 5:** run pages across two real workers, process restart, input
  mutation, deletion, normal/emergency rotation and forced pin reclamation;
  inspect exact expected membership and error status.
  Follow the server's actual cursor after row-count clamping, byte-bounded pages,
  an all-withheld scan chunk and scope reduction. The token the server emitted
  must be acceptable under its own documented continuation parameters.

**Oracle and negative control:** an independent fixed snapshot supplies complete
ordered IDs, including hidden rows between visible rows. Every authorized ID
appears once across pages. Swap a cursor's caller/resource, alter ciphertext or
replay under restricted policy and require failure. A deliberately process-local
key configuration must fail the cross-worker fixture, not merely a unit decode.

## P06-T04 — Exact counts and explicitly approximate protocol estimates

**Dependencies:** P06-T01/T02; P05-T03/T04 for derived count resources.

**Proposed files:** `apps/bigbrotr/src/read/count.rs`;
`crates/bigbrotr-protocol/src/nip45.rs`;
`contracts/read/v1/count.schema.json`; `config/examples/read.toml`;
`docs/user/api/counts.md`; `apps/bigbrotr/tests/read_counts.rs`;
`tests/fixtures/read/counts.json`; `tests/acceptance/cases/read_counts.py`.

**Boundary:** a count request names its resource, visible row unit and
`exact|auto` mode. Exact distinct-union counts either complete within their budget
or refuse. Auto may use only a supported named estimator and must report method,
scope and approximation metadata. Approximation is not a hidden error fallback.

- [ ] **Step 1:** implement exact distinct unions with the same filter/current/
  visibility plan as rows. Separate event, actor, relay, edge and observation
  units; do not sum overlapping filter counts or daily distinct aggregates.
- [ ] **Step 2:** define count-capable resource plans and cost admission before
  executing expensive scans. Keep count limits independent of returned page
  length; row pagination limit is not the population definition.
- [ ] **Step 3:** implement NIP-45 COUNT framing and only supported current HLL
  bindings from the pinned protocol fixtures. Its pubkey-based sketch cannot
  stand in for event-ID interaction counts. Unsupported approximate shapes
  return the documented refusal or bounded exact alternative.
- [ ] **Step 4:** encode large exact API counts losslessly and include source
  generation/exactness; generate NIP-11/capability claims only after tested
  protocol paths are actually enabled.
- [ ] **Step 5:** run packaged HTTP COUNT and relay COUNT against the same fixture,
  including overlapping filters, many events by one author, private rows,
  replacement/deletion and a forced exact-scan budget refusal.

**Oracle and negative control:** independent ID and pubkey sets yield deliberately
different expected counts. Reusing a pubkey HLL for event reactions must fail.
The exact API cannot return an approximate value after timeout. If an estimator
is supported, compare its wire bytes to an independent reference and its error
to measured truth; a claim of exactness must not be inferred from close values.

## P06-T05 — HTTP codec, OpenAPI, token policy and external errors

**Dependencies:** P06-T01–T04; P01-T04/T06/T07. Initial configured hashed-token
and explicit anonymous-public policy use foundation credential types. P08 adds
runtime token management later; this task does not depend on that later UI/API.

**Proposed files:** `apps/bigbrotr/src/interfaces/http/{router,codec,auth,errors}.rs`;
`contracts/http/v1/openapi.yaml`; `config/examples/api.toml`;
`docs/user/api/{quickstart,authentication,errors}.md`;
`apps/bigbrotr/tests/http_api.rs`; `tests/fixtures/read/http_codec.json`;
`tests/acceptance/cases/http_api.py`.

**Boundary:** collection GET and read-only POST routes `/v1/events`,
`/v1/resources/{resource}` and `/v1/resources/{resource}/count` decode to the same
typed read request. Point `/v1/events/{id}` and metadata `/v1/meta` are GET-only.
The [interface catalog](../specification/interface-catalog.md) owns the full
resource registry and P08 artifact/context byte routes. GET uses one percent-encoded JSON filter
parameter plus scalar page/cursor controls; POST uses the versioned JSON envelope.
`GET /v1/capabilities` is the explicit capability descriptor endpoint consumed by
the dashboard; `/v1/meta` contains instance/version and links to that descriptor.
Both derive enabled resource/feature state from the same registry.

- [ ] **Step 1:** implement strict UTF-8/percent decoding, duplicate recognized
  parameter rejection, request/depth limits and lossless array/tag strings.
  Preserve commas, empty values, plus signs, Unicode and whitespace; never split
  arrays on commas or coerce unknown fields into absent filters.
- [ ] **Step 2:** implement scoped hashed bearer tokens, explicit anonymous public
  policy and per-principal limits. Listener defaults to loopback; public binding
  requires explicit configuration. CORS defaults absent. Forwarded client IPs
  are honored only from configured trusted proxy CIDRs.
  Initial bootstrap consumes configured token hashes and never depends on the
  future administration listener being operational. Do not expose a bootstrap
  token-creation endpoint on the public API.
- [ ] **Step 3:** map invalid/auth/forbidden/rate/unavailable/deadline errors to
  400/401/403/429/503/504, expired cursors to 410, incompatible cursors to 409 and
  restricted point lookups to the non-disclosing 404 policy. Include bounded
  machine codes and Retry-After where meaningful, without internal SQL/paths.
- [ ] **Step 4:** generate/validate OpenAPI against executable codecs, including
  exactness, stale metadata, full signed event numbers and lossless large fact
  integers. Add documented curl and generated-client examples.
  Exercise `/v1/capabilities` from an actual generated/dashboard client; stale
  disabled features cannot remain enabled merely because its page assets cached.
- [ ] **Step 5:** run an independent HTTP client against the packaged binary via
  direct and proxy paths; compare GET/POST equivalence, auth revocation, error
  status/body, CORS and cancellation. Inspect actual engine queries and limits.

**Oracle and negative control:** a static hostile/valid encoding matrix includes
`a,b`, `a+b`, empty tags and repeated singleton parameters. A comma-split decoder
must disagree with the oracle. Spoofed X-Forwarded-For from an untrusted peer
cannot evade the public rate limit. A disabled resource must not appear enabled
in generated metadata or return a misleading successful empty list.

## P06-T06 — Owned search index, stable matching and NIP-50 adapter

**Dependencies:** P06-T01–T03; P02-T05/T06/T07; P05-T02.

**Proposed files:** `apps/bigbrotr/src/storage/indexer/search.rs`;
`apps/bigbrotr/src/read/search.rs`;
`crates/bigbrotr-protocol/src/nip50.rs`;
`migrations/clickhouse/0606_search_projection.sql`;
`contracts/read/v1/search.schema.json`; `config/examples/search.toml`;
`docs/user/api/search.md`; `apps/bigbrotr/tests/search.rs`;
`tests/fixtures/read/search.json`; `tests/acceptance/cases/search.py`.

**Boundary:** Indexer-owned complete search generations cover eligible retained
content under a named analyzer/source/visibility policy. Read adapters submit
bounded search plans; a public request cannot trigger a whole index build.
ClickHouse is the selected baseline; no mandatory second search engine.

**Selected analyzer `unicode16-word-v1`:** apply Unicode 16.0 NFKC, full default
non-Turkic case folding, then NFKC again. Use Unicode 16 default UAX-29 word
boundaries; retain word segments containing letters or numbers. For segments
without them, retain each extended grapheme cluster containing an
Extended_Pictographic character as an emoji token; discard remaining punctuation
and whitespace. Preserve token order and duplicates for phrases, and store a
distinct token set for candidates. CJK follows the default word boundaries with
no dictionary segmentation; language-specific relevance is outside this profile.
Whole-content equality uses the normalized full string, with no extra trim.
This is BigBrotr's selected algorithm, informed by the primary
[Unicode 16 case-fold data](https://www.unicode.org/Public/16.0.0/ucd/CaseFolding.txt)
and [text segmentation specification](https://www.unicode.org/reports/tr29/tr29-45.html),
not a claim that CH's default tokenizer implements it.

Perform normalization/tokenization in the shared application analyzer used by
Indexer and queries. Select CH `Array(String)` token indexing for complete
candidates, with ordered tokens retained for bounded exact phrase/relevance
verification before top-K. Current [CH documentation](https://clickhouse.com/docs/reference/engines/table-engines/mergetree-family/textindexes)
documents array token indexes and has/hasAny/hasAll acceleration; it does not make
array phrase acceleration a baseline guarantee. Verify candidates under the
shared scan/time bound or refuse; optional native phrase acceleration must match
this oracle. Account for ordered tokens, distinct terms/postings, normalization
bytes and rebuild overlap in storage/memory measurements.

**Selected NIP-50 grammar:** whitespace-separated unquoted terms and double-quoted
phrases; backslash escapes only quote/backslash inside quotes. Require every term
and every phrase (AND), with phrase token order contiguous. Empty/unclosed quotes
or unsupported escape syntax are invalid. Ignore syntactically valid unquoted
`key:value` extension tokens whose key is unsupported; they do not become normal
terms. A query left with no searchable terms/phrases is invalid. API all/any/phrase
modes remain explicit; its structured options are never ignored. This grammar
is a versioned application mapping, not a full engine query language.

The [parsed search identity](../specification/read-view-contract.md#search-meaning-after-parsing)
defines exact mixed-literal `ranking_text`, `ranking_tokens`, matching AST and
cursor/cache identity. Ignored textual extensions and quote syntax do not become
ranked content. Include its independent mixed-term/phrase fixtures in Step 6.

- [ ] **Step 1:** implement the selected Unicode token/normalization profile in
  `apps/bigbrotr/src/read/search_analyzer.rs`, with
  an independently specified corpus covering Latin, CJK, combining marks, emoji,
  URLs and mixed scripts. Prove the actual pinned CH functions preserve those
  semantics through the pretokenized projection; an unsupported analyzer/engine
  pair is not silently approximated. Include the normalizer/Unicode-data digest
  in index/query compatibility and rebuild tests.
- [ ] **Step 2:** implement all-terms, any-term and phrase matching; plain NIP-50
  text selects all terms and quoted text selects phrases. Unknown search-text
  extensions are ignored as the pinned NIP requires, while malformed structured
  fields are rejected. Freeze grammar and mixed phrase/term precedence in fixtures.
- [ ] **Step 3:** implement deterministic relevance ordering: normalized whole
  content match against parsed `ranking_text`, contiguous `ranking_tokens` match,
  distinct matching term count, then
  authored time descending and event ID ascending. Select top K from the complete
  eligible candidate scope or refuse; never rerank only the first recent page.
- [ ] **Step 4:** build and verify new generations, switch atomically and retain
  old pinned generations. Track disabled/building/ready/lagging/failed/unsupported;
  widen scope only after backfill. Apply policy restrictions before public corpus
  statistics or invalidate affected generations if they use such statistics.
- [ ] **Step 5:** enforce query 4096 bytes/32 terms plus shared scan/time/memory
  bounds, full signed body hydration and search-bound cursors. Keep stemming,
  fuzzy, embeddings and BM25 changes out of this initial profile unless separately
  specified and measured through the optional-engine decision gate.
- [ ] **Step 6:** run HTTP and NIP-50 against actual CH with a high-relevance old
  event beyond the first recent page, decomposed Unicode, a private corpus
  addition, index rebuild, restart and scope restriction.

**Oracle and negative control:** a separate small Unicode matcher and static
ranking tuples determine expected IDs/order. Prefix-candidate reranking must
miss the planted old event and fail. Token-only matching must fail the phrase
fixture. Compare incremental indexing to complete rebuilt search at a common cut;
record query plan/bytes so an accidental whole-table scan is visible.

## P06-T07 — Read-only ArchiveRelay and atomic history/live handoff

**Dependencies:** P06-T01–T04/T06; P01-T03/T05/T06; P02-T05/T06; P05-T02.

**Proposed files:** `apps/bigbrotr/src/interfaces/relay/{server,session,subscription,live,nip11}.rs`;
`crates/bigbrotr-protocol/src/relay_frames.rs`;
`config/examples/archive-relay.toml`; `docs/user/relay/archive-relay.md`;
`apps/bigbrotr/tests/archive_relay.rs`; `tests/fixtures/read/relay.json`;
`tests/acceptance/cases/archive_relay.py`.

**Boundary:** REQ/CLOSE/COUNT own independent session/subscription generations.
Historical reads have per-filter limits before distinct union. A pinned historical
cut and ingestion-feed frontier form one handoff; EOSE belongs only to the still
active successful subscription generation. Normal relay publication honors
logical deletion/expiry while the historical archive retains all signed bytes.

- [ ] **Step 1:** implement strict bounded frames and independently verified
  NIP-42 authentication where configured. Inbound EVENT always returns a read-only
  rejection; authentication never grants implicit write authority.
- [ ] **Step 2:** implement per-filter historical limits, union/dedup and
  replacement of the same subscription ID. Cancel the previous generation and
  prevent its delayed rows/EOSE from reaching the new subscription.
- [ ] **Step 3:** implement atomic snapshot/live handoff including newly acquired
  backdated events. `limit:0` returns no history, emits EOSE after successful
  setup and still receives matching live events. COUNT remains a finite request,
  not a stored subscription or live feed.
- [ ] **Step 4:** bound 128 connections, eight subscriptions per connection,
  64-KiB frames, subscription IDs at most 64 Unicode scalar values/256 UTF-8
  bytes with the shared conservative envelope allowance, 1-MiB per-client output
  buffer, ten-second slow-client deadline,
  900-second feed retention and 3600-second subscription lifetime. Enforce global
  byte permits and refuse/close overflow rather than blocking acquisition.
  Measure complete escaped EVENT frames against the configured wire and buffer
  bounds. A single eligible signed event that cannot fit causes an explicit
  oversized-result CLOSED/refusal (before successful historical EOSE), never
  partial signed fields, silent omission or an unbounded one-row exception.
- [ ] **Step 5:** implement feed exhaustion, policy revocation and source failure
  as explicit CLOSED/NOTICE outcomes. Cancellation must stop or isolate the actual
  engine query; a cancelled wrapper around an unbounded worker is insufficient.
- [ ] **Step 6:** generate truthful NIP-11 support/limitations and run a separate
  real WebSocket client through EOSE races, backfill, duplicate filters, COUNT,
  read-only EVENT, client replacement, slow output and reconnect.

**Oracle and negative control:** an external client records source acquisition
sequence alongside received IDs and expects each handoff event once. Insert an
event between snapshot and feed registration to detect a gap; reuse a subscription
ID while the old query is blocked to detect stale EOSE. A fake writable NIP-11
claim fails the real EVENT check. Stopping Synchronizer must leave committed
historical reads available.

## P06-T08 — NIP-77 serving over an owned reusable set projection

**Dependencies:** P06-T01/T02/T07; P02-T05/T06/T07; P01-T03/T05/T07.

**Proposed files:** `apps/bigbrotr/src/storage/indexer/reconciliation.rs`;
`apps/bigbrotr/src/interfaces/relay/negentropy.rs`;
`crates/bigbrotr-protocol/src/nip77.rs`;
`contracts/read/v1/reconciliation-capabilities.schema.json`;
`config/examples/archive-relay.toml`; `docs/user/relay/reconciliation.md`;
`apps/bigbrotr/tests/nip77_server.rs`; `tests/fixtures/read/nip77.json`;
`tests/acceptance/cases/nip77_server.py`.

**Boundary:** a NEG session pins the visible, retrievable full-event set as
distinct `(created_at,event_id)` pairs ordered by time then ID. It is independent
of provenance collection. Admission-only IDs, claims, restricted/missing bodies
and non-visible replacements are not advertised as retrievable events. The shared
RelayRetrievable predicate also checks the actual escaped body/envelope and
maximum supported subscription-ID allowance; see the exact
[encoding contract](../architecture/search-and-reconciliation.md#relay-encoding-eligibility-and-usable-search-scope).

- [ ] **Step 1:** implement NEG-OPEN/MSG/CLOSE/ERR and the pinned exact timestamp/
  ID wire representation with independent reference fixtures. Keep NEG session
  IDs independent of REQ IDs; reopen replaces only the relevant NEG generation.
- [ ] **Step 2:** build reusable Indexer-owned supported-shape projections in a
  separate owned Rocks directory, reached through bounded application/RPC reads.
  No second writer opens the admission store and no public client triggers a
  billion-record projection build. Expiry/replacement/policy updates affect sets
  even with no new ordinary event arrival.
  Permanently materialize only configured shared shapes; arbitrary authorized
  filter combinations consume bounded temporary/cache state, not an enduring
  per-query index. Cache identity includes source/view/policy generation plus
  encoding, subscription-ID
  allowance and serving-limit revision. Invalidate sets after a limit reduction.
  Negotiate/fetch small, escaping-heavy and oversized stored bodies against the
  same real listener; every advertised ID must be deliverable within its bound.
- [ ] **Step 3:** allow bounded ad-hoc sets only up to 10,000 records. Broad shapes
  require a ready persistent projection; reject any filter limit including zero,
  search, or unsupported combinations rather than changing set semantics.
- [ ] **Step 4:** enforce two sessions/connection, 32 globally, TTL 60 s, idle
  15 s, frame 64 KiB including hex encoding, 64 rounds, session work/output 1 MiB
  and shared session memory 128 MiB. Release pins on close/expiry/revocation.
- [ ] **Step 5:** distinguish pinned comparison membership from later REQ
  retrieval: the latter rechecks current policy and is not an atomic export.
  Explain this limitation in capabilities/client documentation.
- [ ] **Step 6:** run an independently implemented Negentropy client over real
  WebSockets, then fetch differences through REQ and verify exact signed bodies;
  exercise duplicate IDs, extreme times, many sessions, updates during dialogue,
  projection rebuild and revoked source scope.

**Oracle and negative control:** independently compare explicit small set
differences against the completed protocol dialogue. A timestamp-only key must
fail when two events share a time. Missing-body IDs planted in admission must
not appear. A 10,001-record ad-hoc request refuses rather than leaking a truncated
set. Measure reuse so ten broad clients do not each rescan the whole archive.

## P06-T09 — Exact DVM profile, intake, finite queries and encryption

**Dependencies:** P06-T01–T06; P07-T01–T04; P01-T03/T06;
the normative [DVM profile](../specification/dvm-profile.md).

**Proposed files:** `crates/bigbrotr-protocol/src/dvm_profile.rs`;
`apps/bigbrotr/src/interfaces/dvm/{intake,codec,worker,results}.rs`;
`migrations/postgres/0609_dvm_jobs.sql`;
`contracts/dvm/v1/{query,result,feedback}.schema.json`;
`config/examples/dvm.toml`; `docs/user/dvm/{profile,client}.md`;
`apps/bigbrotr/tests/dvm_finite.rs`; `tests/fixtures/read/dvm.json`;
`tests/acceptance/cases/dvm_finite.py`.

**Boundary:** verified directed kind 5999 plus exact `bigbrotr.query/1` marker
becomes a unique `(provider,request_id,profile)` job. Result 6999 and feedback
7000 use the selected schemas. The numeric kind is experimental, not a global
reservation; unprofiled/misdirected traffic is ignored. Public read authority
comes from the requester mapping, not signature validity alone.

- [ ] **Step 1:** implement every request field/default/rejection in the normative
  schema and pin the external protocol/registry revisions. Accept list/count only
  for advertised resources; reject URL/job inputs and arbitrary destinations.
  A namespace collision cannot trigger another application's job.
- [ ] **Step 2:** apply age/future bounds, exact provider, signature and quotas
  before feedback; persist normalized request, policy, resource/source pins and
  replay identity atomically. Rejected valid requests receive at most one bounded
  error; malformed or wrong-provider input receives none.
- [ ] **Step 3:** implement the finite state machine with one page/count per job,
  shared cumulative work budget and durable result artifact. Restart uses the
  same source cut; lost source pins fail explicitly rather than recomputing at
  latest. Enqueue exact signed bytes through P07, never a direct publisher path.
  Enforce the profile's absolute 900-second accepted-job lifetime, 60-second
  queue wait, 15-second query work and 300-second result-ready delivery deadline.
  Bind ReadViewStamp to finite results and subscription batches. Its earlier
  temporal deadline also bounds signing/dispatch/retry and payment quote admission,
  as specified in the read-view contract. Releasing data pins never discards those
  checks. Exercise expiry before sign, after sign/before send and before retry;
  no fresh implicit recomputation or late ordinary delivery is permitted.
  Failed unpaid jobs and paid refund-due jobs cancel undispatched obligations;
  neither is revived by a late ACK, restart or ordinary outbox retry.
- [ ] **Step 4:** implement the explicit NIP-44 v2 private variant and encrypted
  result/feedback envelope. Reject NIP-04/ambiguous downgrade, do not echo private
  `i`/request data in tags, logs or cleartext errors, and keep decryption permission
  separate from signing. Metadata visibility is documented.
- [ ] **Step 5:** enforce four running/64 queued/one per requester and one request
  per ten seconds with burst two. Bound actual escaped/encrypted signed wire
  size, not just payload length; 192-KiB content is additionally limited by NIP-44
  plaintext and each configured relay's total event bound.
- [ ] **Step 6:** run a separate client through the real relay, two DVM workers,
  actual read engine and signer. Inject duplicate intake, lost delivery ACK,
  expired cursor, private-policy restriction, invalid cached ID and quote-heavy
  output. Verify decrypted result and exact durable request/result identities.

**Oracle and negative control:** external client schemas and independent
signature/decryption implementations must agree with the fixture's expected
QueryPage. An unprofiled 5999 event must cause no job, signing or egress. A payload
under the logical byte cap but over the escaped signed cap must fail safely;
measuring content alone is the deliberate failing control.

## P06-T10 — Bounded DVM subscriptions, renewal and cancellation

**Dependencies:** P06-T07/T09; P07-T03/T04; P02-T06/T07.

**Proposed files:** `apps/bigbrotr/src/interfaces/dvm/{subscription,renewal,cancellation}.rs`;
`migrations/postgres/0610_dvm_subscriptions.sql`;
`contracts/dvm/v1/subscription.schema.json`; `config/examples/dvm.toml`;
`docs/user/dvm/subscriptions.md`; `apps/bigbrotr/tests/dvm_subscriptions.rs`;
`tests/fixtures/read/dvm_subscription.json`;
`tests/acceptance/cases/dvm_subscriptions.py`.

**Boundary:** only normal-visible event-resource subscriptions are initially
supported; derived aggregates/counts/search/history remain finite reads. An
accepted subscribe request owns snapshot/feed cut, expiry, batch sequence,
requester/provider identity and durable output obligations. Renewal changes a
lease, not the original query or resource authority.

- [ ] **Step 1:** implement opt-in capacity 32, TTL default 3600/max 86400 s and
  batch cadence minimum/default 60 s. Admit the bounded initial snapshot before
  starting; refuse oversized history rather than streaming it indefinitely.
- [ ] **Step 2:** use the same atomic snapshot/acquisition-feed handoff as the
  relay, including newly acquired backdated events and current-policy visibility.
  Commit output batch bytes, obligations and input cursor together.
- [ ] **Step 3:** emit stable `(subscription_id,batch_seq,event_id)` deduplication
  identities. Retry exact batches at least once. Relay acceptance does not mean
  the requester processed a batch, and a missed feed cannot be silently reset.
- [ ] **Step 4:** implement signed same-requester renewal before expiry and
  matching-author kind-5 exact-job cancellation. Reject another author's control,
  renewal after expiry and mutation of subscribed filters. Cancellation stops
  pending work/undispatched delivery, not remote copies or immutable originals.
- [ ] **Step 5:** handle feed retention exhaustion, pin expiry, policy revocation
  and independent DVM stop with explicit terminal/paused states. A new snapshot
  requires a new subscription request after `resume_unavailable`.
- [ ] **Step 6:** run a real client across worker restarts, a crash after signed
  outbox commit but before cursor acknowledgement, slow relay delivery, late
  backfill, unauthorized cancellation and expiry with no input arrivals.

**Oracle and negative control:** independently ordered acquisition IDs define
expected batches at each persisted cursor. Exactly-once client processing is
not asserted; clients deduplicate repeated delivery. Advance-before-outbox-commit
must lose a planted event under the crash fixture and fail. Renewal replay must
not add TTL repeatedly, and cancellation must not erase the historical source.

## P06-T11 — Optional LND prepaid finite jobs and financial ambiguity

**Dependencies:** P06-T09; P07-T01–T04; P01-T04/T06;
the exact [payment contract](../specification/dvm-profile.md#concrete-optional-payments-lnd-invoice-only-adapter).

**Proposed files:** `apps/bigbrotr/src/interfaces/dvm/{payment,lnd,refund_ledger}.rs`;
`migrations/postgres/0611_dvm_payments.sql`;
`contracts/dvm/v1/payment.schema.json`; `config/examples/dvm.toml`;
`docs/user/dvm/payments.md`; `packaging/compose/lnd-regtest.yaml`;
`apps/bigbrotr/tests/dvm_payments.rs`; `tests/fixtures/read/dvm_payment.json`;
`tests/acceptance/cases/dvm_payments.py`.

**Boundary:** default payments disabled/price zero. Explicit `lnd-prepaid-v1`
uses the operator's LND REST endpoint with only AddInvoice/LookupInvoice method
permissions, trusted TLS and a fixed positive msat price per finite operation.
No wallet/spending credentials or automatic refund/payment RPC exists in BigBrotr.
The invoice preimage is a separately protected receiving secret, not a wallet key.

- [ ] **Step 1:** validate configured endpoint/network/TLS/macaroons/record key,
  fixed price, 300-second quote lifetime and public terms. Admit source pins,
  policy and capacity before offering a quote; payment never expands them.
- [ ] **Step 2:** persist one encrypted random preimage, its SHA-256 hash, quote
  binding and amount before AddInvoice. Validate returned BOLT11 independently.
  On ambiguous creation look up the same hash and retry only the same preimage;
  never generate another invoice to escape an unknown response.
- [ ] **Step 3:** implement bounded polling (two concurrent, four RPC/s, ten-second
  timeout, initial five-second cadence), exact SETTLED/hash/amount/deadline checks
  and one unique settlement-to-job transition. An accepted HTLC or zap receipt
  cannot unlock execution.
- [ ] **Step 4:** separate job and payment states. Cancellation, late settlement,
  overpayment, policy loss or undeliverable paid results creates the specified
  refund-due obligation; keep it beyond terminal-job display retention.
  Operator-reported external refund resolution is an audited private action,
  never an automatic spend or unverified settlement assertion.
- [ ] **Step 5:** prohibit paid recurring jobs in this baseline and test price
  changes only affect new quotes. Restart with missing secrets leaves payments
  unavailable and ambiguous jobs inspectable, not silently free or freshly billed.
  Bound unresolved financial obligations to 64 globally and the configured
  payment-ledger byte reservation. Include creation-unknown, possibly payable
  cancelled/expired and refund-due rows; stop admitting new paid jobs when full
  rather than expiring unpaid obligations or accumulating unlimited private state.
- [ ] **Step 6:** run isolated real LND/Bitcoin regtest with a separate payer;
  drop AddInvoice's response after node commit, restart, settle the recovered
  invoice and verify one execution. Inspect independent node invoice records and
  attempt a spending RPC with the configured macaroon, expecting denial.
  Also settle a quote whose result cannot reach any configured relay through
  the 300-second delivery deadline. Require refund_due, release of source read
  pins after exact result retention, and no later automatic resend after the
  failed destination recovers. Test absent bid as quote permission only.

**Oracle and negative control:** the independently controlled LND node's invoice
state is payment truth. A fake requester `paid` message, HTTP-200 lookup with
OPEN/ACCEPTED status, wrong hash and repeated settlement polling cannot execute
the job. A random-second-invoice retry must fail the single-hash crash fixture.
Only regtest funds are used; missing real regtest evidence is `NOT_RUN`.

## P06-T12 — Cross-surface conformance, recovery and resource fairness

**Dependencies:** core P06-T01–T08 and P05-T13. Free DVM capability adds P06-T09
and P07-T01–T04; subscriptions add P06-T10; advertised paid support adds P06-T11.
Advertised native MCP adds T13; advertised ContextVM adds T14. These are
conditional gate dependencies, not requirements for a deployment that leaves
the adapters disabled. Requiredness is per advertised capability: absent LND may not block a free-only
release or count as passed paid acceptance. The full selected feature delivery
still owes its paid-capability regtest before claiming that optional feature.

**Proposed files:** `tests/acceptance/cases/read_surfaces.py`;
`tests/acceptance/oracles/read_membership.py`;
`tests/fixtures/read/cross_surface.json`;
`contracts/acceptance/read-evidence.schema.json`;
`config/recipes/read-lab.toml`; `docs/user/operations/read-recovery.md`;
`apps/bigbrotr/tests/read_lifecycle.rs`.

**Boundary:** identical admitted scope/cut produces equivalent event membership
and count units through HTTP, ArchiveRelay and finite DVM, plus advertised
MCP/ContextVM, subject to their
documented paging/wire differences. Search and NEG support only their declared
shapes. Compare strict snapshots at the same stamp. For chronological traversal,
compare each page's membership/eligibility under its reported evaluation and
fixed acquisition bound, never infer fixed cross-page membership or count equality.
This scenario exercises the assembled product, not isolated mocks.

- [ ] **Step 1:** freeze a mixed public/private/current/history fixture with
  real full signed events, derived facts, source gaps, empty lists, withheld
  records and backdated arrivals. Compute independent expected IDs/counts and
  normal-versus-history differences before launching the system.
- [ ] **Step 2:** exercise generated HTTP client, independent WS/Negentropy
  clients and the explicit DVM client against packaged services with actual PG/CH
  data. Validate exact response shape, signatures, cursor continuation and
  capability metadata as well as row membership.
  Add an independent 2026-07-28 MCP client and pinned ContextVM client when
  advertised; run the finite journeys and transport-specific negative cases.
- [ ] **Step 3:** run foreground acquisition plus bounded queries, search, COUNT,
  multiple NEG sessions and slow subscribers. Measure aggregate memory, scan,
  CPU, output queues and acquisition latency; refuse excess work fairly without
  granting each surface a full independent machine budget.
  Link one caller's token/key identities and verify a transport switch cannot
  multiply principal limits. Include malformed crypto/wire admission and the
  reserved cancellation lane before expensive query work.
- [ ] **Step 4:** stop each service independently, append input, restart and
  catch up. Kill queries, reset connections, remove a body, invalidate a serving
  incarnation, drop PG and revoke scope with notifications blocked. Verify the
  declared stale/unavailable/refusal behavior and bounded actual cancellation.
- [ ] **Step 5:** capture a run manifest with source cuts, policy/definition
  versions, commands, exact expected/actual membership and measured resources.
  Distinguish protocol conformance, bounded-work evidence, recovery and tested
  workload size; avoid claiming global completeness or universal capacity.

**Oracle and negative control:** compare all surfaces to independently computed
sets, not to one another alone—three adapters can share the same bug. A normal
read that leaks one logically deleted/private event fails even if every adapter
agrees. An adversarial slow client must cause its own bounded close without
stalling acquisition. Required scenarios absent from the evidence fail the phase
gate rather than being counted as successful.

## P06-T13 — Finite MCP gateway over Streamable HTTP

**Dependencies:** P06-T01–T06; P01-T04–T08. Optional protected deployments also
need the configured external authorization server, not a new BigBrotr login
service. Public mode has no external authorization-server dependency.

**Proposed files:** `apps/bigbrotr/src/read/gateways/mcp.rs`;
`contracts/gateways/v1/{tools,request,result}.schema.json`;
`config/examples/mcp.toml`; `docs/user/api/mcp.md`;
`tests/acceptance/cases/mcp_gateway.py`;
`tests/fixtures/read/mcp_gateway.json`.

**Boundary:** an authenticated finite MCP call maps through the existing registry
into one admitted read and returns the exact typed result/error. The adapter is
optional, off in every recipe, and has no arbitrary proxy/management/payment
operation. Internal gRPC remains private.

- [ ] **Step 1:** pin official rmcp and explicitly select protocol 2026-07-28;
  implement server discovery, per-request version metadata/header consistency,
  tools/list and tools/call. Generate the four `bigbrotr.tools/1` definitions from
  the registry; include required modern result/catalog-cache fields. Page whole
  catalog definitions with protected schema/policy-bound cursors and prove each
  definition fits the smaller ContextVM wire allowance after encoding. Refuse
  legacy/unknown versions explicitly, without accidental SDK-default downgrade.
- [ ] **Step 2:** expose `/mcp` on the public read listener using bounded finite
  JSON responses. Validate any present Origin; use public principal by default.
  Protected mode validates exact OAuth issuer/audience/subject and publishes
  resource metadata; never accept management sessions or identity headers as
  authority. Resolve grants and current leases on every request.
- [ ] **Step 3:** preserve resource schema, row units, snapshot/traversal mode,
  source/evaluation stamp and opaque page cursor. Encode structuredContent and
  matching JSON text within the combined 256-KiB response limit; an oversized
  row is a typed refusal. Count remains exact-or-refused when exact requested.
- [ ] **Step 4:** enforce 4 active/16 queued within shared admission, 64-KiB
  request and 20-second total call bound. Propagate disconnect/deadline/revocation
  to engine cancellation and pins. Exclude tasks, subscriptions, sampling,
  elicitation, filesystem roots, payments and stdio from advertised capabilities.
- [ ] **Step 5:** use a separately implemented current MCP client against the
  packaged handler for discover/query/actual-next-cursor/get-event/search/count,
  every advertised resource shape, protected history and unavailable resources.
  Test missing modern fields, batch/SQL/URL input, duplicate keys, wrong audience,
  unknown subject, identity spoof, slow disconnect and dropped-policy-notification
  revocation. Capture actual output after expiry, not only handler return values.

**Oracle and negative control:** the shared independent fixture evaluator owns
membership. A protocol-valid tool call containing SQL must fail application
validation; a gateway that serves the correct rows under an expired lease fails.
Snapshot and traversal fixtures test their different promises. Schema-only
research does not close packaged handler or OAuth acceptance.

## P06-T14 — Experimental finite ContextVM gateway

**Dependencies:** P06-T01–T06/T13 application schemas; P01-T03/T05/T06/T08;
P07-T01–T04 purpose-bound signing/egress primitives. No DVM subscription/payment
dependency. ContextVM remains disabled and unadvertised until this qualification
passes; it is supported optional implementation scope, not a maturity claim.

**Proposed files:** `apps/bigbrotr/src/read/gateways/contextvm.rs`;
`contracts/gateways/v1/contextvm.schema.json`;
`config/examples/contextvm.toml`; `docs/user/api/contextvm.md`;
`tests/acceptance/cases/contextvm_gateway.py`;
`tests/fixtures/read/contextvm_gateway.json`.

**Boundary:** independently verified directed/encrypted Nostr frames carry only
the pinned 2025-11-25 finite MCP subset and share T13's application tool schemas.
Transport author identity constructs the same admitted principal; the adapter
does not create durable DVM jobs or financial obligations.

- [ ] **Step 1:** implement bounded application framing over selected Nostr
  primitives, using pinned ContextVM SDKs as interop oracles. Verify both wrapper
  and inner signed kind-25910 event, recipient, timestamp and JSON-RPC shape;
  support CEP-4 single-layer encrypted inner events with 1059/21059 mirroring.
  Bind signer purpose and 1–4 operator-configured relays; never learn request
  destinations or silently enable SDK oversized-transfer/streaming defaults.
- [ ] **Step 2:** negotiate exact 2025-11-25 before queries and cap session/replay
  state. Identity is inner author, never supplied `_meta` or outer wrapper key.
  Enabled empty allowlist fails configuration; explicit public mode grants only
  public reads. Map provider/author/original-RPC-ID to verified event/token so
  same IDs across authors cannot misroute replies or cancellations.
- [ ] **Step 3:** intersect 2 active/8 queued/1 per author with global admission;
  enforce 32-KiB inner request/result, 128-KiB signed wire and actual NIP-44 limits,
  2-second queue wait and 20-second total deadline. Bound crypto/input queues
  before dispatch. Deduplicate inner IDs without evicting live replay records.
- [ ] **Step 4:** dispatch exact signed bytes with at most two bounded retries,
  rechecking lease/temporal authority before sign/send. No claim of requester
  receipt follows relay OK. Implement signed same-author cancellation; restart
  discards active free calls and requires client retry. Keep announcements,
  payments, CEP-22/41, DVM translation and arbitrary RPC unadvertised.
- [ ] **Step 5:** run packaged server plus a pinned independent ContextVM client
  through initialization/discovery/query/continuation/event lookup and count;
  compare application results with authorized HTTP/MCP fixtures at their declared
  mode/stamps. Test forged signature, wrong recipient/author, spoofed metadata,
  stale/future events, same RPC IDs across authors, hostile early messages,
  duplicate relays, rewrap replay, cross-author cancellation and queue floods.
  Kill/restart, expire before signing/dispatch/retry, revoke with PG partitioned,
  and inspect captured relay traffic plus actual backend cancellation.

**Oracle and negative control:** independent fixture truth plus independently
verified signed captures, not the SDK's self-reported success. An unbounded SDK
receive queue, a cancellation keyed by an unscoped RPC ID, or a completed result
sent under expired authority fails. Local upstream in-memory research establishes
only its exercised cases; production memory, cross-language client, relay and
BigBrotr authority evidence are required separately.

## Design review record

These are five substantive planning reviews and corrections, not executed tests.
The central review ledger records cross-phase review and remaining release gates.

| Round | Concrete finding and disposition |
|---|---|
| 1 — Surface coverage | Unified row/count/cursor semantics while keeping actor HLL distinct; gave search and NEG real Indexer-owned projections, with an explicit DVM profile (T01–T09). |
| 2 — Data/bootstrap | Removed the P08 token-management bootstrap dependency; named `/v1/capabilities`; bound public aggregate inputs and retained history to central policy/evidence contracts (T01/T02/T05). |
| 3 — Crash/authority | Specified absolute cursor and job lifetimes, atomic snapshot/feed/outbox positions, invoice-hash recovery, matching-author cancellation and failed-versus-refund-due delivery deadlines (T03/T07/T09–T11). |
| 4 — Work/state cost | Bounded active cursor pins, unresolved financial obligations, NEG ad-hoc sets and shared buffers; charged pretokenized search/ordered phrase verification and prohibited permanent arbitrary filter indexes (T03/T06/T08/T11). |
| 5 — Packaged client proof | Added actual capability-client checks, oversized single-event refusal, 300-second paid-delivery failure/regtest and three-surface comparison against independent source truth, not adapter agreement (T05/T07/T11/T12). |

## Qualification additions from the complete-document audit

P06-T06/T12 execute the common/rare/phrase/multilingual workload in
[usable search scope](../architecture/search-and-reconciliation.md#relay-encoding-eligibility-and-usable-search-scope),
recording both accepted and refused user queries, full candidate work and total
index footprint. Tiny exact fixtures alone cannot establish a useful default.
P06-T09/T10 implement [separate subscription/control slots](../specification/dvm-profile.md#slot-ownership-and-authenticated-control-under-saturation)
and prove owner renewal/cancellation through a saturated ordinary job queue.
P06-T12 has distinct core, free-DVM, subscription and paid evidence requirements;
unadvertised optional support is unavailable, never silently marked tested.
