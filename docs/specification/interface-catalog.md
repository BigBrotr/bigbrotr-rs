# Interface catalog and stable resource names

Status: selected interfaces under implementation; availability is explicit for
the initial subset below. P06 owns public reads/protocols, P08 private operations/artifacts,
P09 browser clients, and P10 the website. Existing typed policy, budget, snapshot,
error and idempotency contracts apply; a route name does not waive them.

## Public and authorized HTTP data interface

| Method and path | Meaning |
|---|---|
| `GET /v1/meta` | Safe instance/version/about/status metadata and capability link |
| `GET /v1/capabilities` | Caller-visible activated resource/version/filter/order/count/features and unavailable reasons |
| `GET` or read-only `POST /v1/events` | Alias for ordinary `events` collection semantics |
| `GET /v1/events/{id}` | One currently eligible complete signed event; restricted existence uses nondisclosing 404 |
| `GET` or read-only `POST /v1/resources/{resource}` | Typed collection or bounded resource result using the registry below |
| `GET` or read-only `POST /v1/resources/{resource}/count` | Only where the registered row unit admits a meaningful count; exact/estimated state and scope explicit |
| `GET /v1/artifacts/{artifact_id}/manifest` | Exact verified public artifact manifest bytes |
| `GET` or `HEAD /v1/artifacts/{artifact_id}/files/{file_id}` | Catalog-authorized referenced file bytes/metadata, never a client-supplied path |
| `GET /v1/assertion-contexts/{digest}` | Optional hosted `bigbrotr-context/1` original bytes by signed content digest; P07-T08/P08-T04 artifact publication authority |

GET collections use one percent-encoded JSON filter parameter plus scalar
page/cursor controls; POST collections use the same versioned typed envelope.
Point/meta/capability routes do not acquire a meaningless POST variant. Every
resource registers its exact input/output schema, ready input generations,
allowlisted operations, visibility and work plan. Unknown resources/fields/orders
are refused; a SQL table name is not an admissible resource name.

The selected registry is below. `C` means list and count of the specified row
unit, subject to available exact/estimated execution. `B` means a bounded typed
result without a generic count. Point filters do not require a separate route.
All lists use stable identity tie-breakers and the shared cursor contract; large
scientific enumeration uses an authorized export job.

| Resource ID | Row unit and required selectors/order | Input owner / operations |
|---|---|---|
| `events` | Complete ordinary event; Nostr IDs/authors/kinds/time/tag filters; `(created_at DESC,event_id)` | Event/current/visibility views; C |
| `event_metadata` | One event identity plus permitted authored/acquired/observed/body-state metadata; event IDs, author/kind/time; event ID order | Event/receive facts; C; does not reveal withheld body existence |
| `event_history` | Retained signed historical event/version under explicit history grant; event ID or coordinate/author filters; authored-time/ID order | Archive/serving originals; C; history retention differs from ordinary visibility |
| `event_provenance` | One reduced eligible event/endpoint/observer/access witness; event or endpoint selector required, source-origin filter; event/endpoint/time ordering declared | Local and imported evidence stay labeled and distinct; C; provenance required |
| `event_availability` | One executed pair result with selection/request method; event or endpoint plus bounded observed window; observed-time/attempt/ID order | P04-T13/P05-T08; C; no passive last-seen substitute |
| `author_profiles` | One current profile header and parsed state per public key; pubkey filters, pubkey order | Current winner/parser; C; empty/invalid/missing distinct |
| `current_lists` | One whole current list header with bounded entries/entry continuation; pubkey/kind/identifier selectors, coordinate order | Current headers/members; C counts headers, not hidden entry rows |
| `relay_directory` | One endpoint with dated selected attributes; route/capability/eligibility filters, endpoint order or explicit comparable score order | Local/resolved relay views; C; policy-limited directory, not PG registry dump |
| `relay_measurements` | One cut-relative executed relay attempt, typed checks and document refs; endpoint/method/window filters, observed-time/attempt order | P04 local facts; C; excludes non-relay HTTP subjects |
| `relay_documents` | One document occurrence/version for a relay, with bounded original retrieval and parser basis; endpoint/family/window, observed-time/source order | A06 objects/references; C counts occurrences, not distinct bytes |
| `relay_coverage` | One scoped traversal/gap/evidence or `relay.composition/1` summary; endpoint/definition/population/window, scope/source order | P03/P05 coverage/composition; C; definition and observed population required; no universal completeness percentage or current remote inventory claim |
| `relay_overlap` | One verified pair in a declared common universe/candidate generation; endpoints/window/method, ordered endpoint pair | P05 overlap; C; component connectivity is not pairwise identity |
| `relay_availability` | One endpoint/window/method stratum with S/N/U or time-estimator basis explicitly named; endpoint/window, endpoint/stratum order | P05-T08/T12; C; unlike availability definitions never share one unlabeled fraction |
| `network_statistics`, `event_statistics`, `social_statistics` | One named fact/subject/population/window; definition/window and applicable subject filters, definition/subject order | Independent typed fact targets; C counts fact rows, each value has its own unit |
| `scores` | One typed subject/profile/viewpoint/generation score and components; subject type and profile required, score/subject order within comparable strata | Social/relay rank owners; C; equal event/pubkey bytes remain different subjects |
| `assertion_lineage` | One signed output/claim and its method/input/authority basis; event/subject/binding selectors, event/source order | A01/A03/A09 and publication manifests; C |
| `graph_subgraph` | Bounded graph expansion from explicit roots, generation, edge policy, depth and vertex/edge limits | Graph artifact reader; B; truncation/omitted frontier explicit, no full-graph browser fetch |
| `content_search` | One matched eligible event and explained relevance; selected analyzer/query grammar plus supported event filters, relevance/created-time/ID order | Complete search projection; C where whole admitted scope can be counted; never recent-prefix top-K |
| `published_datasets` | One currently published verified artifact; artifact ID/type/created-time filters, `(created_at,artifact_id)` order | P08 artifact catalog/pin/policy; C; separate from a private export job |

Non-relay issuer evidence is available through its attributed assertion/measurement
lineage and scientific exports. It is not inserted into `relay_measurements`
with a fake endpoint. A future generic measurement resource is added only with
a real consumer and a versioned subject schema, not as an arbitrary JSON endpoint.

Composition counts, author concentration and optional viewpoint-conditioned
summaries use the [analytical definitions](../architecture/analytics-detail.md).
Frequently reused outputs have complete CH generations; expensive alternate
populations use bounded jobs/artifacts. Arbitrary public SQL and unlimited
cross-products are not read-core operations. Operators may grant separate direct
SQL access to qualified research views or isolated export copies, with explicit
data authority, quotas and no implied public visibility filtering.

HTTP JSON metadata preserves large integers through the declared lossless scalar
encoding; nested full Nostr events retain their mandated numeric fields. Query
results expose view/cut, freshness, exactness, missing/disabled/refused state and
coverage. Shared bounds and statuses remain in P06. Temporal invalidation uses
the selected read-view contract. Snapshot list/count/next-page use one stamp;
chronological traversal reports per-page current eligibility and a fixed candidate
cut, with separately dated counts and no unchanged-membership claim.

## Artifact and assertion-context bytes

Artifact/file IDs resolve through a verified immutable manifest. Respond with
declared media type, byte length, immutable digest ETag and safe attachment name.
HEAD performs the same authorization and returns no body. Select single byte
ranges for resumable large-file downloads: valid range yields 206 and exact
Content-Range, unsatisfiable byte range 416. Valid multiple ranges are ignored
and return the authorized full representation; multipart output is not selected.
Reject malformed byte syntax with 400 and abusive overlapping/tiny range sets
with 416. Ignore unknown range units and Range on HEAD. Evaluate ordinary
preconditions before Range; If-Range mismatch returns the authorized full
representation. These choices follow [RFC 9110 section 14.2](https://www.rfc-editor.org/rfc/rfc9110.html#section-14.2).
Use a strong digest ETag over exact stored bytes and no dynamic content encoding
that changes their offsets. Bound
stream buffers/concurrency and actual storage reads independently of file size.
P08 tests full, suffix/open-ended, invalid and resumed ranges against file hashes.

Current published state, pin/reference closure and policy are checked before
metadata or bytes and during streaming within the shared five-second authority
bound. Nondisclosing 404 applies to withdrawn/invisible artifacts. A conditional
304 cannot bypass authorization. External caches/copied files cannot be recalled;
application-managed caches revalidate publication/policy, and private output is
never exposed through a public static file directory or unrestricted storage URL.

Hosted assertion contexts use the same artifact publication/pin authority with
artifact kind `assertion_context`; the context's digest selects exact original
bytes. Its configured HTTPS base URL and digest must match the signed event's
reference. Publish and verify accessible bytes before signing a reference;
signing authority alone does not grant artifact publication. If hosting is absent,
disable this extension or use an explicitly configured verified external host;
ordinary compatible NIP-66/85 publication still works. P07/P08 test context fetch
and digest with an independent client, including withdrawal/restricted input.

## Private management and CLI

The private router owns authentication, action grants, CSRF/origin/session rules,
expected revisions, idempotency and desired-versus-applied state. It never accepts
SQL or shell code. All lists use bounded authorized pagination. These routes
complete the workflows promised by P08/P09:

| Private route | Selected action / CLI or browser purpose |
|---|---|
| `POST /management/v1/bootstrap/enroll` | One-time local first-owner enrollment; no anonymous public administration |
| `POST /management/v1/sessions`; `DELETE /management/v1/sessions/current`; `GET /management/v1/me` | Login/logout and effective actor grants |
| `POST /management/v1/sessions/current/reauthenticate` | Recent session/principal/action-bound proof for a sensitive command |
| `GET` / `POST /management/v1/principals` | List or enroll another local account with explicit grants and one-time setup credential |
| `POST /management/v1/principals/{principal_id}/commands` | Typed deactivate/reactivate/reset-credential; actor history retained |
| `POST /management/v1/credentials/reset` | Complete setup/reset using a principal-bound single-use credential; changes credentials only |
| `PUT /management/v1/principals/{principal_id}/grants` | Reauthenticated versioned grant update |
| `GET` / `POST /management/v1/tokens`; `DELETE /management/v1/tokens/{token_id}` | List safe metadata, issue once, revoke; no plaintext/hash disclosure in listing |
| `GET /management/v1/secret-references`; `PUT /management/v1/secret-references/{reference_id}` | Discover/register allowed protected provider handles; no raw secret upload or arbitrary filesystem browser |
| `GET /management/v1/configuration` and `/configuration/revisions`; `POST /configuration/previews` and `/configuration/revisions` under the same prefix | Redacted effective state, validated change preview and atomic revision; CLI config show/validate/preview/apply |
| `GET /management/v1/roles`; `POST /management/v1/roles/{role_instance_id}/commands` | Typed start/pause/drain/resume/stop for already configured supervised roles |
| `GET /management/v1/relays`; `POST /management/v1/jobs` | Operational registry view and typed finite work; enrollment/repair operations use their existing guarded domain commands |
| `GET /management/v1/jobs` and `/jobs/{job_id}`; `POST /jobs/{job_id}/cancel` or `/resume` under the same prefix | Discover durable work, status, cooperative cancellation and resumption of paused work; CLI jobs list/status/cancel/resume |
| `GET` / `POST /management/v1/saved-views`; `PUT` / `DELETE /management/v1/saved-views/{view_id}` | Owner-scoped saved query and revision; reuse reauthorizes |
| `GET /management/v1/artifacts` and `/artifacts/{artifact_id}`; `POST /artifacts/{artifact_id}/publish` or `/withdraw` under the same prefix | Private verified-artifact discovery/publication; CLI artifact publish/withdraw; independent action grants |
| `GET /management/v1/storage`; `GET /management/v1/diagnostics/preview` | Resource/protection state and redacted diagnostic selection; bundle generation is a finite job |
| `GET /management/v1/publishing`; `GET /management/v1/audit` | Exact destination obligations and attributable private audit |

The implemented initial subset is described by the versioned [private session
OpenAPI contract](../../contracts/management/v1/openapi.yaml): login, current-session
logout, self inspection, command-bound reauthentication and scope-grant acceptance
in the trusted-host loopback HTTP profile. Fresh grant enablement requires explicit
`archive.reader.enabled`, separate native read credentials and certified snapshot
custody; an unconfigured or unavailable reader refuses503. Revocation and original
acceptance retries need no Archive. [Native HTTP qualification](../implementation/evidence/p08-native-grant-http-20260923.json)
covers actual enable/retry/restart/revoke at0d95a6a. Its new/retried receipt reports
acceptance only, with exact decimal revisions/time and unobserved application.
Restrictive drain and private history delivery remain unqualified. Its
`bigbrotr-session/1` response carries exact decimal-string revisions/microseconds,
closed stored action names and `authority=observation-only`. Stored grants are not
reusable action capabilities. Cookie/CSRF secrets never appear in the JSON body.
GET self inspection counts as user activity, so the browser must not poll it to
keep an unattended session alive. Full grant/audit and remote-TLS
routes remain separate required implementation. Generated types cover this actual
subset; the current frontend runtime validators cover session observations and
errors. Grant/reauthentication browser consumers remain required; these artifacts
do not advertise the whole table above.

The initial `POST /management/v1/jobs` consumer accepts only an exact-ID private
signed-event export using the shared QueryRequest and approved destination
binding. Its [operator contract](../operations/export.md) specifies immutable
acceptance202, historical retry200 and unavailable dependencies; the generated
management contract references the existing read schema. Migration0813 and job
privileges are startup prerequisites when a destination is explicitly selected.
Without selection, a safe protocol/privilege probe preserves earlier session
operation and permits historical receipt resolution when813 is available.
Destination custody outlives cancelled SQL through actual driver join;
filesystem observation runs outside the asynchronous Control transaction. This
route is under native qualification and reports no execution/artifact readiness.

The selected next history consumer mounts the same
`/v1/resources/event_history` resource on the private origin with a browser-session
authentication adapter. It uses the shared QueryRequest/QueryPage semantics,
requires the separate `history.read` action for the complete visibility reference,
and does not confer export permission. This private cookie binding does not apply
to public HTTP, MCP or other bearer transports. GET query decoding is specific to
the resource route; session and static routes keep their closed request shapes.
The [initial history profile](../../contracts/read/v1/openapi.yaml) now owns the
exact shared request/page subset and [operator guidance](../user/api/history.md).
Its implementation is under native qualification; do not advertise it from
codec or transport tests. The first profile uses one exact event ID, snapshot
mode, a current certified archive head and a complete retained cut. Broader
selected resources, filters, counts and cursors remain required P06 work.
Smaller configured request/page/queue/response/execution/authorization limits apply.
The cold native worker's existing512MiB floor plus parent decode/output overlap
must fit an explicit per-read memory allowance; the common64MiB profile is not
qualified by this implementation. Insufficient allowance refuses capacity.

The initial private static bundle has a closed JSON manifest
`bigbrotr-private-assets/1`: exactly `format`, `base_path` and `files`, with each
file containing `path`, unsigned `bytes` and lowercase hex64 `sha256` over the
exact file bytes. Base is `/` or `/control/`. A nonempty list has at most128 unique
paths, requires `index.html`, excludes the manifest itself and permits only
HTML, JS, CSS, JSON and SVG. Paths are at most256 ASCII bytes; slash-separated
components begin with a letter, digit, underscore or hyphen and continue with
those characters or dots. File/total/manifest limits are2MiB/16MiB/64KiB inclusive.
The digest establishes integrity under a trusted deployment directory, not
publisher authentication. There is no mutable asset generation or cache promotion
from an HTTP request. Future incompatible bundle formats must be explicitly
rejected or migrated; no silent reinterpretation is permitted.

Static GET/HEAD uses the private listener's same literal Host/Origin boundary,
connection budget, deadline and delivery gate. Exact known paths resolve from
verified held bytes; unknown paths return404 and queries are refused. Root maps
to `index.html`, with no SPA catch-all. `Cache-Control:no-store`, nosniff,
same-origin resource policy and frame denial accompany successful assets. Generated
HTML supplies script/style CSP hashes; the Rust header additionally enforces
`frame-ancestors 'none'`, which browsers ignore in CSP meta. No static response
issues cookies or turns session observations into action/data authority.

Private HTML and its HTTP response both select `Referrer-Policy:same-origin`.
This permits referrers only inside the private origin. It also preserves the
browser's usable Origin on same-origin POST/DELETE: Fetch's
[Origin algorithm](https://fetch.spec.whatwg.org/#append-a-request-origin-header)
serializes it as `null` under `no-referrer` for this client mode. Missing, null and
foreign Origin remain refused; Referer is never an authorization fallback. This
header/meta correction changes no API schema or persistent state, but requires
deploying the matching Rust response and rebuilt private bundle together.

Private bundles omit speculative JavaScript preloading in prerendered HTML and
client import dependencies. Ordinary imports and stylesheet loading remain;
failed bootstrap leaves the static reload notice available. The actual missing
module/restored-bundle journey must recover after user reload with normal browser
caching, a real 404 followed by 200, and no request interception or CSP bypass.
This addresses the measured WebKit recovery failure consistent with
[upstream bug 270357](https://bugs.webkit.org/show_bug.cgi?id=270357).
The tradeoff is potentially more sequential dependency requests, not a measured
performance improvement. The correction requires a rebuilt private bundle;
API and persistent formats are unchanged. P09-T01 owns continued qualification.

Setup/reset credentials use a 15-minute maximum lifetime, hash-only storage,
single atomic consumption, bounded guesses and protected one-time display.
Bind each setup/reset challenge to the principal credential/security revision;
atomic consume checks active or explicitly enrollment-eligible account state.
Deactivation, reissue or credential change invalidates outstanding challenges.
Completing a reset neither reactivates an account nor changes grants.
Reauthentication proofs live at most five minutes, bind the actor/session/action
and expected security revision, and are consumed for the accepted sensitive
command; an idempotent retry resolves that command. Password policy and hashing
remain P08-T02. A token/setup response lost after issuance resolves its existing
ID; the original secret is unavailable because only a hash was retained. Offer
explicit revoke/reissue; never silently issue a second credential on retry.

The separate initial first-owner bootstrap keeps P08-T02's ten-minute default.
Job resume requires a paused nonterminal job, its expected revision and current
source/destination/role grants. It continues the same committed recovery unit;
terminal jobs cannot be resurrected. Paused jobs stay paused after worker restart;
cancel-requested jobs only drain toward cancellation. A new deliberate attempt is a new job with
declared lineage. Worker crash recovery resumes eligible nonterminal work without
requiring the operator to mint another job identity.

For the initial actor-owned signed-export resume profile, the job-kind action is
the existing exact-scope `export` grant. Recheck it independently of dataset
`data.read`, scope `history.read` and `private-export`, and the original approved
destination binding. `roles.control` authorizes configured role lifecycle, not
this finite export action; no new generic job-control grant is implied. Only the
original actor may resume this profile. A new command contains the expected job,
configuration and security revisions, with the job ID in the route and a new
resume-command key in `Idempotency-Key`. Identical retries keep that key; it is
distinct from the original export-acceptance key. No input, destination or cap is editable.

Acceptance queues the same committed unit, advances the job revision exactly once,
clears the prior pause reason and records an immutable command/audit receipt in
the same transaction. It requires the retained original cut and export pin, but
does not renew the pin's scheduling lease or create a reader pin. Actual execution
still needs fresh reader authority and positive output exclusion. A currently
authenticated identical retry resolves the original acceptance before current
input grants, destination or job-state checks. It reports that historical accepted
revision, even if a worker has since paused again; it must never requeue twice.
The typed adapter and initial HTTP consumer have scoped native evidence; the typed
client has local contract checks in the [export operation](../operations/export.md).
HTTP receipt format is `bigbrotr-export-resume/1`, with original command/job IDs,
accepted job revision/time, created flag and unobserved execution. New acceptance
returns202; identical historical resolution returns200. No response issues cookies
or touches session activity. Schema0814/explicit SQL prerequisites are probed
without implicit migration; missing optional privileges disable only this command.

Deactivation revokes existing sessions/tokens and starts the restrictive policy
barrier. Reactivation does not resurrect them. Protect the last recoverable owner
and retain audit attribution. Secret-reference restriction similarly invalidates
affected authority; registering a handle does not activate a signer or provider.
Local deployment provisioning supplies secret values. Browser flows can select
safe references and clearly explain the local provisioning step.

Role start/stop acts on configured roles controlled by the receiving supervisor.
It cannot deploy remote containers or create undeclared infrastructure/listeners.
Start rechecks dependencies, budgets, policy and writer exclusion. Stop joins or
positively excludes work before ownership release; restart-required is explicit.
Pause/resume retains the role's recoverable state. Command acceptance and actual
supervisor application are different recorded outcomes.

Finite acquisition uses the same job owner: `monitor availability run --selection
FILE` and `issuer lookup --recipient PUBKEY --profile EVENT_ID` submit typed
`AvailabilityJobRequest` / `IssuerLookupRequest`. CLI and private job API share
source/profile/scope/idempotency validation; status/resume uses the existing job
contract. Neither creates a new perpetual service. Import/export, backup/restore,
scrub/repair/migration and derive/rank commands likewise call their owning tasks.

## Nostr, internal transport and user interfaces

The optional [gateway contract](gateway-contracts.md) adds native MCP Streamable
HTTP at `/mcp` and a separate ContextVM Nostr adapter. Both are disabled in every
supplied recipe. MCP uses explicit protocol 2026-07-28; the initial experimental
ContextVM binding uses 2025-11-25 MCP inside signed kind-25910 messages with
required CEP-4 encryption. ContextVM is not NIP-90 or a DVM profile. Independent
client and bounded-resource qualification gates apply before advertising it.

The finite tools are `bigbrotr_capabilities`, `bigbrotr_query`, `bigbrotr_count`
and `bigbrotr_get_event`, generated from this resource registry. They preserve
registered snapshot/traversal modes, grants, exposure, row units, byte limits,
source/evaluation metadata and opaque continuations. Traversal is current
eligibility per page under a fixed acquisition bound, not list/count equivalence.
No MCP/ContextVM payment, subscription, management, arbitrary proxy or file-fetch
tool is selected. Large artifacts retain authorized HTTP download routes.

ArchiveRelay serves REQ/CLOSE, finite COUNT, configured AUTH, optional NIP-50 and
NIP-77, historical response followed by bounded live delivery. Incoming EVENT
writes are refused. NEG advertises only eligible retrievable bodies; a later
REQ is not an atomic exported snapshot. DVM uses the explicit experimental
`bigbrotr.query/1` profile (5999/6999/7000), finite list/count and optional
subscription/renew/cancel, with a separate optional payment capability.

NIP-66 and NIP-85 publishers consume stored local observations/typed facts and
scores, sign through purpose-bound authority, then use the exact-byte outbox.
Compatible protocol fields and proposed extensions remain separately identified.
The [assertion binding specification](assertion-bindings.md) owns subject/field
meaning. A generic sign-anything or pay-anything public endpoint is not selected.

MCP/ContextVM authentication resolves into the same internal principals and
five-second authorization leases as other reads. Adapter concurrency/queues are
subdivisions of the deployment budget, never independent query allowances.
ContextVM uses a dedicated purpose-limited signer and configured relay set;
Nostr authentication cannot confer history, management or egress authority.

Internal Ingest/Archive/Index/Read/Control gRPC uses the same typed contracts as
in-process calls, with mTLS and bounded messages. It is not the public HTTP API.
Generic public RPC/gRPC, MCP stdio and arbitrary MCP forwarding are not selected.
The public dashboard, private administration and static documentation/blog site
are three separately built interfaces in one repository. Pure Rust libraries are
internal reusable boundaries initially; all packages remain `publish = false`.
An external stable library distribution requires the separate consumer-driven
decision in [development and delivery](../engineering/development-and-delivery.md#package-and-license-policy).

P06/P08/P09 acceptance enumerates this registry, starts the actual packaged
handlers, walks every enabled producer-to-response workflow and verifies denied,
missing, stale, empty, expired and disabled states. Generated OpenAPI/schema/help
must match these names and actual behavior. A route stub or a successful static
build cannot close a promised operation.
