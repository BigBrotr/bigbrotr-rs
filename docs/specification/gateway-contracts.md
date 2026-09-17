# Read gateways and finite agent tools

Status: selected implementation contract, researched on 2026-09-17. This adds
optional MCP and experimental ContextVM adapters to the read product; it authorizes no product
implementation at this planning stage. [Research evidence](../research/evidence/gateway-research.json)
records exact sources and actual probes. The [interface catalog](interface-catalog.md),
[read-view contract](read-view-contract.md) and [shared read core](../architecture/derived-read-publication.md#shared-read-core)
own data meaning. Adapters do not become query engines, privileged principals,
payment authorities or storage owners.

## Selected surfaces

| Surface | Selected purpose and support | Default |
|---|---|---|
| HTTP API | Canonical typed finite resources, dashboard data and authorized artifact downloads | Existing recipe choice; deployment-root default false/loopback |
| ArchiveRelay | Nostr event REQ/COUNT/search/reconciliation and bounded live events | Existing recipe choice; default false |
| MCP Streamable HTTP | Supported optional finite agent access; `/mcp` on the public read listener, no separate daemon/port | False in every supplied recipe |
| ContextVM | Selected experimental optional Nostr access to the finite MCP tools below; dedicated service identity and relay connections; unavailable until P06-T14 qualification | False in every supplied recipe; explicit relays/signer/admission policy required |
| Query DVM | Retained `bigbrotr.query/1` durable query jobs; finite core, subscription extension and paid extension have separate qualifications | Existing false default; subscriptions/payments independently false |
| Internal gRPC | Existing private typed process boundary, mTLS and service grants | Only selected split-process recipe; never public ingress |
| Public generic RPC/gRPC, MCP stdio or arbitrary MCP proxy | Not selected | Unavailable |

MCP's concrete client journey is: discover data, query relay evidence or search,
follow a continuation, and explain the source/coverage. ContextVM carries that
same journey for a Nostr-key client without requiring the custom DVM query
grammar. These are implementation commitments with acceptance below, not claims
of deployed handlers or generic client compatibility. Large exports/downloads
retain their artifact workflow; live Nostr events use ArchiveRelay.

DVM is not replaced: durable request identity, stored signed results, outbox
obligations and financial reconciliation have different semantics. ContextVM
does not translate to 5999/6999/7000 or share DVM request/cancellation identities.

## Protocol versions and library adoption

Official MCP now has stable **2026-07-28**, including per-request version metadata,
`server/discover`, `resultType`, catalog caching, and changed cancellation and
subscriptions. Search results naming only 2025-11-25 are stale for this decision.
The pinned [official changelog](https://github.com/modelcontextprotocol/modelcontextprotocol/blob/f56f204f6290f6531b14d5734eb3e0a10f0eb201/docs/specification/2026-07-28/changelog.mdx)
defines the differences. Select **2026-07-28 for native HTTP**, **2025-11-25 for
the initial ContextVM binding**. ContextVM's inspected specification/integration
still use initialization-era MCP. Do not copy its illustrative `2025-07-02`
version string or claim modern compatibility. A future modern ContextVM binding
requires versioned schema and independent-client acceptance first. Native HTTP
does not need legacy sessions merely because ContextVM supports a legacy binding.

Use official Rust **rmcp 3.4.0**, source
[`fd7811fdaa9fefa1c8034534b4d7a31c97204f89`](https://github.com/modelcontextprotocol/rust-sdk/tree/fd7811fdaa9fefa1c8034534b4d7a31c97204f89),
for native HTTP, freezing product dependency resolution during P01. Set the
version explicitly: its [model source](https://github.com/modelcontextprotocol/rust-sdk/blob/fd7811fdaa9fefa1c8034534b4d7a31c97204f89/crates/rmcp/src/model.rs)
still defines `ProtocolVersion::LATEST` as 2025-11-25. BigBrotr owns authentication,
admission, cancellation and output authority; SDK annotations confer no grants.

Use official **contextvm-sdk 0.2.2**, source
[`848144c0af627a2ffb1793e21ed346e269acddf4`](https://github.com/ContextVM/rs-sdk/tree/848144c0af627a2ffb1793e21ed346e269acddf4),
as an interoperability oracle. Select a bounded application framing adapter over
the project's Nostr verification/encryption/connection primitives, rather than
embedding its whole server worker/proxy. The inspected SDK depends on rmcp
`^1.8`, defaults oversized transfer on, and has unbounded incoming channels and
detached middleware tasks. Its native worker rewrites request IDs to Nostr event
IDs; caller-scoped cancellation therefore needs explicit proof, not inference
from round-trip success. This is a narrow framing module, not new cryptography
or a general MCP implementation. The Rust reference is MIT; TypeScript SDK
0.13.16 at `d00d862ddf27dfd9ddd77f09a47ea2b941b011cf` declares LGPL-3.0-1 and remains
reference material rather than a copied implementation or runtime dependency.

The [ContextVM draft](https://github.com/ContextVM/contextvm-docs/blob/fb5bafc405f7a11b39ff5d46f78c144467df980b/src/content/docs/reference/spec/ctxvm-draft-spec.md)
is JSON-RPC transported in ephemeral Nostr kind **25910**, with `p` addressing
and `e` request-event correlation. It is **MCP over Nostr**, not NIP-90, a DVM kind
allocation, or BigBrotr's DVM profile. MCP tool schemas still define the resource
operations; neither transport supplies BigBrotr's read policy automatically.

## Semantic comparison

| Concern | Native MCP binding | ContextVM binding | Existing DVM |
|---|---|---|---|
| Discovery | `server/discover`, `tools/list`, capability tool | Legacy initialize, tools capability; configured provider/relays; announcements off | Exact profile; optional NIP-89 |
| Delivery | HTTP POST, finite JSON; no GET stream/session ID | Signed 25910 inside required encrypted wrapper; ephemeral request semantics | Durable jobs/results/outbox |
| Identity | Verified OAuth issuer/subject/audience or public principal | Verified **inner** event pubkey | Verified requester pubkey |
| Pagination | Application `next_cursor` and declared snapshot/traversal mode | Same meaning with tighter byte bound | Same strict cut, independently admitted page/job |
| Cancellation | Request disconnect/deadline stops backend | Signed `notifications/cancelled`, scoped to original requester/provider/RPC ID | Durable profile control for accepted job |
| Subscriptions | Not advertised | No subscription tools/CEP-41 | Optional event batches/renewal extension |
| Payment | No payment feature in selected subset | CEP-8 disabled/unadvertised | Separately qualified optional LND workflow |
| Restart | No durable query queue | Active free calls are discarded; client explicitly retries | Durable accepted-job/result recovery |

MCP's `tools/list` cursor pages tool definitions, not data rows. Return as many
whole definitions as fit the adapter bound, in stable tool-name order. Catalog
cursors bind principal, adapter, schema/policy revision, position and a maximum
900-second absolute expiry; incompatible/invalid cursors get `-32602`. They do
not pin event data. Generation must prove that each individual tool definition
fits the smaller ContextVM bound after encoding; otherwise that gateway cannot
be advertised. Compact schema descriptions/local definitions without weakening
validation. Application pagination remains in the tool result. MCP resource notifications
and modern `subscriptions/listen` do not provide the DVM acquisition feed or a
durable signed history, and are not advertised here.

[CEP-8](https://github.com/ContextVM/contextvm-docs/blob/fb5bafc405f7a11b39ff5d46f78c144467df980b/src/content/docs/reference/ceps/cep-8.md)
is draft, with transparent and explicit-gating payment flows. The latter binds
payment to requester and canonical invocation, while leaving failed/lost
execution remedies to server policy and not requiring completed-result replay.
It is not BigBrotr's delivery/refund obligation. No automatic LND/NWC/Cashu or
wallet-spending bridge is selected. CEP-8 adoption would require a buyer journey,
charging unit, exposure deadline and durable reconciliation/refund contract.
Existing paid DVM support retains its own qualification and is not removed.

## Finite application contract: `bigbrotr.tools/1`

| Tool | Exact read-core mapping | Output |
|---|---|---|
| `bigbrotr_capabilities` | Empty object; caller-visible registry/capabilities | Existing document plus application/protocol versions, adapter limits and disabled features |
| `bigbrotr_query` | Required exact resource; collection filters or resource-specific bounded selectors; optional registered projection/order, query mode and page controls | Exact `QueryPage`, or registered bounded `graph_subgraph` result |
| `bigbrotr_count` | Required resource/filters; optional `count_mode="auto"`; compatible view reference from shared contract when supplied | Count, row unit, exactness/method, source and coverage; exact is honored or refused |
| `bigbrotr_get_event` | Full event ID; ordinary current-visibility point lookup | Complete signed event plus metadata or nondisclosing not-found |

Collection resource names are exactly `events`, `event_metadata`, `event_history`,
`event_provenance`, `event_availability`, `author_profiles`, `current_lists`,
`relay_directory`, `relay_measurements`, `relay_documents`, `relay_coverage`,
`relay_overlap`, `relay_availability`, `network_statistics`, `event_statistics`,
`social_statistics`, `scores`, `assertion_lineage`, `content_search`, and
`published_datasets`. `graph_subgraph` is a separate bounded query variant with
no generic count/page-size/cursor. The catalog's C/B distinction and each
resource's required selectors remain authoritative.

Chronological event browsing supports the shared `traversal` default: a fixed
acquisition upper cut and keyset with current authorized eligibility on each
page. It promises neither a globally fixed membership nor list/count equality.
Explicit `snapshot` queries and search/relevance retain their strict mode.
Every result reports mode and evaluation stamp; a count accompanying traversal
is a separately evaluated count, not a claim about every traversed page.

Generate discriminated tool input unions from registered resource schemas; close
application objects to unknown fields. Preserve exact filters, search scope,
lossless fact scalars, sort/projection, graph limits and count support. Collection
defaults remain page 100/max 1000, eight filters and 100 values/field. Explicit
history/provenance grants still apply; unavailable is not empty. No input accepts
SQL, arbitrary URLs/paths, executable expressions or unregistered table names.

There are no management, signing, publication, export-job, payment, file-fetch or
remote-tool-proxy operations. `published_datasets` returns authorized catalog
metadata; large files use existing HTTP artifact routes. Links use configured API
origin/catalog IDs, never storage paths, bearer query parameters or requester
destinations. User event/document content is untrusted evidence, not instructions.

Return the exact resource envelope in `CallToolResult.structuredContent` and one
text block with the same serialized JSON. Account for **both copies and JSON
escaping** in the actual response-byte bound. No generated summary substitutes
for signed fields. Tool read-only/idempotent annotations describe behavior, not
authorization. Modern results include `resultType:"complete"`; modern tool
catalogs use `ttlMs:0`, `cacheScope:"private"`. The legacy encoder omits modern
additions. Application `next_cursor` differs from catalog `nextCursor`; null means
complete. A smaller byte-bounded page preserves the first unreturned row and the
original normalized cursor limits/cut/mode. A row that cannot fit yields
`result_too_large`, never a partial event.

Protocol errors use the selected MCP revision's rules, including `-32602` for
invalid tool arguments in 2026-07-28. Valid execution refusals return
`isError:true` and a typed shared error envelope: `forbidden`, `budget_refused`,
`unavailable`, `cursor_expired`, `cursor_incompatible`,
`temporal_snapshot_expired`, `result_too_large`. Preserve transport-level
401/403/429/503 where appropriate. Errors never disclose SQL/credentials/private
resource existence. Forbidden and absent point lookups have the same shape.

## One authority and budget across adapters

Resolve transport credentials into the same internal principal, grants, scope,
policy epoch/lease, reservation and cancellation context. Only authenticated
transport code constructs this context. Private management may deliberately link
OAuth subjects and Nostr keys to one principal; names/email/claimed `_meta` do
not link identities. Unlinked callers have separate rate buckets but share the
global ceiling, which bounds key-rotation abuse.

Native HTTP defaults to the public principal. Optional protected mode is an
OAuth resource server: one explicit issuer and exact MCP audience, signature/
algorithm/issuer/audience/expiry validation, `(issuer,sub)` mapped to existing
local grants, unknown subjects refused. Publish RFC 9728 protected-resource
metadata and authorization-server discovery. An external authorization server
owns login/consent/token issuance; no second BigBrotr login server is introduced.
Management sessions, arbitrary forwarded identities and wrong-audience tokens
are never MCP authority. Incomplete protected-mode configuration fails validation.
Follow the pinned [MCP authorization specification](https://github.com/modelcontextprotocol/modelcontextprotocol/blob/f56f204f6290f6531b14d5734eb3e0a10f0eb201/docs/specification/2026-07-28/basic/authorization/index.mdx).
Validate any present Origin against an exact allowlist even on loopback; CORS is
not authentication. Use API's trusted-proxy identity rules and public listener.

ContextVM verifies outer signature/recipient/bounds, decrypts, then independently
verifies the full inner signed 25910 event, provider `p`, timestamp and JSON-RPC
shape. Inner author is requester; outer random key and supplied `clientPubkey`
or `_meta` cannot replace it. Invalid/misdirected events get no response. NIP-42
relay access uses its own authority and does not grant application access.

All work intersects shared **16 active/64 queued**, **15 s** engine execution,
**256 MiB** scan, **64 MiB** per-query memory and global output/network limits.
Adapter sublimits are subdivisions, not extra pools. Linked principals share
quotas across API/MCP/ContextVM/DVM/relay. Discovery and malformed input have
bounded rate/bytes/verification cost; reserve a small authenticated control lane
so overload cannot prevent cancellation. Never queue unbounded work before
admission. Account for parsing/escaping/crypto/session/replay memory too.

Preserve the selected mode, `ReadViewStamp`, source/generation, visibility
evaluation and temporal deadline. Traversal obtains each page's current
eligibility stamp under its fixed acquisition bound; it does not reuse an
expired page stamp or claim a cross-page snapshot. Cursors retain 900-second
absolute lifetime and bind principal/profile/
adapter audience, plus ContextVM provider; cross-transport continuation is
initially an explicit incompatibility. Recheck the five-second authority lease
and temporal deadline before serialization, each HTTP write, signing and relay
dispatch/retry. PG failure cannot extend authority. Discard expired results;
already transmitted bytes cannot be recalled. Internal gRPC carries the same
validated context through mTLS/service grants and cannot broaden it.

## ContextVM wire and lifecycle subset

Use pinned CEP-4 **single encrypted signed-inner-event wrapping** with NIP-44,
not the full NIP-59 seal/rumor stack. Require encryption; accept wrapper 1059 or
21059 and mirror incoming kind. Advertise support tags. CEP-19 remains draft;
1059 may persist on relays. Encryption does not imply invisible traffic or
remote deletion of ciphertext. Enforce actual signed/escaped inner size and
NIP-44 limits in addition to the JSON-RPC bound.

Support initialize/initialized, ping, tools/list, tools/call and
notifications/cancelled only. Negotiate exactly 2025-11-25 and refuse queries
before successful initialization. Bounded provider/requester session state holds
version/capabilities, never stale grants. Require unique active JSON-RPC IDs per
requester; other requesters may use the same ID without crosstalk.

Map `(provider, requester, original RPC ID)` to verified request-event ID and
cancellation token. Responses preserve RPC ID and `e` references the **inner**
request event. Signed same-requester cancellation uses that map, never a bare ID,
claimed pubkey or wrapper-event ID. Unknown/completed controls are ignored;
notifications get no response. Stop backend work/new dispatch on cancellation;
the absolute 20-second call limit bounds a lost cancellation message.

Deduplicate verified inner event IDs across relays before admission. Replay does
not extend deadlines or execute a second active query. Keep replay records for
the acceptance horizon; refuse new events when full rather than evicting live
entries. A new signed event/RPC ID is a new admitted query, with a new cut unless
it carries a valid cursor. Free calls do not claim exactly-once execution across
restart; dropped calls require explicit client retry and ordinary admission.

Dispatch one bounded exact signed response only to configured relays. A request
cannot add destinations/NIP-65 discovery. Relay OK means relay acceptance, not
client receipt. At most two retries use identical bytes within the original
authority/20-second deadline. Use common supervised egress and a purpose-limited
ContextVM signer, but do not claim DVM durable outbox obligations for this free
ephemeral binding. Restart discards pending responses; it cannot resume expired
authority.

## Named configuration defaults

These are selected initial limits, not measured performance claims. Deployment
security/listener/secret roots require restart; policy/quotas use the existing
restrictive activation barrier and enclosing configuration authority mode.

| Field | Default / interpretation |
|---|---|
| `mcp.enabled`, `contextvm.enabled` | false in every recipe |
| `mcp.route`, `.protocol_version` | `/mcp`, explicit `2026-07-28` |
| `mcp.auth.mode` | `public`; alternative `oauth2-resource` |
| `mcp.auth.issuer`, `.audience`, `.resource_url` | Unset; all required for OAuth; HTTPS except isolated loopback tests |
| `mcp.allowed_origins` | Empty; refuse any present Origin until configured |
| `mcp.resources` | `public-enabled` registry policy; restricted additions require grants |
| `mcp.max_concurrent`, `.queue_max` | 4 / 16 within shared 16 / 64 |
| `mcp.request_max_bytes`, `.response_max_bytes`, `.call_timeout_s` | 64 KiB / 256 KiB / 20 s total; engine at most 15 s |
| `mcp.subscriptions`, `.tasks`, `.payments`, `.stdio` | Unsupported, false; reject true |
| `contextvm.protocol_version` | Fixed initial `2025-11-25` |
| `contextvm.relays`, `.signer_ref` | Empty/unset; 1–4 explicit allowed relays and signer required when enabled |
| `contextvm.auth.mode`, `.allowed_pubkeys` | `allowlist`, empty; enabled empty allowlist invalid; explicit `public` mode gives unknown keys public grants |
| `contextvm.encryption`, `.accept_wrap_kinds` | `required`, `[1059,21059]`; no plaintext read fallback |
| `contextvm.announce`, `.discover_relays` | Unsupported/false in this initial profile; reject true |
| `contextvm.request_max_bytes`, `.response_max_bytes` | 32 KiB / 32 KiB complete inner JSON-RPC; independently check encoded event/NIP-44 bounds |
| `contextvm.wire_max_bytes` | 128 KiB, intersect configured relay bound |
| `contextvm.max_concurrent`, `.queue_max`, `.per_requester_active` | 2 / 8 / 1 within shared quotas |
| `contextvm.request_rate_per_s`, `.request_burst` | 0.1 / 2 per requester, plus global limiter |
| `contextvm.call_timeout_s`, `.queue_wait_s` | 20 / 2; total includes admission/output, remaining budget bounds engine |
| `contextvm.request_max_age_s`, `.future_skew_s` | 60 / 10 on inner event; no extension of authority |
| `contextvm.max_sessions`, `.session_ttl_s` | 128 / 300 idle seconds; reinitialize after expiry |
| `contextvm.replay_max_entries`, `.replay_ttl_s` | 4096 / 70 from first receipt; refuse before premature eviction |
| `contextvm.dispatch_retries` | At most 2, same bytes/original deadline |
| `contextvm.oversized_transfer`, `.open_stream`, `.payments` | Unsupported/false; never inherit SDK defaults |

Transport input queues and concurrent signature/decryption work must be bounded
by instance reservations before application dispatch. Disabled adapters create
no routes, relay connections, signer leases, payment connections or discovery
tasks. Optionality does not multiply global resource capacity.

## Implementation and release acceptance

P06-T01 registers tool mappings/capabilities; T03 binds gateway cursor audiences;
T05 retains HTTP semantics. **P06-T13 — Finite MCP gateway** follows T01–T06 and
foundation authority/transport. **P06-T14 — Experimental finite ContextVM gateway** follows
T13 application schemas and P07-T01–T04 signing/egress primitives. T14 does not
depend on DVM subscriptions/payments. Proposed modules are application-private
`apps/bigbrotr/src/read/gateways/{mcp,contextvm}.rs`, with generated
`contracts/gateways/v1/`. P08/P09 expose applied status and typed failures; P10
documents the independent client journeys and pins. Existing T12 includes each
gateway conditionally when advertised, without making disabled gateways a
dependency of core API/ArchiveRelay release. Resource readiness vocabulary is
the shared [operations contract](operations-contract.md), distinct from process
health and response completeness.

Actual packaged acceptance uses real policy/read stores and independent clients:
discover → relay-directory query → returned continuation → event lookup;
search/count equivalence at one authorized cut; every advertised registry
variant; empty/disabled/stale/missing generations; oversized complete events.
Follow the actual cursor after byte-bound and scan-only-progress pages.

Negative cases: duplicate JSON keys, SQL/URL/path arguments, batch requests,
version/header mismatch, missing modern result/catalog fields, forged authors,
wrong recipient, spoofed `_meta`, wrong OAuth audience/unknown subject,
unauthorized history, private-influenced public aggregates, expiry/clock skew,
early/duplicate initialize, duplicate relays, cross-author ID/cancellation,
completed-call cancellation and queue/crypto floods. Capture memory and engine
work; an SDK input queue growing before admission must fail qualification.

Revoke a linked principal across all adapters with notifications dropped, then
stop PG; output stops by lease/drain bound. Expire between result creation and
sign/send; inspect actual captured output. Cancel/disconnect slow work and prove
backend stop and permit/pin release. Restart verifies free-call discard and the
separate durable DVM behavior. Excluded payment/streaming/management/discovery
capabilities must remain absent.

Recorded research passes **13 schema checks and 20 upstream wire/in-memory
integration tests**, plus signature/encryption/allowlist negative probes in the
evidence file. It establishes selected protocol behavior, not BigBrotr acceptance,
cross-language interoperability, OAuth deployment fitness or throughput. Release
requires the packaged cases, an independent 2026-07-28 MCP client and a pinned
ContextVM client completing the finite journey. Failed qualification keeps that
adapter unavailable with an explicit reason, not silent protocol substitution.
