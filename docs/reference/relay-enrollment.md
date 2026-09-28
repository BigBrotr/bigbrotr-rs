# Initial operational relay enrollment

P02-T01 Step6 adds a bounded no-auth command over the existing private
[endpoint registry](relay-endpoint-registry.md). This is a storage/adapter
implementation with [initial actual-engine evidence](../implementation/evidence/p02-relay-enrollment-native-20260928.json);
[COMMIT/expiry/lock-order evidence](../implementation/evidence/p02-relay-enrollment-faults-native-20260928.json)
qualifies those command boundaries. [Focused relational evidence](../implementation/evidence/p02-relay-enrollment-relations-native-20260928.json)
adds twelve live-owner omissions/mismatches with valid controls and complete
rollback checks. The installed acquisition service, HTTP/CLI
integration, restrictive application, socket grants, protected
access, multiple selected CIDRs and overlays remain selected work.

`relay.enrollment.change` accepts an existing EndpointId, a complete visibility
QualifiedRef, one direct route, the desired enabled flag, an explicitly nullable
expected enrollment generation and positive configuration/security revisions.
It requires a current browser session, matching CSRF and, for fresh commands,
the dataset `policy.change` permission. `principals.manage` alone does not permit
this operation. No new permission bundle or reauthentication proof is introduced:
the command changes source policy rather than a principal's privileges.

The visibility reference must name kind4 in this dataset. It is declared intent,
not evidence that the dictionary mapping exists or is public. This command may
run with Archive unavailable. Before requesting events, the acquisition issuer
must resolve the exact committed descriptor and current source-to-visibility
permission. Admission independently rechecks that authority. A private route
cannot produce public eligibility even if a caller names a public visibility
reference. Reader scope grants retain their separate native allocation contract.

A public/no-auth access context has null attribution label and is shared for the
same complete visibility reference. Its operational AccessScopeId is a nonzero
bytes16 identifier, distinct from an archive QualifiedRef. It does not contain an
endpoint, route, process or challenge. Changing visibility creates a separate
access/enrollment key; it does not withdraw the old enrollment. Provenance-off
acquisition must not allocate an archive endpoint/access mapping simply because
these operational identities exist.

The initial route is a closed `{"class":"public"}` or
`{"class":"private","cidr":"10.2.0.0/16"}` record. A private CIDR has clear host
bits, an ordinary IPv4/IPv6 prefix length and no mapped IPv6 spelling. Equivalent
IPv6 text normalizes to the same bytes. Public routes require current public-route
enablement. Fresh private enablement must fit one complete deployment root rule
matching the endpoint or its exact hostname, effective port and entire CIDR.
Several adjacent roots are not implicitly combined to approve a larger request.
Withdrawals and historical retries do not require a removed root exception.

At each actual attempt, static destination policy intersects that exact endpoint
and selected CIDR with the current deployment roots. Public and private never
fall back to one another. Both root and selection independently require a
single-address exception for metadata/link-local destinations; narrowing one
cannot upgrade a broad rule on the other side. Policies retain the complete
endpoint path/query and share immutable root rules through Arc rather than cloning
up to256 full endpoint strings per attempt. These static checks grant no socket,
authentication, quota, reachability or visibility authority.

Migration0019 appends after0003 and preserves the33 previously applied checksums.
`relay_access_contexts` and `network_routes` are immutable operational identities
created with their first accepting command. RouteId is nonzero bytes16, scoped to
the dataset and endpoint. Full versioned route bytes determine equality: v1 public
is byte00; private is byte01, address family04/06, prefix length and4/16 network
bytes. Unique index values are bounded by19bytes; no endpoint URL enters a B-tree
key. These are operational bytes, not an archival kind6 descriptor codec.

`relay_enrollments` has one row per dataset/endpoint/access context. Generation1
requires first enablement and null expected generation. Withdrawal retains the
row and advances its generation. Re-enable and effective route changes advance
again without wraparound. Unknown withdrawal, stale generation and unchanged
desired state conflict. A withdrawal must name the current route; it cannot
silently change that route too. Bans remain independent policy and must dominate
a later action issuer; this command does not clear one or start validation work.

Acceptance uses the existing actor/key command ledger, immutable detail and audit
in one synchronized transaction. The command digest is SHA256 of:

- `bigbrotr/relay-enrollment-change/1\0`;
- dataset, authenticated actor, command key, endpoint and visibility dataset
  (five bytes16 values), kind4 as big-endian uint16, visibility allocation domain
  as bytes16 and ordinal as big-endian uint64;
- enabled byte00/01, expected generation (zero for null), expected configuration
  and security revisions (three big-endian uint64 values), then exact route bytes.

The typed adapter acquires actual Control, dataset, actor account and session
locks before domain rows. Queries after the separate dataset lock use READ
COMMITTED snapshots. Named multiline statements keep provisional writes and
COMMIT visible; there is no generic command framework. Deferred checks require
exact command/detail/audit/current state, identity creation attribution, current
heads, the actual owner, current session/account revisions, permission and
unexpired session/owner scheduling authority. An internal xid8 prevents a second
transition for the same enrollment in one transaction, even after early constraint
evaluation. It is not a portable source identity.

PostgreSQL [constraint triggers](https://www.postgresql.org/docs/16/sql-createtrigger.html)
can be forced early with SET CONSTRAINTS. The adapter preserves initial deferral;
raw SQL that forces evaluation and then waits cannot claim commit-time expiry.
The relational digest helper reads identities and is STABLE, not falsely
IMMUTABLE; the fixed binary route validator is immutable. See PostgreSQL16's
[volatility contract](https://www.postgresql.org/docs/16/xfunc-volatility.html).

Historical retry authenticates the current actor/session and CSRF, then resolves
the original exact key/digest before fresh permission, CAS or root checks. It
returns only original acceptance and does not touch session idle time, rewrite
history or reapply an old enable. No receipt means completed restrictive drain.
Later actions must bind current enrollment generation and route identity as well
as security policy, close on invalidation/expiry and retain actual socket custody.

Known business refusals roll back all provisional writes. Cancellation, backend
failure or lost COMMIT poisons the Control session; its owner retains and joins
the driver before releasing resources. A received BB001 expiry is an explicit
rejected COMMIT; transport loss remains CommitUnknown. Fresh ownership plus the
same authenticated actor/key/command resolves that ambiguity.

The [laboratory recipe](../../experiments/relay-enrollment/README.md) distinguishes
implemented cases from executed evidence. Whole-step and M1 acceptance remain open.

## Finite current source operations

The initial `ControlSession::run_relay_source` consumer holds Control, dataset
and current enrollment locks through one borrowed direct socket operation. It
requires migration0020, the exact enabled current/historical generation and route,
a no-auth access context and its full committed `StoredVisibility` reference.
It decodes the retained normalized endpoint and canonical route without accepting
a caller replacement. Public visibility refuses private routes and query-bearing
URLs; an explicit public query exception is not implemented in this profile.

Both complete accepted documents must be v2 and independently pass the trusted
source validator. The Sync configuration consumer restores them against actual
deployment roots, checks selected/enabled Sync, exact fixed-list membership and
endpoint/host bans, then intersects the selected route with current roots. These
decisions and the decoded data do not escape as a reusable grant. The current
consumer is a finite real-engine probe; installed Sync integration remains open.

The local lease starts before schema/lock I/O, uses only positive actual Control
remainder and never exceeds five seconds or the caller deadline. Each network
poll additionally checks observed backend loss, including decrypted TLS buffers;
replacing an attached liveness predicate permanently refuses the attempt. Socket
and callback work drop before rollback releases locks. Delivery after rollback
checks the client, original deadline and lease again. Cancellation or uncertain
cleanup poisons the session until actual driver retirement. Undetected partitions
remain bounded by the lease; arbitrary synchronous callbacks cannot be preempted.

No persistent format changes accompany this adapter. Subsequent admission still
needs current source/policy checks; this is not a WebSocket, coverage or complete
signed-event journey. The native profile and first three actual engine scenarios
passed at8c4d12c; see the [bounded report](../implementation/evidence/p04-source-transport-native-20260928.json).
[Actual cleanup schedules](../implementation/evidence/p04-source-cleanup-native-20260928.json)
qualify shorter owner expiry, rollback timeout and late-ready delivery, including
a discriminating negative deadline variant. [Seven concurrent SQL schedules](../implementation/evidence/p04-source-races-native-20260928.json)
qualify post-wait current source/policy reads and locks through socket drop and
rollback in both orders. Privileged clone-only writers are not concurrently
authenticated management commands. Installed acquisition and capacity remain open.
