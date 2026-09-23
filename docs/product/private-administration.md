# Private administration and accessible operation

## Decision

Ship a private management interface as well as the public data dashboard and CLI.
The owner explicitly values accessibility and useful management features. This
adds real security and state-management responsibilities; it is not just Grafana
with a settings page. All three surfaces use the same typed domain operations and
authorization rules. Grafana remains an optional detailed operations tool.

Use the dashboard's frontend stack and shared visual components, with a distinct
private administration API/listener and authentication boundary. Public data
access never grants administrative operations. A deployer can host only the
public frontend or only the private panel; static bundles contain no secrets.

## Pages and useful workflows

| Area | Operator can do | Context the interface must explain |
|---|---|---|
| Overview | Inspect health, current work and storage forecast | Started vs ready vs caught up; fresh, stale and unknown data |
| Services | Start, drain, pause, resume and stop configured supervised roles | Actual dependencies, retained data, pending work and affected capabilities; no arbitrary remote deployment |
| Collection | Configure event filters, provenance and network routes | New coverage epoch, historical gaps, auth scope and predicted extra work |
| Relays | Inspect measurements, enrollment, backoff and exclusions | Remote failure versus local inability to measure; view original evidence |
| Jobs | Start/inspect/cancel imports, exports, derivation and ranking | Durable progress, snapshot/algorithm pinning, restart/resume behavior |
| Queries | Use safe query builder and saved views | Exact/estimated results, generation, budgets, filters and export options |
| Storage | Inspect archive/online/index/pending bytes separately | Hot/cold placement, compaction reserve, capacity trend and finite outage buffer |
| Backups | Configure destination, run backup, verify and rehearse restore | Successful upload versus verified restoration; protection class and last drill |
| Publishing | Configure destinations and named signer references | Which identity and algorithm/viewpoint signs; queued/accepted/rejected outcomes |
| Users/access | Manage roles, API tokens and revocation | Public resource policy separate from operator permissions |
| Configuration | Edit useful settings, preview diff, validate and apply | Effective source, units, reload/restart effect and unavailable/locked fields |
| Audit/help | Inspect changes, correlated errors and guided recovery | Redacted diagnostic bundle and documentation for the exact installed version |

Do not expose every internal library tuning knob as a form field. Common settings
have plain explanations and examples; advanced groups expose documented policies
and budgets without overwhelming the first screen. A disabled feature explains
what it needs. A stale result retains its last useful value with an age label.

## Configuration transactions

Bootstrap paths, database connectivity and listener/security roots come from
operator-owned deployment configuration. Ordinary policy has one instance-wide
authority mode. Managed mode seeds a PG desired-state document once and UI/CLI edit
that versioned authority. Declarative mode keeps the operator document authoritative;
the panel previews/exports patches, and a typed activation accepts its validated
digest/revision. A changed mounted file is staged, not an invisible live override.
The panel shows mode, desired/applied revision, staged drift and locked deployment
roots. It never silently rewrites mounted files. Restrictive emergency runtime
locks are visible, nonpersistent and separate from durable policy. The
[authority transition contract](../specification/operations-contract.md#configuration-authority-and-exact-validation)
defines mode-switch CAS, restart, conflict, restore and bootstrap behavior.

An edit creates a candidate revision with expected previous revision. Validate
schema, permissions, dependency effects, resource envelope and operational changes;
show the diff and consequences; atomically accept the revision by compare-and-set.
Services report applied/draining/restart-required/failed per revision. A revision
being saved is not proof that all services applied it. Conflicting editors receive
a merge/reload explanation. Rollback creates a new revision and cannot reconstruct
provenance that was never collected or undo external publication.

CLI and UI invoke the same idempotent management commands. Long jobs return a
persistent job ID, progress and cancellation endpoint. Closing the browser neither
cancels an import nor loses its job receipt. Cancelling preserves already committed
data and stops new work at a documented boundary. Risky changes use concrete
preview/confirmation; routine navigation and reversible tuning do not require a
ceremony. This is product UX, not additional approval requirements for this plan.

## Access model

Default to one shared instance dataset with resource visibility rules and quotas,
not tenant-isolated independent archives. Use viewer, operator and administrator
roles as initial named permission bundles; authorize actions/resources explicitly
so deployments can later customize roles without duplicating business logic.
Authenticated/private relay data stays restricted through derived facts, exports
and publication. A public count or graph must not leak restricted source data.

Private management defaults to loopback or a private network with authentication.
Bootstrap an initial owner through a one-time local setup mechanism with a short
lifetime; protect and rotate it after enrollment. Never expose unauthenticated
administration because the first user has not yet signed in. Remote deployment
requires TLS; browser sessions use secure cookie/CSRF/origin controls appropriate
to the deployment. API tokens are scoped, hashed at rest, revocable and audited.
Optional external identity integration can be added behind this permission model.
Nostr ownership proof alone does not grant operator rights without enrollment.

Secret forms write protected secret references through a dedicated mechanism,
never ordinary configuration history. Reveal neither current keys nor raw DSNs.
Diagnostics redact credentials, private endpoints and payloads by default. Import
sources and backup destinations use guarded source adapters, not arbitrary shell
commands from a browser. Administrative actions have attributable audit records,
including rejected attempts and partial application.

## Practical acceptance

From a fresh release, enroll the owner, open the local panel, identify one admitted
event and its origin, pause/resume the indexer, see queries remain explicitly stale,
change a budget, start a projected export, reconnect the browser and resume its
job view. Run and verify a backup, restore separately and compare results. Exercise
reader/operator/admin permissions, concurrent config edits, expired credentials,
malicious event content, keyboard-only operation and failure explanations. The
website must teach this same journey using the installed release's real controls.

Success is measured by successful setup and recovery, understandable errors,
documentation task completion, useful research outputs and sustainable maintenance.
Stars are not an acceptance test and feature quantity alone does not establish
usability. A polished small interaction can be more valuable than an obscure
advanced control exposed without an explanation.

## Restrictive-change activation barrier

Saving desired configuration is distinct from a security restriction becoming
fully applied. Exposure, token, egress, visibility and signer restrictions create
a pending security revision. Affected roles invalidate old authorization, drain
or close affected streams and acknowledge the new revision. An unreachable role
must lose its short-lived authorization lease before the change is reported
applied. The UI/CLI reports pending, applied or blocked roles; it never reports
instant global revocation based only on a PG row update.

Every serving/signing/egress action checks a current policy authorization lease;
expired leases fail closed if control state is unavailable. Choose and publish a
bounded maximum activation interval (initial target 5 seconds plus explicitly
bounded in-flight drain), and test dropped invalidation notifications, paused
roles and PG outages. Notifications accelerate invalidation but are not its proof.
Old pagination tokens are explicitly invalidated when their visibility policy
is revoked. Already delivered data cannot be recalled. Long exports recheck
policy at bounded chunks and may terminate with an attributable reason.

## Initial authentication and action permissions

Select local enrolled accounts with server-side opaque sessions as the initial
mechanism. Bootstrap through a cryptographically random one-time owner token in a
0600 local file/explicit CLI output, never routine logs. Initial lifetime: 10
minutes, configurable; successful enrollment invalidates it. Sessions use hashed
server-side credentials, expiry/revocation and the same security revision checks;
initial session maximum 8 hours and idle expiry 30 minutes, configurable. Remote
management requires TLS and secure cookies. External identity and Nostr login
can later authenticate an enrolled principal without changing authorization.

The first browser-session storage profile uses the selected eight-hour/thirty-minute
lifetimes, at most eight retained sessions per account and a twenty-second maximum
between charging a login attempt and the final issuance credential check. This
is not a maximum COMMIT latency. Password work runs
outside SQL transactions; issuance rechecks the complete credential and a separate
account authentication generation after hashing. Deactivation/reactivation and
credential replacement advance that generation; ordinary grant changes do not.
Session issuance generates fresh independent session/CSRF secrets. A lost issuance
response cannot recover plaintext from their hashes: retry is an explicit new
login, subject to the same limits, never an automatic credential reissue.

The initial persistent attempt profile uses 256 account-name hash buckets and 256
socket-peer hash buckets per dataset, with five and twenty attempts respectively
per sixty-second window. IPv4-mapped IPv6 peers share the IPv4 bucket. Unknown names
never create identities or unbounded counter keys. Hash collisions conservatively
deny attempts; successful authentication does not reset the counters. These are
explicit initial implementation limits, not a measured capacity claim. The
configurable session/security profile still requires an explicit accepted-policy
upgrade; adding registry defaults must not silently reinterpret stored documents.
Internal refreshes do not count as browser activity. Revocation is terminal, and
touching after an idle or absolute deadline cannot revive a session. A stored
session observation is not a data-action lease or evidence of completed drain.

The initial HTTP consumer is a trusted-host loopback profile: exact configured IP
and port in Host/Origin, HTTP1 with sixteen owned connections, no keepalive, an
eight-KiB request body and five-second total connection deadline. Its session
cookie is HttpOnly and both session/CSRF cookies are SameSite=Strict, Path=/,
without Secure on this explicitly local HTTP profile. A mutation requires the
exact Origin and an explicit CSRF header bound to that session. Cookies are
host-scoped, **not port-isolated**: another service on the same cookie host can
receive them. Use this profile only where those services are trusted; remote
HTTPS remains separate implementation and acceptance work. Forwarding headers
and hostname aliases cannot silently widen the authority.

The supervisor shares one process envelope with the Control connection, queues,
actual sockets and one password worker. Disconnect does not release hash memory
before wiping/join. Queued requests and completed hashes past the HTTP deadline
cannot initiate issuance; an already admitted SQL operation is collected under
its own authority budget, suppressing a late response. Every actual socket write
checks a suspend-aware delivery window. Shutdown invalidates that gate and joins
sockets/hash work before a graceful Control release. Accepted policy changes
currently require restart; this is not the later applied/draining barrier.
These controls require the executable/engine/browser evidence of P08-T02 before
advertising session or administration readiness. General login audit, configurable
lifetimes, reauthentication, grants and full private administration remain required.

The first session mutation audit profile adds migration0807. New issuance and
explicit logout append `browser.session.issue` or `browser.session.revoke` with
outcome `accepted` atomically with the session change. The actor is the actual
local principal, subject is the non-secret session ID, and configuration/security
revisions reference the validated snapshot. No password, token, credential hash,
raw username or peer address is copied to the receipt. Accepted means committed,
not delivered to the browser; ambiguous COMMIT retains that distinction.

Only a new revocation transition appends logout audit. An identical retry keeps
the original row, while reset/deactivation and pre-upgrade revocations receive
no retroactive logout attribution. Deleting terminal credential rows to enforce
the eight-session cap never deletes their audit. An audit INSERT failure rolls
back the session mutation and makes that Control connection unavailable. Existing
policy documents and bootstrap receipts keep their meanings; no implicit policy
upgrade or historical backfill occurs. This profile does not complete rejected
request audit, the configurable365-day retention/maintenance consumer or audit
capacity admission. Audit rows remain retained until that maintenance path exists.

| Action | Initial permission bundle | Additional requirement |
|---|---|---|
| Read allowed data/status | Viewer | Resource/input visibility permits that principal |
| Pause/resume roles; tune approved budgets | Operator | No implicit exposure, new network route or signer activation |
| Import from configured source | Operator with import grant | Allowed source adapter/root; cannot read arbitrary host paths or traverse symlinks |
| Export public-eligible data | Operator with export grant | Approved destination and current source-generation scope |
| Export private/auth-scoped data | Explicit private-export grant | Dataset permission plus destination authorization; not inherited from generic job control |
| Change exposure, private routes or security policy | Administrator | Preview, activation barrier and attributable audit |
| Configure/activate signer or publisher | Explicit signing-administration grant | Key reference, identity/function policy and public-eligible input generation |
| Manage users/administrator grants | Administrator/owner grant | Reauthentication for privilege changes; no privilege escalation through config |
| Execute arbitrary shell commands | No web permission | Not a management feature |

The first scoped grant command changes one `history.read` grant for a complete
visibility QualifiedRef. It carries the target principal, desired enabled flag,
expected configuration/security/account-grant revisions, and an allocation
selection `(catalog_id, frontier_revision)` when enabling. Disabling carries no
allocation selection: an unavailable archive must not prevent revocation of an
existing grant. The seven initial dataset actions keep their existing meaning.
The private PUT `/management/v1/principals/{principal_id}/grants` now dispatches
this command with an explicit `proof_id`, the same Origin/session/CSRF boundary
and Idempotency-Key used for reauthentication. Path and body target must agree.
Revocation and historical acceptance paths need no Archive. Fresh enablement
selects native allocation only with the explicit deployment root
`archive.reader.enabled=true` (default false), local Control/Archive custody and
separate catalog read credentials. Native prerequisite refusal rolls back without
consuming the proof or stopping healthy Control. The new consumer is awaiting
exact-revision native HTTP qualification; restrictive drain and history delivery
remain required.

Reauthentication binds the same live session, actor and exact command key/digest.
The proof is a server-side identifier requiring the original session and CSRF,
expires within five minutes, and records the credential/authentication/grant and
security/configuration revisions observed after password verification. Issuance
uses the same persistent account/peer attempt limits and bounded owned Argon2
worker as login. Current credentials, session state and `principals.manage` must
be checked again after hashing and when accepting the sensitive command.
The initial808 profile retains at most32 proofs per account. A verified identical
retry within the same session returns its original proof without extending expiry
or repeating audit. A changed command under that live key conflicts. Expired
proofs are removed under the account lock; their audit remains. Proof identifiers
are references, not bearer credentials. The subsequent810 grant transaction must
check the common accepted-command ledger before consuming a proof, preserve
per-scope generation tombstones, and retain pending restrictive application until
affected workers and delivery have actually drained.

The command fingerprint uses SHA256 over ASCII
`bigbrotr/scope-grant-change/1` plus NUL, dataset16, authenticated actor16,
command-key16, target-principal16, scope `(dataset16,kind u16,domain16,ordinal u64)`,
action byte1 for `history.read`, enabled byte0/1, then expected configuration,
security and account-grant revisions as u64. Finish with allocation-present
byte0/1, and when present catalog16/frontier-revision u64. Integers are big endian;
JSON metadata uses canonical positive decimal strings and nonzero lowercase hex
identities. The independent golden vectors cover enable/revoke. Neither a matching
fingerprint nor a proof identifier alone confers authorization.

Default import/export filesystem access stays under configured data roots; object
storage credentials and allowed buckets/prefixes are explicit. Browser jobs never
accept an arbitrary shell command, secret file path or unsigned executable plugin.
A role's ability to start a job is distinct from authority over the job's data.

## Local first-owner transition

The first CLI consumer is `access owner-token issue --username NAME --request-id
HEX --file CONFIG`, `access owner-token status --file CONFIG`,
`access owner-token revoke --challenge-id HEX --file CONFIG`, and
`access enroll-owner --challenge-id HEX --token-ref file:/absolute/path
--password-ref file:/absolute/path --file CONFIG`. These finite deployment
commands use the protected, non-migrator Control credential. They are not remote
owner recovery and cannot replace an enrolled owner. The private HTTP consumer
remains separate acceptance work.

Issue binds the intended lowercase ASCII account name (1–64 letters, digits,
dot, underscore or hyphen) and caller-supplied nonzero bytes16 request ID. It
returns one random bytes32 token as lowercase hex through explicit output.
Only its domain-separated SHA-256 is stored. Repeating that ID resolves metadata
with `secret-unavailable`; it cannot return or mint another secret. A different
ID requires explicit revocation of any pending challenge, including expired
ones. A consumed or revoked challenge remains terminal. One pending challenge
and one initial owner are allowed per dataset. Unknown COMMIT is resolved by
the same request ID/status under the same database lock before any replacement.

Enrollment reads exact token/password bytes from protected regular files;
neither is a command-line value. Passwords contain 12 or more Unicode scalar
values, at most 1024 UTF-8 bytes, and no control characters; do not trim or
normalize them. The token file contains exactly 64 lowercase hex bytes.
Enrollment atomically checks the challenge, ten-minute expiry and attempt bound,
creates the immutable human principal, local credential and explicit dataset
administrator action rows, consumes the challenge and appends its audit receipt.
Thirty-two wrong-token attempts exhaust a challenge. Concurrent or repeated
consumption cannot replace a password or create another owner. Local status
resolves a lost successful enrollment response; retrying enrollment is refused.

The initial administrator bundle contains `data.read`, `status.read`,
`roles.control`, `budgets.change`, `policy.change`, `principals.manage` and
`audit.read`, each scoped to this dataset. It excludes import, export,
private-export, artifact publication and signing administration. Stored grants
are policy input, never a reusable authorization capability: command/output
consumers still require authenticated identity and current restrictive authority.
Enrollment does not activate a dataset security revision, start collection,
create a session, enable publication or issue a runtime lease.

The first credential format is `argon2id-v1`: Argon2 version 0x13, 65536 KiB,
three passes, one lane, 16-byte salt, 32-byte result. A hash admits one CPU item
and its memory before allocation, uses a fallible fixed-size block allocation,
and retains its permit through block wiping and completion. Cancellation cannot
free capacity while hashing continues. The local finite CLI runs one hash; a
future listener must additionally bound peer/account attempts and worker joins.

### Local account inspection

`access me --username NAME --password-ref file:/absolute/path --file CONFIG`
is the first credential verification consumer. It requires the same protected
deployment connection and returns only that account's identity, credential/grant
revisions and stored dataset actions. One bounded database snapshot binds those
fields to the exact `argon2id-v1` credential; a disabled account, wrong password
or absent name has no successful account response. Unknown names still incur
one baseline hash. Password bytes are neither trimmed nor normalized.

This finite, read-only local inspection does not return a session or a reusable
authorization proof. Its result is explicitly an observation; another action must
authenticate and authorize again. A null security revision is reported as null,
without activating policy or turning stored actions into live authority. It
creates no audit row or login attempt state. Remote login, rate limits, session
transactions, rejected-request audit and the restrictive activation barrier remain
required separate consumers; this command must not be exposed as a remote route.

### Scope-grant acceptance boundary

The additive0810 schema and explicitly required0811 correction extend the common
`(actor_id, request_key)` command ledger with `scope-grant.change`.
A fresh command flips one existing scope's state
or enables its first generation. Repeated desired state under a *new* key is a
conflict; revoking an unknown scope creates no tombstone. Accepted commands advance
the target account revision exactly once and the specific scope generation by one.
Disabled rows retain their generation, preventing revoke/re-enable ABA. The original
seven dataset grants remain separate.

Each acceptance atomically retains the command, immutable detail, exact audit and
current grant, advances the account CAS, and consumes the same-session proof. The
SQL boundary recomputes the fixed-width command fingerprint. Actor and target
account locks use principal-ID order. A self-grant validates the proof's old account
revision and its own exact increment separately. Each target revision and each
scope generation is unique in permanent command history, including two commands
attempted in one transaction.

A historical retry still requires a currently valid session and matching CSRF.
It resolves the authenticated actor/key/digest before a new proof, permission,
CAS or Archive lookup; it returns only the original acceptance. It cannot reapply
an old enable after revocation or assert that restrictive work has drained.
Enablement requires actual joined native allocation evidence matching the retained
catalog barrier, snapshot, topology and every certified source bound, including
zero. The accepting transaction checks the active protocol1 pin, complete positive
retention claims and actual Control holder under the shared pin lock, sampling
the clock after waits and again at its final constraint check. Revocation needs no
Archive dependency. The grant route reports only immutable acceptance with
`application=unobserved` and `authority=acceptance-only`, including historical
retries; no receipt means applied drain. Listener startup now requires explicit811,
the command/detail/current-grant table privileges, proof-column UPDATE for row
locks and the two invoker helper EXECUTE privileges. A selected native reader
also checks its actual Control observation/pin/helper privileges before binding,
including recovery-history SELECT and the frontier row-lock column privilege.
No startup migration runs.
Successful native HTTP grant execution, restrictive drain and private history
remain unqualified until their full consumers and practical acceptance exist.

[Native evidence at4b9511d](../implementation/evidence/p08-scoped-grants-native-20260923.json)
qualifies the scoped storage/adapter boundary, including actual password/native
allocation, grant/revoke/retry, delayed expiry and atomic omissions. It does not
complete private administration, authorize delivery or qualify COMMIT-loss recovery.

The native allocation consumer observes one immutable sealed selection, retains
local custody and a manager transaction stabilizing its exact native snapshot,
then registers a certified pin. A joined blocking reader returns owned allocation
evidence; the second acceptance rechecks the live browser session, proof and CAS.
The same application/native resource ledger covers Control, manager, HTTP, hash
work and native output. Control refresh uses absolute deadlines throughout native
work; request expiry never extends acceptance. Signal/authority loss stops HTTP
admission and delivery before bounded native join. Known joined results release
the pin and read-only scratch; unknown termination retains both for recovery.
Cleanup has a separate finite allowance and cannot admit new work. Grant receipts
contain only historical acceptance, so their delivery needs no continued native
pin. They remain unsuitable for history delivery or restrictive-drain claims.
