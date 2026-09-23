# Explicit scoped data permissions

The private development API accepts `history.read`, `export` and `private-export`
as separate permissions for one complete visibility scope. An administrator does
not receive these permissions automatically. Export permissions do not grant
history reads, publication or signing. The durable export job and destination
approval workflow remain implementation work; this page documents grant commands.

Apply migration 812 explicitly before starting the updated private listener. It
extends the existing grant tables without rewriting historical commands or adding
default permissions. See [schema upgrades](../../operations/schema-upgrades.md).
The current browser boundary is the trusted-host loopback profile described in
[private administration](../../product/private-administration.md).

Authenticate a local account, then submit a command to
`POST /management/v1/sessions/current/reauthenticate` with the password and exact
desired `command`. Use the session cookie, explicit CSRF token, exact Origin and
a fresh nonzero lowercase hexadecimal Idempotency-Key of 32 characters. That proof
is bound to the same session, actor, command key and complete command digest.
Changing even the action requires a different command and new proof.

Send the same command with its returned `proof_id` to
`PUT /management/v1/principals/{principal_id}/grants`. The command includes:

- Target principal and one action from the three values above.
- Complete scope: dataset ID, dictionary kind 4, allocation domain and ordinal.
- Desired enabled state and expected configuration, policy and account-grant
  revisions, encoded as canonical positive decimal strings.
- For enablement, the catalog ID and certified frontier revision containing the
  actual scope allocation. Revocation instead uses `allocation: null`.

Fresh enablement requires the selected native Archive reader and a retained
certified snapshot pin. Revocation can proceed with Archive unavailable. Each
accepted change advances the account revision and the selected action/scope's
generation once. Revoking export leaves a private-export grant and a history-read
grant on that same scope unchanged. Disabled rows retain their generation.

The receipt identifies immutable acceptance through its command key. It reports
`application: unobserved` and `authority: acceptance-only`; it is neither an
authorization lease nor proof that restrictive work has drained. Resolve a lost
reply by retrying the identical command and key under a current session of the
same actor. A historical enable retry cannot undo a later revocation. Reusing
that key for another action is a conflict.

The [OpenAPI contract](../../../contracts/management/v1/openapi.yaml) owns the
exact request and response shapes. Permissions are only one prerequisite for
their consumers: export acceptance still needs current source scope, dataset
permission, an approved destination and atomic job/input retention.
