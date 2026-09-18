# Initial Control runtime

The implementation candidate adds the persistent PostgreSQL Control owner:

```sh
bigbrotr serve --file /etc/bigbrotr/root.toml --roles control
```

Use an existing validated root with Control assigned locally, protected application
credentials, migration 0007, accepted initial configuration and the explicitly
activated first security revision with `administration.enabled=false`. For this
initial backend-only slice, set that value in a fresh candidate **before**
initialization/activation; the default recipes enable administration and must be
refused until its listener is implemented. Editing an already accepted managed
seed does not disable administration. Follow the explicit migration/initialization
commands in [development build](../development/build.md) and the separate
[owner enrollment guide](administration/enrollment.md). `serve` does not initialize,
migrate or enroll. The process emits an `idle` JSON lifecycle record after it has
obtained backend ownership and validated both stored documents. This record is
local status, not a reusable authorization credential.

Omitting `--roles` uses the root's explicit assignment. This initial slice refuses
any selected role other than Control before opening credentials. Selecting a
subset never starts the omitted roles. It opens no listener or native store and
makes no acquisition, publication or read-service readiness claim. The complete
product and the ordinary Compose installation remain in development.

The accepted PostgreSQL configuration/security documents remain authoritative;
changes to a managed seed or staged declarative file do not activate themselves.
Every poll checks both complete documents against the immutable startup roots.
Policy authority lasts at most `runtime.authorization_ttl` (five seconds by
default), measured from request start with the suspend-aware clock. Polling runs
at half that TTL or the ownership heartbeat, whichever is shorter. Only the
separate `runtime.heartbeat_interval` renews the scheduling row. A paused process
checks expiry before attempting another operation.

SIGINT or SIGTERM while idle performs a bounded fenced release, closes and joins
the driver, then emits `stopped` and exits 0. Cancellation during an operation
closes the connection and exits incomplete; a lost release/acquisition COMMIT
response remains unknown. Database/policy failure exits without continuing work.
This candidate requires an explicit restart (or an external supervisor) after
recovery; it does not yet implement automatic reconnect/backoff. A fresh process
must obtain the actual backend lock before replacing prior owner metadata.
Never infer exclusion from a scheduling TTL or from the lifecycle record alone.

Exit codes: 1 unavailable/I/O, 2 usage, 3 invalid role/configuration/policy or
concurrent owner, 4 cancelled/unavailable ownership or unknown durable outcome.
Messages omit credential paths and database error details. Runtime records are
private operational output; do not publish them as a public health API.

The [native acceptance matrix](../../experiments/control-runtime/README.md) is a
required gate. Tests described there are not executed evidence until a report
records their exact revision and outcome. Public grants/revocation, native writer
handoff, maximum-document/resource behavior and the complete installed journey
remain separate requirements.
