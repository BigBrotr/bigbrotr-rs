# Initial Control runtime

The initial implementation runs the persistent PostgreSQL Control owner:

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
at half the remaining validity or the next ownership heartbeat, whichever is
sooner. Time spent waiting for a response consumes validity; it never extends
the request-start expiry. Only the
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
Lifecycle and error output cannot hold up supervision: a bounded diagnostic
queue writes independently. Shutdown waits at most 200 ms for output after
backend cleanup, then terminates the process and its diagnostic writer. A
blocked or failed destination may lose records and changes an otherwise
successful exit to 1, even when the owner release committed. Inspect durable
owner state to resolve that distinction; missing output is not a rollback.

The [native acceptance evidence](../implementation/evidence/p01-control-runtime-native-20260918.json)
records 16 actual executable/PostgreSQL scenarios at `6be8515`, with independent
inspection of 240 relations and all seven process outputs. The
[reproducible matrix](../../experiments/control-runtime/README.md) defines that
scope. Public grants/revocation, native writer handoff, maximum-document/resource
behavior and the complete installed journey remain separate requirements.

The [monitor regression evidence](../implementation/evidence/p01-monitor-corrections-native-20260918.json)
adds delayed successful acquisition/polls, actual full output pipes and bounded
process termination at `2c0dfc1`, plus all sixteen prior process cases. It also
confirms an open maximum-policy defect: a candidate can fit compact JSON while
exceeding PostgreSQL's JSONB text limit. Maximum-document acceptance is incomplete.
