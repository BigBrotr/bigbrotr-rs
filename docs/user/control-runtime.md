# Initial Control runtime

The initial implementation runs the persistent PostgreSQL Control owner:

```sh
bigbrotr serve --file /etc/bigbrotr/root.toml --roles control
```

Use an existing validated root with Control assigned locally, protected application
credentials, the current explicit Control migrations, accepted initial
configuration and an activated security revision. Accepted
`administration.enabled=true` additionally requires migration0806 and the browser
session SQL privileges, then binds the configured loopback private listener.
Its initial routes provide login, self inspection and logout; they grant no data
read or management-command authority. `administration.enabled=false` starts no
listener. Editing an already accepted managed
seed does not disable administration. Follow the explicit migration/initialization
commands in [development build](../development/build.md) and the separate
[owner enrollment guide](administration/enrollment.md). `serve` does not initialize,
migrate or enroll. The process emits an `idle` JSON lifecycle record after it has
obtained backend ownership and validated both stored documents. This record is
local status, not a reusable authorization credential.

When the deployment assigns Archive locally, Control first inventories every
issued publication intent through a fixed startup cutoff, in pages of 64. The
`archive_intents` field reports that cutoff, observed prefix and page count.
`resolution=not-performed` and `publication_authority=not-granted` are explicit:
a nonempty inventory contains recovery obligations; an empty inventory grants no
Archive readiness. Historical catalog mismatches or holes refuse before `idle`.
Policy/owner supervision continues between pages, and a signal or failed query
closes and joins the driver. Control opens no catalog connection for this inventory. It first opens the
explicitly registered local coordinator state and holds its permanent custody
through driver join. Run the separate [state initialization command](archive-initialization.md)
after catalog installation/recovery; missing or incompatible bindings refuse
before connecting to Control. The binding does not resolve pre-protocol attempts. A deployment without a local Archive assignment reports
`state=not-selected`. This addition does not enable `--roles archive`.

After the actual Control claim, the current startup also requires the
exact SQL coordinator binding established by the separate `archive
coordinator-initialize --file PATH` command. Missing migration/binding or a mismatch
refuses before inventory/idle without creating a binding. The earlier schema9
inventory could observe pre-protocol rows; this stricter startup preserves them
and refuses their implicit adoption. No complete recovery route for that historical
case is advertised. The [eleven native command/startup cases](../implementation/evidence/p02-coordinator-cli-native-20260919.json)
pass at `5050999`. Older inventory evidence below retains its original revision and
scope; neither result enables Archive dispatch.

Omitting `--roles` uses the root's explicit assignment. This initial slice refuses
any selected role other than Control before opening credentials. Selecting a
subset never starts the omitted roles. It opens no native store and makes no
acquisition, publication or data-read readiness claim. The complete
product and the ordinary Compose installation remain in development.

To serve the initial private panel from that same listener, build the
[static bundle](../development/build.md#private-browser-consumer) and set deployment
roots before starting Control:

```toml
[administration.assets]
directory = "/opt/bigbrotr/private-panel"
base_path = "/"
```

The directory must be absolute with no symlink in any path component. The default
null directory leaves the three session API routes available without a panel.
A bundle built for `/control` instead requires `base_path="/control/"` and is
opened at `/control/` (including its trailing slash). Static and API requests use
the configured literal loopback IP and port; `localhost` aliases are refused.
This HTTP profile assumes trusted services on the same host: browser cookies are
host-scoped, not isolated by port. Remote TLS remains a separate implementation gate.

An explicitly configured bundle is verified before connecting to Control, even
if accepted policy subsequently disables the listener. Missing, mismatched or
oversized assets fail startup. All bytes stay in a shared 20MiB process-budget
reservation; per-file limits are2MiB, the bundle is at most16MiB/128files and its
manifest at most64KiB. The ordinary login must still fit the remaining envelope.
Restart explicitly to install another bundle; changing files beneath a running
process does not change its verified bytes. The private `idle` record reports
base path, counts and manifest digest, never the host directory. It is an
integrity observation, not release authentication.

The panel supports one observed session, explicit refresh and logout. It does not
poll to keep idle sessions alive. A timeout during login/logout displays an
unconfirmed outcome; inspect the current session or explicitly retry the intended
action. Local expiry and changes in another tab clear prior observations. A
static loading/reload notice remains available if the startup script fails.
The full administration workflows remain under implementation.

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
confirmed the then-open compact-JSON/JSONB size mismatch. The later
[JSONB limit qualification](../implementation/evidence/p01-jsonb-limit-native-20260918.json)
records its correction and exact boundary cases; the earlier report is historical
evidence, not an outstanding MON-005 failure.

[Startup inventory qualification](../implementation/evidence/p02-control-archive-inventory-native-20260919.json)
at `a010c15` (harness `d32d303`) exercises eight actual executable cases, including
130 retained intents, restart, historical catalog mismatch and cancelled startup.
A blocked PostgreSQL read can outlive local driver termination; its session fence
continues to refuse a successor with Busy until the backend actually exits. The
case observes that fence, releases the owned test blocker, then verifies a fresh
owner. It does not treat local driver join as remote backend termination.

The [coordinator-state native evidence](../implementation/evidence/p02-coordinator-state-native-20260919.json)
at `952fd9a` with harness `f822978` records 16 actual CLI/Control cases, 98 passing
Rust tests plus one explicitly ignored holder fixture, and the bounded 134-intent
adapter. Actual bind aliases, live contention, SIGKILL/restart, partial bindings and
blocked inventory cancellation are exercised. The local fixture creates an
initialization reservation only; it does not install a native scientific catalog.
No pre-protocol history adoption, Archive dispatch or protection is qualified.
