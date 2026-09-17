# PostgreSQL COMMIT fault proxy

This standard-library Python experiment supplies a controlled wire fault for
P02-T01 migration tests. It is not a production proxy, SQL implementation,
transaction receipt authority or substitute for PostgreSQL execution. Use only
an inventoried disposable PostgreSQL lab with synthetic credentials and data.
Both listener and upstream address are hard-coded to IPv4 loopback. There is no
remote address or credential option.

The proxy accepts one connection, closes its listener, forwards protocol 3.0
startup/authentication and bounded messages, then exits. It recognizes only a
frontend simple Query whose entire NUL-terminated body is `COMMIT`, with ASCII
case/whitespace and one trailing semicolon permitted. Combined statements,
comments, `END`, `COMMIT AND CHAIN` and extended-protocol COMMIT are not targets.
Ordinary extended messages are forwarded without interpretation. SSLRequest,
GSSENCRequest, CancelRequest and other startup versions are refused before an
upstream connection is opened. The tested client must explicitly use plaintext
on its declared isolated loopback boundary; this is not a TLS downgrade helper.

The selected tokio-postgres 0.7.18 `Transaction::commit` uses
`Client::batch_execute("COMMIT")`; `simple_query.rs::encode` emits a frontend
Query. The wire framing and CommandComplete interpretation follow PostgreSQL 16's
[message formats](https://www.postgresql.org/docs/16/protocol-message-formats.html)
and [simple query flow](https://www.postgresql.org/docs/16/protocol-flow.html).

## Modes and exact fault boundary

| Mode | Observable proxy behavior |
|---|---|
| `pass` | Forward every allowed frame unchanged; control for the experiment |
| `before-forward` | Read and recognize COMMIT, then close both sockets before writing any of that frame to the upstream transport |
| `after-complete` | Forward recognized COMMIT; when its backend `CommandComplete` body is exactly `COMMIT\0`, suppress that whole frame and close both sockets. A coalesced ReadyForQuery frame is also never forwarded |
| `hold` | Retain the COMMIT frame before forwarding, emit `held`, keep the upstream connection open, and await an explicit stdin action or deadline |

The proxy does not invent PostgreSQL responses. A backend ErrorResponse or a
CommandComplete for ROLLBACK does not trigger `after-complete`. No observed matching
fault is a failed experiment, even if the client exits. A second recognized
COMMIT in this one-session fixture is refused. Test retries through a fresh proxy
or directly against the same PostgreSQL catalog as the scenario requires.

`hold` is also the precommit boundary for the actual migration runner: its DDL
and ledger INSERT finish before it sends COMMIT. While held, a separate direct
observer can inspect the original backend and race a migration/inspection against
its transaction lock. Then send `close` to prevent forwarding or `release` to
allow the actual COMMIT. The proxy cannot keep a server alive: server idle/statement
limits, backend termination or transport failure can end this window. The current
runner's idle-in-transaction timeout is 10 seconds, so coordinate the observer
promptly. Client disconnect during a hold does not itself release COMMIT; the
upstream remains held until another terminating event or explicit control.

## Invocation and local control

Run from a clean exact source revision. Choose an owned upstream loopback port;
check that any explicitly chosen listener port is free. The default listener port
is zero, allowing the OS to choose an unused port. For example:

```sh
python3 experiments/control-migrations/fault_proxy.py \
  --upstream-port 55432 --mode after-complete --deadline-seconds 120
```

The first `ready` JSON line reports the chosen `listen_port`, process ID, source
file SHA-256 and limits. Point only this experiment's actual migration client at
that endpoint, retaining the original database/user/credential reference. The
upstream database port and all fixtures remain owned by the external lab driver.
A separate observer connects directly to the upstream, not through this proxy.

For `hold`, keep stdin as a pipe or terminal. The only accepted lines are:

```text
status
release
close
```

`status` emits safe current counters. `release` is valid only after `held`; it
unblocks exactly the retained COMMIT once. `close` closes the session, and when
held reports `held-close`. Unknown/oversized commands stop the experiment without
printing their contents. Stdin EOF never releases COMMIT; the total deadline
still closes it. A non-pollable stdin is allowed for automatic modes but refused
for `hold`. Use a driver that continuously drains stdout and writes control lines
only after observing the required event. There is no second control listener,
network administration endpoint, control file polling or credential-bearing URL.

Stdout consists of JSON lines with format `bigbrotr-pg-fault-proxy/1`. `ready`,
`held`, `released`, `fault`, `status` and final `receipt` are safe metadata. The
final receipt includes fixed reason code, message-tag counts as two-digit hex,
frame count, declared wire bytes, recognized/forwarded COMMIT counts, backend
COMMIT completions and completions handed to the client transport. None of these
fields establishes that a client received a packet or that storage is durable.
No startup parameters, username/database strings, SQL, row bytes, authentication
payload, BackendKeyData secret, raw exception or payload digest is recorded.

Exit 0 means the selected fault was injected, or the `pass`/explicitly released
hold session ended normally. Exit 3 means timeout/refusal/missing fault or another
incomplete experiment; argument errors use argparse's exit 2. Missing final
receipt is always incomplete. SIGINT/SIGTERM request bounded closure and exit 3.
Inspect the exact counters and independent database state, not just the exit code.
A backend COMMIT receipt is never a substitute for the scenario's external ledger,
schema and restart checks.

## Bounds and backpressure

| Resource | Default / hard configurable range |
|---|---|
| Accepted data sessions | One total; listener closes after first accept |
| Startup packet | 64 KiB maximum, checked before body allocation |
| Frame length including its four-byte length field | 1 MiB; `--max-frame-bytes` 8 bytes to 16 MiB |
| Declared wire bytes across both directions, including startup | 64 MiB; `--max-wire-bytes` 64 bytes to 1 GiB |
| Tagged messages across both directions | 20,000; `--max-messages` 1 to 1,000,000 |
| Whole data session including wait for first connection/hold | 120 seconds; `--deadline-seconds` 0.1 to 600 |
| Upstream connect / final socket drain | 5 seconds / 2 seconds |
| Control line / total control commands | 32 bytes / 32 commands |

There is one forwarding coroutine per direction. Each validates the length before
reading a body, retains at most one current frame, writes it and awaits transport
backpressure before reading the next. The asyncio stream limit is 16 KiB; Python
transport chunks, frame copies, socket buffers and interpreter overhead are
additional memory. These finite logical limits are not a measured RSS guarantee.
Limits include forwarded passwords and SQL without logging their contents. They
also bound startup and malformed/unfinished frame work. Half-close is treated as
session termination; no general-purpose pipelined proxy behavior is advertised.
The controller must drain stdout; a stalled local output sink is not a qualified
fault/deadline case. Monotonic timings are local experiment bookkeeping, not a
policy lease or suspend-aware authority clock.

## Local verification and required PostgreSQL evidence

```sh
python3 -m unittest discover -s experiments/control-migrations \
  -p test_fault_proxy.py -v
```

The tests start the actual proxy executable and independent fake peers over real
loopback sockets. On macOS arm64 with Python 3.9.6, 18 cases passed during this
slice: transparent/fragmented auth and Query frames; before/after COMMIT boundaries;
coalesced reply suppression; held close/release/status; errors/SQL substrings/
extended messages as negative controls; frame/startup/control length refusal;
wire/message budgets; truncated frames; unsupported TLS negotiation; deadlines;
and refusal of a second session. Password, backend-key, SQL and error sentinels
are asserted absent from proxy output. Initial tests failed before the proxy
existed. Raw local logs/inventory belong under ignored `.research/`.

**Actual PostgreSQL, the BigBrotr executable, SCRAM/TLS interoperability, durable
absence/presence, engine crash/restart and native Linux execution are NOT_RUN in
this slice.** The integrating driver must record exact source/binary/engine/config
identities, run both committed and absent lost-reply branches, independently
inspect the same database under the migration lock, and compare receipt checksum
and schema together. An old backend still holding that lock must keep resolution
unavailable. Run a normal pass control, demonstrate the fault counters actually
changed, and preserve all unavailable/unknown outcomes. No product test hook or
faked engine receipt is supplied by this experiment.

## Real migration fault driver

`fault_cases.py` consumes an already running lab from `lab.py`. It does not create
or restart PostgreSQL, alter authentication, rebuild the executable, remove data,
or use SSH. Run from the lab state's same clean evolving Git source checkout;
`--build` names the existing exact artifact directory whose receipt and binary
hash must match `resources.json`. The lab's original client build/source/runtime
mounts are inspected before another container is created. The initial driver development referenced
application revision `6f7c03d` and PostgreSQL 16.15. A newer clean build can be used
when its receipt matches the lab state and its migration SQL matches the harness;
the actual application and harness revisions are always recorded separately.

```sh
python3 experiments/control-migrations/fault_cases.py \
  --root /owned/lab/runtime-v2 \
  --build /owned/lab/build-6f7c03d \
  --run-name faults_a
```

Use actual inventoried paths. The run name contains 1–20 lowercase letters or
underscores. Existing output directories, databases, configuration files and
container names are refused. PostgreSQL and the original client must already be
running, with host authentication explicitly SCRAM, including loopback. The
original laboratory that inherited Docker initdb loopback trust is not an input
for these SCRAM fault cases. Run sequentially with other laboratory work; a
concurrent PostgreSQL restart or source/artifact change invalidates this run.

The driver creates one additional labeled container named from the state's
client plus `-fault-<run-name>`, sharing only the PostgreSQL network namespace.
It uses the original readonly build/source mounts and writable owned runtime
mount, 2 CPUs, 4 GiB memory and memory+swap limit, 256 PIDs, readonly root, no
capabilities, no-new-privileges and no published ports. It verifies those limits
and records a separate random ownership label. All clients created for these
cases, including concurrent inspectors and retries, run in that container.
`finally` stops only this exactly labeled container and retains it for inventory;
it never stops the original client or PostgreSQL. A local `docker exec` wrapper
exit is not treated as remote process exclusion.

Every case creates its own database and two immutable configuration paths: direct
`127.0.0.1:5432` and the proxy's chosen loopback port. No persistent file is
rewritten to switch an in-flight client from one endpoint to the other. The actual
CLI performs a migration through the proxy. The expected observations are:

| Case | Proxy / real CLI | Fresh direct resolution and one retry |
|---|---|---|
| `pass` | COMMIT and reply forwarded; applied, exit 0 | Version 1; retry current, zero applied, exact schema/data and receipt unchanged |
| `before` | COMMIT withheld and sockets closed; outcome-unknown, exit 4 | No control namespace; retry applies exactly version 1 |
| `after` | Backend COMMIT completion observed, reply suppressed; outcome-unknown, exit 4 | Exact version 1 receipt/schema; retry current without rewriting either |
| `release` | Hold COMMIT; observe an actual inspector waiting on its shared advisory lock; release and receive applied, exit 0 | The already-waiting inspector must see version 1; fresh direct resolution/retry preserve it |
| `close` | Hold COMMIT; observe the waiter, its real five-second lock timeout/refusal, and the original owner still holding the exclusive lock; close, causing outcome-unknown, exit 4 | Fresh inspection sees absence; retry applies exactly version 1 |

For each hold, `pg_stat_activity` and `pg_locks` must show the same owner PID idle
in transaction with the exact granted ExclusiveLock and a different inspector
PID with an ungranted ShareLock and `Lock/advisory` wait event. The checks use the
actual migration lock pair and two-int advisory namespace. They never infer a
lock wait solely from elapsed time. In `close`, the original owner must still be
observed after the inspector refuses; otherwise the scenario is incomplete.
The ten-second idle timeout remains unchanged. A slow host that misses this
window produces FAIL/UNKNOWN, never a substituted successful case. PostgreSQL's
[wait-event catalog](https://www.postgresql.org/docs/16/monitoring-stats.html)
defines the lock observation.

A fresh actual `migrate inspect` resolves each outcome under the product's
transaction lock. Independent psql then checks namespace absence or all ten
expected control tables plus the exact receipt checksum/name/version/compatibility
metadata. That checksum comes from the original binary source revision's SQL,
which must still match the harness checkout. The driver also records
`lab.schema_digest`; because that pg_dump includes data, this is labeled
`schema_and_data_sha256` and compared within one database across a current retry,
not between databases with different receipt timestamps. Backend disappearance
is observed after resolution and retry. No committed-absence conclusion follows
from proxy socket closure alone.

The new `<run-name>-faults/report.json` retains each step, safe proxy events,
actual CLI envelopes, external rows/digests, process-lock observations, source/
artifact/engine identities and stopped container identity. Updates replace this
owned report atomically. A failed scenario stops the run; later cases remain
NOT_RUN, partial observations remain, and unknown execution/cleanup is explicit.
Proxy/concurrent-client stdout and stderr are capped at 64 KiB each; only closed
metadata and parsed migration envelopes are saved. Raw SQL, passwords, backend
keys and raw exception/stderr bytes are never saved. SQL/normal CLI/dump calls
reuse the existing finite lab helpers. Every required case must be PASS for exit
0; missing receipts, empty observations or an unconfirmed container stop prevent
that result. SIGKILL/host loss may leave the last report RUNNING and resources
retained; that is incomplete evidence requiring operator reconciliation.

Driver status at implementation: **actual PostgreSQL cases NOT_RUN**. Local checks
exercise process-output bounds, timeout refusal, safe metadata validation and
exclusive file creation, plus Python parsing/help. Those checks do not simulate a
successful database scenario or qualify commit recovery. Root's exact-source
native run must supply the five real outcomes before this driver contributes any
P02-T01 acceptance evidence.
