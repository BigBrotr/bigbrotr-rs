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
