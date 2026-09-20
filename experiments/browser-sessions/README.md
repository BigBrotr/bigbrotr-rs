# Browser session implementation experiments

These fixtures require separately inventoried, isolated PostgreSQL and trusted
exact-Git application source. They never operate an existing installation. All
credentials are synthetic and remain in protected ignored laboratory paths.

`native.py` runs the storage adapter fixture `browser_sessions_probe`. Its qualified
source/engine/artifact identities and practical limits are in
[the storage evidence](../../docs/implementation/evidence/p08-browser-session-storage-native-20260920.json).

`http_native.py` builds the actual `bigbrotr` executable and `browser_http_probe`,
runs clock/TCP tests and scoped Clippy, then executes the fixture against native
PostgreSQL. The fixture uses the actual migrate, initialize, activate and enrollment
CLI before starting `serve --roles control`. Raw TCP clients exercise the real
router, HTTP parser, cookie issuance and Control transactions; no in-memory server
or substitute hasher satisfies this journey.

The HTTP fixture checks missing migration and one missing table privilege before
bind, accepted-policy/file drift, Origin refusal before attempt charging, wrong
password, login/self observation, two mixed session/CSRF pairs, continuous four-
client traffic for twelve seconds, same-cookie process restart, a slow body whose
admitted SQL finishes after the HTTP deadline, confirmed logout, signal with
incomplete sockets and a policy-head change observed inside session SQL. A lab-only
trigger records entry before its delay; unchanged session rows and a subsequent
real self request distinguish collected SQL from a poisoned Control connection.
The independent PG collector still has to verify durable state and transport
cleanup; successful exit alone is not acceptance.

[Native HTTP evidence](../../docs/implementation/evidence/p08-browser-http-native-20260920.json)
records ten actual executable/engine scenarios at5a4e0d1, independent terminal
SQL and complete owned-resource cleanup. Its small fresh workload and precise
failure schedules do not qualify the missing product journeys below.

The fixture is not a browser UI test. Browser cookie-host behavior, active-hash
shutdown, ambiguous session COMMIT, PostgreSQL crash with live cookies, account
lifecycle/action grants, login audit, remote TLS, general drain and mixed/aged
capacity qualification remain required. Do not label missing cases PASS.

The subsequent `--faults-only` fixture mode waits for at least32MiB resident growth
in a fresh actual Control process after a login request, stops that owned process,
confirms the growth remains resident and no session has been issued, then queues
TERM/CONT or kills the parent. It observes client closure, process/backend
termination, no issuance and a successful explicit successor login. This Linux
measurement does not identify an exact instruction inside Argon2 or establish a
universal shutdown bound. [Native fault evidence](../../docs/implementation/evidence/p08-browser-http-faults-native-20260920.json)
records three cases at661b54b plus the separate actual Management::stop join
discriminator; failed fixture paths do not establish a TCP thread join. The driver
compares all source hashes against the qualified baseline except the explicit
fixture/docs allowlist; no modified product implementation can pass that check.

`--commits-only` exercises actual login/logout through the existing bounded
`archive-engine/native_commit_proxy.py`. The new fixed targets select the session
INSERT or revocation UPDATE, so owner/renewal/touch transactions cannot shift the
fault boundary. Each case requires one completed mutation and selected COMMIT,
then verifies the actual HTTP outcome, specific CommitUnknown refusal and durable
rows. Pass, before-forward and after-complete schedules are distinct. Existing
rows/xmin must survive issuance; logout binds the expected credential digest and
preserves other rows. Unknown logout emits no cookie-clearing response; confirmed
retry preserves the revocation. Lost issuance requires an explicit new login.

The same mode ends with a live-cookie PostgreSQL crash. The separately owned host
controller `http_crash.py` validates both container identities/labels, observes the
postmaster PID/start time and two live sessions, sends SIGQUIT, then requires a new
postmaster and unchanged rows/xmin before releasing the fixture marker. This is
PostgreSQL's [immediate shutdown](https://www.postgresql.org/docs/16/server-shutdown.html),
which requires recovery on restart; it is not a host power-loss or filesystem-fault
claim. The actual old Control must fail closed, and a successor must accept the
retained cookie. Markers alone and compile-only checks do not qualify this case.
[Native COMMIT/recovery evidence](../../docs/implementation/evidence/p08-browser-http-commits-native-20260920.json)
records nine cases at e65116b, including the actual owned PostgreSQL SIGQUIT.
The initial c98398b fixture failed before readiness because it used an unsupported
configuration key. That failure and its untouched catalog remain retained; the
corrected endpoint was qualified in a new lab.


`audit_native.py` qualifies the additive0807 session mutation audit through the
actual adapter and executable. It requires17 distinct storage cases,12 ordinary
HTTP cases and8 selected COMMIT cases, plus schema/token and proxy regressions.
The `--audit-commits-only` fixture mode uses a separate isolated database and omits
only the host-controlled PG crash; it never claims that unexecuted crash passed.
Migration checks start with real legacy audit rows, preserve every logical field,
and refuse missing or mismatched policy/config bindings. An injected audit error
must roll back issuance/terminal cleanup or logout; a new Control incarnation
retries the operation without changing established attribution. Missing807 is
isolated from missing privileges, and missing audit INSERT prevents startup.

Each issue/logout COMMIT case independently binds audit to the actual principal,
session and revisions. The wire observer correlates extended Parse/Bind/Execute
responses through Sync boundaries, so the following audit INSERT cannot count as
the session mutation. It supports the closed unnamed-portal fixture profile and
refuses unsupported response shapes. The selected behavior follows PostgreSQL16
[message flow](https://www.postgresql.org/docs/16/protocol-flow.html) and
[message formats](https://www.postgresql.org/docs/16/protocol-message-formats.html).
A synthetic wire transcript reproduced the old observer counting two mutations;
that transcript is a fixture discriminator, not engine durability evidence.
The upgrade reports relation file identity/xmin separately: PostgreSQL16.15
rewrites the audit heap when adding the constrained domain. Comparing xmin
across that DDL incorrectly rejects valid preservation; ordinary retry/rollback
comparisons still include xmin. The original failed runs remain retained.
[Native audit evidence](../../docs/implementation/evidence/p08-session-mutation-audit-native-20260920.json)
at591c75f records all17/12/8 cases plus8 proxy tests, independent terminal SQL and
source/artifact/resource checks. No PG-crash/browser rerun or large-history
migration/capacity claim is inferred from this scoped qualification.
