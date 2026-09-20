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
