# Native private panel experiment

This is a trusted, isolated laboratory fixture, not an installed command or a
release qualification. `browser_http_probe --panel` uses the real migration,
initialization, activation and enrollment commands, actual Control process and
PostgreSQL before launching the exact Playwright1.63.0 client. Each browser/mount
pair requires a fresh database, output directory and container. No API body is
mocked and CSP is never bypassed.

`native.py` has three bounded stages: `rust`, `frontend`, and
`browser chromium|firefox|webkit root|control`. The host supplies exact Git archive
source and its manifest at `/out/build`, verified native Archive inputs, and
Node26.4.0/pnpm9.15.9 at `/tools`. The frontend stage uses a separate exact archive
copy for generated files, frozen installation without lifecycle scripts, and
checks original source hashes before and after builds. Browser stages consume
the built artifacts through read-only mounts; `/case` holds private synthetic
fixture data. Each actual engine runs in the isolated PostgreSQL network namespace
with no host port or external route. The frontend dependency fetch stage has
network access but no engine secrets, Docker socket or release credentials.

The eight journeys cover root/subpath startup, native form/keyboard/mobile
layout, invalid and wrong password, exact stored permissions, actual cookie
attributes, CSRF/second-port Origin denial, retained cookie after process restart,
cross-tab logout, new login/reload, and missing bootstrap recovery. SQL compares
complete session and attempt-bucket rows including xmin before/after denials and
session rows across restart before `/me`. The absent bootstrap request is rewritten
to an actually missing server path; its real404 is required. This is a controlled
request fault, not a claim about an actual deployment rollout. Screenshots are
synthetic; arbitrary exception text, headers and cookies are not exported.

Node teardown failure fails the journey. The wrapper checks `/proc` for remaining
processes, but the host must still wait/stop and inspect the dedicated container
before declaring cleanup. Playwright launches separate browser process groups;
Node/probe exit or process-group kill alone cannot collect every stopped orphan.
Never reuse a case container after a failure. Retain failed output and collect
the entire owned container, including on timeout. Independently inspect PostgreSQL
durable state and unrelated host resources before calling the run qualified.

Failure diagnostics retain bounded management method/path/status and classify
Origin as expected/null/missing/foreign; they never retain header values or bodies.
Chromium's selected cross-origin fetch additionally requires CDP wire status403:
a CORS failure can suppress Playwright's normal Response event. All engines also
submit an actual cross-origin HTML form and require its observable403 and exact
foreign Origin. Both attempts are inside the independent unchanged-SQL boundary.
A rejected fetch or timeout alone is never a passed status oracle. Failure
locations and fixed notice categories distinguish denied requests from the
fixture's own assertions.

Browser versions come from the exact npm package's `browsers.json`: Chromium
revision1243, Firefox1543 and WebKit2359. The pinned official Noble image provides
their native dependencies and browser files, but does not supply the repository's
npm dependency package or selected Node version. Reference upstream
[Docker guidance](https://playwright.dev/docs/docker) and
[exact browser registry](https://github.com/microsoft/playwright/blob/1b025d7e20a026371cd5f98ba0cdce48892737c8/packages/playwright-core/browsers.json).

The [initial native matrix](../../docs/implementation/evidence/p09-private-panel-browser-failures-20260920.json)
failed. [Discriminating diagnostics](../../docs/implementation/evidence/p09-private-panel-browser-diagnostics-20260920.json)
confirmed the document referrer-policy defect in Firefox/WebKit and the distinct
Chromium CORS observation error. The corrected matrix still requires execution.
Actual reports must bind source/tree, lockfiles,
native/browser executables, image digests, bundle hashes, resource limits and
terminal inventory. Compile/syntax checks do not execute browser journeys. Remote
TLS, large blocked static response, real expiry/late reply races, broad accessibility,
full administration, installed history/export and release journeys remain separate
requirements; this experiment does not mark P08/P09/M1 complete.
