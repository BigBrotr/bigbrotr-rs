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
session rows across restart before `/me`. The fixture copies the verified bundle
into its private case directory. With the
server stopped, it omits the start module from both disk and the closed manifest,
then starts the actual Rust process. The page must observe a real 404 for that
module. Restoration reinstalls the exact original module/manifest and restarts;
clicking the visible reload link must fetch the real module with 200 and restore
the enabled login form. Both restarts assert unchanged session/bucket rows and
loaded manifest identities. No browser request is intercepted, so normal browser
cache behavior remains active. This is a deliberate incomplete-asset deployment;
removing a declared file without adjusting its manifest correctly refuses startup.
Screenshots are
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
Chromium CORS observation error. The [corrected matrix](../../docs/implementation/evidence/p09-private-panel-referrer-native-20260920.json)
passes all eight journeys on Chromium and the first seven on Firefox/WebKit.
[Further diagnostics](../../docs/implementation/evidence/p09-private-panel-bootstrap-diagnostics-20260920.json)
show two real Firefox 404s, invalidating the exact-one count assumption. WebKit
navigated but never retried the module after removing interception; explicit reload
also failed. Playwright's exact WKPage.updateRequestInterception toggles resource
caching, so that instrumentation is not transparent. The [actual deployment fault](../../docs/implementation/evidence/p09-private-panel-bootstrap-real-native-20260920.json)
subsequently reproduced WebKit's failure without interception, while Chromium
and Firefox recovered on both mounts. The selected private build now omits
speculative JavaScript preloads (Kit prerender links and Vite dynamic dependencies),
consistent with the upstream modulepreload cache defect. The [corrected native matrix](../../docs/implementation/evidence/p09-private-panel-native-20260920.json)
at b6d9d7e passes all six pairs with the same real fault and restoration. Ordinary
imports, CSS and CSP remain in use. All required broader qualifications below
remain separate.
Further diagnostics retain bounded request categories, status,
navigation counts and numeric matcher results. An explicit browser reload
after a failed link is diagnostic only: the original failure remains FAIL.
The Chromium wire observer correlates either CDP event order, with finite pending
and output capacities; reordered-event and capacity regressions run in the frontend
stage.
Actual reports must bind source/tree, lockfiles,
native/browser executables, image digests, bundle hashes, resource limits and
terminal inventory. Compile/syntax checks do not execute browser journeys. Remote
TLS, large blocked static response, real expiry/late reply races, broad accessibility,
full administration, installed history/export and release journeys remain separate
requirements; this experiment does not mark P08/P09/M1 complete.
