# Current execution checkpoint

Observed 2026-09-18. Replace this record as work advances; it is navigation,
not another plan. Follow the [resumption procedure](agent-handoff.md#resume-from-one-verified-checkpoint)
and verify Git, artifacts and owned process state before trusting it.

| Field | Last observed state |
|---|---|
| Stage | Owner-started product implementation, authorized 2026-09-17; [Q30](../planning/questions.md) and [mandate](../planning/mandate.md). The complete selected product remains the objective. |
| Active slice | P02-T02 admission prerequisites: owned-event verification and bounded native receipt inspection implemented and qualified within their recorded Mac/Linux scope. Atomic native admission and native-owner authority remain next; no admission implementation yet. First durable signed event/archive/read/export/restart and the complete selected product remain required. |
| Repository state | Mac main product source is 41fff54e26e28b596b78c24199db860accd9fa81 after d0644ac. The containing checkpoint commit adds only reviewed qualification documentation/evidence. Last fetched origin/main before publication was c584f22443e55576a92d3b6e58a91b973902de84, with no competing changes. Root owned only those documentation edits at authoring; resolve containing HEAD, clean state and normal push from Git on resumption. Tested Control runtime source remains 6be8515. |
| Reading | Root read complete store/options/tests, model core codecs and effect/source types, identity/data/storage/operations/component contracts and affected key format. Pending read uses the exact pinned native snapshot API; independent reviewer found one decoder-error classification bug, reproduced and fixed. P08-T02/T03 authority and initial credential contracts inspected. Argon2 0.6.0 selected with explicit memory; scoped upstream review and independent C baseline recorded in owner-enrollment Mac evidence. Later archive/read/UI boundaries remain incompletely inspected and unimplemented. |
| Integrated checks | Actual binary 9cd823a on native Tux Linux amd64/PG16.15: 14 CLI/harness cases and 74 SQL cases PASS, including 56 exact SQLSTATE negatives, external exact rows for all 13 added tables, eligible upgrade preservation and atomic populated-topology refusal. Corrected harness 145eb7c checks mounted artifact identity and complete CLI output. [Evidence](evidence/p02-ingest-control-engine-20260918.json). All 386 clean Git source entries/archive and downloaded executable SHA 4b36ae76... independently verified. Nine targeted Mac migration tests and Clippy PASS. |
| Prior evidence | Initial 0001 native CLI/schema/privilege, COMMIT ambiguity/two-apply, TLS/mTLS and seven relational races retain their recorded scope: [engine](evidence/p02-control-engine-20260917.json), [fault/TLS](evidence/p02-control-faults-tls-20260917.json), [races](evidence/p02-control-races-20260917.json). CI 35284179910 at 8d1db2b audited: three ZIPs, 391 exact source entries, native toolchain receipts, 208 Linux practical cases and 154 complete traces verified. Pre-Rocks source, Mac practical cases skipped, binaries not retained; no storage/PG/release qualification. Audit path/hash in inventory. |
| Native storage | [Mac evidence](evidence/p02-ingest-store-mac-20260918.json): 145 workspace tests at 94a40de and Clippy/fmt PASS. [Linux evidence](evidence/p02-ingest-store-linux-20260918.json): fresh source release build, 144 workspace tests, 398 source entries/archive and downloaded ELF verified. Ten Linux store tests plus three actual native profile and two static profile tests; Mac-only alias case absent on Linux. [Pending reader evidence](evidence/p02-ingest-pending-mac-20260918.json): five Mac native/codec and nine key tests PASS; Clippy PASS. PEND-01 classification bug fixed; independent review completed without residual concrete findings. The later full native CI at6314639 also executes the Linux pending reader tests (scoped test result, not admission/crash qualification). [Receipt codec evidence](evidence/p02-ingest-receipt-codec-20260918.json): 33 integrated Mac model tests (six receipt cases) and Clippy PASS; native receipt persistence remains unimplemented. [Dictionary evidence](evidence/p02-ingest-dictionary-codec-20260918.json): 166 integrated Mac workspace tests (nine dictionary tests), fmt and Clippy PASS; native library rebuilt for this feature tuple. No native dictionary allocation/admission claim. |
| Policy candidate | [Mac evidence](evidence/p01-policy-seed-mac-20260918.json): 23 existing and four new actual CLI tests, one literal encoding test, Clippy/fmt PASS. Independently frozen bytes/digest; signed-zero defect reproduced and fixed. No durable policy write, activation or P01-T04 completion claimed. |
| CI limit | [Recorded cancellation](evidence/ci-time-limit-20260918.json): amd64 exceeded the60min job bound during its second native compilation. The workflow now allows120min; subsequent run35293792908 at4f2ad35 completed success for the three foundation jobs (new artifact audit not yet executed). |
| Float correction | [Mac evidence](evidence/p01-policy-float-roundtrip-20260918.json): upstream regression reproduced at the actual stored-policy boundary, then fixed by exact float decoding.183 workspace tests and Clippy PASS; the native JSONB fractional case also PASS at5dc6a7f (configuration engine evidence below). |
| Bootstrap candidate | [Mac evidence](evidence/p01-control-bootstrap-mac-20260918.json):187 tests/Clippy/fmt PASS. [Native evidence](evidence/p01-control-bootstrap-native-20260918.json):25 actual PG cases PASS; all223 final relations across9 databases and6 complete215-field policies independently checked. Bootstrap remains separate from enrollment/grants. |
| Configuration engine | [Native evidence](evidence/p01-config-inspection-native-20260918.json): source5dc6a7f,20 real Linux amd64/PG16.15 cases PASS. All215 non-root settings compared in8 accepted reports; each invocation left the full Control dump unchanged. Exact446 source files, downloaded executable c79c4c42... and native tool hashes verified. That earlier run did not qualify bootstrap, authentication, activation or maximum load. |
| Enrollment candidate | [Mac evidence](evidence/p08-owner-enrollment-mac-20260918.json):192 workspace tests before final refinements; affected final checks/Clippy/fmt/deny passed. [Native evidence](evidence/p08-owner-enrollment-native-20260918.json):69 harness cases,15 SQL negatives,257 relations across9 primary databases and6 actual password hashes independently checked; populated upgrade and ten-database PG crash/restart passed. No session, live policy or writer lease is implied. |
| Resources | All 1e87ce3 build/runtime clients stopped-retained, IDs/labels verified; 13 databases/internal network retained. Exact 501-file input, CLI f884f6e0... and probe 230ec81a... audited. At that earlier observation the shared checkout was1e87ce3; current resource state is below. Separate fresh CI source at runs/ci-0dabfaa/source is clean and mounted read-only; bigbrotr-implementation-ci-0dabfaa stopped-retained (exit 0, OOM false), owner 24373528e10936f41d0c8209ad2280c4, image sha256:71b2d400...; 2CPU/6GiB/512PIDs, no ports. See laboratory inventory for full IDs. |
| Registry engine | [Native registry evidence](evidence/p02-ingest-registry-native-20260918.json):2032042 Linux amd64/PG16.15, five scenarios PASS. All433 exact source files and downloaded CLI/probe bytes verified. Before/after COMMIT loss, actual cancelled/concurrent lock schedules, complete rows across23 tables, refusals and stale state inspected. Scope excludes product policy/bootstrap/native authority. |
| Findings/gates | 0002 refuses pre-existing unregistered topology atomically; no inferred source authority. Review fixed five schema cases and two acceptance oracle flaws. Eight actual lock schedules at 15c14ed plus final 23-relation inspection and backend cleanup PASS; [race evidence](evidence/p02-ingest-control-races-20260918.json). All 389 source files/archive and downloaded executable a3345bf7... verified. Native owner fencing, typed runtime coordination, real external receipts, retention/reclamation, durable signed-event/archive/read/export/restart and later product work remain. |
| Next bounded action | After verifying the containing qualification commit/push, implement P02-T02 step 2's first actual synchronized write: dictionary bootstrap through EvidenceIntent with contiguous source/allocation state, request detail and pending marker in the same batch, consumed by the existing native readers. Establish the real Control/native authority call chain before exposing it as admission; a RegistrationObservation, manifest or caller-constructed value cannot grant authority. Proceed to the existing signed-event oracle and full event write set; do not stop at additional read helpers. |

Local authentication Mac checks: [evidence](evidence/p08-local-authentication-mac-20260918.json), 194 workspace tests, Clippy/fmt/structural checks PASS. Native Linux/PG: [evidence](evidence/p08-local-authentication-native-20260918.json), 16 CLI cases and independent 29-table audit PASS; owned runtime stopped-retained.

Keep secrets and raw logs outside this checkpoint. Unknown writes remain unknown
until their owning recovery protocol resolves them. Resolve the containing commit
and current remote state from Git; this note is not push evidence.

Initial activation: [Mac](evidence/p01-initial-activation-mac-20260918.json) 197 workspace tests and [native](evidence/p01-initial-activation-native-20260918.json) scoped qualification. These do not complete P01-T04/T06 or P08-T03.

Control backend: [Mac](evidence/p01-control-owner-mac-20260918.json) 198 workspace tests; [native](evidence/p01-control-owner-native-20260918.json) scoped owner/registry qualification. The earlier 7c0c94f lab remains stopped-retained; current run is recorded above. Source/helper commits and native qualification are pushed in 38ccd0b; verify live Git before resuming.

Control failure/recovery: [native evidence](evidence/p01-control-owner-faults-native-20260918.json). Actual freezes measured 63.73s and 63.70s against 60s scheduling TTL; 13 full Control dumps survived PG SIGKILL/restart. An early supplemental SQL invocation failed its read-only dump while the main harness was still running; the retained completed rerun passed. Release-COMMIT remains NOT_RUN; the later Control process matrix below exercises actual policy-refresh cancellation.

Trusted Tux CI: [native evidence](evidence/p01-trusted-ci-tux-20260918.json), full foundation at 0dabfaa: 197 Rust tests, 20 Python controls, 104 practical checks, 503 exact source files and downloaded ELF 8163423a... verified. Four corrected CI subprocess controls separately PASS on Mac and native Tux at 95ac5c9. Both containers freshly verified stopped-retained, full IDs/labels in inventory. Missing executable accounting was reproduced and fixed; the complete suite was not rerun for this runner-only correction. GitHub billing remains an external NOT_RUN, not product failure.

Control process: [Mac evidence](evidence/p01-control-runtime-mac-20260918.json) records
199 tests at the initial candidate; the final administration-gate correction has
nine targeted tests and Clippy PASS. [Native evidence](evidence/p01-control-runtime-native-20260918.json)
qualifies actual executable `6be8515`, SHA `0ee6429c...`, with 16 PG16.15 cases,
511 exact source files, full 240-relation audit and seven complete lifecycle
outputs/owner exclusion digests. Real SIGINT/SIGTERM, policy-expiry pause,
cancelled lock wait and PG SIGKILL/restart executed. No application/blocker backend
remained. Maximum documents/slow output, automatic reconnect, broader grants,
native handoff and complete installed journeys remain NOT_RUN.

New common foundation CI at `6314639`: all 14 steps, 198 Linux Rust tests, 21
Python controls and 104 practical checks PASS. Independently audited 510 source
files, downloaded ELF `5bc9a759...`, 17 signed outputs and 77 complete syscall
traces. One macOS-only alias case explains the platform test count difference.
That source lacked the enabled-administration refusal and does not qualify the
corrected runtime. Full CI was not repeated at `6be8515`; actual corrected native
build/matrix and targeted Mac checks supply its scope. GitHub billing remains
external NOT_RUN; trusted Tux runs use the shared CI entrypoint without release
credentials or untrusted PR execution.

Current resources: shared Tux checkout remains clean detached `6be8515`, no
consumers. Both new native jobs are stopped-retained, exit0/OOMfalse; final full
inventory found zero running BigBrotr containers and the same eight unrelated
running containers as both preflights. No resource was deleted:

- `runs/ci-d0644ac`, container `8c00542b34ab40c228356ad41056aaa7391d9be3eb8db454c2fea55e10f237f1`,
  owner `5eac340e8bea599cefb97ee0516b61fa`: all 14 shared CI steps, 202 Rust tests,
  21 Python controls and 104 practical checks PASS. Independent audit compared
  513 exact source files, ELF `000451d95977a80b4759c969f9a83454815148f26dce7f36895e92f3952c4266`,
  17 complete signed outputs and 77 full syscall traces.
- `runs/receipts-41fff54`, container `2ac31a2f76fc46d56f68136906f33f243eb2852aaabcf30a3f67d62eb75efb10`,
  owner `3f610d43b3f9881ec93199bb7f8215cb`: six codec and five actual native
  receipt tests PASS. All 517 actual source files compared against Git archive;
  both test executables downloaded/hashed, Rust/Cargo/native tools independently
  checked. Native read assertions are test-harness observed; no post-test external
  DB inspection, admission or crash qualification is claimed.

Raw records/observers are under `.research/implementation/owned-event/`,
`.research/implementation/ci/native-d0644ac/` and
`.research/implementation/receipt-reader/`. The local receipt artifact observer
initially failed because Python lacks `hashlib.file_digest`; its corrected bounded
streaming SHA-256 audit passed on the same downloaded binaries without changing
expected results. Full IDs, resource limits and terminal records are in the
laboratory inventory and linked native evidence.

Orchestration findings remain explicit in native evidence: original `6314639`
preflight used the wrong source path and its shell continued launching; inventory
was reconciled afterward. Corrected build and runtime each followed a separate
successful preflight. The reused CI auditor expected20 controls instead of21;
source inspection confirmed the added missing-tool test before correcting only
the observer. The cleanup observer initially mistook the idle helper's SIGTERM143
for a required zero exit; actual product exits were independently checked.

Next-slice reading completed: P02-T02 and key/pending formats; full identity/encoding,
dictionary descriptors, model admission/dictionary/effect, store/pending/manifest,
current Control owner/runtime and affected policy lifecycle. Signed admission
fixture now read completely through lossless compact per-section output after
the earlier truncated dump. Ingest registry adapter and its tests are fully
read. EvidenceIntent already supplies the closed bounded dictionary bootstrap
intent. Exact rocksdb0.25.0/librocksdb-sys0.19.0+11.8.1 write entrypoints, native
options and WriteBatch interfaces inspected; scoped source hashes/intervals at
`.research/implementation/control-runtime/admission-source-review.json`. Native
WriteBatch capacity is not a maximum; bound/account values before FFI insertion.
No admission code changed or new admission experiment executed yet.

Automatic objective state was freshly read from the session service after the
new candidate work: `active`, objective still the complete selected product.
The earlier `usageLimited` observation is superseded; no limit was reset and no
replacement agents were spawned. Preserve the objective across continuations.

Owned-event verifier: ten protocol tests (three new), five real CLI cases and the
independent 15-variant/14-ID checker pass on Mac. See
[Mac evidence](evidence/p02-owned-event-mac-20260918.json) and
[native evidence](evidence/p02-owned-event-native-20260918.json).
The initial missing-API compile failure is not a behavioral regression failure.

Receipt reader: five actual engine cases, six unchanged golden codec cases and
stable combined workspace Clippy pass on Mac; both suites pass on native Tux.
See [Mac](evidence/p02-receipt-reader-mac-20260918.json) and
[native evidence](evidence/p02-receipt-reader-native-20260918.json).
Full request/dataset/digest binding, retained permits, original context and
key/value non-mutation are checked. Inspection does not resolve missing detail,
retire producers, authorize progress or implement admission writes. Full CI was
not repeated at 41fff54; that revision received the selected native suites.
