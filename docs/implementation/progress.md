# Current execution checkpoint

Observed 2026-09-17. Replace this record when work advances; it is navigation,
not another plan. Follow the [resumption procedure](agent-handoff.md#resume-from-one-verified-checkpoint)
and verify Git, actual artifacts and owned process state before trusting it.

| Field | Last observed state |
|---|---|
| Stage | Owner-started product implementation, authorized 2026-09-17; [Q30](../planning/questions.md) and [mandate](../planning/mandate.md). Full selected product remains the objective. |
| Active slice | P01-T02/T03 first lossless signed-event/verification slice; root integrates. Full selected product remains the objective; no complete product task claimed. |
| Repository state | Mac worktree `/Users/vincenzo/Documents/GitHub/BigBrotr/bigbrotr-rs`, branch `main`; last observed HEAD and private origin/main `a289df47f595562caed932b7882cb3cb4f9c7df8`. Two foundation commits pushed; no force/rewrite. Root-owned dirty model/protocol crates, CLI verifier, tests/fixtures, schema/version metadata, dependency/build/CI updates and supporting docs/evidence. No contributor conflict or merge/rebase. Resolve the containing candidate from Git. |
| Reading | Initial nine entrypoints and foundation dependencies read completely. Added complete data, identity/encoding, storage catalog, naming, operations, configuration/runtime and protocol-evolution contracts; owning code and exact crypto/Serde boundaries inspected. Frozen signed events/actions and generator inspected; later storage/columnar/read consumers remain unimplemented and not fully inspected for their implementation yet. |
| Decisions | Existing architecture remains selected. Narrow Q31 dependency decision uses strict BigBrotr types plus direct secp256k1 0.33.1/sys0.14.1, avoiding permissive SDK parsing and a duplicate crypto line. No new master plan, registry publication, legacy mutation or public release. |
| Foundation evidence | Exact source `a289df4`: clean Mac/Tux builds, Tux P01-T01 12/12 PASS, Rust3/3 and runner4/4 PASS. GitHub Actions run35256322733 completed success on native Linuxamd64/arm64 and Macarm64; downloaded Linux reports both12/12 PASS. See [foundation evidence](evidence/p01-foundation-20260917.json). |
| Current event evidence | Dirty source: 3 foundation process tests, 5 signed-event process tests and 4 protocol tests PASS; fmt/Clippy/doc and dependency advisory/license/source checks PASS. Python independent fixture verifier PASS. Compile with intentionally broken dependency crypto cfg correctly refuses. AI review reproduced invalid ignored UTF-8; root also reproduced positional-array and signed-empty-tag acceptance, then fixed all with regressions. Preflight charge changed64→96/node after Vec minimum-allocation analysis. Latest runner control count5 and native exact event build still pending final candidate. |
| Resources | Last inspected Tux lab `/home/tux/bigbrotr-implementation-20260917`: detached `a289df4` clean; three owned containers exited, latest exit0/Pid0/OOMKilled=false. No running owned job. [Inventory](../research/laboratory-inventory.json) records lab image/directories, downloaded exact dependency sources, caches and private CI evidence. Unrelated/retired resources untouched. Root's local tests/builds may run; join tool sessions and verify outputs rather than infer completion. |
| Remaining gates | P01-T02 portable identities/CBOR/engine mappings, T03 WS/cache/filter/endpoint and all runtime/storage/acquisition/read/UI/release/capacity journeys remain. Native storage/TLS/extension tuple unqualified. No durable signed event yet. [Readiness](../planning/specification-readiness.md) retains gates. |
| Next bounded action | Finish whole event diff/runner review and local checks, commit coherent candidate, transfer exact Git revision to inventoried Tux lab, run clean release plus p01-event-cli and native tests; push verified private work and observe native CI. Then implement P01-T02 identity/fingerprint and P02 control/admission risk prerequisites for M1. |

Keep secrets/raw logs outside this checkpoint. Unknown write outcomes remain
unknown until their owning recovery protocol resolves them. Resolve the containing
commit and current remote state from Git; this note is not push evidence.
