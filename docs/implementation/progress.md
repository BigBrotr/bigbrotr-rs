# Current execution checkpoint

2026-10-26, Tux. **Replacement full-product goal ACTIVE.** Q37/Q38/Q40
remain; Q41 requires verified hosted Actions and Q42 records the owner's public
repository transition. Continue the existing P01-T08 first-launch delivery increment.
No official release, package publication or production deployment is authorized.

## Current increment and GitHub boundary

Integrated/published base is `2d207233a3cd54f0f80a0999acae337cfec69b97` on `main`.
This checkpoint's commit reconciles public instructions, adds conservative CI
selection and verified native-download reuse, and corrects two mechanical Clippy
fixture decoders. Local CI controls (9), formatting, actionlint1.7.12, planning artifacts and
535 document fragments PASS; critical CI source review found no blocker. Hosted
qualification of these changes is still required.

[Actions run40303460687 attempt3](https://github.com/BigBrotr/bigbrotr-rs/actions/runs/40303460687/attempts/3)
executed on Linux amd64/arm64 and macOS after the visibility change. All three
passed toolchain/format then FAILED Clippy's constant-chunk lint; later checks
are NOT_RUN. Attempt2 still had a billing admission refusal with zero steps.
See the [existing CI observation](evidence/ci-billing-block-20260918.json), which
preserves the original failures rather than rewriting them as success.

The protected public-exposure audit is inventoried under
`implementation_ci_publication_20261026`: 1,606 commits,10,050 blobs and501
available Actions log snapshots, plus the first two new diagnostic artifacts;
no operational credential confirmed. Synthetic candidates and scope limits are
recorded in its redacted summary. GitHub secret scanning and push protection are
now enabled; zero current alerts do not prove the asynchronous scan complete.
Later logs/artifacts require their own inspection. Source/history and lab metadata
are public; sensitive raw laboratory evidence stays protected and untracked.

Pending separate work: candidate intake `616fa952e55524ff293c70c0c7aa6108814cf289`
in `.research/m1-candidate-intake-20261026` (10 offline controls PASS, no runtime),
and first-launch case `4c15897b1fc726601eacff308afb236376621316` in
`.research/p01-first-launch-20261026`, branch `work/p01-first-launch` (10 new/2
affected controls PASS, agent source review complete; real engines NOT_RUN).
Root's original candidate draft is still untracked until reconciled with that commit.
The tutorial draft below remains separate. Do not claim these branches integrated.

## Integrated result and valid evidence

The previous fresh-launch result is integrated through `2d207233`; only P01-T09
Step3 is marked complete. Qualified product source remains c49cfe5c; binary/image
are unchanged. The current Rust edits affect test fixture decoding only.

**PASS:** fresh extracted19-member bundle,15 actual initialization/enrollment
commands, signed admission and Archive publication. Complete independent49 native
rows/four input/outcome receipts, typed progress C4/A4 and12 dependencies, Archive
headers/receipts and real C API readback agree. Science is8 dictionaries plus one
event/eligibility/receive/provenance,4 commits, frontier12/snapshot5. Initial native
owner closes and releases. A separate ordinary Control lifetime reads the exact
signed event and exports353-byte JSONL with complete manifest/inventory; independent
77-table checks and unchanged catalog/Archive files pass. Both lifetimes exit0.
This qualifies ordinary Control restart, not Ingest successor/resend or all M1.

The [laboratory inventory](../research/laboratory-inventory.json), key
`implementation_m1_first_launch_20261026`, binds original evidence and all resources.
External root: `/home/tux/bigbrotr-implementation-20260917/runs/m1-first-launch-0f80280`.
Its `read-profile-v3/journey-verified.json` owns the connected result; root
`journey-verified.json` owns the earlier initial Archive proof. Reuse these valid
results at their recorded identities instead of rebuilding/repeating them.
The oracle source review and23 original plus7 targeted negative controls remain
valid; d2ef7de0 is integrated. No new build or runtime campaign ran for this pause.

## Partial work and retained problems

- Tutorial draft17de200de05a9ae093ecd51b002ee06a3055c59d is committed and pushed to
  `origin/work/m1-client`, clean in `.research/m1-client-20261026`. It is **not
  integrated**: author syntax/link/evidence checks passed, root integration review
  and alignment with Q40 portable user guidance remain. It explicitly identifies
  missing delivered relay/client/controller inputs; do not call the bundle a
  self-contained first-event distribution.
- Root's reusable orchestration remains outside Git under the external root:
  bootstrap-replay.py, initial-journey.py, export-journey.py, verify-journey.py.
  Their hashes are inventoried. This is a delivery gap, not clean-checkout
  acceptance. Promote appropriate responsibilities into the existing harness.
- Original instance-v2 authorized history256MiB returned429; export NOT_RUN.
  Preserve its failure/triage. Corrected v3 restores established history1GiB /
  export256MiB; worker512MiB and total application/native6GiB unchanged. No accepted
  policy rewrite or weakened guard/oracle. Original orchestration/environment
  failures remain linked; initial a99 Sync timeout still has unknown cause.
- Ingest recovery/retirement/successor/resend on v3 are **NOT_RUN**. No restart
  script or product implementation was started. Minimal Index/readiness and
  dependency-outage boundaries remain open; later milestones remain required.

## Resources and next action

Current agents: the candidate-intake agent completed its local commit; runtime
harness agent completed4c15897b; the security/CI reviewer completed with
no source mutations. No local product build or engine campaign has started for
this increment. The hosted attempt3 jobs have terminated in failure.

The prior owned PGs e6780dc1/f2996d13 and application/client/probe processes were
stopped at the last verified safe boundary; `read-profile-v3/goal-transition-stop.json`
records exact identities. These historical stopped observations are not authority
to mutate resources without a fresh ownership check. Preserve database/input/logs,
original failures, external roots and worktrees; no cleanup was performed here.

Next: publish the coherent CI/public-boundary correction and inspect its actual
hosted reports while integrating the reviewed intake and first-launch case. Run
that maintained case against a fresh owned instance and the retained exact bundle,
with declared pinned images and verified network custody; never upload its raw
credential/session/Control captures. Reconcile the saved tutorial with the actual
portable command. Then extend this same path to missing Ingest restart/resend.
M1 still needs connected Ingest succession/catch-up and minimal Index/readiness/
dependency-outage evidence; later obligations remain in their existing tasks.
Do not block M1 on a general historical cleanup or all P02.

The source-reviewed minimal restart uses the same v3 store: restore the retained
`export-v1/initial-config.toml` deployment (reader disabled, no destination), then
`ingest recover --abandon-acquisition`, `ingest recover --retire-initial`, then
`serve --activate-initial-successor --roles control,ingest,sync`. Abandonment already
includes positive replay/source closure/final ACK; no separate preliminary recovery
is needed. Current schema40 validates C4/A4; legacy A0 expectations do not apply.
Reuse existing live-client transport and preserve the original visibility, body,
eligibility and first receive. Provenance-off needs actually accepted policy;
restoring local config cannot change it. Reconcile retained410d804/61a3de1/9359721
proofs, then exercise only missing connected coverage, including Archive catch-up.
