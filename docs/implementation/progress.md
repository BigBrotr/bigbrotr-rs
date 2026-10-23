# Current execution checkpoint

Observed 2026-10-23 on Tux. Goal **ACTIVE** after explicit resume; M1–M5
incomplete. Root owns integration/runtime. Q37 model allocation and Q38 evidence
reuse remain in force. The preceding turn made progress by preserving and pushing
the safe checkpoint; no unresolved active driver was inherited.

## Objective and revision

P02-T02 Steps4/5/7: classified producer checkpoint → synchronized native ACK →
restart. Main941847c contains the corrected initial result assembly. Product
fcad4c9 and reviewed fixture f5cd4e9 remain the source basis. Current change fixes
activated-only phase selection among ordinary quiet connections, with retained
context verification, plus the checkpoint/report/inventory update.

The [owning recovery report](evidence/p02-producer-recovery-native-20260920.json),
`initial_successor_preparation_20261012.repeatable_succession_20261015.
producer_checkpoint_ack_preparation_20261020.native_ack_source_preparation`,
owns exact reviews, qualification receipts, failures and the partial-loss handoff.
The [inventory](../research/laboratory-inventory.json) owns resource/worktree custody.

## Completed evidence and remaining work

- **PASS:** product/source review; component build fcad4c9 with 258 tests in
  19 groups, 30 required controls, build and both Clippy selections. Eight ELF
  hashes authenticated. Build artifacts remain in
  `/home/tux/bigbrotr-implementation-20260917/runs/producer-ack-fcad4c9/out`.
- **PASS:** fixture critical review; all 22 fixture owners match f5cd4e9 and
  661 compiled inputs match the qualified build. Default initial verifier and
  38 controls pass; three original successor contexts and 63 controls pass.
- **FAIL preserved:** the aggregate default-regression attempt stopped on old
  activated965 input lacking the already-required ACL capture. Unchanged fcad
  baseline refuses the same input. No invented ACL or relaxed check; original
  exact-source965 qualification remains historical evidence.
- **PASS:** actual initial ACK run941847c and strict raw replay (2,136 bound
  files, 48 controls). Native75/sequence95; I=A=C6 and22 dependencies. Three new
  mutations cross window2, then proof-only resend adds no native write.
- **FAIL preserved:** original initiale8f7e4a final result assembly duplicated
  status. Activated941847c verifier indexed a missing phase on quiet wires.
  Corrected activated verifier passes the original complete context and41
  controls (I=A=C4,20 dependencies,native110/sequence134); full raw qualification
  of that original FAIL is not claimed.
- **NOT_RUN:** corrected full activated2 run, successor3 ACK and connected
  lost-completion recovery. Source/component/default results remain reusable.

## Preserved agent work and resources

`producer_ack_loss_resume` is active on the preserved loss fixture; effective
Astra/xhigh verified in its new runtime metadata. It performs source-only work;
root remains the sole runtime owner. Previous agents remain stopped.
Under `/home/tux/bigbrotr-rs/.research/`:

- `producer-ack-sync-20261020`, branch `work/producer-ack-sync`, clean fcad4c9;
  `producer-ack-storage-20261020`, branch `work/producer-ack-storage`, clean
  fff6901; both source packets are integrated.
- `producer-ack-fixture-20261020`, branch `work/producer-ack-fixture`, clean
  f5cd4e9; positive fixture integrated, author and critical reviewer completed.
- `producer-ack-loss-20261020`, branch `work/producer-ack-loss`, HEAD
  dc9bfe4c0675e5baff5742aaff82399bea0d2f17: **six uncommitted partial files**.
  Resume starts from six preserved partial files; the new author is completing
  them. Not integrated or pushed; source freeze/review/runtime remain pending. Exact
  hashes and diff are preserved in its
  `.research/producer-ack-loss-source-20261020/paused-partial-v1.json` and `.diff`.

All component, initial/activated runtime and offline drivers are joined. Existing
owners recorded stopped client/PG with no cleanup errors or unrelated changes;
root additionally checked initial941 custody before the activated invocation.
All `.research`, worktrees, datasets, failures/logs and external artifacts remain.
No cleanup runs. Inspect ownership/state before reuse.

## Precise next step

Run corrected activated2 ACK from a new clean detached source using qualified
fcad4c9 artifacts and existing `bindings.py LAB --ingest-installed-live-ack BUILD`,
then strict `python3 -B -S` raw replay. Continue actual successor3. Do not repeat
initial941: the changed branch is activated-only. Finish and critically review
the isolated ACK-loss fixture before its actual execution.

GC, preparation-hole, mixed-restore/refusal/resource gates and original a99 Sync
timeout cause remain open. NoGC64/32MiB is temporary, not capacity qualification.
Q33 CI billing, arm64 and signing/registry remain separate gates. No public release
or overall-completion claim.
