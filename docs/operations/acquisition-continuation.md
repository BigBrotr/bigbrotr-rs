# Continue a settled finite acquisition job

This development capability uses migration0045. Compiled qualification and the
installed same-job continuation, pending-plan refusal and seal/adoption-COMMIT
loss pairs
are recorded in the [acquisition evidence](../implementation/evidence/p03-initial-sync-native-20261002.json).
Current-authority/expiry and integrity refusals, exact uncertain-seal retry and
later handoff profiles remain separate qualification gates. It preserves an
existing finite job across native succession; it does not resume an old native writer.
The [owning contract](../reference/producer-request-inputs.md#explicit-settled-finite-job-continuation-0045-source-under-qualification)
defines the exact evidence, authority and remaining scope.

Use the existing authenticated acquisition acceptance/status API described in
[acquisition scopes](../reference/acquisition-scopes.md). Preserve its JobId and
original deadline. Keep the accepted configuration/policy revisions, actor grants,
scope and source generation/route unchanged. A job whose original deadline has
elapsed cannot obtain more execution time through this procedure.

Apply migration0045 with the separate migration credential and provision its
explicit application permissions from
[the maintained role script](../../packaging/compose/provision.sql). Stop and join
the original Serve owner, including its native worker and connection driver.
A lease timeout or absence of an HTTP reply does not prove those owners stopped.
Resolve original requests using the existing native recovery path. Retain all
inputs, receipts, facts and database history.

For an initial native lifetime, use the same deployment/root:

```sh
bigbrotr ingest recover --file instance.toml --seal-acquisition
bigbrotr ingest recover --file instance.toml --retire-initial
bigbrotr serve --file instance.toml --activate-initial-successor --roles control,ingest,sync
```

Sealing closes and verifies the source, then retains a seal for every eligible
queued claim of that original owner, up to64. It leaves each job's owner, state
and revision unchanged. Any pending/unknown request plan, incomplete teardown,
unsupported interruption, missing native proof or uncovered mutation refuses the
seal. Neither a short page nor EOSE alone supplies the required checkpoint.
The source can already be legitimately closed when sealing refuses a pending
plan. A failed recovery can also leave its fresh Control row active until its
lease expires, even after the process and connection driver have joined. Preserve
that closed source and pending history; this refusal creates no continuation
seal and authorizes no implicit retirement, activation or resumed wire work.
Retirement then consumes the exact sealed workflow obligations. It retains all
scientific data and original attribution. The final Serve operation prepares and
activates a fresh native candidate, validates the retained seals through its
actual live writer and atomically adopts each same JobId before any wire work.

For an already activated epoch2, replace the first two commands with
`--seal-activated-acquisition` and `--retire-activated-initial`, then use
`serve --activate-successor --roles control,ingest,sync`. For an activated later
epoch, first complete `--close-successor-source` in a separate recovery pass,
then use `--seal-successor-acquisition`, `--retire-successor` and the same
`serve --activate-successor` path. A lifetime with zero new quanta still owns its
adopted job and needs its explicit seal or existing abandonment operation.

Observe the same job through the authenticated status API after restart. Its
frozen horizon, global queries spent, boundary gaps and attempted prefix continue
from retained evidence. Status remains qualified coverage with explicit gaps;
no new JobId, refreshed deadline, erased gap or complete-history claim follows.
Each native producer remains bounded to64 mutation requests. Full500-event pages,
partitioning, sparse-window growth and general scheduling retain their own gates.

Exit4 or a lost COMMIT response remains unresolved. Stop/join that owner and
inspect retained state under fresh exclusive recovery before retrying identically.
For a lost seal COMMIT reply, the seal may be absent or already retained and
unadopted. The failed command can leave its source closed and its fresh Control
row active after the process/backend join. Observing either seal outcome alone
authorizes no implicit retry, retirement, activation or new wire work.
An exact committed seal keeps its original sealer/time. A committed adoption and
job-owner change form one transaction. An unknown adoption COMMIT stops startup
with `initial native batch unavailable: Quantum(CommitUnknown)` before its
activated/startup report or any new wire request. Native preparation/activation
can already be retained, with fresh Control and Ingest rows active even after
the writer and connection driver have joined. Preserve that candidate and both
possible job/seal cuts; neither active SQL rows nor activation metadata grant
permission to reconstruct the lost writer or retry its work.
Until adoption is positively confirmed, no new
wire work is authorized. A lost activated candidate is never recreated from its
activation row; use the existing recovery/retirement procedure and a newly minted
successor. Whole-intent abandonment remains a separate explicit cancellation
choice and never masquerades as continuation.

Continuation also rechecks the original actor's active account and exact Control
grant, the original deadline, and the unchanged accepted policy/source. A job
authority refusal can occur after fresh native activation: startup stops with
`initial native batch unavailable: Control(Backend)`. That broad diagnostic alone
does not identify the guard. The maintained refusal profiles require the actual
executed `require_acquisition_continuation_authority` error `BB001` with reason
`acquisition job requires current authority and original remaining duration`,
while fresh Control/Ingest leases and root authorization remain valid. Preserve
the unadopted seal, old job and activated candidate; do not extend the deadline
or infer permission to retry. These profiles cover exact actor-grant removal and
genuine original expiry; other authority and lost-candidate operations retain
their separate qualification gates.
