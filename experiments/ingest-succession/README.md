# Repeatable succession portable fixtures

`portable-handoff-v2.json` is generated independently from the field tables in
[the manifest contract](../../docs/reference/ingest-manifest-format.md#repeatable-owner-handoff-selected-not-yet-implemented)
at revision `12c3534c6c970839a4371fc00b4ac3eac6730cb0`. The generator uses Python
standard-library byte construction, `struct` and SHA-256. It neither imports a
product codec nor executes Rust, SQL or a native store.

Run the deterministic selfcheck from the repository root:

```sh
python3 -B experiments/ingest-succession/generate.py
```

`--write` regenerates the checked-in JSON. Default execution compares all generated
bytes with that file. Selfchecks inspect field offsets, lengths, key identities,
chain links, hashes and profile arithmetic. They do not execute the product's
decoder or claim that it rejects the malformed vectors.

The JSON contains:

- `valid`: locally valid value/key bytes and explicit decoded fields. Unsigned
  64-bit fields are decimal strings. `profile_epoch_admitted` tests only the
  target epoch cap, not native authority, registry consistency or namespace
  eligibility. Empty/sparse namespace vectors are codec cases; connected work
  independently requires all nine kinds. High-epoch vectors are not complete
  historical lineages.
- `invalid`: concrete local format/transition violations with a base vector and
  reason. Product tests should reject each value. `truncation_sweeps` names valid
  vectors whose every proper byte prefix must also be rejected.
- `chain_checks`: exact legacy-to-v2 and v2-to-v2 links, plus locally valid values
  with a wrong parent hash, slot or adjacent manifest. These negative cases need
  the retained parent; they are not local codec errors.
- `closure_vectors`: independently assembled source/empty closure preimages and
  hashes. Request/outcome bytes are synthetic hash inputs, not decodable positive
  outcomes or admitted scientific facts. Legacy closure scalars are explicitly
  unverified fixture data. Activated-zero and empty-zero use distinct domains.
- `profile`: the separate membership, compact-metadata and input-spool ceilings.
  Epoch255 fits32,640 memberships; epoch256 needs32,896 and refuses even though
  its metadata formula equals256KiB. Formula arithmetic is not an allocation,
  transport-buffer, disk/RSS or recoverability measurement.

Both terminal kinds are portable data only. No fixture proves retirement,
activation absence, exclusivity, root/sequence binding, positive replay, workflow
completion or an installed journey. Those boundaries remain separately required.

`native.py` runs exact-source codec, structural lineage (`lineage`) or migration
catalog/early CLI (`schema`) checks in the existing isolated laboratory. Schema
mode retains its CLI and build identity for a separate actual PostgreSQL run;
passing that build does not execute SQL. The current checkpoint and evidence own
completed run identities and failures.

`replay` combines the ordered positive lineage tests and affected legacy recovery,
dictionary and source-closure checks with the schema/CLI build. Its native fixtures
install epoch3 bytes directly: they do not qualify a connected successor writer.

`history` checks the typed PostgreSQL history state selection, native lineage and
legacy handoff/empty components, plus schema/CLI compilation. Its state tests do
not connect to PostgreSQL; real copy, lock and cancellation checks remain separate.
`history-probe` only builds and lints the finite probe and CLI for those engine
checks. It runs no tests; reuse of earlier component evidence requires unchanged
compiled product inputs. The engine invokes `successor-lineage-history` with an
explicit metadata byte capacity on disposable database/native copies.

`transition` builds the CLI/probe and runs the native lineage/handoff/empty tests
and PostgreSQL transition component tests, followed by Clippy. The latter check
state and reservation rules without a database. Real preparation, gap recovery,
empty retirement and installed writer journeys require separate connected runs.
`transition-probe` builds and lints the finite transition probe/CLI without
repeating component tests. Earlier tests apply only to their recorded inputs;
any reuse across later edits requires an explicit review of the affected boundary.

`replay.py RUN_ROOT` independently reduces the captured SQL experiment below,
binding both origins, complete rows, raw output and portable hashes. It requires
the completed two-origin run by default. Explicit `--origin activated
--allow-failed-run` permits retrospective inspection of a completed origin while
retaining the original failed-run status. It never runs an engine or mutates captures.

`../control-migrations/repeatable_succession_cases.sql` is a separate SQL-only
fixture. It requires disposable copies of both genuinely retired epoch2 origins,
explicit migration0038 and the additional privileges in `schema_grants.sql`.
Its negative DELETE/TRUNCATE cases require separate temporary fixture privileges
on those copies. It checks complete control rows, transaction boundaries and
terminal deltas; it cannot prove native effects, positive replay or writer custody.

`history_replay.py RUN_ROOT` independently checks the three finite connected-history
cases. It binds original terminal2 captures, migration38 output, full SQL/xmin,
native rows/sequence, inputs, Control refresh/end and complete driver observations.
Literal receipt/closure construction is separate from the Rust implementation.
The reader only reads retained artifacts; it never starts an engine or repairs a
capture. Artificial reader controls are separately labeled and do not qualify a
connected product run. See the current checkpoint for actual execution status.

`transition.py RUN_ROOT BUILD_ARTIFACTS` exercises nine finite invocations on
three fresh copies: preparation3, fresh empty retirement3, exact retirement retry,
and preparation4 from each genuine terminal2 origin, plus a future-metadata
refusal. It requires observed native closure, joined drivers and backend absence
before inspecting or reopening each copied store. Original stores remain unopened.
`transition_replay.py RUN_ROOT` independently reconstructs the portable bytes and
full SQL/native deltas from retained raw captures, including each process boundary.
These processes close their candidates; the resulting receipts cannot be used to
adopt a writer. Interrupted gap/CLOSING recovery, activation and installed journeys
remain separate qualifications.

`transition_faults.py RUN_ROOT BUILD_ARTIFACTS` uses the same resource owner and
one fresh activated-terminal2 copy for five processes: prepare with the COMMIT11
reply removed after server completion, direct gap recovery, empty retirement with
COMMIT11 stopped before forwarding, direct empty completion and exact retry.
The unchanged loopback wire proxy creates both interrupted states. The finite
probe's optional proxy argument changes only Control transport; its observer stays
direct. Missing native/driver/proxy custody prevents native reopen; available
SQL/input captures remain provisional. Proxy collection runs even if decoding or
writing the process response fails, retaining malformed original bytes.

`transition_fault_replay.py RUN_ROOT` reads those retained captures independently,
including the exact wire boundary, full75-table/native state chain and replacement
Control's exclusion of an unreleased predecessor. Caller CommitUnknown is kept
distinct from the observed durable outcome. These five processes qualify neither
activation nor admission, installed supervision, cancellation/expiry or RSS.

The `ingest-succession-writer` build profile checks the continuous native writer
and PostgreSQL activation bindings, plus the affected shared admission,
dictionary and input components. Its native fixtures use an explicitly synthetic
confirmed-activation premise. The profile preserves test executables and builds
the CLI/probe; actual SQL authority and connected writer delivery require a
separate live process fixture.

This profile excludes the packaged Archive-worker integration test, whose asset
and environment belong to its own installed lab. The `writer-followup` variant
runs only the dictionary/input/PG groups plus compilation and Clippy. It does not
repeat native admission/writer tests; reuse their separately recorded per-test
results only when the product inputs match exactly. An earlier aggregate failure
remains a failure even when its other individual tests passed.

`live.py RUN_ROOT BUILD_ARTIFACTS` uses three fresh copies for five finite
processes: live epoch3 from each original, and prepare3/empty3 followed by a new
same-process live4 candidate. Each live process activates its own prepared
candidate, refreshes Control, renews the Ingest lease and exercises a dictionary,
an historical AlreadyDurable event, a new signed event, exact retries, visibility
read and two certifications. A successful process must close native custody and
join both drivers; its captured completion event is selected explicitly.

`live_replay.py RUN_ROOT` independently checks full SQL/native/input state and
literal portable bytes. The finite import API does not claim a real source job,
installed acquisition, activated retirement, COMMIT-loss recovery or scale/RSS
qualification. Raw results and failure artifacts stay separate from artificial
reader protocol checks, which may reuse their model to construct transcripts.

`live_faults.py RUN_ROOT BUILD_ARTIFACTS` uses four fresh activated-terminal2
copies and eight finite processes. It removes the activation COMMIT12 response
or the first dictionary outcome COMMIT16 response, before forwarding and after
server completion. The shared proxy invocation retains COMMIT11 as its default
for the earlier transition fixture. The actual wire counters must independently
confirm the selected boundary; a caller's `CommitUnknown` does not decide which
state persisted.

`live_fault_replay.py RUN_ROOT` checks the complete SQL/native/input chain and
the fresh process's disposition. Only the copy without committed activation may
complete empty retirement. The other three fresh history processes must explicitly
refuse their activated nonterminal head, preserving native originals and any
committed SQL copy. This fixture establishes interrupted states and the current
refusal boundary; positive activated recovery and missing-copy repair require
their own implementation and qualification. Missing proxy/native/driver custody
forbids native reopen; malformed response bytes and provisional SQL/input
observations remain retained. See the existing checkpoint for execution status.

The `ingest-succession-recovery` build profile checks the activated-family native
and PostgreSQL recovery owners and affected lineage, input, prefix and legacy
components. Nested test groups run once. It preserves exact CLI, probe and native
test executables and runs Clippy; it does not execute a connected recovery or
qualify source closure, retirement, installed succession or unresolved tails.

`recovery.py RUN_ROOT BUILD_ARTIFACTS` uses six pinned actual final states from
prior live and COMMIT-loss runs. Each fresh database/native copy runs activated
recovery and then a separate retry process. The interrupted origins include the
already recorded fresh history refusal; they are never rolled back to an earlier
capture. Only positively closed native custody, joined drivers and backend absence
permit reopening a copy. Original stores remain unopened and their physical files,
inputs, full SQL and catalogs must remain identical. The finite scenario does not
qualify source/workflow closure, retirement, installed supervision or recovery
COMMIT-loss schedules. Execution status belongs to the existing checkpoint.

`recovery_replay.py RUN_ROOT` independently replays all twelve raw process
transcripts against literal receipts, closure bytes and full SQL/native/input
state. It distinguishes original copies from a missing current copy, preserves
historical xmin/recorder/time, checks one native closing mutation and no repeated
mutation on retry, and requires closed/joined custody before the next process.
Its artificial protocol/file checks reuse their model to generate transcripts;
those checks are separate from actual product qualification.

The existing recovery build profile also covers the successor source wrappers and
native existing-barrier observation through its lineage and transition test groups.
It still does not run a connected engine. `source.py RUN_ROOT BUILD_ARTIFACTS`
uses six actual recovered endpoints for first source publication and fresh retry,
requiring zero native writes throughout. Its output is `RUN_ROOT/source-close`;
`RUN_ROOT/source` remains the exact Git checkout. This first wave excludes missing
copy repair, corrupt closed-source states, source COMMIT loss and retirement.

`acquisition.py RUN_ROOT BUILD_ARTIFACTS` creates a genuine activated-zero successor
with a claimed acquisition job and a separate accepted but unclaimed intent. Four
processes establish that state, close its source, abandon the whole claimed intent,
and observe an exact retry. `acquisition_replay.py RUN_ROOT` checks the complete
SQL/native/input chain and original job/session attribution. This finite API
scenario does not execute network acquisition or invent historical authority.

`retirement.py RUN_ROOT BUILD_ARTIFACTS` copies three pinned, source-closed endpoints.
It retires the genuine abandoned-claim and dictionary-only heads, checks an exact
retirement retry, and prepares/activates a fresh live epoch4 from each. The third
origin must refuse its unbound Event during existing-copy replay. Retirement
preserves native bytes, input files and the original source row; its terminal
transaction changes twelve target rows and records the actual transaction/Control.

`retirement_claim_veto.py RUN_ROOT BUILD_ARTIFACTS` uses a separate exact run root.
It creates a genuine queued zero-request claim from the original empty predecessor,
closes its source, and requires retirement inspection to refuse. The unresolved
clone remains preserved. Neither suite abandons work implicitly for cleanup.

`retirement_capture_replay.py RUN_ROOT [SECOND_ROOT]` binds all nine raw process
transcripts, sidecar bytes, original file censuses and successive full states to
the independent `retirement_replay.py` reducer. The queued-claim diagnostic binds
the exact PostgreSQL ERROR/context to the observed backend and clock window; the
default server log does not supply SQLSTATE. Successful retirement and expected
business refusal both require actual closed/joined custody before native reopen.
The recovery build profile covers the new native/PG retirement groups. These
finite suites exclude retirement COMMIT loss, expiry/cancellation, installed
lifetimes, import logical closure, ACK/GC and capacity/RSS qualification.

`retirement_faults.py RUN_ROOT BUILD_ARTIFACTS` uses four fresh genuine-gap copies
for six processes. The unchanged wire proxy interrupts final COMMIT10 before
forwarding or after server completion; each loss is followed by a fresh retry.
Two other processes wait on the genuine final job row while the same pending
typed operation observes cancellation or real PostgreSQL UTC expiry. The fixture
never changes the job or lease to manufacture the outcome. Unknown process,
proxy or holder custody prevents external native inspection.

`retirement_fault_capture_replay.py RUN_ROOT` binds those captures, original
physical files, complete state chains, backend absence receipts and server logs
to `retirement_fault_replay.py`. A caller's `CommitUnknown` is separate from the
observed persistent result; retry after completion must preserve the original
terminal receipt and timestamp. This finite qualification uses the existing
`transition-probe` build profile, which runs compilation and Clippy without new
Rust tests. Reuse of prior component tests requires unchanged product sources.
Actual run status belongs to the checkpoint; installed lifetimes, additional
failure boundaries, capacity and RSS remain separate obligations.

The `ingest-succession-installed` build profile compiles the installed CLI and
finite inspection probe, runs the Serve ownership/supervision unit tests and
actual early-entrypoint checks, and runs Clippy. It preserves the binary test
executable separately from the normal CLI. The exact4cfac91 run passes20 tests
and all four commands; the existing evidence register binds its source and
artifacts. Unchanged native/PG component results retain their earlier exact
scope. This build does not execute the three installed HTTP/Sync lifetimes or
qualify their renewal, provenance or shutdown against actual engines.
