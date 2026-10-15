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
