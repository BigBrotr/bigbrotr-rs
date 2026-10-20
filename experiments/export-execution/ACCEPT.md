# Genuine signed-event acceptance setup

Qualification source only; compilation, helper execution and runtime are
**NOT_RUN**. This additive fixture supplies the acceptance responsibility missing
from the frozen measurement packet `c68ac35`. It changes neither production
policy/deadlines nor the old probe modes. The original signed recipe, measurement
reader, worker, memory runner and output oracle remain unchanged.

The new command is:

```text
export-execution-probe accept /lab/accept.json
```

It uses a real Control claim, password challenge/verification/session, an exact
stored Archive selection, actual catalog barrier, reader pin and native visibility
observation, then `ControlSession::accept_export`. It verifies the historical
same-key retry and reads back `Accepted` with no reservation. There is no queue
poller, `reserve_export`, `start_export`, output writer or publisher in this mode.
This does not certify that the selected event was published: the caller must bind
that fact independently to the genuine ingestion/publication and final output.

## Root-owned fresh setup

1. Authenticate exact source/probe build and unchanged packaged `9a7fd81` worker
   (executable, library, extension and bundle verification). Create separately
   inventoried Control/catalog/data/state/destination/scratch resources. Apply the
   real migrations/grants and explicitly initialize catalog, same-root coordinator
   and destination with the existing CLI owners. The old small `E_public` export
   seed is not a substitute. Initial fixture policy is recorded before claims;
   no live policy, source cut, Archive receipt or job row is manufactured.
2. Reconstruct only the independently frozen recipe:

   ```text
   python3 experiments/export-execution/memory_relay.py \
     tests/fixtures/protocol/archive-event-response.json /lab/event.json
   ```

   Recipe SHA256 is
   `815bf9e7e0fbcda73a77fbc3f91bebe0ec5c5f356e01bd665f695a8edb5af963`;
   the complete signed body is exactly94,577bytes, SHA256
   `5f880ef37e4bdafb8dd37c42a796afa5c4db1a2f9a5363b625c3bbaba2b78727`.
   No key, signature or alternative content is generated.
3. Use the existing actual login/enrollment/acquisition/initial Sync+Archive
   path on this fresh root. The controlled relay retains its existing handshake,
   masked REQ/CLOSE checks, bounded3s socket, signal/thread/process custody and
   raw traces. For this explicit case only, call
   `memory_relay.send_event(connection, actual_subscription_id, frozen_body)`;
   record its returned frame metadata and retain the raw frame bytes. The helper
   emits the unmasked FIN/text frame with shortest 64-bit length encoding.
   Its128KiB transport cap admits this fixed input; it does not alter Sync's
   configured256KiB event envelope. A repeated frame can exercise deduplication.
   EOSE and close use the existing small-frame encoder. Do not pass this body to
   the legacy `<65536` encoder or silently monkeypatch that shared helper.
4. Observe the actual positive publication/frontier on the same physical roots,
   independently compare the retained event ID, full content (including NUL,
   UTF-8 and escapes), ordered/duplicate tags and signature, then stop and join
   publisher, native worker and drivers. Bind native/input/Archive receipts,
   catalog/source files and exact backend absence. Do not copy the old journey's
   fixed source cuts/snapshot numbers or simply replace its small event oracle.
5. With ordinary Control/reader, use the existing authenticated
   reauthentication + `accept_scope_grant` path for `history.read`, `export` and
   `private-export` on the actual archived visibility. No Export request is made
   to that Serve process. Stop/join it before the acceptance probe. A configured
   scheduler may be present: there must be no accepted Export job to race.
   Do not disable scheduling as a way to create accepted authority.
6. Write `accept.json` from observed exact identities/bindings/documents and the
   authenticated principal's credential file. Run the new finite probe, retain
   stdout/stderr/ledger, exact PID/start/exit and Control/catalog backends. The
   successful final event is emitted only after catalog and Control joins.
   Its actual job ID supplies the unchanged measurement descriptor. Use new
   scratch/ledger paths and a fresh dedicated measurement cgroup, then invoke
   `memory_run.py` and the independent `memory_oracle.py` as documented in
   `MEMORY.md`. No setup RSS is relabelled as measurement RSS.

The additive [fresh setup composition](FRESH_SETUP.md) supplies that explicit relay
callback, genuine initialization/grants and separate accept-only follow-up.
It remains source-only and requires root's exact artifact/resource/custody inputs.
All commands above are proposed runtime commands, not executed evidence.

## Closed acceptance descriptor

`format` is `bigbrotr-export-accept-setup/1`. It contains the same `name`,
`dataset`, `instance`, `control`, `catalog`, `reader_user`, `reader_password_ref`,
`config`, `policy`, `data_root`, `state_root`, `destination`, `scratch`, `ledger`,
`worker_root`, `worker_bytes`, `worker_sha256`, `limits` fields and meanings as
the measurement descriptor. It contains no supplied `job` or native receipt.
Instead it requires `username`, `password_ref`, persistent `command_key` and
the actual `ExportCommand` JSON: `format`, exact-ID `query`, `allocation`,
`destination`, `expected_config_revision`, `expected_policy_revision`.
The query must select frozen ID
`05e909d2c260edbb52e21e3813a492ade70523c8493fb0ae5a9a37a04354db31`;
the allocation is an observed catalog/frontier, not a guessed snapshot.
Destination ID/digest must match its genuine initialized marker.

The baseline keeps the full1MiB event envelope. The setup's different native
operation is only a512-byte visibility fact read (two-row bound); its parent
overlap is `64*4096` bytes. Preflight before claim conservatively admits Control
credential copies+528MiB, catalog copies+80MiB, reader1MiB, password hash65MiB,
visibility overlap, caller-supplied native engine+reserve and128KiB control.
The existing16MiB fixture allowance is retained independently. Hash and native
work run serially and are actually joined. No ledger reservation is a measured
RSS or per-export256MiB claim.

Control lease/SQL timeouts remain5s, native query<=4s, capture/reader pin30s.
The outer root wrapper must bound the whole setup to120s, retain all output, and
classify a forced/missing terminal as UNKNOWN; never reopen roots merely because
that timeout expired. No step retries an unknown COMMIT. Keep the original
command key and actual rows for explicit recovery. The known same-session retry
uses the identical key and requires the same immutable job/time with no commit.
The temporary reader pin is released only after the native join and known
acceptance; the separately committed Export pin remains retained. Original and
checked destination, visibility observation/grant, coordinator, transport grants
and drivers survive fallible work through actual joins. CatalogCapture is dropped
or finished under its actual driver/coordinator contract; no guessed rollback.

## Independent expected observations

Acceptance must add exactly one authenticated `job.export.accept` command/audit,
one input-complete Accepted job at revision1, its immutable `export_jobs` row,
the exact complete input bounds of the selected sealed frontier, and a distinct
durable Export pin/retention claims. There is no reservation/artifact/unit or
execution attempt. The prior three scope-grant histories, all source/native/input
bytes and all Archive publication/frontier/catalog/file state remain exact.
Browser login/revoke and the temporary Reader pin lifecycle are separately
expected; current Control claim/release uses its true backend/incarnation.
Keep all75table captures, including xmin, so existing rows and the same-key retry
cannot be normalized. SQL shape, pin purposes/holders and unchanged science must
be checked from raw state, not from `state:"accepted"` telemetry.

`accept-returned` binds the actual commit-start flag, error and receipt;
`accepted-job` records receipt/retry, query/catalog/frontier and job revision.
Every successful event sequence ends with real driver joins, grant release and
`export-accept-setup-complete`. Errors preserve an unknown accepted-job/pin
possibility and never emit that final event. Fresh process/backend exclusion and
source/build/credential provenance are root-owned checks outside this probe.
The measurement's unchanged oracle compares the entire signed JSONL and manifest
against the independent frozen recipe. Publication, five completed exports,
smaller envelopes and the256MiB gate remain unqualified here.
