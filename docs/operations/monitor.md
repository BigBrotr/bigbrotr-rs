# One passive Monitor source

The development executable supports one explicitly selected passive WebSocket
operation during **fresh** initial Ingest ownership. This slice is under
qualification. It does not implement ordinary Monitor cadence, fleet scheduling,
the other default checks, restart adoption or complete dispatch recovery.

Prepare the ordinary initialized Control database, current migrations through
0044 and the packaged narrow Control privileges. Activate accepted configuration
and policy with local Control, Ingest and Monitor placement. Both accepted
documents must enable Monitor and select exactly these settings:

```toml
[monitor]
enabled = true
checks = ["open", "read"]
store_checks = ["open", "read"]
check_timeout = "15s"
retries = 1
```

The normal default includes additional checks and is explicitly refused by this
command. This fixed profile also requires the ordinary `network.connect_timeout`
of 10s, `network.max_message_bytes` of 2MiB and `pipeline.parse_arena_bytes` of 8MiB;
changed limits are refused. Its normalized event verification bound is 256KiB.
The fixed passive profile makes one connection and sends one REQ with
`kinds:[1],limit:1`. It verifies correlated kind1 EVENT candidates sequentially,
accepts an empty correlated EOSE, observes AUTH without responding, and sends no
signed response, publication or second probe. At most three EVENT candidates,
including unrelated candidates, are processed. A valid first event remains
measurement metadata if a later failure occurs. Event bodies are never admitted
or archived by this path.

Create an owned mode 0700 directory on the trusted local host. Supply an absolute
canonical selection path beneath it; the directory must already exist and its
identity must remain stable. Leave the file absent initially:

```sh
bigbrotr serve --file instance.toml --initialize-ingest \
  --roles control,ingest,monitor --monitor-once /var/lib/bigbrotr/monitor-once.json
```

The command explicitly creates a fresh Native store and reports its actual
`public_scope` and `private_scope`. Use the existing private management interface
to register the endpoint and enroll its exact access scope, visibility and direct
route. Then atomically publish a regular owned mode 0600 selection file with one
hard link and no more than 4096 bytes. Use the canonical IDs and generation from
the actual management receipts; the following IDs are illustrative:

```json
{
  "format": "bigbrotr-monitor-once-v1",
  "endpoint_id": "11111111111111111111111111111111",
  "access_scope_id": "22222222222222222222222222222222",
  "enrollment_generation": "1",
  "route_id": "33333333333333333333333333333333",
  "visibility": "private"
}
```

IDs are nonzero lowercase hexadecimal strings. Generation is a canonical nonzero
u64 decimal string. Visibility is exactly `public` or `private`, selecting the
corresponding newly initialized scope. Unknown or duplicate fields refuse. The
actual enrollment, endpoint, route, visibility and both accepted policy documents
are checked again by the guarded dispatch. A private route or query-bearing URL
cannot be declared in public visibility. Protected-file reads reject symlinks,
unsafe permissions, excessive size and changed directory/file identity or content
metadata. An absent file is an armed wait; the first existing selection is consumed
once. A malformed selection is an explicit failure. The command neither deletes
the file nor rearms it, and does not use it as a durable dispatch queue.

One fresh positive dispatch COMMIT returns the only runnable token. Its original
authority is at most 5 seconds, including setup; the nominal15-second read profile
does not extend that authority. All socket, verifier, capture and admission
capacity is reserved before the probe. The existing Native owner remains held
through the borrowed socket operation and actual socket teardown. Once the source
has returned, fresh current Control/Native authority admits only the compact
MeasurementEvidenceIntent through the ordinary producer protocol. The known
initial measurement completes the generic producer checkpoint and confirmed
Native ACK before reporting completion. Outcome and checkpoint commit together;
only a positively confirmed checkpoint COMMIT permits the Native ACK. The
checkpoint carries the exact dictionary and measurement dependency union and
creates no acquisition progress. Cancellation, uncertain COMMIT or failed Native
ACK leaves the result parked with its evidence retained; a PG outcome alone is
not completion authority.

JSON diagnostics use `bigbrotr-monitor-once/1`. The initial Ingest report contains
`monitor_once.state="armed"`. Later lines report `selected`, `completed`, `no-io`
or `parked`. A completed `result="admitted"` includes the exact `attempt_id`,
seven full context references, effective authority ceiling, `request_id`,
`request_digest`, and `measurement` coordinates (`dataset_id`, `shard_id`,
`owner_epoch`, `sequence`, `payload_digest`). Retain those coordinates for later
Archive measurement inspection. These diagnostics grant no delivery authority
and always report `ready:false`.

A positively proved pre-I/O refusal records an operational closure without an A05
sample. An outstanding dispatch, lost no-I/O proof, failed capture finalization,
or uncertain input/reservation/outcome stops the one-shot progression. Known fixed
capture metadata stays in RAM until owner shutdown; no terminal result or
CompletionUnknown is invented. Preserve the original Native inputs and Control
history for recovery. Missing producer reservation/receipt is not proof that a
probe did not run and never permits a fresh probe. Complete durable lost-RAM
dispatch recovery remains a separate gate.

Optional existing Archive and Index roles may be selected; Index still requires
`--initialize-index`, and Archive requires its ordinary explicitly initialized
catalog/coordinator/worker prerequisites. Simultaneous Sync is refused by this
finite producer sequence. The fresh path uses the two original dictionary
requests, then one six-descriptor request and one Measurement request. It creates
no acquisition job/scope, event fact, receive/provenance record or Sync progress.
The four requests reach generic checkpoint C4 and Native I4/A4 with source cut10.
All original inputs, outcomes and facts remain retained. ACK returns request
window credit, without removing the producer-detail or dispatch ceilings. Open
prefix retirement refuses a producer with Monitor-attributed requests before
native retirement or unlink: dispatch settlement and attempt uniqueness still
need those rows. Full monitor-aware closure preservation and retirement remain
separate gates.
`--monitor-once` means one source operation, not a process lifetime: serve keeps
running for management, publication and supervised shutdown. Stop it normally
after required Archive publication; no writer is reopened on restart.
