# Private historical event reads

This integration is under qualification, not an advertised release capability.
It is the first consumer of the shared [QueryRequest](../../../contracts/read/v1/request.schema.json)
and [QueryPage](../../../contracts/read/v1/response.schema.json), mounted at
`GET` or `POST /v1/resources/event_history` on the private administration origin.
The [OpenAPI profile](../../../contracts/read/v1/openapi.yaml) describes this subset.

Use a live local browser session and its explicit `X-CSRF-Token`. POST also needs
the exact same-origin `Origin` and JSON content type. Grant `history.read` for the
complete dataset/kind/domain/ordinal scope through the reauthenticated grant
workflow. Dataset read permission or administrative status alone is insufficient.
The cookie adapter does not authorize public HTTP, export or other transports.
Reads do not refresh session inactivity; explicit user session inspection owns
that activity update and must not run as an unattended keepalive.

POST example (replace the fixture identities with authorized values):

```json
{"version":1,"resource":"event_history","mode":"snapshot","scope":{"dataset_id":"01010101010101010101010101010101","dictionary_kind":4,"allocation_domain":"04040404040404040404040404040404","ordinal":"1"},"filters":[{"ids":["0000000000000000000000000000000000000000000000000000000000000000"]}]}
```

GET carries `scope` and `filters` as individually percent-encoded JSON query
values, plus optional version/mode/page_size. Decode once; literal `+` remains
`+`. Duplicate or unknown fields are errors. This first profile requires exactly
one ID and snapshot mode; broader filters, cursors, projections and counts remain
required implementation work. Unsupported constraints return400.

Success contains the original complete signed event or an empty `items` array.
Empty means a completed lookup in the authorized scope and certified source cut;
an unavailable engine never becomes an empty successful result. Read stamps name
the exact archive snapshot, sealed frontier, complete ordered stream bounds and
policy epoch. Metadata integers use decimal strings; signed Nostr fields retain
protocol numbers. Immutable history has no ordinary expiry generation/deadline.
Responses are private, `no-store`, and issue no cookies.

The current native profile allows one in-flight read, at most16 queued commands,
8192 request bytes, one1MiB native row and2MiB serialized response. Smaller
configured request/page/queue/response/execution/authorization limits apply.
Its engine has an existing512MiB worker floor, plus parent decode/output and
credential/transport reservations. A `read.execution.max_memory_bytes` allowance
below that complete cost refuses capacity; the common64MiB default does not
qualify this cold native reader. Set a deliberate sufficient allowance (the tiny
qualification fixture uses1GiB) within the deployment's actual process budgets.
This is not a throughput or common-profile memory qualification.

Before event data scans, admission charges two complete physical file passes for
both payload and eligibility tables at the selected snapshot, without credit for
predicate pruning. It verifies canonical local file lengths against catalog
metadata and caps the population at2048 files and256MiB charged bytes, or the
smaller configured scan limit. This conservative rule can refuse a selective read
on a large archive. It does not bound OS read amplification; bind-time metadata
allocation remains inside the separate native memory/deadline limits. The paired
parent/worker private protocol must be deployed together. No persistent format
or SQL checksum changes. Known scan/row exhaustion has a dedicated private
worker refusal exit, accepted only after positive reap and empty output; it
becomes HTTP429, while failed engine access remains503. Neither is an empty page.

A five-second absolute connection ceiling, the configured execution deadline,
session/read authorization lifetime and actual granted pin lifetime all constrain
work. Renewing Control cannot extend the latter two local windows. Every actual
socket write rechecks them. Expiry/disconnect cancels native work; a positively
joined child and positively terminated HTTP consumer precede pin release. A
connection may close without a complete error response when authority expires.
Unknown worker, Control or delivery outcomes retain the obligation for recovery.
Standalone export/restart and the broader selected resource registry remain open.
