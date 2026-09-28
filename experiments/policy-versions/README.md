# Persisted policy version compatibility

This bounded acceptance recipe uses the existing exact-Git native runner:
`experiments/network-transport/lab.py REVISION TREE policy-versions`.
It requires the separately retained, hash-verified 6c52067 CLI from
`runs/ingest-visibility-6c52067/out/artifacts`; it never rebuilds that old revision.
The runner reuses the exclusively locked compiler cache, creates one new internal
PostgreSQL lab, and preserves stopped containers, databases and native stores.
The explicit /28 must pass the existing network/route overlap preflight.

The old actual CLI initializes v1 managed/declarative databases. The new actual
CLI inspects them, retries initialization, activates/replays the accepted revision
and distinguishes implicit empty defaults from explicitly staged bans. Fresh v2
initialization/activation runs in both modes, including a separate nonempty
normalized ban set. Frozen settings, literal domains and independent Python
digests supply expected documents; current product output does not supply them.
All Control relations and row xmin are captured around each finite CLI command.
Corrupt metadata and malformed normalized bans live only in separate clones.

The actual Control/Ingest probe reuses its existing dictionary bootstrap,
identical retry, native reopen and durable read journey for v1 and explicit v2.
The new mode changes only its independent expected document/digest. Existing
raw-byte dictionary oracle checks nonreceipt facts/indexes/counters; historical
configuration, policy, command, audit and principal rows remain unchanged.
Application-driver termination is checked before stopping the owned laboratory.

This does not qualify populated upgrades, acquisition issuance, restrictive
drain, socket admission, capacity, arm64 or release. Original failures remain
FAIL; required scenarios not executed remain incomplete. Results belong in the
existing implementation evidence and laboratory inventory.

The policy header reader uses pinned serde/serde_core1.0.229 and
serde_json1.0.151. Source inspection of `de/ignored_any.rs` and
`de.rs::{ignore_value,deserialize_ignored_any}` confirms that ignored settings
are scanned without constructing a second Value tree; iterative scratch still
consumes memory bounded by the already capped document. This header scan is not
the full closed setting validator. The existing512MiB parse allowance remains
conservative, not a measured RSS or throughput claim. Endpoint identity keeps
the existing pinned url2.5.8 normalizer; no new dependency or migration is added.
