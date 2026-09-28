# Acquisition registration laboratory

This trusted native Linux experiment qualifies only the internal P03-T01
registration boundary. The `acquisition-scope` build profile uses the existing
exact-Git runner and exclusively locked1154757 compiler cache. It builds the
actual CLI and `acquisition_scope_probe`, plus catalog/encoding checks and Clippy.
Build success is not engine qualification.

From the clean candidate checkout under its inventoried run root, run:

```sh
python3 -B experiments/acquisition-scope/run.py /absolute/run/root
```

The engine recipe borrows the stopped, ownership-verified source-recheck PG16.15.
It checks the retained `bb_transport_8c4d12c_pass` basis against its original
complete table evidence and creates new retained clones. No original database,
Rocks store or archive is edited. No new network, build cache or volume is created.
The isolated client joins only that PG network namespace with read-only source,
artifacts and owned fixture-secret mounts; credentials are never printed.

The [first-wave report](../../docs/implementation/evidence/p03-acquisition-scope-native-20260928.json)
records six executed cases: normalized retry/restart and distinct
access/visibility/selector identity; forced candidate collision saturation with
full-byte equality; scope-FK failure rolling back the provisional selector;
server-observed owner expiry rejecting COMMIT; explicit missing migration; and
historical identity recovery after a clone-only policy-revision advance. The
latter retains both original accepted documents; it does not qualify management
activation. An independent Python/SQL observer compares literal selector documents,
full scope dimensions, all other Control tables and driver termination. Populated
migration checks prior rows/checksums and heap filenodes plus identical reapply.

Run results, exact artifact identities and terminal audits determine which cases
actually executed. Failure logs and clones stay retained. Lost COMMIT/cancellation,
concurrent locks, corruption/relational negative controls and installed Sync are
explicit later acceptance, never inferred from this first wave. No capacity,
arm64, M1 or release claim follows from these fixtures.
