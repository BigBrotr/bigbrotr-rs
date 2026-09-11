# Revision 6 — reconcile the entire design and test a counterexample

The final pass cross-read service contracts against shared data/runtime/security
contracts and implementation order. It changed the design in these concrete ways:

1. Exact edge admission replaces the earlier lossy-cache-only suggestion, avoiding
   repeated-sweep archive growth. Compact dictionaries and their backup/portable
   representation are explicit. Billion-scale layout candidates retain the same
   exact semantics and must be compared including write amplification.
2. A real CH experiment reproduced old-snapshot loss under logical-edge replacing
   merges. Immutable record keys preserved the old frontier across forced merges,
   duplicate inserts and a delayed table family. The test is intentionally tiny;
   PG publication orchestration, real schema joins and scale remain future tests.
3. Serving publication now defines immutable source/projection IDs, deterministic
   chunks, source-cut vectors across shard epochs, complete-family receipts,
   synchronized serving persistence, incarnation and retained page-token state.
4. Turning provenance collection off preserves earlier history; a new instance
   with it off allocates no edge history. Those two operations are not conflated.
5. Exporter now distinguishes native DuckLake deployment backup, portable Parquet,
   full JSONL and projected analytical data. Archive-maintenance owns one scrub
   ledger; Exporter requests work instead of scheduling a duplicate scrubber.
6. Private administration separates desired from applied security revision, uses
   bounded authorization leases, has an action/permission matrix and protects
   source/destination paths and signer activation. PG outage is not permission to
   serve forever from stale authorization.
7. Public aggregate/graph generations bind public-eligible inputs. Filtering
   private rows after graph computation would still leak their influence; source
   scope is part of the generation and publication identity.
8. Recipe dependencies now state which roles can operate without CH. A friendly
   recipe name cannot hide different data guarantees or tenant isolation.
9. The roadmap covers all historical service responsibilities, public dashboard,
   private administration, the separate website workstream and distributed growth.
   Real-entrypoint tests and empirical decision points are assigned explicitly.

## Validation and remaining limits

Four new probes are documented: SDK handler/parser, RocksDB process death,
DuckLake/PG process death and concurrent append, and CH immutable-frontier merges.
Prior transport/ranking/storage comparisons are imported as scoped evidence.
Mechanical checks validate links, fences, JSON examples/evidence, Python syntax
and selected hashes. They do not prove architecture or future product behavior.

Remaining work belongs to implementation: final Rust storage adapter, full schema
and receipt orchestration, hardware/ENOSPC/fencing tests, realistic billion-row
query/edge layout, full concurrent load, browser/website execution and release
installation/restore. The plan supplies concrete contracts and test outcomes to
establish, not fabricated completion of those future tasks. No product code has
been implemented and no upstream proposal has been sent.
