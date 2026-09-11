# Revision 3 — test the proposed technology boundaries

Completed experiments changed the design rather than merely confirming it.

* The PostgreSQL-backed DuckLake probe demonstrated atomic three-table visibility
  around process death and receipt-based retry under one publisher. It also
  demonstrated two concurrent check-then-insert transactions both committing the
  same logical batch. The plan now requires exclusive archive publication and
  real fencing for takeover; it does not invent DuckLake UNIQUE constraints.
* A test injected into the pinned Rust Nostr SDK handler reproduced acceptance
  of a changed-content message after the valid claimed ID had warmed its cache.
  The existing saved-ID test misses the warmed-cache sequence. The plan owns full
  validation at admission and binds verification cache entries to actual payload.
* Sixteen upstream relay-parser tests passed, including a NIP-67 fixture that
  intentionally discards the hint. Passing tests therefore do not establish the
  information fidelity needed by BigBrotr. Preserve the bounded wire envelope.
* Reconsidering Parquet batch size against read latency separated synchronized
  admission, archive publication and online indexing. Indexing consumes durable
  pending work without waiting for large Parquet files to close.
* Prior storage experiments challenge the historical ID-ordered CH layout; they
  are too small to settle final DDL. The comparison and realistic part-count
  validation remain explicit, with logical correctness required in both layouts.

Reproduction sources and result JSON are tracked under experiments/ and
research/evidence/. These are scoped process and component probes; they do not
certify the future application or hardware power-loss behavior.
