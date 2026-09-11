# Revision 4 — failure ownership and long-term growth

The recovery pass found that process leases alone do not fence stale writers, and
that an admission ACK, archive snapshot and CH read frontier cannot be treated as
one durability guarantee. The failure matrix now specifies each boundary, backlog
limit, recovery action and evidence requirement. The RocksDB process-death probe
supports synchronized atomic admission, while hardware failure and the final Rust
adapter remain untested.

An independent challenge exposed a long-term provenance flaw in the earlier
bounded-cache proposal: every complete resweep could append the same edge again,
approaching delivery-history volume. Revised selection: exact permanent compact
edge admission inventory when provenance is enabled; append only new edges and
strictly earlier evidence corrections. This spends a P-sized index to avoid
D-sized archive growth. Stable observer/auth context and recoverable ordinal
mappings are required. Provenance-off pays neither cost.

The CH review also rejected logical-edge replacement before a published frontier:
a merge could erase the old fact needed by a pinned reader. Preserve immutable
source record IDs through replay/merges, filter before reduction, publish only
after deterministic projection chunks succeed, and compact into retained
versioned generations. An ACK in PG cannot prove a lost CH disk still has data.

The selected historical archive retains NIP-09 targets and requests. Ordinary
relay visibility applies logical deletion; exception-driven physical removal is
separate and cannot revoke previously copied exports. Low TrustRank never
causes automatic data loss. Security and recovery tests now exercise real
entrypoints, ambiguous ACKs, old-owner resumption, unavailable stores and overload.
