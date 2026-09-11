# Revision 2 — service contracts and unnecessary combinations

Completed as a design revision, not implementation validation.

The six acquisition contracts and downstream service inventory exposed hidden
coupling: an indexer depending on a running synchronizer, one-cycle supervisors
around continuous streams, discovery cursors based on author-controlled time,
and public capability flags without working data. The revised design uses durable
input contracts, distinct lifecycles, admitted-order cursors and answer-quality
metadata. Code crates, services and processes are explicitly different boundaries.

Rereading the original four storage combinations showed that omitting signed
fields complicates import, verification, serving and migration. Following a
comparison with full archival plus projected exports, remove the body-loss axis.
Keep original signatures and all signed fields. Provenance and filters remain
orthogonal; no TrustRank pruning. This follows a technical argument, not merely
an instruction to adopt a user's suggested technology.

The owner's naming follow-up is handled with one product/image and transparent
recipes. A friendly new LilBrotr nickname cannot silently reinterpret the legacy
runtime profile. Configuration now needs generated defaults, units, reload classes,
provenance history and a preflight that reports actual topology.

Sources and detailed contracts are in acquisition-services.md,
derived-read-publication.md and dashboard-and-website.md. Cross-component storage
publication and failure cases remain the next substantive challenge.
