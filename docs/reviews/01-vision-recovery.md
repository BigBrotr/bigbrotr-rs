# Revision 1 — recover intent before selecting tools

Status: completed scoped source reconstruction; service-specific reading is recorded
in the research reports. This is not a line-by-line audit of the changing Go tree.

Material revisions to the earlier architecture sketch:

1. Restore all fifteen services and their data contracts, not just collection,
   storage and ranking. Include imported assertions, publications and operation
   recovery state in the permanent-data model.
2. Treat dashboard and website as different products sharing one contract and
   brand. The existing site teaches Python and requires versioned migration.
3. Reopen historical restrictions such as no RPC and one RunOnce loop for every
   service. They conflict with independent deployment and resident listeners.
4. Replace a single completeness cursor with traversal, durable acquisition,
   archive, serving and scoped evidence frontiers.
5. Preserve optional provenance physically: an off setting removes edge writes,
   edge indexes, exports and provenance-derived computations.
6. Record the owner's equal-quality instruction rather than force a preferred
   first-launch product area. Record private temporary repository naming.

Next revision must establish ownership and default/reload contracts, then test
whether the proposed storage and SDK boundaries can implement them.
