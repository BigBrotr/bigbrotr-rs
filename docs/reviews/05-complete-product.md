# Revision 5 — installation, interfaces and maintenance

The product pass replaced a separate-dashboard-only framing with one monorepo,
independent static frontend builds, and three deliberate interfaces: public data
exploration, private administration and CLI. The existing 37-page site teaches
Python; a Rust release requires versioned migration and rewritten executable
examples, not a cosmetic rename. The historical dashboard phase and additional
website phase are separately mapped in PLAN.md.

Private administration adds versioned managed config, per-service application
status, permissions, jobs that survive browser disconnects, redacted signer
references, storage forecasts and verified-backup workflows. Deployment-locked
settings remain visible and cannot be silently overwritten. Frontend feature
visibility is not backend authorization. A shared dataset with scoped access is
the default, without claiming tenant-isolated archives.

First use is a complete local experience: sensible bounded collection and local
inspection, clear empty/stale states, signing and public exposure explicit, full
import/export/restore commands. Friendly recipe names do not create separate
images or storage schemas. Accessibility and actual browser/tutorial journeys are
required validation; no browser/product build has been performed in this stage.

Protocol research adds an interoperable base plus documented extension proposals
for richer measurements/assertions. Current NIP-90 status, NIP-85 algorithm keys,
NIP-66 timeout ambiguity and NIP-09 serving semantics replace stale historical
assumptions. No proposed upstream issue or PR has been sent.

Final reconciliation now checks these expanded surfaces against data ownership,
visibility, resource limits, exact provenance and the implementation sequence.
