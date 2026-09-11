# DuckLake / PostgreSQL publication probe

Run `probe.py` with Python 3.13 and DuckDB 1.5.5 against a fresh disposable
PostgreSQL 16 catalog. Set `LAB_ROOT` to a new lab directory with a `results/`
subdirectory and optionally `LAB_CATALOG_DSN` for that disposable catalog. The
script creates tables; never reuse a live catalog. DuckDB extensions are installed
in the laboratory directory. Fixture rows are synthetic, not signed Nostr events.

Recorded container image:
`sha256:f1c3376c26f2609ab9f29f71f824103fe2fcd8ee0346485cb6122a4f93df6f94`.
The container had 0.5 CPU, 256 MiB, no swap and 100 tasks; the client process group
had 1 CPU, 768 MiB, no swap and 100 tasks. It used only loopback port 48432. Local
trust authentication was used for disposable synthetic data only.

The probe commits and rolls back across event, edge and receipt tables; kills a
writer before commit and after commit before application receipt; retries using
a receipt; and starts two independent transactions that both observe no receipt
before inserting the same logical batch. Both concurrent transactions committed,
leaving duplicate rows and duplicate receipts. A single publisher's receipt
check is useful; concurrent check-then-insert is not uniqueness enforcement.

Measured process group: 4.248 s runtime, 3.597 CPU s, 228.5 MiB peak, no swap.
The own PostgreSQL container was stopped after the probe. No other service or
repository was modified. SIGKILL is not power loss, device failure, filesystem
corruption or validation of DuckLake's underlying fsync behavior.
