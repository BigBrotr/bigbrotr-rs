# Current persisted policy and incompatible-format refusals

Run the bounded native recipe through
`experiments/network-transport/lab.py REVISION TREE policy-versions`.
It builds the current checkout with declared dependencies and creates a fresh
isolated PostgreSQL lab. No old executable or private historical build path is a
prerequisite. The existing lab runner owns container/network/cache lifecycle.

The current single policy schema is `bigbrotr-product-policy/3`. The complete
explicit independent fixture is [policy-v3.json](../../tests/fixtures/configuration/policy-v3.json);
Python compact JSON and SHA256 supply its expected bytes and digest. It includes
explicit bans and receipt-retention64. Managed/declarative initialization,
inspection, identical retry, first activation and activation retry preserve the
accepted document. Staging retention62 does not replace accepted64; declarative
activation refuses drift. Current initial Control/Ingest writes, native reopen
and durable reads retain the existing independent dictionary/scientific oracle.

Separate cloned fixtures exercise superseded formats1/2, future4, missing bans,
missing retention and non-normalized bans, including recomputed digests. The
current CLI/Control owner must refuse them while preserving the full Control
relations. Healthy originals, all failed outcomes and resource receipts remain
retained. A before/after relation comparison is not a proof of no physical WAL or
row-lock writes: Control acquires transaction/row locks before policy validation.

Historical format1/2 source, inputs and executed evidence remain bound to their
original revisions. This source recipe no longer claims compatibility with those
formats. It does not perform a populated migration or repair incomplete documents.
Actual native execution remains required for this new schema; static tests are
not engine qualification. OPEN-prefix retirement, Live policy application,
physical-growth grants, pressure/fault capacity and release remain separate gates.
