# Workload acceptance reference probe

This standard-library Python experiment checks the paper workload arithmetic and
one bounded virtual scheduler. It is separate from the actual signed semantic
fixture, storage qualification and future BigBrotr product tests.

```sh
python3 experiments/workload-contract/probe.py --output docs/research/evidence/workload-contract.json
```

The scheduler has 500 endpoints due twice in 600 ticks, 10 work quanta/tick, and
simultaneous control, live, endless backfill and maintenance work. A class gets its
minimum share before unused one-quantum capacity is borrowed oldest-first. Literal
expected due sets require every endpoint in both cycles; offered work must equal
completed + refused + still queued work. The deliberately wrong priority policy
has excellent control response time while starving every monitor obligation, and
must fail the independent oracle. A second negative control discards the backlog
from reporting and must also fail.

Arithmetic checks retain exact rational mean event size, measurement cadence and
three-node stationary graph solution. They verify the fixed fleet table's U<=P<=D
and show that its 100k payload target alone exceeds one 1 Gbit link. This does not
predict compressibility, signature CPU, disk usage or real network throughput.
The complete 2,000-delivery quota window selects C=2 from existing edges and S=1
from known-body/new-edge deliveries, with P=1,000 already including S. Two further
negative controls misplace S or overlap C/S; placing S in an existing-edge bucket
would introduce a previously unseen scoped witness and raise actual P to 1,001.

Evidence records the source hash, all offered outcomes, maximum queue/wait values,
negative controls and NOT EXECUTED scope. Ticks/quanta are dimensionless; model
results cannot establish wall-clock SLOs, real socket fairness, engine durability,
native termination, fleet capacity or release quality. Those evidence boundaries
are assigned in the [workload contract](../../docs/specification/workload-contract.md).
No network, Tux resource, external engine or credential is used by this probe.

`measurement-fixture.json` is a literal domain input/expected-output fixture for
four execution/cancellation schedules and one retained NIP-11 document. It is not
the product wire schema and was not executed against a network peer. The future
adapters must reproduce its outcome/timestamp/denominator/reference assertions.
The separate valid signed event/graph corpus belongs to
`experiments/identity-contract/signed-fixture.json`.
