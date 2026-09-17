# Operations contract reference model

Isolated research for S03/S04/S05/S08; no product adapter or service. Python's
standard library is sufficient. From the repository root:

```sh
python3 experiments/operations-contract/model.py --output docs/research/evidence/operations-contract.json
```

The model executes 120 permutations of five admission/handoff actions and then
recovers with the same request identity. Invalid prerequisite transitions refuse;
every intermediate modeled durable state is checked. Separate schedules cover a
two-request window with a longer page, a verified request delayed past producer
closure, a stale owner epoch, mixed owner/producer backup cuts, an unresolved
archive commit, partial CH families (including zero-row completion), incarnation
replacement, dirty-only derivation through three lost responses, durable job
acceptance/cancellation and ambiguous signed delivery after policy/temporal expiry.

Seven negative controls deliberately construct forbidden states: a mixed backup
cut plus ACK over a hole, a second archive attempt while unknown, partial serving
activation, dropped derived pending output, unauthorized resend and changed retry
bytes. The expected oracle failure is PASS for each negative control; an oracle
accepting one raises an assertion and fails the script. Final expected fact sets,
target outcomes and source cuts are literal fixture results, not outputs copied
from a production reducer. Recipe arithmetic and an abstract acyclic wait graph
are checked separately; this graph is a design fixture, not extracted runtime code.

Output includes the model source SHA-256, execution time, individual scenarios,
negative-control count and explicit NOT EXECUTED scope. The checked-in evidence
is small/reproducible; no downloaded data, external network, credentials, containers
or Tux resources are used. Re-running updates only the named evidence file.

This model treats operations as in-memory atomic transitions. Copying state
represents a modeled caller restart; it is not a killed process or a disk flush.
Set membership stands for logical immutable CH identity and does not test engine
merges or retry behavior. A modeled epoch check does not establish actual old-owner
exclusion. The script cannot establish native-memory bounds, starvation freedom,
power-loss recovery, catalog/file durability, backup compatibility or performance.
The [operational contract](../../docs/specification/operations-contract.md#fault-schedules-and-implementing-agent-handoff)
assigns each real fault test to its implementing task.
