# First-owner native acceptance

`run.py` consumes one newly created `control-migrations/lab.py` lab and the exact
clean Git revision used by its mounted executable. Run as the unprivileged lab
owner on native Linux amd64. It never reuses an existing report or emits token
output; generated credentials remain under the lab's protected `secrets/`.

The executable migrates, initializes, issues/revokes/consumes tokens, persists
credentials/grants/audit and resolves enrollment after restart and actual COMMIT
faults. The harness independently reads all 29 relations, checks the literal
administrator action set and scope, validates token hashes, compares full dumps
for read-only/refused operations and executes 15 exact SQLSTATE negatives.
Expiry waits the real ten-minute lifetime. Fault clients have separate ownership
labels and are stopped in `finally`; the base lab must be stopped separately.

```sh
python3 experiments/local-enrollment/run.py --root /absolute/new/lab
```

`reference_vector.c` is a standalone oracle compiled against the official PHC
reference at `f57e61e19229e23c4445b85494dbf7c07de721cb`, with `src/ref.c` and
`ARGON2_NO_THREADS`. Its synthetic password, salt bytes02, Argon2id version19,
65536KiB/3/1 and 32-byte result independently fix the Rust unit-test expectation.
The checkout/build belong under ignored research inventory, not product sources.

This does not qualify browser sessions, action enforcement, remote routes,
reauthentication, recovery/last-owner protection, native arm64, power loss,
maximum authentication load or a release. Issuance response-loss and runtime
memory measurements remain separate required cases. The protected
`credential-observations.json` retains test credential hashes/salts separately
from the shareable report. Run the pinned C oracle with each observed salt as its
single hex argument to independently compare the complete stored hash; record
that additional execution before claiming credential-value verification.

The dated `upgrade.py`, `crash.py` and `hash_process.py` follow-ups operate on the
inventoried ownerone lab and exact 992f0dc executable. They are concrete research
reproductions, not generic installation helpers: preserve their recorded ownership
checks when adapting them to another newly inventoried lab. Upgrade compares all
existing 801 content against additive 805 and refuses downgrade; crash checks ten
complete database dumps around actual PostgreSQL SIGKILL. The hash probe measures
ten finite rejected attempts that still perform the selected hash, separately from
successful enrollment or mixed-workload capacity. Their execution source hashes
are retained with the native evidence; no product source is edited on Tux.

`self_inspection.py --root LAB` exercises `access me` in a new owned database
on a clean matching artifact. It verifies the whole account report against
literal expected actions, actual enrollment and a separately computed PHC C
credential; wrong/disabled/missing identities, revision mismatch, changed/empty
grants and protected-file/deployment/SQL privilege refusals must leave the complete
schema dump unchanged. PostgreSQL restart must preserve the final state. This
runner does not qualify remote authentication, sessions or action enforcement.
