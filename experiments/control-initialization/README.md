# Native explicit Control initialization

Run `python3 experiments/control-initialization/run.py --root LAB_ROOT` on a new
owned lab created by `experiments/control-migrations/lab.py setup`, using a clean
exact-source build. The runner uses actual CLI commands, PostgreSQL 16.15,
application SCRAM credentials and a loopback PostgreSQL protocol fault proxy.
It creates isolated databases and four labeled bounded fault containers; those
containers are stopped and retained. The owning caller stops the base PG/client
lab after inspecting the final report and data. No previous lab is reused.

The 25 cases cover atomic initial rows, replay, file drift, wrong roots/mode,
migration privileges, immutable attribution/audit, restart, declarative setup,
missing migration, mid-transaction privilege failure, incomplete prior state,
COMMIT loss before/after forwarding, a competing blocked initializer and process
termination while COMMIT is held, each followed by serialized resolution.

The independent policy literal is the frozen configuration-inspection fixture;
its lineage and limitations are documented there. All 25 relations are inspected
at each initial/fault boundary, full policy values/digests and actor/audit bindings
are compared, and no other data may appear. This qualifies configuration
initialization only. Human enrollment, credential issuance, active security policy,
writer ownership, signed-event admission and complete installation remain later
work. A committed harness or planned invocation is not executed native evidence.
