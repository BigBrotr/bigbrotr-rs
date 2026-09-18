# Initialize Control once

This development command accepts the first durable configuration. It does not
install the product, enroll an administrator or start collection. Build the
[current binary](../development/build.md) and provision PostgreSQL 16.15 with
separate migrator and application logins first. Keep `fsync` and `full_page_writes`
enabled, and configure the application password through a protected local file.

Create and review a candidate, then explicitly migrate the configured database:

```sh
bigbrotr config init --recipe archive-only --output instance.toml
bigbrotr config validate --file instance.toml
bigbrotr migrate plan --file instance.toml --user bigbrotr_migrator --password-ref file:/run/secrets/bigbrotr-migrator --target 801
bigbrotr migrate apply --file instance.toml --user bigbrotr_migrator --password-ref file:/run/secrets/bigbrotr-migrator --target 801
```

Before migration, edit the candidate's endpoint/database, transport and protected
application credential reference to match the provisioned deployment. Static
validation does not check their availability. The migrator owns the schema; the
application role must have no inherited ownership, superuser or schema creation
rights. The initializer actively refuses these privileges.

A provisioning owner grants the current initializer these permissions in the
application database (using the configured role name):

```sql
GRANT USAGE ON SCHEMA control TO bigbrotr_control;
GRANT SELECT ON ALL TABLES IN SCHEMA control TO bigbrotr_control;
GRANT INSERT ON control.datasets, control.config_revisions,
    control.principals, control.management_audit TO bigbrotr_control;
GRANT UPDATE(config_revision) ON control.datasets TO bigbrotr_control;
```

These are Control initialization/read permissions, not a general service or
management grant template. Protect this credential as deployment authority.
Then run:

```sh
bigbrotr config initialize --file instance.toml
bigbrotr config inspect --file instance.toml
```

Success returns `initialized`, config revision `1`, a stable installation actor
and audit ID. One transaction writes the dataset, normalized configuration,
installation actor and audit receipt. The actor records the PostgreSQL session
role and local effective UID; it has no login, token or administrator privileges.
The security and topology heads remain absent. Managed and declarative modes are
both supported for this first acceptance.

A repeated command returns `already-initialized` with the original receipt.
Comments or changed candidate policy are reported as drift and leave the stored
configuration unchanged. Neither mode permits reseeding existing state. Wrong
instance/dataset, mode mismatch, incomplete pre-existing state or historical rows
without an initialization receipt are refused. Use the future explicit management
activation/reconciliation workflow for changes; do not manually repair immutable
rows or delete data to make initialization succeed.

Exit `4` with `outcome-unknown` means COMMIT may have succeeded. Preserve the same
dataset/instance and retry initialization: the database lock serializes resolution
against the earlier transaction. A lost stdout response has the same recovery
rule. `config inspect` can observe the accepted configuration, but it is not a
replacement for serialized initialization resolution. Other failures return a
redacted reason; unavailable PostgreSQL is never treated as a fresh database.

The operation has a 30-second transaction deadline, 20-second SQL statement,
5-second lock and 10-second idle-transaction limits. Its conservative 512 MiB
allowance plus connection preparation is retained through driver shutdown and
output; it is not a measured peak-memory bound. The source and stored-document
limits remain 10 MiB. Static candidate validation also counts the database's
JSONB rendering, including added spaces and expanded numeric exponents. A candidate
whose compact encoding fits but whose persisted text exceeds 10 MiB is refused
before acceptance; reduce its policy document rather than changing SQL limits.
This stage provides no release installation, Linux arm64
bootstrap, owner enrollment or signed-event admission qualification.

Executed evidence at `aad4ddc`: [25 native Linux amd64/PG cases](../implementation/evidence/p01-control-bootstrap-native-20260918.json), including their exact scope and exclusions.

The next explicit development operation is [local first-owner enrollment](../user/administration/enrollment.md)
after migration 805. Its separate evidence does not expand the older bootstrap
qualification above.

[First security activation](initial-activation.md) is separately explicit after
migration 802. It may precede or follow owner enrollment; it does not start roles.

[Maximum policy-size evidence](../implementation/evidence/p01-jsonb-limit-native-20260918.json)
separately qualifies `c7e0c09`: a 10 MiB JSONB policy survives initialization,
inspection, first activation and identical retries; larger persisted renderings
are refused before acceptance. This does not measure peak memory or fleet capacity.
