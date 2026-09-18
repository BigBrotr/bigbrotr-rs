# Activate the first security revision

The development executable can explicitly accept the first security snapshot of
an [initialized Control database](control-initialization.md). This prepares the
policy prerequisite for runtime work; it does not start collection or issue a
writer lease. Human [owner enrollment](../user/administration/enrollment.md) is a
separate operation and may happen before or after this transition.

Apply the next catalog step using the separate migration credential:

```sh
bigbrotr migrate apply --file instance.toml --user bigbrotr_migrator --password-ref file:/run/secrets/bigbrotr-migrator --target 802
```

Migration identifiers are reserved by responsibility. Application order is
0001, 0002, 0801, 0805, then 0802; the command resolves that exact order.
The provisioning owner retains initialization permissions and adds:

```sql
GRANT SELECT ON ALL TABLES IN SCHEMA control TO bigbrotr_control;
GRANT INSERT ON control.policy_revisions, control.management_commands TO bigbrotr_control;
GRANT UPDATE(policy_revision) ON control.datasets TO bigbrotr_control;
```

These are protected local deployment permissions, not public-listener grants.
The application must remain separate from superuser/schema ownership. Inspect
accepted configuration and retain one nonzero 32-character lowercase hexadecimal
request ID for this operation:

```sh
bigbrotr config inspect --file instance.toml
bigbrotr config activate-initial --file instance.toml --request-id 41414141414141414141414141414141 --expected-revision 1
```

Use the actual accepted configuration revision as `--expected-revision`. In managed
mode, changes to the original file's policy seed remain unused. In declarative
mode, its normalized policy must match the accepted document; comments/equivalent
units are harmless, changed policy requires the later configuration-edit workflow.
The complete accepted policy is revalidated against deployment roots in both modes.

Success returns `activated`, security revision `1`, the original installation
actor and a new audit ID. One transaction preserves the accepted document/digest,
security head, audit and command receipt. No previous topology, role, writer epoch, job
or policy history may exist, including retired runtime roles. This first barrier
has no runtime participants; it does not qualify revocation of running services.

If the reply is lost or exit 4 reports `outcome-unknown`, retry the identical ID,
expected revision and deployment. A committed attempt returns `already-activated`
with the original receipt; an uncommitted attempt can complete once. Resolution
also works after later configuration revisions. A new ID cannot activate again,
and reusing an ID with changed content fails. Preserve ambiguous state and resolve
it; do not delete receipts or alter immutable history to force success.

Exit 3 reports invalid metadata, policy, deployment, prerequisites or conflicts.
Exit 1 covers unavailable/refused database operations and output I/O. Transaction,
SQL, lock and idle bounds are 30, 20, 5 and 10 seconds respectively. The finite
parse reservation is 512 MiB plus connection preparation, retained through driver
shutdown/output; this is not a measured RSS ceiling. Document limits remain 10 MiB.

Runtime policy refresh, native-owner startup, subsequent configuration edits and
the full release installation remain implementation work. Native evidence is
recorded separately and must name the exact executable and exercised scenarios.

[Executed native evidence](../implementation/evidence/p01-initial-activation-native-20260918.json)
qualifies source fe63862 on Linux amd64/PostgreSQL 16.15: 30 main scenarios,
three FK lock schedules, six SQL negatives, subsequent owner enrollment and
450 final relation observations. It retains failure-class and release limits.
