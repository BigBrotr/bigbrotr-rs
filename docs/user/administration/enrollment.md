# Enroll the first local owner

The development executable supports local enrollment through the protected
Control database credential. Start with [Control initialization](../../operations/control-initialization.md).
This command does not start a private web listener or create a browser session.

Apply migration 805 with the separate migrator, then have the provisioning owner
grant the application role its new table permissions:

```sh
bigbrotr migrate apply --file instance.toml --user bigbrotr_migrator --password-ref file:/run/secrets/bigbrotr-migrator --target 805
```

```sql
GRANT SELECT ON ALL TABLES IN SCHEMA control TO bigbrotr_control;
GRANT INSERT ON control.local_accounts, control.local_credentials,
    control.principal_grants, control.owner_challenges TO bigbrotr_control;
GRANT UPDATE(state, failed_attempts, consumed_principal, consumed_at_us)
    ON control.owner_challenges TO bigbrotr_control;
```

Retain the initialization permissions. These are local provisioning permissions,
not a template for a public service. The application credential must remain
separate from schema ownership and migration privileges.

Choose a nonzero 32-character lowercase hexadecimal request ID and retain it
until the operation resolves. The example ID is an idempotency identifier, not a
secret. Issue the token only to an explicit local output you can protect:

```sh
bigbrotr access owner-token issue --file instance.toml --username alice --request-id 31313131313131313131313131313131
```

The response contains the token once. Store its exact 64 lowercase hex characters
in a new file owned by your effective user with mode 0600 or 0400; use the same
protection for your chosen password. Neither file may be a symlink, nor may its
parent path traverse one. Use absolute paths. Do not include a trailing newline.
Passwords require at least 12 Unicode characters, at most 1024 UTF-8 bytes and
no control characters; spaces and Unicode are preserved exactly.

```sh
bigbrotr access enroll-owner --file instance.toml --challenge-id 31313131313131313131313131313131 --token-ref file:/run/secrets/bigbrotr-owner-token --password-ref file:/run/secrets/bigbrotr-owner-password
bigbrotr access owner-token status --file instance.toml
```

Enrollment creates the intended account, versioned Argon2id credential, explicit
dataset administrator grants and audit receipt in one transaction. Its token
expires after ten minutes and is consumed once. A replay cannot replace the
password. Import, export, private export and signing need additional explicit
grants. Enrollment itself supplies no runtime lease or active security revision.

If issuance output is lost, repeat the same request ID to resolve its metadata;
the secret is unavailable. Revoke that challenge explicitly, then issue a new
request ID. An expired or exhausted pending token also requires revocation:

```sh
bigbrotr access owner-token revoke --file instance.toml --challenge-id 31313131313131313131313131313131
```

Exit 4 / `outcome-unknown` requires serialized status resolution before retry.
A successful status identifies the original principal and enrollment audit ID.
Once an owner exists these commands cannot issue another owner token or reset
its password. Account recovery, login/sessions, remote management, grant changes
and full private-panel acceptance remain implementation work.

The [native enrollment evidence](../../implementation/evidence/p08-owner-enrollment-native-20260918.json)
records the tested Linux amd64 revision, actual expiry and concurrent/recovery
cases, populated migration and process-crash checks. It does not qualify a
packaged installation, Linux arm64 or the remaining administration surfaces.
