# Actual PostgreSQL TLS and client-certificate cases

Status: **NOT_RUN** on PostgreSQL. The local Mac check generated and verified six
ephemeral certificate chains with LibreSSL 3.3.6, checked private file modes and
exercised log-oracle negative controls. It did not connect to PostgreSQL. A real
CLI configuration check found the recipe-default problem described below; it is
not a passing TLS case. Native Linux execution supplies separate revision-bound
evidence.

`tls_cases.py` imports the adjacent `lab.py`; it does not copy orchestration or
create another container/service lifecycle. It operates only on the named,
label-verified containers and bind mounts in an existing `resources.json` from
that harness. The baseline must be the owned PostgreSQL 16.15 plaintext lab, with
its client container and exact built executable. No external network is used.
Run it alone while smoke/schema/crash actions on that lab are stopped.

```sh
python3 experiments/control-migrations/tls_cases.py \
  --root /absolute/owned/laboratory-root --run-name tls_first
```

The name must contain 1–20 lowercase letters/underscores. The root is canonical
and absolute. Source must be clean and the imported harness must belong to the
same source checkout. The script verifies the mounted executable hash against
the lab's build receipt and records application source, harness revision/hashes,
container/image identities, server version and timestamps. It refuses an existing
run directory, certificate directory or database instead of resetting one.
The host needs `openssl` and Python with the standard Unix `fcntl` module. The
lab already supplies Docker, PostgreSQL's observer tools and the actual binary.

## Inputs and outcomes

An isolated database `bb_<run-name>_tls` is explicitly migrated with the real
binary, then used only for `migrate inspect`. Its initial control schema/data hash
must match after the TLS cases and restoration. A protected PKI directory under
`<root>/secrets/<run-name>-tls` contains two temporary RSA-2048/SHA-256 roots,
a server certificate and three client identities. Every generated file finishes
with mode 0600 beneath a 0700 directory. The certificates expire after two days;
this is laboratory provisioning, not product credential management. Secret keys
and bootstrap/application passwords are never emitted into reports or arguments.
Only public certificate hashes are recorded.

The server certificate's DNS SAN is `db.tls.bigbrotr.invalid`, while the actual
route is the existing `postgres:5432` Docker alias. This tests the adapter's
explicit expected server name independently of the address used to connect.
The good client CN is exactly `bigbrotr_migrator`; negative clients either carry
a different CN signed by the same CA or the correct CN signed by the wrong CA.

Ten named cases cover:

1. A real plaintext baseline against the newly migrated database.
2. TLS with the exact CA and explicit expected name.
3. A valid optional client certificate before HBA requires one.
4. Refusal when the client trusts the wrong server CA.
5. Refusal for an incorrect expected server name.
6. Successful mandatory mTLS for the exact client identity.
7. Refusal of no client certificate under mandatory mTLS.
8. Refusal of a valid certificate with the wrong client CN.
9. Refusal of a client certificate signed by the wrong CA.
10. Exact configuration restoration, successful original plaintext access and
    unchanged control schema/data.

Every negative runs a successful connection before and after it on the same
route. A generic backend error cannot pass: the negative must also produce the
specific new server TLS alert/certificate rejection and no authorization record.
Success requires the PostgreSQL authorization log to name the fixture database
and user and show TLS 1.2/1.3, cipher and at least 128 cipher bits. Only bounded,
allowlisted observations/counts are recorded; raw logs and unexpected CLI output
are not copied into reports. A log window shift, absent signal or unexpected
failure is a failed/unfinished case, never an inferred successful rejection.

PostgreSQL's [TLS contract](https://www.postgresql.org/docs/16/ssl-tcp.html)
defines CA checking and the additional client identity check under
`clientcert=verify-full`. The exact PostgreSQL 16.15
[authentication source](https://github.com/postgres/postgres/blob/REL_16_15/src/backend/libpq/auth.c),
[TLS error source](https://github.com/postgres/postgres/blob/REL_16_15/src/backend/libpq/be-secure-openssl.c)
and [authorization log source](https://github.com/postgres/postgres/blob/REL_16_15/src/backend/utils/init/postinit.c)
were inspected for these observations. rustls 0.23.45's `src/error.rs` maps unknown
issuer and wrong-name failures to the expected different alerts. If the actual
transport does not deliver those signals, retain the failure and investigate;
do not replace the oracle with “any nonzero exit”.

## Configuration changes and restoration

Before changing server TLS state, the actual CLI statically validates every
candidate. The script copies exact HBA, `postgresql.auto.conf`
and base `postgresql.conf` bytes and modes into the protected run directory.
`ALTER SYSTEM` changes only SSL file/settings, connection logging, log prefix and
message locale. HBA prepends two database/user-specific rules: deny non-TLS TCP,
and use SCRAM on TLS with an optional, then mandatory, verified client identity.
The original HBA remains below them, including the isolated socket observer.
Each reload checks container labels/IDs and parses the resulting HBA. No unrelated
container, host PostgreSQL, network, volume or published port is changed.

`finally` restores the exact HBA and auto-configuration files, reloads the owned
server, checks original settings/file bytes/modes, and runs the original CLI
connection. If the owned server stopped unexpectedly, restoration can start that
same verified container after restoring its files. No container is replaced or
deleted. A failed restoration leaves `restoration=FAIL` and protected backups;
retain the resources and repair the owned lab explicitly. The run never reports
PASS merely because its test cases passed while restoration failed.

SIGKILL, host loss or interpreter death cannot run `finally`. In that case, use
`original-*` backups and recorded hashes in the retained run directory, verify
`resources.json` ownership and container identities, then restore the two changed
files and reload only that server. Do not run another TLS case or treat a stale
`RUNNING` report as complete. The advisory file lock prevents another instance of
this TLS script; other lab commands must be serialized by the integrating owner.

Reports are `<root>/<run-name>-tls/result.json`. A complete run has ten PASS cases,
zero failed/skipped/NOT_RUN, `restoration=PASS`, and unchanged logical storage.
New database, PKI, original-config backups, candidate TOML files and the lock file
remain inventoried there for the owner's explicit cleanup. This does not qualify
live production credentials, revocation/rotation, arbitrary TLS/provider matrices,
engine upgrades, storage races or any full product milestone.

## Configuration regression discovered during preparation

The local executable built from `140e264` plus the control-schema slice rejected
the candidate; inspection confirmed unchanged relevant configuration code in
`6f7c03d`. That code reintroduces a recipe's plaintext `local_boundary`
after a TOML changes the TLS mode to `verify-full` and omits that irrelevant key.
TOML has no null override, so offline validation refuses the otherwise complete
TLS candidate. The fixture does not work around that product defect by weakening
validation or injecting a different configuration path. Root owns the focused
configuration fix and a newly identified binary. Qualify the TLS cases only with
the corrected exact artifact; keep the earlier refusal as evidence of the defect.
