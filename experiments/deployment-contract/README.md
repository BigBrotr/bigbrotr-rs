# Deployment capability and Compose parser research

This isolated model checks the [deployment contract](../../docs/specification/deployment-contract.md)
against frozen hand-authored input/output fixtures. It is not a proposed product
resolver or packaging scaffold. Symbolic endpoint IDs stand for already normalized
identities; TLS, grants, reachability and frontiers are supplied facts, not probed
engine guarantees. No legacy files, Tux resources, real relay or product process
is touched.

```sh
python3 experiments/deployment-contract/probe.py --compose --evidence docs/research/evidence/deployment-contract.json
```

Python uses only its standard library. `--compose` additionally calls the installed
Docker Compose parser with the local inert fixture and an empty interpolation file.
It runs `version` and `config` only: no Docker daemon operation, image pull,
container, volume, secret, listener or database is created. Placeholder images
must never be started. Omit `--compose` to run only the model; parser results then
explicitly say NOT_RUN. Parser absence/failure fails a requested parser run.

The 26 cases include fixed A while unrelated enrolled B and mentioned C exist,
unverified operator enrollment with Validator absent, empty fixed versus idle
enrolled sets, missing inventory in auto/forced NEG, monitor archive without CH,
immutable serving without collectors, wrong PG/CH dataset or frontier, expiry,
missing ordered-set owner, remote collectors with local archive jobs confined to
one storage host, invalid cross-host local paths, object reader credentials,
duplicate Rocks owners, trusted private infrastructure versus untrusted relays,
TLS failure and positive writer exclusion. Nine deliberately wrong rules must
disagree with literal expected results; they are not accepted alternate behavior.

Compose inspection independently checks unprofiled default services, explicit
profile selection, retained dependency and loopback host publishing. It does not
test actual profile startup, host reachability or firewall behavior. The primary
Docker documents in the contract, not this parser check, describe startup semantics.

[Recorded evidence](../../docs/research/evidence/deployment-contract.json) binds
the exact source/fixture hashes, Python/Compose versions and executed case names.
P01/P03/P04/P08 still owe actual native/container installed journeys, engine
authority and identity checks, real sockets, TLS, credential rotation, bounded
outages and old-owner exclusion. MODEL_PASS cannot close those release gates.
