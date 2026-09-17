# Relay composition semantics research

This isolated experiment checks the proposed relay-content populations and
incremental sufficient statistics. It is not product implementation, a live
relay census, a performance benchmark or external protocol interoperability.
The [owning definition](../../docs/architecture/analytics-detail.md#relay-content-composition-from-a-declared-population)
selects the metrics and distinguishes default materialization from bounded
on-demand/offline research.

From the repository root, with the already used `secp256k1==0.14.0` Python
package available, choose a new output directory:

```sh
python3 experiments/relay-composition/probe.py --output .research/relay-composition/run-01
```

The command refuses to overwrite that directory. It uses Python and SQLite
locally, with no Go execution, relay/HTTP requests, containers or Tux resources.
Signing reuses the existing identity experiment's helper and deliberately public
test secrets; both libsecp256k1 and the independent slow BIP340 group-equation
checker verify the nine source events and one synthetic publication. The equation
checker is research code, not a proposed production crypto implementation.

An incremental membership/refcount reducer is compared after every source action
with a fresh SQLite correlated-set/grouping oracle. The fixture includes physical
duplicates, a strictly earlier witness correction, two events from one key,
duplicate topics, private evidence, an imported presence claim, a replacement
witnessed at another endpoint, an expiring event and an unscored author. Static
expected counts additionally check both implementations:

| Population | Events | Author keys | Sum of squared author counts |
|---|---:|---:|---:|
| Historical witnessed set | 6 | 3 | 14 |
| Locally visible before expiry | 5 | 3 | 9 |
| Locally visible after expiry | 4 | 3 | 6 |

The run recorded 22 cut comparisons, 22 duplicate-replay invariance checks,
two zero-arrival expiry comparisons, a changed event-time window, and curator
bands that distinguish known zero from unknown. Thirteen wrong interpretations
differ from the independent/static expectations, including counting deliveries,
including imported/private evidence, summing partition distincts, counting repeated
topic tags, ignoring replacement/expiry, subtracting only one from the square sum,
and equating unknown scores with zero. Changed signed content and context bytes
fail their respective checks. A mismatched definition ID with a correct digest
for the wrong object's bytes demonstrates that hashing alone is not a semantic
binding; the malformed object and its reference are retained in the fixture.

The signed kind-30166 fixture contains an optional context digest and categorical
basis label; it does not place composition into standard `k`, `comment_cnt` or
`rank` tags. Its metadata is a small research subset, not the complete future
schema. No client retrieves its deliberately nonexistent `.invalid` URLs.
Two possible later remote inventories share the identical local history; the
experiment demonstrates why that history cannot determine current remote count.

The [evidence](../../docs/research/evidence/protocol-composition-research.json)
records exact source/fixture hashes, primary NIP pins, selected read-only Go
excerpts, and outcomes. Raw generated artifacts remain under ignored `.research/`.
No persistent database crash, production parser/authority, resource bound,
statistical representativeness or hosted extension-client behavior is qualified.
The probe-served population is specified in the owning contract but is not
exercised through its real acquisition producer by this small experiment.

## ClickHouse materialized-view counterexamples

The coordinator can additionally run `ch_materialized.py` against an already
started, disposable local ClickHouse HTTP port. It uses only Python's standard
library, creates a fresh UUID-named database and drops only that database after
recording results. It starts no services or containers:

```sh
python3 experiments/relay-composition/ch_materialized.py --url http://127.0.0.1:48723 --output .research/relay-composition/ch-mv-run-01
```

Three independent literal expectations distinguish an insert-time aggregate from
the required logical result: replay leaves one source event after a Replacing
merge but two in a naive count MV; changing a join's right-side dimension updates
a fresh SELECT but leaves the prior MV output unchanged; passing an actual
two-second expiry changes a fresh eligibility query to zero but leaves the MV
count at one. Expiry polling sleeps at most 50 ms per iteration and has a finite
deadline. No TTL is configured; the third case tests query-time visibility rather
than TTL scheduling. Version, SQL/responses, source/transcript hashes and observed
answers are recorded in the requested output directory. `--keep-database` retains
the script's own database for inspection.

This probe does not reject appropriate materialized views. It demonstrates the
need for logical admission, explicit changes and complete-generation publication
around accelerators. It is not a product implementation, capacity result or a
test of the planned RocksDB/CH/PG recovery transaction.

The root coordinator executed this probe on 2026-09-17 in a fresh isolated Tux
container running ClickHouse **26.5.1.882**, with reported limits of 2 CPUs and
3 GiB. All three literal expectations passed and the probe's database was removed.
The [evidence record](../../docs/research/evidence/protocol-composition-research.json)
contains the complete result, exact script/transcript hashes and 67 HTTP commands;
responses were checked for HTTP errors and `X-ClickHouse-Exception-Code`, including
errors that could accompany HTTP 200. Raw query responses are retained under the
ignored `.research/object-storage-20260917/ch-mv-run-01/` path. The elapsed polling
time is fixture orchestration, not a performance claim. These engine observations
do not qualify the planned production update/recovery protocol.
