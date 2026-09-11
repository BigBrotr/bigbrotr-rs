# Parquet target versus hard-cap counterexample

This isolated real-engine probe challenges the plan's former phrase "at most
64,000 rows per group". On DuckDB 1.4.5, requesting `ROW_GROUP_SIZE 64000` for
200,000 integer rows produced groups of 65,536, 65,536, 65,536 and 3,392 rows.
The standalone readback's count, bounds and arithmetic sum match the fixture.
Thus the requested row count is not a hard upper bound on output groups.

Reproduce in a new ignored directory:

```sh
python3 experiments/parquet-targets/probe.py --output .research/parquet-targets/run-01
```

The script refuses to overwrite, uses one DuckDB thread and a 128 MB engine
memory setting, and writes about 233 KiB. This is not an RSS measurement, a
DuckLake/Rust integration test, or a throughput/power-loss test. No networking,
Tux service or legacy file is used. The negative control is the concrete false
claim that every group must contain at most 64,000 rows.

[Recorded result and hashes](../../docs/research/evidence/parquet-targets.json).
[DuckDB's row-group documentation](https://duckdb.org/docs/current/data/parquet/tips)
explains the target/minimum and vector granularity. Actual BigBrotr native-worker
limits, supported options and failure recovery remain P02 implementation work.
