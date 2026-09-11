# Relay profile arithmetic probe

This standard-library probe independently enumerates each minute midpoint in the
[general-read-relay/1 hand fixture](../../docs/architecture/analytics-detail.md#relay-profile-general-read-relay1).
It selects the latest anchor, applies the 20-minute hold, preserves unknown
anchors, and optionally stops old carry at minute 60. All fixture boundaries are
minute-aligned, so one-minute enumeration gives exact durations for this fixture.
Reported totals are assertions after enumeration, not inputs to it.

| Fixture | S / F / U minutes | A | Covered fraction | Lower p50 (µs) | Q |
| --- | --- | --- | --- | --- | --- |
| Baseline | 40 / 15 / 1385 | 8/11 | 11/288 | 200000 | 0.7615694869496978 |
| Policy boundary at 60 | 30 / 15 / 1395 | 2/3 | 1/32 | 200000 | 0.7236906990709098 |

Both have six classified in-window attempts and four successful latencies.
The latency component is `exp(-0.2) = 0.8187307530779818`; quality is
`0.625*A + 0.375*L`. Coverage is reported separately.

The negative controls distinguish three incorrect implementations: probe-count
availability gives `4/6` instead of baseline `8/11`; averaging the two middle
latencies gives `250000` instead of `200000`; ignoring the unknown anchor gives
`S40/F30/U1370`. A five-attempt all-failure control has `A=0`, passes the sample
gate, and withholds `L` and `Q` because there are no successful latencies.

Run from the repository root with Python 3 and a fresh ignored output directory:

```sh
python3 experiments/relay-profile/probe.py --output .research/relay-profile/run-02
```

Existing output directories are refused. The recorded run used `run-01`, Python
3.9.6, and no third-party dependencies. The script captures the normative section,
checks that its source file stays unchanged during execution, and records exact
inputs, command, source/program hashes, states hashes, and results. The recorded
[evidence](../../docs/research/evidence/relay-profile.json) includes the full result
and captured section, with hashes of the ignored output artifacts. Reproduction
changes command/output metadata; arithmetic and minute-state hashes should match
while the fixture and program are unchanged.

This checks arithmetic only. It does not exercise the product, a runtime window
engine, correction/expiry handling, clock correctness, non-minute-aligned cases,
networking, DuckDB, or Tux. No capacity, statistical-confidence, or client-utility
claim follows from it.
