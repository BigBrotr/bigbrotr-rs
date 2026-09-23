# Job-command decode allocation probe

Isolated research against the actual linked Rust library. This is not a product
allocator, process RSS measurement, fuzz proof or native qualification.

Build the exact trusted source normally and preserve Cargo's artifact messages:

```sh
cargo build -p bigbrotr --lib --message-format=json > .research/decode-build.jsonl
python3 experiments/job-command-decode/measure.py --build-messages .research/decode-build.jsonl --output .research/decode-unknown --case unknown
python3 experiments/job-command-decode/measure.py --build-messages .research/decode-build.jsonl --output .research/decode-projection --case projection
```

The standalone executable delegates to `System` and tracks live Rust allocation
sizes during the actual `JobCommand` deserialization, excluding the already owned
wire body. The body is added to the assertion against the existing64KiB request
reservation. Product crates retain `forbid(unsafe_code)`; unsafe delegation exists
only in this isolated measurement. No database, credentials or network is used.
Each output records compiler, probe/library/executable hashes and actual result.
An existing output directory is refused to preserve previous attempts.

The unknown-field input has1500 one-element nested arrays in an unsupported
field (6023bytes). The projection input substitutes2049 empty strings into the
independent complete command fixture (6832bytes); this shape is outside the initial
shared read profile. Before correction, observed Mac debug peaks were258008 and
98529bytes respectively. The corrected closed-map/null-only decoder measured225
and341bytes for these two exact inputs, with zero retained bytes. Unit regressions
also use a counted reader to prove refusal before loading either nested value.
These measurements cover only the named inputs and allocator, not all possible
requests or operating-system overhead. Raw results are inventoried by the active
implementation checkpoint; native HTTP remains a distinct qualification.
