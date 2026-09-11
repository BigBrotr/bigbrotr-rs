# Archive field and semantics probe

This isolated planning experiment writes real Zstandard Parquet with DuckDB and
reads event fields in a fresh standalone process. It checks nested tag/content
fidelity, unsigned extreme timestamps, source-cut-relative correction answers and
document/dictionary closure. A transparent small reference model illustrates
repeat suppression and the empty-list/backfill failure; it is not a test of the
future Rust admission/derivation implementation.

Event IDs/signatures here are synthetic format fixtures. This experiment makes
no Nostr cryptographic-validity, durability or production-capacity claim.

With the recorded DuckDB Python version installed, use a new output directory:

```sh
python3 experiments/archive-model/probe.py --output .research/archive-model/run-02
```

The probe refuses to overwrite an existing run. It limits the actual engine to
two threads and 256 MB. No network, Tux process, legacy repository, live database
or product service is used. Generated files stay ignored.

The result distinguishes six wrong interpretations from the intended oracle.
It also records wide-row versus object/reference Parquet sizes: repeated columns
can already compress well, and a separate file has overhead. Content addressing
provides identity/reference/reparse benefits; it is not a universal promise that
two files will always be smaller than one compressed table.

Primary format references:
[DuckDB Parquet support](https://duckdb.org/docs/stable/data/parquet/overview)
and [Parquet logical types](https://parquet.apache.org/docs/file-format/types/logicaltypes/).
Recorded results are in [evidence](../../docs/research/evidence/archive-model.json).

Review found the first draft omitted `pubkey`; an independent required-column
check failed on that artifact. The corrected run verifies all seven event fields,
including exact author bytes, with limits on the standalone reader too. The
retained evidence is for run02; the earlier ignored run is superseded.
