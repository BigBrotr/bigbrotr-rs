# Observation bytes versus separate document objects

This local DuckDB 1.4.5 probe found a size crossover. With eight repeated 1 KiB
documents, wide rows were slightly smaller in one observation file; separate
objects were smaller when those same observations crossed 16 small files.
With no document reuse within a visibility namespace, wide rows were smaller in
every tested case. Neither layout is a universal compression winner.

These are measured Parquet file sizes, not production capacity or latency
results. The [evidence](../../docs/research/evidence/document-layout.json) retains
both executed runs, exact program/output hashes, actual row groups and oracles.
No Tux service, network, installation, persistent server or legacy source was used.

## Equivalent candidates and constraints

Each case has 16,384 observations, 256 repeating subject IDs and 1,024-byte
document bodies. Both candidates retain observation/subject IDs, observation
time, visibility namespace, method, outcome, SHA-256 document digest, length
and exact original bytes. The wide candidate stores these in each observation;
the split candidate stores narrow references plus unique
`(visibility_namespace, digest, length, bytes)` object rows. Reported split bytes
include every reference and object file, their digests, namespaces and footers.

Low churn uses eight distinct bodies in each of two namespaces: 16 logical
objects reused across observations. High churn introduces a new body for each
adjacent public/restricted pair: 8,192 distinct byte strings and 16,384 namespaced
objects, with no reuse within one namespace. This deliberately includes identical
bytes under different visibility identities; it is not an authorization test.
Compressible bodies are deterministic repetitive JSON; high-entropy bodies are
deterministic SHAKE-256 bytes. These are synthetic document fixtures, not measured
NIP-11 distributions or signed Nostr events.

The primary comparison preserves observation order for wide/reference files and
first-observation order for object files. New objects are written only in their
first-occurrence publication batch; there is no later object compaction. Both
layouts use ZSTD level 3 and a 16,384-row group target, with either one 16,384-row
observation file or 16 files of 1,024 rows. Observed metadata confirms ZSTD and
one row group per file. Low-churn objects occupy one 16-row group; high-churn
objects follow the same one/16-file grouping as observations. The target is not
being asserted as a universal hard row-count cap.

The process uses one DuckDB thread and a 192 MB engine memory setting (reported
as 183.1 MiB), with a 32 MB scratch setting. Python generates/inserts and reads
128 rows at a time; the engine holds one bounded case. The independent comparison
keeps a 512 KiB ordered row-hash vector and a 16 KiB presence vector, not all
document bodies in a Python map. Extension auto-install/loading is disabled.
Each run refuses an existing output directory and checks a 250,000,000-byte
output budget before/after writes. Both recorded runs, their manifests and the
preserved first source total **85,667,881 bytes**, below that bound. The engine
memory setting is not a measured process RSS limit.

## Primary measured results

Bytes below come from the chronology-controlled run. A negative change means
the split files consumed fewer bytes than wide files for that fixture.

| Churn | Body entropy | Observation files | Wide bytes | Split bytes | Split change |
|---|---|---:|---:|---:|---:|
| Low | Low | 1 | 62,295 | 63,064 | +1.23% |
| Low | Low | 16 | 109,693 | 87,706 | −20.04% |
| Low | High | 1 | 70,452 | 71,254 | +1.14% |
| Low | High | 16 | 240,333 | 95,896 | −60.10% |
| High | Low | 1 | 354,304 | 622,256 | +75.63% |
| High | Low | 16 | 394,421 | 671,379 | +70.22% |
| High | High | 1 | 8,765,900 | 9,033,835 | +3.06% |
| High | High | 16 | 8,804,805 | 9,081,743 | +3.15% |

There was no runtime failure. The first run did contain a comparison confound:
objects were ordered by namespace/digest, separating adjacent duplicate bytes.
Its high-churn/high-entropy one-file split occupied 17,438,720 bytes, versus
9,033,835 with first-observation ordering. Those files and the exact original
source remain under `.research/document-layout/run-01/`; the corrected comparison
is under its `order-control/` subdirectory. The evidence retains both. Compression
locality and file boundaries therefore matter independently of logical object
identity; the first result must not be attributed solely to normalization.

## Logical verification and limits

For every candidate, Python compares all 16,384 reconstructed rows, subject/time/
namespace metadata, digest, length and exact body against the deterministic input.
It joins reference/object streams itself on namespace plus digest, rejects orphan
or duplicate objects, checks complete unique observation membership and compares
ordered full-row hashes. DuckDB supplies Parquet decoding for both layouts;
PyArrow was absent, so this is not an independent Parquet-codec verification.

Two deliberately failing comparator controls were observed in every case:
omitting a restricted object cannot be repaired by the identical public object,
and a changed body byte cannot retain a valid logical comparison. These mutate
the oracle's input stream, not the stored Parquet files. A separate actual rerun
against the existing output directory failed with `FileExistsError` and preserved
the existing result hash. Every artifact hash, byte count and row-group record
was subsequently rechecked, including actual codec metadata.

No RSS, CPU, elapsed-time, I/O, query latency, lookup-index size, garbage collection,
backup amplification or crash behavior was measured. Both namespaces share files
in this fixture; physically separating them would change compression opportunities.
Production selection still needs representative document distributions, reference
lookup/maintenance costs and the selected privacy/retention layout. This probe
supports conditional physical design while preserving document identity and exact
reference reconstruction in either representation.

## Reproduction

Use an existing Python environment with DuckDB 1.4.5; the script installs nothing.
Choose a new ignored directory. The default preserves first-observation ordering:

```sh
python3 experiments/document-layout/probe.py --output .research/document-layout/new-run
```

To repeat the initial ordering sensitivity with the current script, choose another
new directory and add `--object-order namespace-digest`. The exact first executed
source is also preserved locally as
`.research/document-layout/run-01/source-before-order-control.py`; its source hash
differs from the current script because the ordering control was then added.
Generated files stay ignored; only the probe, this explanation and concise
machine-readable measurements are research artifacts for review.
