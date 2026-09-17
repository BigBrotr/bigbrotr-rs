# Optional S3 archive research

These programs are isolated engine experiments, not product code or deployment
recipes. The [report](../../docs/research/tux-object-storage-20260917.md) and
[evidence](../../docs/research/evidence/object-storage-20260917.json) state actual
scope, corrected oracles and limitations. Never run against an existing database,
shared archive or another operator's containers.

`setup.py` is a host-side script restricted to a new
`/home/tux/bigbrotr-object-storage-20260917` directory. It refuses reuse and requires
200 GiB free before creation. It uses exact image digests, owned names, loopback
ports, bounded cgroups and persistent lab mounts. Garage has one replica;
Postgres trust authentication and plaintext loopback are lab-only conveniences,
not an authenticated production deployment. Secrets are generated into mode-600
files and omitted from tracked reports.

Create the worker virtual environment inside its Python image, then install:

```text
duckdb==1.5.5
pyarrow==21.0.0
coincurve==21.0.0
psycopg2-binary==2.9.10
boto3==1.42.70
```

The recorded pip freeze captures transitive versions; extension binary hashes
are in the inventory. The research installs extensions explicitly. Production
instead ships a verified offline bundle and disables automatic installation.

Mount the completed prior qualification lab at `/baseline:ro` in the worker.
Only its deterministic fixture generator and first 16 Parquet source files are
read. To reproduce elsewhere, generate the identical first 1,048,576 rows with
the [locked fixture](../storage-qualification/README.md); retain input hashes.
The previous environment's retired lab paths are not substitutes for this fresh lab.

Execution order, each command in its stated host/worker environment:

1. Host: `python3 setup.py`; copy research sources into the new lab's `src/`.
2. Worker: `venv/bin/python src/probe.py`. Local then S3 load and signed readback;
   four multitable transactions each, no other archive writers/maintenance.
3. Host: `python3 src/prepare_reader.py`. Provision the restricted reader; the
   recorded first whitespace-parser failure used corrected `--existing` recovery.
4. Run `reader.py` in a separate same-image container mounting only source code,
   expected fixture, reader secret, extensions, venv and output reports. Mount
   neither local archive, baseline input nor Garage's data. Use the recorded
   mounts/resources from the inventory; no passing isolation claim from a script
   launched inside the full-access writer container.
5. Coordinator establishes no writers/maintenance; host `copy_catalog.py` dumps
   `catalog_s3` and restores a fresh `catalog_restore`. Then worker `faults.py`
   kills its own precommit child, checks missing objects, repairs them and copies/
   hashes the catalog-selected closure into the alternative root.
6. Worker `verify_boundaries.py` performs the separately recorded stronger
   multipart/range/list/permission/all-family closure checks. Keep the first run's
   reports and source hashes; do not relabel its weaker assertions.
7. Host `inventory.py --label LABEL` captures nonsensitive process/image/mount
   state. The final version also inventories the separately created CH MV probe.
   Stop only owned named containers after workloads exit; record final state.

`probe-original.py` in ignored/remote research material reconstructs the exact
initial executed source and matches its report's SHA. The tracked helper later
adds an explicit path-override argument used by the restore probe. Initial
`reader.py` and `copy_catalog.py` are deliberately narrow one-shot harnesses;
their PASS does not establish repeat-safe job recovery or reusable backup fencing.
The corrected boundary probe has fresh-run guards and terminal atomic reports.
Initial limitations remain disclosed rather than silently repaired in old evidence.

The first S3 primitive check used periodic bytes, so it proves its recorded
206/length/read behavior only. Its LIST result was not an exact membership oracle.
The later position-dependent full/cross-boundary/range and catalog-closure checks
are the stronger results. A precommit caller kill after INSERT is not an in-flight
upload kill; a loopback S3 server is not a WAN or independent failure domain.

The complementary [`ch_materialized.py`](../relay-composition/ch_materialized.py)
uses a separately provisioned fresh CH 26.5.1.882 instance at loopback port 48723,
2 CPU/3 GiB, with the same pinned image as the prior storage lab. It creates a
UUID-qualified database, records every query, tests three literal counterexamples
and removes only its own database. It starts/stops no server and proves no capacity.
Its source/result/transcript hashes and container identity belong in the evidence.

Retain generated sources, results, failed attempts and data only under the owned
lab and ignored `.research/object-storage-20260917/`. Do not copy credentials,
Docker environments, generated datasets or database volumes into Git. Images,
volumes and retained bytes remain inventoried after stopped processes release
runtime resources.
