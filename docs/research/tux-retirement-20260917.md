# Tux retirement and fresh research mandate

Date: 2026-09-17. Status: remote retirement completed within the verified scope;
no new benchmark or product implementation started.

The owner first authorized elaborate, realistic and large experiments on `ssh tux`
where they can decide critical alternatives. The subsequent instruction explicitly
declared all existing BigBrotr material on that host deprecated and to be deleted.
That authorization supersedes protection of the old remote project deployments;
the neighboring local Go repository and unrelated host workloads remain excluded.

## Removed resources

The [machine-readable record](evidence/tux-retirement-20260917.json) identifies:

- Seven project containers, including the old ClickHouse/PostgreSQL research labs.
- Eleven volumes: ten project-named volumes and the research PG anonymous volume.
- Two dedicated Docker networks.
- Two user service definitions and two dedicated runner tmux sessions.
- All 314 explicitly inventoried target paths, including old checkouts, test
  groups, laboratory data, runner installations, dedicated caches, individually
  named BigBrotr binaries inside the shared Go cache, and identified test logs.

The path count includes parent directories and individual files, not a recursive
file count. One enabled-service symlink was already removed by disabling its unit.
The remaining root-owned ClickHouse directory required a disposable helper using
an already cached image, no network, and a bind mount restricted to that exact
authorized directory; the helper was automatically removed. No global Docker
prune, shared-cache clear, host restart or unrelated process termination occurred.

Root-filesystem available space increased by approximately 427 GiB, reaching
about 1,008 GiB. RAM-backed `/tmp` recovered about 4.69 GiB. These are differences
between dated host observations, not exclusive-device accounting; concurrent
host activity can affect them. Large future fixtures belong on disk, not tmpfs.

## Verification and boundaries

The final sweep found no selected path remaining, no BigBrotr name or Go module
root in scanned accessible locations, no project Docker resource, no attributed
process or system/user unit file, and no project reference in the user crontab or
the checked top-level test files. All 20 unrelated inventoried containers retained
their running/stopped state. Six permission-denied directories belonging to
unrelated applications or system services were not entered.

This is an ownership-based deployment cleanup, not forensic erasure of shared
system journals, generic compiler caches or every disk byte. Generic engine
images and unrelated resources remain. GitHub-side runner registration records
were not changed by the SSH cleanup; their local installations are gone.

Local tracked reports, experiment sources and selected historical result files
remain intact. The full September 10 bundle's recorded local `/private/tmp` path
was absent when rechecked, so its old hash alone does not make that entire bundle
available. Old Tux generated datasets and environments have been deleted. Do not
describe an old stopped container or laboratory path as ready for reuse.

The [laboratory inventory](laboratory-inventory.json) records retired status and
the ignored local operation manifests. New research must provision a fresh,
separately inventoried environment and follow the
[extended qualification protocol](../engineering/capacity-and-benchmarks.md#extended-preimplementation-qualification-on-tux).
No result in this retirement record qualifies the new architecture's performance.
