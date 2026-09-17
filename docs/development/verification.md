# Practical implementation verification

The shared runner currently implements only P01-T01's executable boundary.
Additional P01-T08 cases and engine/fixture orchestration are still required.
No planned task ID silently maps to a successful empty scenario.

After an exact-source build, run on Linux as an unprivileged user with `strace`:

```sh
python3 tests/acceptance/run.py p01-t01 \
  --binary .research/build-candidate/bigbrotr \
  --source-root .research/build-candidate/source \
  --source-manifest .research/build-candidate/source-manifest.json \
  --build-result .research/build-candidate/build-result.json \
  --evidence .research/p01-t01
```

Use a fresh evidence directory. The case invokes the actual binary outside the
checkout in a read-only temporary working directory and a minimal environment.
It checks help/version, missing/invalid arguments, unknown commands, JSON build
identity against independent input hashes, the complete source manifest and the
supplied build-result digest, created files and actual network/file mutation
syscalls. Linux `strace` observes attempts outside the working directory too; merely
disabling networking is insufficient. Missing tracing, writable root-user context
or absent source/build manifests is NOT_RUN and fails this practical gate.
Mac Rust process tests cover the portable CLI behavior; they do not substitute
for the syscall observation.

Exit codes are 0 PASS, 1 FAIL and 2 NOT_RUN. Reports contain nonempty checks,
requiredness, collected/executed/failed/skipped/not-run counts, input hashes and
environment. Raw process outputs and traces remain in the ignored evidence
directory. Cleanup terminates/joins the owned process group and removes only the
runner-owned temporary directory. Timeout or interruption cannot produce PASS.
Manifest comparison establishes equality, not independent release authenticity.
Retained concise evidence distinguishes this
foundation from an eventual full bundle or storage/protocol qualification.

The runner's negative controls remove the executable, substitute a shell script
printing a plausible version, supply an invalid manifest and leave a hanging
descendant. Missing execution must be NOT_RUN; wrong output/manifest and timeout
must FAIL, and the descendant must no longer be running.
These controls are separate from the product and never alter the product to make
an oracle pass. The Rust process tests compare SHA-256 against the actual inputs.
