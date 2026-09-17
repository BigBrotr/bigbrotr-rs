# Practical implementation verification

The shared runner implements P01-T01's executable boundary and the bounded
`p01-event-cli` signed-event diagnostic slice of P01-T02/T03, plus
`p01-config-static` for P01-T04's implemented offline configuration commands.
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

Replace `p01-t01` with `p01-event-cli` to additionally verify the 17 independently
signed fixture variants, compare every decoded field and exercise changed payload,
forged signature, signed empty tag, invalid unknown UTF-8 and over-limit wire input.
It runs the same exact supplied artifact; no fixture signing or network peer is
required. This does not mark P01-T03's controlled WebSocket scenario complete.

Use `p01-config-static` with the same build/source arguments and another evidence
path to exercise all six checked-in recipe examples, validate/show, generated
schema/reference equality, selected roles/dependencies, source attribution,
redaction, lossless owner epochs, missing input and malformed/over-limit input.
The recipe role/dependency expectations are literal independent fixtures, not
candidate-generated expected answers. Two explicit `config init` calls must create
only the requested files with mode 0600 and distinct nonzero candidate identities;
an existing output must remain byte-identical. Initialization is the intentional
write operation: its network calls are observed, while file membership, contents
and permissions are inspected separately. This case does not claim a syscall
proof against transient/outside-directory mutations during initialization.

Every read-only config invocation is traced for attempted networking, file
mutation and opening staged synthetic secret references. A real secret canary
exists so an accidental read cannot hide behind a missing file. `doctor` must
return exit 4, `ready=false`, and explicit NOT_RUN live adapters/authority; its
honest incomplete diagnostic is the expected behavior of this static slice.
It does not complete P01-T04's live PG/CH/TLS/dataset/grant, durable authority,
resource measurements or packaged installation acceptance. The reported static
capability never implies those runtime guarantees.

Use a fresh evidence directory. The case invokes the actual binary outside the
checkout in a read-only temporary working directory and a minimal environment.
It checks help/version, missing/invalid arguments, unknown commands, JSON build
identity against independent input hashes, the complete source manifest and the
supplied build-result digest, created files and actual network/file mutation
syscalls. The executing runner's hash must match the selected source; the config case
also compares its helper module and records every fixture/input digest. The event
case observes every positive and negative invocation too. Linux `strace` observes
attempts outside the working directory too; merely
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
printing a plausible version, supply an invalid manifest/malformed fixture, echo
unverified events, create/unlink a transient file only during event verification,
and leave a hanging descendant. Config controls reject hard-coded validation
success and an otherwise plausible report that enables signing, exposes a public
listener, starts extra roles or fabricates applied authority. A Linux process
control opens the synthetic secret, creates/unlinks an outside-cwd temporary file
and creates a socket; the observer must detect each actual effect. Literal trace
controls also reject failed descriptor-based permission/owner changes. The process
side-effect controls require Linux strace; missing execution must be NOT_RUN;
wrong output/manifest and timeout
must FAIL, and the descendant must no longer be running.
These controls are separate from the product and never alter the product to make
an oracle pass. The Rust process tests compare SHA-256 against the actual inputs.
