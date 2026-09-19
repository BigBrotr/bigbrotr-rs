# Captured JSONL sources

The P03-T08/P03-T09 source prerequisite captures and verifies a single explicitly
supplied uncompressed `nostr-jsonl/1` file. Its first consumer is the finite
signed-admission qualification client. This is not the installed import command,
an authorized job, current destination-policy preflight or a durable checkpoint.
The selected import/status/resume commands and other formats remain required.

`SourceSnapshot::capture` opens a regular file with no-follow, nonblocking and
close-on-exec flags. It reserves the initial length and one file slot from a
shared `ScratchSpace`, creates a private temporary file, checks removal of its
name before copying input, and captures with a 16KiB buffer. The complete captured
size and SHA-256 are known before any record is exposed. Source length/mtime
changes during capture are refused. An expected full identity also detects
same-size rewrites independently of timestamps. Without an expected manifest,
the result identifies the captured byte stream; it does not prove that the
original path held those bytes at one instant during concurrent modification.

The captured descriptor has no public write/clone/path API. Replacing or rewriting
the original file after capture cannot change records read from this capture.
It is ephemeral: no fsync or durable staging receipt is claimed, and process death
discards it. A later resumable job must retain the complete source/namespace/shard,
parser profile, limits, scope and policy binding, then recapture and compare the
source identity. Same path or offset alone never permits resume. Neither source
opening nor scratch-root ownership authorizes importing from that path.

The scratch allowance bounds logical bytes and live files across concurrent
captures, including empty sources. It is not a reservation of physical filesystem
space; ENOSPC and other I/O failures remain failures. The caller supplies an
operator-owned directory without group/other write permission and retains one
shared allowance for its workers. Ancestor aliases, privileged processes and
malicious processes with the same OS identity are outside this boundary.
Successful unlinked captures release capacity only after their descriptor closes.
If unlink/link-count confirmation fails, no payload has been copied: retain the
allowance and make this scratch owner unavailable. Inventory and clean its owned
empty artifacts before replacing that owner; blindly constructing a new allowance
over the same root is not recovery. Ordinary capture failures after successful
unlink close the file and return capacity without changing the source.

`JsonlReader` takes the captured descriptor and a memory/item permit before
allocating its raw-line buffer. The reservation covers the configured wire and
parser bounds plus 64KiB for buffered reading and metadata. The verifier's
existing node/string/depth/event bounds still apply. A record borrows the reader's
permit until destruction; its explicit destructor enforces Rust drop checking,
so another record cannot overlap its allocation and the reader cannot release
its permit early. A consumer that copies an event first reserves separate memory.

Positions contain the full source identity, one-based line number, zero-based
half-open byte range and SHA-256 of the raw record including its LF/CRLF. LF
delimits records; CR remains JSON whitespace and counts toward the wire limit.
A nonempty final unit at exact EOF is a record even without LF. Empty input has
zero records; a blank line is an invalid record. Oversized units are drained and
hashed in bounded chunks before the next line is considered. No complete raw
rejected body is retained. Complete events use the shared independent NIP-01 ID
and Schnorr verifier, preserving signed integers and ordered tags and rejecting
ambiguous duplicate critical keys.

A returned position only identifies bytes that were classified. It does not
authorize progress, and resource exclusions remain distinct from invalid data.
Current policy, admission outcomes, sidecar obligations and terminal-versus-retry
classification belong to the job consumer. I/O failure, truncation of captured
storage or cancellation stops this reader without emitting a completed partial
unit. Restart recaptures the source under its expected identity. Cancellation is
cooperative between chunks and around bounded event verification; a regular-file
kernel I/O call is not a hard wall-clock deadline. Finite process supervision
remains responsible for that operational boundary.

The qualification client consumes the frozen 353-byte signed corpus through this
reader, compares its source identity to the independent lineage digest, and emits
line1 at bytes `[0,353)`. Bootstrap request1 leaves the logical import prefix zero;
event request2 and the distinct AlreadyDurable request3 retain their existing
request semantics. No job checkpoint, ACK or GC is introduced.
