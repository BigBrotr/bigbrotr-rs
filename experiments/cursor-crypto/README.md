# Cursor AEAD dependency probe

This isolated research package exercises the exact primitive selected for
P06-T03 Step1. It implements no product cursor, keyring or authorization API.
The package is unpublished and uses full-round XChaCha20-Poly1305, with default
features disabled and `zeroize` enabled. `AeadInOut` operates on caller-owned
slices with a separate tag; no allocating AEAD convenience API is selected.

Run from the repository root:

```sh
cargo run --locked --manifest-path experiments/cursor-crypto/Cargo.toml \
  --target-dir .research/checkouts/cursor-dependency-audit-20260925/target
```

The checked-in eleven vectors cover the published
[expired draft's A.3.1 example](https://datatracker.ietf.org/doc/html/draft-irtf-cfrg-xchacha-03#appendix-A.3.1),
empty input and lengths around 16/64-byte boundaries through 4096 bytes.
The published vector retains its IETF Trust provenance; it is not an IETF
standardization claim. `fixtures.py` independently generated the additional
public values through PyNaCl 1.6.2/libsodium, and checks the published answer
before emitting anything. The recorded native oracle-module hash identifies
that implementation; it is not a product dependency. Ordinary execution consumes
the frozen JSON and does not regenerate its expected answers.

The probe compares every encryption byte and tag, then decrypts. It independently
changes every ciphertext/tag byte and each key/nonce/AAD, requiring both refusal
and unchanged caller ciphertext. There are exactly 4915 negative calls:
4706 plaintext bytes +11×16 tag bytes +11×3 context changes. Fixed public nonces
are fixtures only; production issuance must use fresh OS randomness.

The initial run completed all cryptographic checks but failed its incorrectly
hand-counted terminal assertion 5015 versus 4915. The original source/log and
corrected run are retained in the inventory; no library failure was inferred.
The [report](../../docs/research/evidence/cursor-crypto.json) records source,
artifact, workload and platform identities. This does not qualify nonce
generation, protected storage, rotation, cross-worker/restart continuation,
resource accounting, Linux or the integrated product.
