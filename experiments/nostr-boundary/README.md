# Nostr SDK boundary probe

This is a research patch for an upstream source checkout, not product code or a
proposed fix. Source: https://github.com/nostrdevkit/nostr at
`5c669a498e5f8dd1ec5713b11fdb92bcd858ad73`.

Clone that commit into an isolated checkout, apply `cached-id-probe.patch`, then:

```sh
cargo +1.95.0 test -j 2 -p nostr-sdk bigbrotr_probe_cached_id_with_changed_content -- --nocapture
```

Direct Cargo's target and dependency cache to a laboratory directory. The test
intentionally passes when it reproduces undesirable source behavior: a valid
message primes the ID cache; a changed-content message with the same claimed ID
then reaches the accepted result, despite failing independent event verification.
This exercises the actual SDK handler, without network transport. The preexisting
saved-ID test stores directly in the database and does not warm that cache; it
does not cover this sequence. No upstream report or publication has been sent.

The same checkout parses NIP-67 EOSE hints but discards them from the typed
message. See `nostr/src/message/relay.rs`, variant and deserializer. The design
therefore keeps a bounded wire envelope, full signed-payload validation and
provenance evidence outside assumptions made by the high-level SDK.
