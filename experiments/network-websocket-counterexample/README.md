# Framing-inclusive message limit counterexample

This isolated unpublished package reproduces a mismatch in the selected
Tungstenite0.29 framing engine. It is not a product adapter or release test.
Three real loopback TCP peers send independently literal RFC6455 frames after
a raw-socket setup (no HTTP/TLS handshake). The configured frame/message payload
limit is16 bytes. A14-byte payload plus2-byte header is the positive control;
a16-byte payload plus2-byte header and nine empty fragments both consume18 bytes.
The latter two must be refused by BigBrotr's framing-inclusive message contract,
but the upstream payload configuration accepts them.

```sh
CARGO_TARGET_DIR="$PWD/.research/targets/product" cargo run --locked --manifest-path experiments/network-websocket-counterexample/Cargo.toml
```

Expected result for this dependency counterexample: exit101, three true acceptance
fields, and the final contract assertion failure. Preserve that original failure;
it is not a product PASS. The source originally ran from the inventoried ignored
copy on the Mac, with identical source/manifest/lock bytes. Its standalone lockfile
is separate from the unchanged product dependency lock. No Tux or capacity claim.
