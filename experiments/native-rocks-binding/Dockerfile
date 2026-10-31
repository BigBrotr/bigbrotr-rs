# Development laboratory only; this is not a BigBrotr distribution image.
FROM rust:1.98.1-bookworm@sha256:9a73a5088750b4c95158ab26629c854c3d6fc4b173cb7bc8079ad252d8ed7bfa
LABEL org.bigbrotr.purpose="isolated-native-rocks-binding-qualification"

RUN apt-get update \
    && apt-get install --no-install-recommends -y libclang-dev python3 \
    && rm -rf /var/lib/apt/lists/*

ENV LIBCLANG_PATH=/usr/lib/llvm-14/lib
ENV CARGO_BUILD_JOBS=2
# Context is a bounded exact-tree export preserving experiment/vendor paths.
WORKDIR /source/experiments/native-rocks-binding
COPY vendor/rocksdb/ /source/vendor/rocksdb/
COPY vendor/librocksdb-sys/ /source/vendor/librocksdb-sys/
COPY vendor/rocksdb.provenance.json vendor/librocksdb-sys.provenance.json /source/vendor/
COPY experiments/native-rocks-binding/ ./
RUN cargo build --locked --release

ENTRYPOINT ["python3", "driver.py", "--binary", "/source/experiments/native-rocks-binding/target/release/native-rocks-binding", "--cargo-lock", "/source/experiments/native-rocks-binding/Cargo.lock"]
