# Development laboratory only; this is not the product distribution image.
FROM rust:1.98.1-bookworm@sha256:9a73a5088750b4c95158ab26629c854c3d6fc4b173cb7bc8079ad252d8ed7bfa
LABEL org.bigbrotr.purpose="isolated-foundation-acceptance"
RUN apt-get update \
    && apt-get install --no-install-recommends -y python3 strace clang llvm libclang-dev g++ nodejs \
    && rm -rf /var/lib/apt/lists/* \
    && rustup component add --toolchain 1.98.1 rustfmt clippy
