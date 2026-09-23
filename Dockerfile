# syntax=docker/dockerfile:1
FROM rust:1.94-slim-bookworm AS builder

RUN apt-get update &&  \
    apt-get install -y protobuf-compiler && \
    apt-get install -y build-essential libssl-dev openssl pkg-config

WORKDIR /src/dshackle-archive

# The sources (including .git, which the build script reads the commit from) are mounted only for the build, so they
# never become a layer. The mount is read-only, so the build goes to a cache dir, and the binary is copied out of it
# in the same step because a cache mount isn't a part of the image.
RUN --mount=type=bind,target=/src/dshackle-archive \
    --mount=type=cache,target=/usr/local/cargo/registry \
    --mount=type=cache,target=/usr/local/cargo/git \
    --mount=type=cache,target=/build/target \
    CARGO_TARGET_DIR=/build/target cargo build --release --locked && \
    cp /build/target/release/dshackle-archive /opt/dshackle-archive

FROM debian:bookworm-slim

RUN apt-get update && apt install -y openssl ca-certificates

COPY --from=builder /opt/dshackle-archive /opt/

ENV LANG=C.UTF-8
ENV LC_ALL=C.UTF-8
ENTRYPOINT ["/opt/dshackle-archive"]
