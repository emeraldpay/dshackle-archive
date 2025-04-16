FROM rust:1.85-bookworm AS builder

RUN apt-get update && apt-get install -y protobuf-compiler
COPY ./ /src/dshackle-archive

WORKDIR /src/dshackle-archive

RUN cargo fetch --verbose
RUN cargo build --release

FROM debian:bookworm

RUN apt-get update && apt install -y openssl ca-certificates

COPY --from=builder /src/dshackle-archive/target/release/dshackle-archive /opt/

ENTRYPOINT ["/opt/dshackle-archive"]
