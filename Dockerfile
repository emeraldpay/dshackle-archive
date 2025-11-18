FROM rust:1.90-slim-bookworm AS builder

RUN apt-get update &&  \
    apt-get install -y protobuf-compiler && \
    apt-get install -y build-essential libssl-dev openssl pkg-config
COPY ./ /src/dshackle-archive

WORKDIR /src/dshackle-archive

RUN cargo fetch --verbose
RUN cargo build --release

FROM debian:bookworm-slim

RUN apt-get update && apt install -y openssl ca-certificates

COPY --from=builder /src/dshackle-archive/target/release/dshackle-archive /opt/

ENV LANG=C.UTF-8
ENV LC_ALL=C.UTF-8
ENTRYPOINT ["/opt/dshackle-archive"]
