ARG BASE=native

# Distroless image if not building native dependencies
FROM rust:1.90-slim-bookworm AS base-native

# Install build environment, header files and libraries if building libicu
FROM rust:1.90-slim-bookworm AS base-icu
RUN apt update && apt install -y pkg-config libclang-dev libicu72 libicu-dev

# Fork base layer depending on whether we build native dependencies
FROM base-${BASE} AS builder
ARG CARGO_ARGS=""

WORKDIR /app

COPY Cargo.toml Cargo.lock /app/
COPY crates/libmotiva/Cargo.toml /app/crates/libmotiva/
COPY crates/motiva/Cargo.toml /app/crates/motiva/
COPY crates/macros /app/crates/macros

RUN \
    mkdir -p crates/libmotiva/src crates/libmotiva/benches crates/motiva/src && \
    echo 'fn main() {}' | tee crates/libmotiva/src/lib.rs crates/libmotiva/benches/scoring.rs crates/motiva/src/main.rs

RUN cargo build --release --bin motiva ${CARGO_ARGS}

COPY . /app/

RUN \
    touch /app/crates/libmotiva/src/lib.rs /app/crates/motiva/src/main.rs && \
    cargo build --release --bin motiva ${CARGO_ARGS}

FROM gcr.io/distroless/cc:latest

COPY --from=builder /app/target/release/motiva /motiva
# Fallible step, will only copy libicu files if they exist
COPY --from=builder /usr/lib/x86_64-linux-gnu/libicu* /usr/lib/x86_64-linux-gnu/

ENTRYPOINT [ "/motiva" ]
CMD []
