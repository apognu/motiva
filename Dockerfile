FROM rust:1.89-slim-bookworm AS builder
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

ENTRYPOINT [ "/motiva" ]
CMD []
