FROM rust:1.89-slim-bookworm AS builder
ARG CARGO_ARGS=""

WORKDIR /app

COPY Cargo.toml Cargo.lock /app/
COPY macros /app/macros
RUN mkdir -p src benches && echo 'fn main() {}' | tee src/main.rs src/lib.rs benches/scoring.rs
RUN cargo build --release --bin motiva ${CARGO_ARGS}

COPY . /app/
RUN touch /app/src/main.rs /app/src/lib.rs && cargo build --release --bin motiva ${CARGO_ARGS}

FROM gcr.io/distroless/cc:latest
COPY --from=builder /app/target/release/motiva /motiva

ENTRYPOINT [ "/motiva" ]
CMD []
