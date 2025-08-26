FROM rust:1.89.0-alpine3.22 AS builder
WORKDIR /app

RUN apk add -U gcc g++

COPY Cargo.toml Cargo.lock /app/
COPY macros /app/macros
RUN mkdir -p src benches && echo 'fn main() {}' | tee src/main.rs src/lib.rs benches/scoring.rs
RUN cargo build --release --bin motiva

COPY . /app/
RUN touch /app/src/main.rs /app/src/lib.rs && cargo build --release --bin motiva

FROM alpine:3.22.1
COPY --from=builder /app/target/release/motiva /motiva

ENTRYPOINT [ "/motiva" ]
CMD []
