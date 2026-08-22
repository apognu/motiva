FROM lukemathwalker/cargo-chef:latest-rust-1.98.0-slim-bookworm AS base
RUN apt update && apt install -y pkg-config libssl-dev

FROM base AS planner

WORKDIR /app

COPY . .
RUN cargo chef prepare --bin motiva --recipe-path recipe.json

FROM base AS builder
ARG CARGO_ARGS=""

WORKDIR /app

COPY --from=planner /app/recipe.json recipe.json
RUN cargo chef cook --release ${CARGO_ARGS} --recipe-path recipe.json

COPY . /app/

RUN apt update && apt install -y git
RUN cargo build --release --bin motiva ${CARGO_ARGS}

FROM gcr.io/distroless/cc:latest

LABEL org.opencontainers.image.source="https://github.com/apognu/motiva"
LABEL org.opencontainers.image.licenses="MIT"
LABEL org.opencontainers.image.description="Sanctions screening tool"

COPY --from=builder /app/target/release/motiva /motiva

ENTRYPOINT [ "/motiva" ]
CMD []
