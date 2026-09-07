# Where the motiva binary comes from:
#  - source: build it inside Docker (default, for local builds)
#  - prebuilt: copy it from dist/motiva (used in CI, where it was already built)
ARG BINARY_SOURCE=source

FROM lukemathwalker/cargo-chef:latest-rust-1.98.0-slim-trixie AS base
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
RUN cargo build --release --bin motiva ${CARGO_ARGS} && cp target/release/motiva /motiva

FROM builder AS source

FROM scratch AS prebuilt
COPY --chmod=755 dist/motiva /motiva

FROM ${BINARY_SOURCE} AS binary

FROM gcr.io/distroless/cc-debian13

LABEL org.opencontainers.image.source="https://github.com/apognu/motiva"
LABEL org.opencontainers.image.licenses="MIT"
LABEL org.opencontainers.image.description="Sanctions screening tool"

COPY --from=binary /motiva /motiva

ENTRYPOINT [ "/motiva" ]
CMD []
