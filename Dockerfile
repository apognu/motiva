ARG BASE=native

# Distroless image if not building native dependencies
FROM lukemathwalker/cargo-chef:latest-rust-1.93.0-slim-bookworm AS base-native

# Install build environment, header files and libraries if building libicu
FROM lukemathwalker/cargo-chef:latest-rust-1.93.0-slim-bookworm AS base-icu
RUN apt update && apt install -y pkg-config libclang-dev libicu72 libicu-dev

FROM base-${BASE} AS planner

WORKDIR /app

COPY . .
RUN cargo chef prepare --bin motiva --recipe-path recipe.json

# Fork base layer depending on whether we build native dependencies
FROM base-${BASE} AS builder
ARG CARGO_ARGS=""

WORKDIR /app

COPY --from=planner /app/recipe.json recipe.json
RUN cargo chef cook --release ${CARGO_ARGS} --recipe-path recipe.json

COPY . /app/

RUN cargo build --release --bin motiva ${CARGO_ARGS}

FROM gcr.io/distroless/cc:latest

COPY --from=builder /app/target/release/motiva /motiva
# Fallible step, will only copy libicu files if they exist
COPY --from=builder /usr/lib/x86_64-linux-gnu/libicu* /usr/lib/x86_64-linux-gnu/

ENTRYPOINT [ "/motiva" ]
CMD []
