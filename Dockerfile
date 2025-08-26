FROM rust:1.89.0-alpine3.22 AS builder

WORKDIR /app
COPY . /app

RUN apk add -U gcc g++
RUN cargo build --release

FROM alpine:3.22.1
COPY --from=builder /app/target/release/motiva /motiva

ENTRYPOINT [ "/motiva" ]
