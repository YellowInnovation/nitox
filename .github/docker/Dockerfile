FROM rust:1-slim

RUN mkdir -p /var/nitox
COPY ../../Cargo.* /var/nitox
COPY ../../src/ /var/nitox
WORKDIR /var/nitox

RUN cargo build
