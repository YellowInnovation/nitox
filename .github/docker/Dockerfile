FROM rust:1-slim

RUN mkdir -p /var/nitox
COPY Cargo.lock /var/nitox
COPY Cargo.toml /var/nitox
COPY src/ /var/nitox
WORKDIR /var/nitox

RUN cargo build
