FROM rust:1-slim

RUN mkdir -p /var/nitox
RUN ls -al
COPY Cargo.lock /var/nitox
COPY Cargo.toml /var/nitox
COPY src/ /var/nitox
WORKDIR /var/nitox

RUN cargo build
