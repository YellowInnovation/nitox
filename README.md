# Nitox - Tokio-based async NATS / NATS Streaming Server client

[![Crates.io](https://img.shields.io/crates/v/nitox.svg)](https://crates.io/crates/nitox)
[![docs.rs](https://docs.rs/nitox/badge.svg)](https://docs.rs/nitox)

## Description

Nitox is a `tokio`-based client for NATS. We wrote it because the existing library is sync and does not fit our needs.

A lot of features are currently missing, so feel free to contribute and help us building the best Async NATS client ever!

Missing features:

- [x] Find a way to integration test the reconnection mechanism - The 10M requests integration test provokes a slow consumer and a disconnection, which is handled well
- [x] Auto-pruning of subscriptions being unsubscribed after X messages
- [ ] Handle verbose mode
- [x] Handle pedantic mode - Should work OOB since we're closely following the protocol (Edit: it does)
- [x] Switch parsing to using `nom` - well, our parser is speedy/robust enough so I don't think it's worth the effort
- [x] Add support for NATS Streaming Server - Available under the `nats-streaming` feature flag!

*There's a small extra in the `tests/` folder, some of our integration tests rely on a custom NATS server implemented with `tokio` that only implements a subset of the protocol to fit our needs for the integration testing.*

## Documentation

Here: [http://docs.rs/nitox](http://docs.rs/nitox)

## Installation

```toml
[dependencies]
nitox = "0.2"
```

With NATS Streaming Server support enabled

```toml
[dependencies]
nitox = { version = "0.2", features = ["nats-streaming"] }
```

## Usage

```rust
extern crate nitox;
extern crate futures;
use futures::{prelude::*, future};
use nitox::{NatsClient, NatsClientOptions, NatsError, commands::*, streaming::*};

fn connect_to_nats() -> impl Future<Item = NatsClient, Error = NatsError> {
    // Defaults as recommended per-spec, but you can customize them
    let connect_cmd = ConnectCommand::builder().build().unwrap();
    let options = NatsClientOptions::builder()
        .connect_command(connect_cmd)
        .cluster_uri("127.0.0.1:4222")
        .build()
        .unwrap();

    NatsClient::from_options(options)
        .and_then(|client| {
            // Makes the client send the CONNECT command to the server, but it's usable as-is if needed
            client.connect()
        })
        .and_then(|client| {
            // Client has sent its CONNECT command and is ready for usage
            future::ok(client)
        })
        .and_then(|client| {
            // Also, you can switch to NATS Streaming Server client seamlessly
            future::ok(NatsStreamingClient::from(client))
        })
}
```

## Examples

In order to run the examples, you need a nats server listening to port 4222 on your localhost. If you have docker set up on your computer, you can run a nats server image in debug mode with this command :

```bash
$ docker run -p 4222:4222 nats -DV # -DV is for debug and verbose mode
```

Check out the examples and run them using cargo :

```bash
$ cargo run --example publish # Publish a message and log it in a subscriber
$ cargo run --example request_with_reply # Publish a request and receive a response
$ cargo run --example subscribe_ten_messages # Subscribe to 10 messages, send 15 and notice only 10 of them have been handled
```

## License

Licensed under either of these:

- Apache License, Version 2.0, ([LICENSE-APACHE](LICENSE-APACHE) or
   [https://www.apache.org/licenses/LICENSE-2.0](https://www.apache.org/licenses/LICENSE-2.0)
- MIT license ([LICENSE-MIT](LICENSE-MIT) or
   [https://opensource.org/licenses/MIT](https://opensource.org/licenses/MIT))

## Why "Nitox"

Standing for Nitric Oxide, an important chemical involved in communication between neurons; It's highly related to the acronym behind NATS, and you should ask the team behind it for the meaning! (*or look in the git history of `gnatsd`'s repo*)

## What is NATS

NATS Server is a simple, high performance open source messaging system for cloud native applications, IoT messaging, and microservices architectures.

More details at [NATS.io](https://nats.io/)

## Yellow Innovation

Yellow Innovation is the innovation laboratory of the French postal service: La Poste.

We create innovative user experiences and journeys through services with a focus on IoT lately.

[Yellow Innovation's website and works](http://yellowinnovation.fr/en/)
