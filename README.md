# Nitox - Tokio-based async NATS client

[![Crates.io](https://img.shields.io/crates/v/nitox.svg)](https://crates.io/crates/nitox)
[![docs.rs](https://docs.rs/nitox/badge.svg)](https://docs.rs/nitox)

## Description

Nitox is a `tokio`-based client for NATS. We wrote it because the existing library is sync and does not fit our needs.

A lot of features are currently missing, so feel free to contribute and help us building the best Async NATS client ever!

Missing features:

- [x] Find a way to integration test the reconnection mechanism - but it has actually been hand-tested and works
- [x] Auto-pruning of subscriptions being unsubscribed after X messages - It's actually a bug, since a stream stays open albeit sleeping
- [ ] Handle verbose mode
- [x] Handle pedantic mode - Should work OOB since we're closely following the protocol (Edit: it does)
- [ ] Switch parsing to using `nom` - We're not sure we can handle very weird clients; we're fine talking to official ones right now
- [ ] Add support for NATS Streaming Server - Should be pretty easy with `prost` since we already have the async architecture going on

*There's a small extra in the `tests/` folder, some of our integration tests rely on a custom NATS server implemented with `tokio` that only implements a subset of the protocol to fit our needs for the integration testing.*

## Documentation

Here: [http://docs.rs/nitox](http://docs.rs/nitox)

## Installation

```toml
[dependencies]
nitox = "0.1"
```

## Usage

### Client setup

In order to subscribe to topics or publish messages, we first need to create a client.

The first thing a client needs to do is send a CONNECT command to the server,
as you can see below :

```rust
extern crate nitox;
extern crate futures;
use futures::{prelude::*, future};
use nitox::{NatsClient, NatsClientOptions, NatsError, commands::*};

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
}
```

### Subscribe to a channel

Once our nats client has been created, we can now subscribe to channels, and get ready to handle messages.

In order to subscribe to channels, we can use the SubCommand builder :

```rust
    let ready_to_listen_client = connect_to_nats()
        .and_then(|client| {
            client.subscribe(SubCommand::builder().subject("MyTopic").build().unwrap())
        });
```

Our client is now listening to messages being sent in the "MyTopic" channel.

Incoming messages are being handles as a stream, so it might be a good idea to take a step back and [learn more about futures and streams](https://dev.to/mindflavor/rust-futures-an-uneducated-short-and-hopefully-not-boring-tutorial---part-1-3k3).

### Sending a message to a channel

As we did for topic subscription, in order to send messages to channels using a connected client, we can use the PubCommand builder :

```rust
    connect_to_nats().and_then(|client| {
            client.publish(PubCommand::builder().subject("MyTopic").payload("Hello world!").build().unwrap())
        });

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
