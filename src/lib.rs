///! # Nitox - Tokio-based async NATS client
///!
///! [![Crates.io](https://img.shields.io/crates/v/nitox.svg)](https://crates.io/crates/nitox)
///! [![docs.rs](https://docs.rs/nitox/badge.svg)](https://docs.rs/nitox)
///!
///! ## Description
///!
///! Nitox is a `tokio`-based client for NATS. We wrote it because the existing library is sync and does not fit our needs.
///!
///! A lot of features are currently missing, so feel free to contribute and help us building the best Async NATS client ever!
///!
///! *There's a small extra in the `tests/` folder, some of our integration tests rely on a custom NATS server implemented with `tokio` that only implements a subset of the protocol that fits our needs for the integration testing.*
///!
///! ## Documentation
///!
///! Here: [http://docs.rs/nitox](http://docs.rs/nitox)
///!
///! ## Installation
///!
///! ```toml
///! [dependencies]
///! nitox = "0.1"
///! ```
///!
///! ## Usage
///!
///! ```rust
///! extern crate nitox;
///! extern crate futures;
///! use futures::{prelude::*, future};
///! use nitox::{NatsClient, NatsClientOptions, NatsError, commands::*};
///!
///! fn connect_to_nats() -> impl Future<Item = NatsClient, Error = NatsError> {
///!     // Defaults as recommended per-spec, but you can customize them
///!     let connect_cmd = ConnectCommand::builder().build().unwrap();
///!     let options = NatsClientOptions::builder()
///!         .connect_command(connect_cmd)
///!         .cluster_uri("127.0.0.1:4222")
///!         .build()
///!         .unwrap();
///!
///!     NatsClient::from_options(options)
///!         .and_then(|client| {
///!             // Makes the client send the CONNECT command to the server, but it's usable as-is if needed
///!             client.connect()
///!         })
///!         .and_then(|client| {
///!             // Client has sent its CONNECT command and is ready for usage
///!             future::ok(client)
///!         })
///! }
///! ```
///
///! ## License
///!
///! Licensed under either of these:
///!
///! - Apache License, Version 2.0, ([LICENSE-APACHE](LICENSE-APACHE) or
///!    [https://www.apache.org/licenses/LICENSE-2.0](https://www.apache.org/licenses/LICENSE-2.0)
///! - MIT license ([LICENSE-MIT](LICENSE-MIT) or
///!    [https://opensource.org/licenses/MIT](https://opensource.org/licenses/MIT))
///!
///! ## Why "Nitox"?
///!
///! Standing for Nitric Oxide, an important chemical involved in communication between neurons; It's highly related to the acronym behind NATS, and you should ask the team behind it for the meaning! (*or look in the git history of `gnatsd`'s repo*)
///!
///! ## Yellow Innovation
///!
///! Yellow Innovation is the innovation laboratory of the French postal service: La Poste.
///!
///! We create innovative user experiences and journeys through services with a focus on IoT lately.
///!
///! [Yellow Innovation's website and works](http://yellowinnovation.fr/en/)

#[macro_use]
extern crate derive_builder;

extern crate failure;
#[macro_use]
extern crate failure_derive;

extern crate serde;
#[macro_use]
extern crate serde_derive;
extern crate serde_json;

extern crate bytes;
extern crate rand;

#[macro_use]
extern crate log;

extern crate futures;
extern crate native_tls;
extern crate tokio_codec;
extern crate tokio_executor;
extern crate tokio_tcp;
extern crate tokio_tls;
extern crate url;

#[macro_use]
mod error;

// TODO: Handle verbose mode
// TODO: Switch parsing to using `nom`
// TODO: Support NATS Streaming Server

pub use self::error::*;
pub mod codec;
mod protocol;
pub use self::protocol::*;

pub(crate) mod net;

mod client;
pub use self::client::*;
