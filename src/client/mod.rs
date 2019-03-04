use futures::stream;
use net::*;

/// Sink (write) part of a TCP stream
type NatsSink = stream::SplitSink<NatsConnection>;
/// Stream (read) part of a TCP stream
type NatsStream = stream::SplitStream<NatsConnection>;
/// Useless pretty much, just for code semantics
type NatsSubscriptionId = String;

mod ack_trigger;
pub(crate) use self::ack_trigger::*;
mod sender;
pub(crate) use self::sender::*;
mod multiplexer;
pub(crate) use self::multiplexer::*;
mod client;
pub use self::client::*;
