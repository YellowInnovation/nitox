use super::protocol;
use std::io;

macro_rules! from_error {
    ($type:ty, $target:ident, $targetvar:expr) => {
        impl From<$type> for $target {
            fn from(s: $type) -> Self {
                $targetvar(s.into())
            }
        }
    };
}

/// Error enum for all cases of internal/external errors occuring during client execution
#[derive(Debug, Fail)]
pub enum NatsError {
    /// Building a command has failed because of invalid syntax or incorrect arguments
    #[fail(display = "CommandBuildError: {}", _0)]
    CommandBuildError(String),
    /// Generic IO error from stdlib
    #[fail(display = "IOError: {:?}", _0)]
    IOError(io::Error),
    /// Occurs when the client is not yet connected or got disconnected from the server.
    /// Contains `Some<io::Error>` when it's actually a disconnection or contains `None` when we are not connected at all
    #[fail(display = "ServerDisconnected: {:?}", _0)]
    ServerDisconnected(Option<io::Error>),
    /// Protocol error
    #[fail(display = "ProtocolError: {}", _0)]
    ProtocolError(protocol::CommandError),
    /// Occurs if we try to parse a string that is supposed to be valid UTF8 and...is actually not
    #[fail(display = "UTF8Error: {}", _0)]
    UTF8Error(::std::string::FromUtf8Error),
    /// Error on TLS handling
    #[fail(display = "TlsError: {}", _0)]
    TlsError(::native_tls::Error),
    /// Occurs when the host is not provided, removing the ability for TLS to function correctly for server identify verification
    #[fail(display = "TlsHostMissingError: Host is missing, can't verify server identity")]
    TlsHostMissingError,
    /// Cannot parse an URL
    #[fail(display = "UrlParseError: {}", _0)]
    UrlParseError(::url::ParseError),
    /// Cannot parse an IP
    #[fail(display = "AddrParseError: {}", _0)]
    AddrParseError(::std::net::AddrParseError),
    /// Occurs when we cannot resolve the URI given using the local host's DNS resolving mechanisms
    /// Will contain `Some(io::Error)` when the resolving has been tried with an error, and `None` when
    /// resolving succeeded but gave no results
    #[fail(display = "UriDNSResolveError: {:?}", _0)]
    UriDNSResolveError(Option<io::Error>),
    /// Cannot reconnect to server after retrying once
    #[fail(display = "CannotReconnectToServer: cannot reconnect to server")]
    CannotReconnectToServer,
    /// Something went wrong in one of the Reciever/Sender pairs
    #[fail(display = "InnerBrokenChain: the sender/receiver pair has been disconnected")]
    InnerBrokenChain,
    /// The user supplied a too big payload for the server
    #[fail(
        display = "MaxPayloadOverflow: the given payload exceeds the server setting (max_payload_size = {})",
        _0
    )]
    MaxPayloadOverflow(u32),
    /// Generic string error
    #[fail(display = "GenericError: {}", _0)]
    GenericError(String),
    /// Error thrown when a subscription is fused after reaching the maximum messages
    #[fail(display = "SubscriptionReachedMaxMsgs after {} messages", _0)]
    SubscriptionReachedMaxMsgs(u32),
    #[fail(display = "RequestTimeoutError: The NATS request reached its configured timeout")]
    RequestTimeoutError,
}

impl From<io::Error> for NatsError {
    fn from(err: io::Error) -> Self {
        match err.kind() {
            io::ErrorKind::ConnectionReset | io::ErrorKind::ConnectionRefused => {
                NatsError::ServerDisconnected(Some(err))
            }
            _ => NatsError::IOError(err),
        }
    }
}

impl<T> From<::futures::sync::mpsc::SendError<T>> for NatsError {
    fn from(_: ::futures::sync::mpsc::SendError<T>) -> Self {
        NatsError::InnerBrokenChain
    }
}

from_error!(protocol::CommandError, NatsError, NatsError::ProtocolError);
from_error!(::std::string::FromUtf8Error, NatsError, NatsError::UTF8Error);
from_error!(::native_tls::Error, NatsError, NatsError::TlsError);
from_error!(String, NatsError, NatsError::GenericError);
from_error!(::url::ParseError, NatsError, NatsError::UrlParseError);
from_error!(::std::net::AddrParseError, NatsError, NatsError::AddrParseError);

impl From<::tokio_timer::timeout::Error<NatsError>> for NatsError {
    fn from(_: ::tokio_timer::timeout::Error<NatsError>) -> Self {
        NatsError::RequestTimeoutError
    }
}
