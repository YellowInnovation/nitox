use NatsError;

/// Error enum for all cases of internal/external errors occuring during client execution
#[derive(Debug, Fail)]
pub enum NatsStreamingError {
    #[fail(display = "NatsError: {}", _0)]
    NatsError(NatsError),
    #[fail(display = "ProtobufDecodeError: {}", _0)]
    ProtobufDecodeError(prost::DecodeError),
    #[fail(display = "ProtobufEncodeError: {}", _0)]
    ProtobufEncodeError(prost::EncodeError),
    #[fail(display = "CannotAck for GUID {}", _0)]
    CannotAck(String),
    #[fail(display = "ServerError: {}", _0)]
    ServerError(String),
    #[fail(display = "Please provide a Cluster ID")]
    MissingClusterId,
    #[fail(display = "An error has occured in the Subscription Stream")]
    SubscriptionError,
}

impl<T> From<futures::sync::mpsc::SendError<T>> for NatsStreamingError {
    fn from(_: futures::sync::mpsc::SendError<T>) -> Self {
        NatsStreamingError::SubscriptionError
    }
}

from_error!(NatsError, NatsStreamingError, NatsStreamingError::NatsError);
from_error!(
    prost::DecodeError,
    NatsStreamingError,
    NatsStreamingError::ProtobufDecodeError
);
from_error!(
    prost::EncodeError,
    NatsStreamingError,
    NatsStreamingError::ProtobufEncodeError
);
