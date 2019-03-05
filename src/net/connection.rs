use futures::prelude::*;
use parking_lot::RwLock;
use std::{net::SocketAddr, sync::Arc};
use tokio_executor;

use error::NatsError;
use protocol::Op;

use super::{NatsClientTlsConfig, connection_inner::NatsConnectionInner};

macro_rules! reco {
    ($conn:ident) => {
        *$conn.state.write() = NatsConnectionState::Disconnected;

        tokio_executor::spawn($conn.reconnect().map_err(|e| {
            debug!(target: "nitox", "Reconnection error: {}", e);
            ()
        }));
    };
}

/// State of the raw connection
#[derive(Debug, Clone, Copy, PartialEq)]
pub(crate) enum NatsConnectionState {
    Connected,
    Reconnecting,
    Disconnected,
}

/// Represents a connection to a NATS server. Implements `Sink` and `Stream`
#[derive(Debug)]
pub struct NatsConnection {
    /// indicates if the connection is made over TLS
    pub(crate) is_tls: bool,
    /// Server standardized IP address
    pub(crate) addr: SocketAddr,
    /// Host of the server; Only used if connecting to a TLS-enabled server
    pub(crate) host: Option<String>,
    /// First message sent by the server. This is always `INFO` (until proven otherwise)
    /// and it's stored only during TLS connections, because we have to parse the first message
    /// before upgrading the connection.
    pub(crate) first_op: Option<Op>,
    /// TLS config for client verification; Only used if configured previously
    pub(crate) tls_config: NatsClientTlsConfig,
    /// Inner dual `Stream`/`Sink` of the TCP connection
    pub(crate) inner: Arc<RwLock<NatsConnectionInner>>,
    /// Current state of the connection
    pub(crate) state: Arc<RwLock<NatsConnectionState>>,
}

impl NatsConnection {
    /// Tries to reconnect once to the server; Only used internally. Blocks polling during reconnecting
    /// by forcing the object to return `Async::NotReady`/`AsyncSink::NotReady`
    fn reconnect(&self) -> impl Future<Item = (), Error = NatsError> {
        *self.state.write() = NatsConnectionState::Reconnecting;

        let inner_arc = Arc::clone(&self.inner);
        let inner_state = Arc::clone(&self.state);
        let maybe_host = self.host.clone();
        let tls_config = self.tls_config.clone();
        NatsConnectionInner::connect_and_upgrade_if_required(maybe_host, &self.addr, tls_config)
            .and_then(move |inner| {
                {
                    *inner_arc.write() = inner;
                    *inner_state.write() = NatsConnectionState::Connected;
                }
                debug!(target: "nitox", "Successfully swapped reconnected underlying connection");
                Ok(())
            })
    }
}

impl Sink for NatsConnection {
    type SinkError = NatsError;
    type SinkItem = Op;

    fn start_send(&mut self, item: Self::SinkItem) -> StartSend<Self::SinkItem, Self::SinkError> {
        if match self.state.try_read() {
            Some(state) => *state != NatsConnectionState::Connected,
            _ => true,
        } {
            return Ok(AsyncSink::NotReady(item));
        }

        if let Some(mut inner) = self.inner.try_write() {
            match inner.start_send(item.clone()) {
                Err(NatsError::ServerDisconnected(_)) => {
                    reco!(self);
                    Ok(AsyncSink::NotReady(item))
                }
                poll_res => poll_res,
            }
        } else {
            Ok(AsyncSink::NotReady(item))
        }
    }

    fn poll_complete(&mut self) -> Poll<(), Self::SinkError> {
        if match self.state.try_read() {
            Some(state) => *state != NatsConnectionState::Connected,
            _ => true,
        } {
            return Ok(Async::NotReady);
        }

        if let Some(mut inner) = self.inner.try_write() {
            match inner.poll_complete() {
                Err(NatsError::ServerDisconnected(_)) => {
                    reco!(self);
                    Ok(Async::NotReady)
                }
                poll_res => poll_res,
            }
        } else {
            Ok(Async::NotReady)
        }
    }
}

impl Stream for NatsConnection {
    type Error = NatsError;
    type Item = Op;

    fn poll(&mut self) -> Poll<Option<Self::Item>, Self::Error> {
        if match self.state.try_read() {
            Some(state) => *state != NatsConnectionState::Connected,
            _ => true,
        } {
            return Ok(Async::NotReady);
        }

        if let Some(mut inner) = self.inner.try_write() {
            match inner.poll() {
                Err(NatsError::ServerDisconnected(_)) => {
                    reco!(self);
                    Ok(Async::NotReady)
                }
                poll_res => poll_res,
            }
        } else {
            Ok(Async::NotReady)
        }
    }
}
