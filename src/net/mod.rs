use futures::prelude::*;
use parking_lot::RwLock;
use std::net::SocketAddr;
use std::sync::Arc;

pub(crate) mod connection;
mod connection_inner;

mod tls;
pub use self::tls::*;

use error::NatsError;

use self::connection::NatsConnectionState;
use self::connection_inner::*;

pub(crate) use self::connection::NatsConnection;

/// Connect to a raw TCP socket
pub(crate) fn connect(addr: SocketAddr) -> impl Future<Item = NatsConnection, Error = NatsError> {
    NatsConnectionInner::connect_tcp(&addr).map(move |socket| {
        debug!(target: "nitox", "Connected through TCP");
        NatsConnection {
            is_tls: false,
            addr,
            host: None,
            state: Arc::new(RwLock::new(NatsConnectionState::Connected)),
            inner: Arc::new(RwLock::new(socket.into())),
        }
    })
}

/// Connect to a TLS over TCP socket. Upgrade is performed automatically
pub(crate) fn connect_tls(
    host: String,
    addr: SocketAddr,
    config: Option<NatsClientTlsConfig>,
) -> impl Future<Item = NatsConnection, Error = NatsError> {
    let inner_host = host.clone();
    NatsConnectionInner::connect_tcp(&addr)
        .and_then(move |socket| {
            debug!(target: "nitox", "Connected through TCP, upgrading to TLS");
            NatsConnectionInner::upgrade_tcp_to_tls(&host, socket)
        })
        .map(move |socket| {
            debug!(target: "nitox", "Connected through TCP over TLS");
            NatsConnection {
                is_tls: true,
                addr,
                host: Some(inner_host),
                state: Arc::new(RwLock::new(NatsConnectionState::Connected)),
                inner: Arc::new(RwLock::new(socket.into())),
            }
        })
}
