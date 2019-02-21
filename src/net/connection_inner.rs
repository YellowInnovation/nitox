use crate::codec::OpCodec;
use crate::protocol::Op;
use futures::{
    future::{self, Either},
    prelude::*,
};
use native_tls::TlsConnector as NativeTlsConnector;
use std::net::SocketAddr;
use tokio_codec::{Decoder, Framed};
use tokio_tcp::TcpStream;
use tokio_tls::{TlsConnector, TlsStream};

use crate::error::NatsError;

/// Inner raw stream enum over TCP and TLS/TCP
#[derive(Debug)]
pub(crate) enum NatsConnectionInner {
    /// Raw TCP Stream framed connection
    Tcp(Box<Framed<TcpStream, OpCodec>>),
    /// TLS over TCP Stream framed connection
    Tls(Box<Framed<TlsStream<TcpStream>, OpCodec>>),
}

impl NatsConnectionInner {
    /// Connects to a TCP socket
    pub(crate) fn connect_tcp(addr: &SocketAddr) -> impl Future<Item = TcpStream, Error = NatsError> {
        debug!(target: "nitox", "Connecting to {} through TCP", addr);
        TcpStream::connect(addr).from_err()
    }

    /// Upgrades an existing TCP socket to TLS over TCP
    pub(crate) fn upgrade_tcp_to_tls(
        host: &str,
        socket: TcpStream,
    ) -> impl Future<Item = TlsStream<TcpStream>, Error = NatsError> {
        let tls_connector = match NativeTlsConnector::builder()
            .build()
            .map_err(|err| Either::B(future::err(err.into())))
        {
            Ok(connector) => connector,
            Err(e) => {
                return e;
            }
        };

        let tls_stream: TlsConnector = tls_connector.into();
        debug!(target: "nitox", "Connecting to {} through TLS over TCP", host);
        Either::A(tls_stream.connect(&host, socket).from_err())
    }
}

impl From<TcpStream> for NatsConnectionInner {
    fn from(socket: TcpStream) -> Self {
        NatsConnectionInner::Tcp(Box::new(OpCodec::default().framed(socket)))
    }
}

impl From<TlsStream<TcpStream>> for NatsConnectionInner {
    fn from(socket: TlsStream<TcpStream>) -> Self {
        NatsConnectionInner::Tls(Box::new(OpCodec::default().framed(socket)))
    }
}

impl Sink for NatsConnectionInner {
    type SinkError = NatsError;
    type SinkItem = Op;

    fn start_send(&mut self, item: Self::SinkItem) -> StartSend<Self::SinkItem, Self::SinkError> {
        match self {
            NatsConnectionInner::Tcp(framed) => framed.start_send(item),
            NatsConnectionInner::Tls(framed) => framed.start_send(item),
        }
    }

    fn poll_complete(&mut self) -> Poll<(), Self::SinkError> {
        match self {
            NatsConnectionInner::Tcp(framed) => framed.poll_complete(),
            NatsConnectionInner::Tls(framed) => framed.poll_complete(),
        }
    }
}

impl Stream for NatsConnectionInner {
    type Error = NatsError;
    type Item = Op;

    fn poll(&mut self) -> Poll<Option<Self::Item>, Self::Error> {
        match self {
            NatsConnectionInner::Tcp(framed) => framed.poll(),
            NatsConnectionInner::Tls(framed) => framed.poll(),
        }
    }
}
