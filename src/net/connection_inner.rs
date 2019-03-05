use super::NatsClientTlsConfig;
use codec::OpCodec;
use futures::prelude::*;
use futures::future::{self, Either};
use native_tls::TlsConnector as NativeTlsConnector;
use protocol::Op;
use std::net::SocketAddr;
use tokio_codec::{Decoder, Framed, FramedParts};
use tokio_tcp::TcpStream;
use tokio_tls::{TlsConnector, TlsStream};

use error::NatsError;

/// Inner raw stream enum over TCP and TLS/TCP
#[derive(Debug)]
pub(crate) enum NatsConnectionInner {
    /// Raw TCP Stream framed connection
    Tcp(Framed<TcpStream, OpCodec>),
    /// TLS over TCP Stream framed connection
    Tls((Framed<TlsStream<TcpStream>, OpCodec>, Option<Op>)),
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
        config: NatsClientTlsConfig,
    ) -> impl Future<Item = TlsStream<TcpStream>, Error = NatsError> {
        let mut builder = NativeTlsConnector::builder();
        if let Some(i) = config.identity().unwrap() {
            builder.identity(i);
        }

        if let Some(c) = config.root_cert().unwrap() {
            builder.add_root_certificate(c);
        }

        let tls_connector = builder.build().unwrap();
        let tls_stream: TlsConnector = tls_connector.into();
        debug!(target: "nitox", "Connecting to {} through TLS over TCP", host);
        tls_stream.connect(&host, socket).from_err()
    }

    pub(crate) fn connect_and_upgrade_if_required(host: Option<String>, addr: &SocketAddr,
                                                  tls_config: NatsClientTlsConfig)
                                                 -> impl Future<Item = Self, Error = NatsError>
    {
        let addr = *addr;
        Self::connect_tcp(&addr).map(NatsConnectionInner::from).and_then(move |inner| {
            debug!(target: "nitox", "Connecting to {} and checking if it could be upgraded to TLS", addr);
            if let Some(host) = host {
                Either::A(
                    inner.into_future().map_err(|(e, _)| e).and_then(move |(op, inner)| {
                        match inner {
                            NatsConnectionInner::Tcp(framed) => {
                                debug!(target: "nitox", "Upgrading to TLS after first op.");
                                let old_parts = framed.into_parts();
                                let (socket, read_buf, write_buf) = (old_parts.io, old_parts.read_buf, old_parts.write_buf);
                                Either::A(Self::upgrade_tcp_to_tls(&host, socket, tls_config).map(move |socket| {
                                    debug!(target: "nitox", "Storing first op {:?} for later use.", op);
                                    let mut new_parts = FramedParts::new(socket, OpCodec::default());
                                    new_parts.read_buf = read_buf;
                                    new_parts.write_buf = write_buf;
                                    NatsConnectionInner::Tls((Framed::from_parts(new_parts), op))
                                }))
                            },
                            _ => Either::B(future::ok(inner)),
                        }
                    })
                )
            } else {
                Either::B(future::ok(inner))
            }
        })
    }

    pub(crate) fn first_op(&self) -> Option<Op> {
        if let NatsConnectionInner::Tls((_, ref op)) = self {
            op.clone()
        } else {
            None
        }
    }
}

impl From<TcpStream> for NatsConnectionInner {
    fn from(socket: TcpStream) -> Self {
        NatsConnectionInner::Tcp(OpCodec::default().framed(socket))
    }
}

impl Sink for NatsConnectionInner {
    type SinkError = NatsError;
    type SinkItem = Op;

    fn start_send(&mut self, item: Self::SinkItem) -> StartSend<Self::SinkItem, Self::SinkError> {
        match self {
            NatsConnectionInner::Tcp(framed) => framed.start_send(item),
            NatsConnectionInner::Tls((framed, _)) => framed.start_send(item),
        }
    }

    fn poll_complete(&mut self) -> Poll<(), Self::SinkError> {
        match self {
            NatsConnectionInner::Tcp(framed) => framed.poll_complete(),
            NatsConnectionInner::Tls((framed, _)) => framed.poll_complete(),
        }
    }
}

impl Stream for NatsConnectionInner {
    type Error = NatsError;
    type Item = Op;

    fn poll(&mut self) -> Poll<Option<Self::Item>, Self::Error> {
        match self {
            NatsConnectionInner::Tcp(framed) => framed.poll(),
            NatsConnectionInner::Tls((framed, _)) => framed.poll(),
        }
    }
}
