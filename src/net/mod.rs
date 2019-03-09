use futures::prelude::*;
use native_tls::{Certificate, Identity};
use parking_lot::RwLock;
use std::net::SocketAddr;
use std::sync::Arc;

pub(crate) mod connection;
mod connection_inner;

use error::NatsError;

use self::connection::NatsConnectionState;
use self::connection_inner::*;

pub(crate) use self::connection::NatsConnection;

/// TLS configuration for the client.
#[derive(Clone, Default)]
pub struct NatsClientTlsConfig {
    pub(crate) identity: Option<Arc<(Vec<u8>, String)>>,
    pub(crate) root_cert: Option<Arc<Vec<u8>>>,
}

impl NatsClientTlsConfig {
    /// Set the identity from a DER-formatted PKCS #12 archive using the the given password to decrypt the key.
    pub fn pkcs12_identity<B>(mut self, der_bytes: B, password: &str) -> Result<Self, NatsError>
        where B: AsRef<[u8]>
    {
        self.identity = Some(Arc::new((der_bytes.as_ref().into(), password.into())));
        self.identity()?;
        Ok(self)
    }

    /// Set the root certificate in DER-format.
    pub fn root_cert_der<B>(mut self, der_bytes: B) -> Result<Self, NatsError>
        where B: AsRef<[u8]>
    {
        self.root_cert = Some(Arc::new(der_bytes.as_ref().into()));
        self.root_cert()?;
        Ok(self)
    }

    pub(crate) fn identity(&self) -> Result<Option<Identity>, NatsError> {
        if let Some((b, p)) = self.identity.as_ref().map(|s| &**s) {
            Ok(Some(Identity::from_pkcs12(b, p)?))
        } else {
            Ok(None)
        }
    }

    pub(crate) fn root_cert(&self) -> Result<Option<Certificate>, NatsError> {
        if let Some(b) = self.root_cert.as_ref() {
            Ok(Some(Certificate::from_der(b)?))
        } else {
            Ok(None)
        }
    }
}

impl ::std::fmt::Debug for NatsClientTlsConfig {
    fn fmt(&self, f: &mut ::std::fmt::Formatter) -> ::std::fmt::Result {
        f.debug_struct("NatsClientTlsConfig")
            .field("identity_exists", &self.identity.is_some())
            .field("root_cert_exists", &self.root_cert.is_some())
            .finish()
    }
}

/// Connect to a raw TCP socket
pub(crate) fn connect(addr: SocketAddr) -> impl Future<Item = NatsConnection, Error = NatsError> {
    NatsConnectionInner::connect_tcp(&addr).map(move |socket| {
        debug!(target: "nitox", "Connected through TCP");
        NatsConnection {
            is_tls: false,
            tls_config: Default::default(),
            addr,
            first_op: None,
            host: None,
            state: Arc::new(RwLock::new(NatsConnectionState::Connected)),
            inner: Arc::new(RwLock::new(socket.into())),
        }
    })
}

/// Connect to a TLS over TCP socket. Upgrade is performed automatically
pub(crate) fn connect_tls(host: String, addr: SocketAddr, tls_config: NatsClientTlsConfig) -> impl Future<Item = NatsConnection, Error = NatsError> {
    let inner_host = host.clone();
    let inner_config = tls_config.clone();
    NatsConnectionInner::connect_and_upgrade_if_required(Some(host), &addr, tls_config)
        .map(move |socket| {
            debug!(target: "nitox", "Connected through TLS");
            NatsConnection {
                is_tls: true,
                tls_config: inner_config,
                first_op: socket.first_op(),
                addr,
                host: Some(inner_host),
                state: Arc::new(RwLock::new(NatsConnectionState::Connected)),
                inner: Arc::new(RwLock::new(socket.into())),
            }
        })
}
