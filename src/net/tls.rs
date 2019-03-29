use error::NatsError;
use std::sync::Arc;
// Written by @wafflespeanut from @Naamio
use native_tls::{Certificate, Identity};

/// TLS configuration for the client.
#[derive(Clone, Default)]
pub struct NatsClientTlsConfig {
    pub(crate) identity: Option<Arc<(Vec<u8>, String)>>,
    pub(crate) root_cert: Option<Arc<Vec<u8>>>,
}

impl NatsClientTlsConfig {
    /// Set the identity from a DER-formatted PKCS #12 archive using the the given password to decrypt the key.
    pub fn pkcs12_identity<B>(mut self, der_bytes: B, password: &str) -> Result<Self, NatsError>
    where
        B: AsRef<[u8]>,
    {
        self.identity = Some(Arc::new((der_bytes.as_ref().into(), password.into())));
        self.identity()?;
        Ok(self)
    }

    /// Set the root certificate in DER-format.
    pub fn root_cert_der<B>(mut self, der_bytes: B) -> Result<Self, NatsError>
    where
        B: AsRef<[u8]>,
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
