use crate::protocol::{Command, CommandError};
use bytes::{BufMut, Bytes, BytesMut};
use serde_json as json;

/// As soon as the server accepts a connection from the client, it will send information about itself and the
/// configuration and security requirements that are necessary for the client to successfully authenticate with
/// the server and exchange messages.
///
/// When using the updated client protocol (see CONNECT below), INFO messages can be sent anytime by the server.
/// This means clients with that protocol level need to be able to asynchronously handle INFO messages.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, Builder)]
pub struct ServerInfo {
    /// The unique identifier of the NATS server
    #[builder(setter(into))]
    pub(crate) server_id: String,
    /// The version of the NATS server
    #[builder(setter(into))]
    pub(crate) version: String,
    /// The version of golang the NATS server was built with
    #[builder(setter(into))]
    pub(crate) go: String,
    /// The IP address used to start the NATS server, by default this will be 0.0.0.0 and can be configured with
    /// `-client_advertise host:port`
    #[builder(setter(into))]
    pub(crate) host: String,
    /// The port number the NATS server is configured to listen on
    #[builder(setter(into))]
    pub(crate) port: u32,
    /// Maximum payload size, in bytes, that the server will accept from the client.
    #[builder(setter(into))]
    pub(crate) max_payload: u32,
    /// An integer indicating the protocol version of the server. The server version 1.2.0 sets this to 1 to indicate
    /// that it supports the “Echo” feature.
    #[builder(default)]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) proto: Option<u8>,
    /// An optional unsigned integer (64 bits) representing the internal client identifier in the server. This can be
    /// used to filter client connections in monitoring, correlate with error logs, etc…
    #[builder(default)]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) client_id: Option<u64>,
    /// If this is set, then the client should try to authenticate upon connect.
    #[builder(default)]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) auth_required: Option<bool>,
    /// If this is set, then the client must perform the TLS/1.2 handshake. Note, this used to be ssl_required and has
    /// been updated along with the protocol from SSL to TLS.
    #[builder(default)]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) tls_required: Option<bool>,
    /// If this is set, the client must provide a valid certificate during the TLS handshake.
    #[builder(default)]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) tls_verify: Option<bool>,
    /// An optional list of server urls that a client can connect to.
    #[builder(default)]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) connect_urls: Option<Vec<String>>,
}

impl ServerInfo {
    pub fn builder() -> ServerInfoBuilder {
        ServerInfoBuilder::default()
    }
}

impl Command for ServerInfo {
    const CMD_NAME: &'static [u8] = b"INFO";

    fn into_vec(self) -> Result<Bytes, CommandError> {
        let json_cmd = json::to_vec(&self)?;
        let mut cmd: BytesMut = BytesMut::with_capacity(7 + json_cmd.len());
        cmd.put("INFO\t");
        cmd.put(json_cmd);
        cmd.put("\r\n");

        Ok(cmd.freeze())
    }

    fn try_parse(buf: Bytes) -> Result<Self, CommandError> {
        let len = buf.len();

        if buf[len - 2..] != [b'\r', b'\n'] {
            return Err(CommandError::IncompleteCommandError);
        }
        // Check if we're still on the right command
        if buf[..4] != *Self::CMD_NAME {
            return Err(CommandError::CommandMalformed);
        }

        Ok(json::from_slice(&buf[4..len - 2])?)
    }
}

#[cfg(test)]
mod tests {
    use super::{ServerInfo, ServerInfoBuilder};
    use crate::protocol::Command;

    static DEFAULT_INFO: &'static str = "INFO\t{\"server_id\":\"test\",\"version\":\"1.3.0\",\"go\":\"go1.10.3\",\"host\":\"0.0.0.0\",\"port\":4222,\"max_payload\":4000,\"proto\":1,\"client_id\":1337}\r\n";

    #[test]
    fn it_parses() {
        let parse_res = ServerInfo::try_parse(DEFAULT_INFO.into());
        assert!(parse_res.is_ok());
        let cmd = parse_res.unwrap();
        assert_eq!(cmd.server_id, "test");
        assert_eq!(cmd.version, "1.3.0");
        assert_eq!(cmd.proto, Some(1u8));
        assert_eq!(cmd.go, "go1.10.3");
        assert_eq!(cmd.host, "0.0.0.0");
        assert_eq!(cmd.port, 4222u32);
        assert_eq!(cmd.max_payload, 4000u32);
        assert!(cmd.client_id.is_some());
        assert_eq!(cmd.client_id, Some(1337u64));
    }

    #[test]
    fn it_stringifies() {
        let cmd = ServerInfoBuilder::default()
            .server_id("test")
            .version("1.3.0")
            .proto(Some(1u8))
            .go("go1.10.3")
            .host("0.0.0.0")
            .port(4222u32)
            .max_payload(4000u32)
            .client_id(Some(1337))
            .build()
            .unwrap();

        let cmd_bytes_res = cmd.into_vec();
        assert!(cmd_bytes_res.is_ok());
        let cmd_bytes = cmd_bytes_res.unwrap();

        assert_eq!(DEFAULT_INFO, cmd_bytes);
    }
}
