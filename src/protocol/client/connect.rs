use crate::protocol::{Command, CommandError};
use bytes::{BufMut, Bytes, BytesMut};
use serde_json as json;

/// The CONNECT message is the client version of the INFO message. Once the client has established a TCP/IP
/// socket connection with the NATS server, and an INFO message has been received from the server, the client
/// may send a CONNECT message to the NATS server to provide more information about the current connection as
/// well as security information.
#[derive(Debug, Default, Clone, PartialEq, Serialize, Deserialize, Builder)]
#[builder(default)]
pub struct ConnectCommand {
    /// Turns on +OK protocol acknowledgements.
    pub verbose: bool,
    /// Turns on additional strict format checking, e.g. for properly formed subjects
    pub pedantic: bool,
    /// Indicates whether the client requires an SSL connection.
    pub tls_required: bool,
    /// Client authorization token (if auth_required is set)
    #[serde(skip_serializing_if = "Option::is_none")]
    auth_token: Option<String>,
    /// Connection username (if auth_required is set)
    #[serde(skip_serializing_if = "Option::is_none")]
    user: Option<String>,
    /// Connection password (if auth_required is set)
    #[serde(skip_serializing_if = "Option::is_none")]
    pass: Option<String>,
    /// Optional client name
    #[serde(skip_serializing_if = "Option::is_none")]
    #[builder(default = "self.default_name()?")]
    pub name: Option<String>,
    /// The implementation language of the client.
    #[builder(default = "self.default_lang()?", setter(into))]
    pub lang: String,
    /// The version of the client.
    #[builder(default = "self.default_ver()?", setter(into))]
    pub version: String,
    /// optional int. Sending 0 (or absent) indicates client supports original protocol. Sending 1 indicates that the
    /// client supports dynamic reconfiguration of cluster topology changes by asynchronously receiving INFO messages
    /// with known servers it can reconnect to.
    #[serde(skip_serializing_if = "Option::is_none")]
    protocol: Option<u8>,
    /// Optional boolean. If set to true, the server (version 1.2.0+) will not send originating messages from this
    /// connection to its own subscriptions. Clients should set this to true only for server supporting this feature,
    /// which is when proto in the INFO protocol is set to at least 1.
    #[serde(skip_serializing_if = "Option::is_none")]
    echo: Option<bool>,
}

impl ConnectCommand {
    pub fn builder() -> ConnectCommandBuilder {
        ConnectCommandBuilder::default()
    }
}

impl ConnectCommandBuilder {
    fn default_name(&self) -> Result<Option<String>, String> {
        Ok(Some("nitox".into()))
    }

    fn default_ver(&self) -> Result<String, String> {
        Ok(env!("CARGO_PKG_VERSION").into())
    }

    fn default_lang(&self) -> Result<String, String> {
        Ok("rust".into())
    }
}

impl Command for ConnectCommand {
    const CMD_NAME: &'static [u8] = b"CONNECT";

    fn into_vec(self) -> Result<Bytes, CommandError> {
        let json_cmd = json::to_vec(&self)?;
        let mut cmd: BytesMut = BytesMut::with_capacity(10 + json_cmd.len());
        cmd.put("CONNECT\t");
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
        if buf[..7] != *Self::CMD_NAME {
            return Err(CommandError::CommandMalformed);
        }

        Ok(json::from_slice(&buf[7..len - 2])?)
    }
}

#[cfg(test)]
mod tests {
    use super::{ConnectCommand, ConnectCommandBuilder};
    use crate::protocol::Command;

    static DEFAULT_CONNECT: &'static str = "CONNECT\t{\"verbose\":false,\"pedantic\":false,\"tls_required\":false,\"name\":\"nitox\",\"lang\":\"rust\",\"version\":\"1.0.0\"}\r\n";

    #[test]
    fn it_parses() {
        let parse_res = ConnectCommand::try_parse(DEFAULT_CONNECT.into());
        assert!(parse_res.is_ok());
        let cmd = parse_res.unwrap();
        assert_eq!(cmd.verbose, false);
        assert_eq!(cmd.pedantic, false);
        assert_eq!(cmd.tls_required, false);
        assert!(cmd.name.is_some());
        assert_eq!(cmd.name.unwrap(), "nitox");
        assert_eq!(cmd.lang, "rust");
        assert_eq!(cmd.version, "1.0.0");
    }

    #[test]
    fn it_stringifies() {
        let cmd = ConnectCommandBuilder::default()
            .lang("rust")
            .version("1.0.0")
            .name(Some("nitox".into()))
            .build()
            .unwrap();

        let cmd_bytes_res = cmd.into_vec();
        assert!(cmd_bytes_res.is_ok());
        let cmd_bytes = cmd_bytes_res.unwrap();

        assert_eq!(DEFAULT_CONNECT, cmd_bytes);
    }
}
