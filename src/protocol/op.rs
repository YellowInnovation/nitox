use super::{commands::*, Command, CommandError};
use bytes::Bytes;

/// Abstraction over NATS protocol messages
#[derive(Debug, Clone, PartialEq)]
pub enum Op {
    /// **SERVER** Sent to client after initial TCP/IP connection
    INFO(ServerInfo),
    /// **CLIENT** Sent to server to specify connection information
    CONNECT(ConnectCommand),
    /// **CLIENT** Publish a message to a subject, with optional reply subject
    PUB(PubCommand),
    /// **CLIENT** Subscribe to a subject (or subject wildcard)
    SUB(SubCommand),
    /// **CLIENT** Unsubscribe (or auto-unsubscribe) from subject
    UNSUB(UnsubCommand),
    /// **SERVER** Delivers a message payload to a subscriber
    MSG(Message),
    /// **BOTH** PING keep-alive message
    PING,
    /// **BOTH** PONG keep-alive message
    PONG,
    /// **SERVER** Acknowledges well-formed protocol message in `verbose` mode
    OK,
    /// **SERVER** Indicates a protocol error. May cause client disconnect.
    ERR(ServerError),
}

impl Op {
    /// Transforms the OP into a byte slice
    pub fn into_bytes(self) -> Result<Bytes, CommandError> {
        Ok(match self {
            Op::INFO(si) => si.into_vec()?,
            Op::CONNECT(con) => con.into_vec()?,
            Op::PUB(pc) => pc.into_vec()?,
            Op::SUB(sc) => sc.into_vec()?,
            Op::UNSUB(uc) => uc.into_vec()?,
            Op::MSG(msg) => msg.into_vec()?,
            Op::PING => "PING\r\n".into(),
            Op::PONG => "PONG\r\n".into(),
            Op::OK => "+OK\r\n".into(),
            Op::ERR(se) => format!("-ERR {}\r\n", se).as_bytes().into(),
        })
    }

    /// Tries to parse from a pair of command name and whole buffer
    pub fn from_bytes(buf: Bytes, cmd_idx: usize) -> Result<Self, CommandError> {
        let mut cmd_name = vec![0; cmd_idx];
        cmd_name.copy_from_slice(&buf[..cmd_idx]);

        Ok(match &*cmd_name {
            ServerInfo::CMD_NAME => Op::INFO(ServerInfo::try_parse(buf)?),
            ConnectCommand::CMD_NAME => Op::CONNECT(ConnectCommand::try_parse(buf)?),
            Message::CMD_NAME => Op::MSG(Message::try_parse(buf)?),
            PubCommand::CMD_NAME => Op::PUB(PubCommand::try_parse(buf)?),
            SubCommand::CMD_NAME => Op::SUB(SubCommand::try_parse(buf)?),
            UnsubCommand::CMD_NAME => Op::UNSUB(UnsubCommand::try_parse(buf)?),
            b"PING" => {
                if buf == "PING\r\n" {
                    Op::PING
                } else {
                    return Err(CommandError::IncompleteCommandError);
                }
            }
            b"PONG" => {
                if buf == "PONG\r\n" {
                    Op::PONG
                } else {
                    return Err(CommandError::IncompleteCommandError);
                }
            }
            b"+OK" => {
                if buf == "+OK\r\n" {
                    Op::OK
                } else {
                    return Err(CommandError::IncompleteCommandError);
                }
            }
            b"-ERR" => {
                if &buf[buf.len() - 2..] == b"\r\n" {
                    Op::ERR(ServerError(std::str::from_utf8(&buf[1..])?.into()))
                } else {
                    return Err(CommandError::IncompleteCommandError);
                }
            }
            _ => {
                if buf.len() > 7 {
                    return Err(CommandError::CommandNotFoundOrSupported);
                } else {
                    return Err(CommandError::IncompleteCommandError);
                }
            }
        })
    }

    pub fn command_exists(cmd_name: &[u8]) -> bool {
        match cmd_name {
            ServerInfo::CMD_NAME => true,
            ConnectCommand::CMD_NAME => true,
            Message::CMD_NAME => true,
            PubCommand::CMD_NAME => true,
            SubCommand::CMD_NAME => true,
            UnsubCommand::CMD_NAME => true,
            b"PING" => true,
            b"PONG" => true,
            b"+OK" => true,
            b"-ERR" => true,
            _ => false,
        }
    }
}

// TODO: Write tests
