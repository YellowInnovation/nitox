use crate::protocol::{commands::SubCommand, Command, CommandError};
use bytes::{BufMut, Bytes, BytesMut};

/// UNSUB unsubcribes the connection from the specified subject, or auto-unsubscribes after the
/// specified number of messages has been received.
#[derive(Debug, Clone, PartialEq, Builder)]
pub struct UnsubCommand {
    /// The unique alphanumeric subscription ID of the subject to unsubscribe from
    #[builder(setter(into))]
    pub sid: String,
    /// An optional number of messages to wait for before automatically unsubscribing
    #[builder(default)]
    pub max_msgs: Option<u32>,
}

impl UnsubCommand {
    pub fn builder() -> UnsubCommandBuilder {
        UnsubCommandBuilder::default()
    }
}

impl From<SubCommand> for UnsubCommand {
    fn from(cmd: SubCommand) -> Self {
        UnsubCommand {
            sid: cmd.sid,
            max_msgs: None,
        }
    }
}

impl Command for UnsubCommand {
    const CMD_NAME: &'static [u8] = b"UNSUB";

    fn into_vec(self) -> Result<Bytes, CommandError> {
        // Computes the string length of the payload_len by dividing the number by ln(10)
        let (mm_len, mm) = self.max_msgs.map_or((0, 0), |mm| {
            (((mm + 1) as f64 / std::f64::consts::LN_10).ceil() as usize, mm)
        });

        let len = 8 + self.sid.len() + mm_len;

        let mut bytes = BytesMut::with_capacity(len);
        bytes.put("UNSUB\t");
        bytes.put(self.sid);
        if mm_len > 0 {
            bytes.put(b'\t');
            bytes.put(mm.to_string());
        }
        bytes.put("\r\n");

        Ok(bytes.freeze())
    }

    fn try_parse(buf: Bytes) -> Result<Self, CommandError> {
        let len = buf.len();

        if buf[len - 2..] != [b'\r', b'\n'] {
            return Err(CommandError::IncompleteCommandError);
        }

        let mut split = buf[..len - 2].split(|c| *c == b' ' || *c == b'\t');
        let cmd = split.next().ok_or_else(|| CommandError::CommandMalformed)?;
        // Check if we're still on the right command
        if cmd != Self::CMD_NAME {
            return Err(CommandError::CommandMalformed);
        }

        let sid: String = std::str::from_utf8(split.next().ok_or_else(|| CommandError::CommandMalformed)?)?.into();

        let max_msgs: Option<u32> = match split.next() {
            Some(mm) => Some(std::str::from_utf8(mm)?.parse()?),
            _ => None,
        };

        Ok(UnsubCommand { sid, max_msgs })
    }
}

#[cfg(test)]
mod tests {
    use super::{UnsubCommand, UnsubCommandBuilder};
    use crate::protocol::Command;

    static DEFAULT_UNSUB: &'static str = "UNSUB\tpouet\r\n";

    #[test]
    fn it_parses() {
        let parse_res = UnsubCommand::try_parse(DEFAULT_UNSUB.into());
        assert!(parse_res.is_ok());
        let cmd = parse_res.unwrap();
        assert_eq!(cmd.sid, "pouet");
        assert!(cmd.max_msgs.is_none());
    }

    #[test]
    fn it_stringifies() {
        let cmd = UnsubCommandBuilder::default().sid("pouet").build().unwrap();

        let cmd_bytes_res = cmd.into_vec();
        assert!(cmd_bytes_res.is_ok());
        let cmd_bytes = cmd_bytes_res.unwrap();

        assert_eq!(DEFAULT_UNSUB, cmd_bytes);
    }
}
