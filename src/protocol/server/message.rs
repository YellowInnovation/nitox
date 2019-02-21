use crate::protocol::{Command, CommandError};
use bytes::{BufMut, Bytes, BytesMut};

/// The MSG protocol message is used to deliver an application message to the client.
#[derive(Debug, Clone, PartialEq, Builder)]
#[builder(build_fn(validate = "Self::validate"))]
pub struct Message {
    /// Subject name this message was received on
    #[builder(setter(into))]
    pub subject: String,
    /// The unique alphanumeric subscription ID of the subject
    #[builder(setter(into))]
    pub sid: String,
    /// The inbox subject on which the publisher is listening for responses
    #[builder(default)]
    pub reply_to: Option<String>,
    /// The message payload data
    #[builder(setter(into))]
    pub payload: Bytes,
}

impl Message {
    pub fn builder() -> MessageBuilder {
        MessageBuilder::default()
    }
}

impl Command for Message {
    const CMD_NAME: &'static [u8] = b"MSG";

    fn into_vec(self) -> Result<Bytes, CommandError> {
        let rt = if let Some(reply_to) = self.reply_to {
            format!("\t{}", reply_to)
        } else {
            "".into()
        };

        let cmd_str = format!("MSG\t{}\t{}{}\t{}\r\n", self.subject, self.sid, rt, self.payload.len());
        let mut bytes = BytesMut::with_capacity(cmd_str.len() + self.payload.len() + 2);
        bytes.put(cmd_str.as_bytes());
        bytes.put(self.payload);
        bytes.put("\r\n");

        Ok(bytes.freeze())
    }

    fn try_parse(buf: Bytes) -> Result<Self, CommandError> {
        let len = buf.len();

        if buf[len - 2..] != [b'\r', b'\n'] {
            return Err(CommandError::IncompleteCommandError);
        }

        if let Some(payload_start) = buf[..len - 2].iter().position(|b| *b == b'\r') {
            if buf[payload_start + 1] != b'\n' {
                return Err(CommandError::CommandMalformed);
            }

            let payload: Bytes = buf[payload_start + 2..len - 2].into();

            let mut split = buf[..payload_start].split(|c| *c == b' ' || *c == b'\t');
            let cmd = split.next().ok_or_else(|| CommandError::CommandMalformed)?;
            // Check if we're still on the right command
            if cmd != Self::CMD_NAME {
                return Err(CommandError::CommandMalformed);
            }

            let payload_len: usize =
                std::str::from_utf8(split.next_back().ok_or_else(|| CommandError::CommandMalformed)?)?.parse()?;

            if payload.len() != payload_len {
                return Err(CommandError::CommandMalformed);
            }

            // Extract subject
            let subject: String =
                std::str::from_utf8(split.next().ok_or_else(|| CommandError::CommandMalformed)?)?.into();

            let sid: String = std::str::from_utf8(split.next().ok_or_else(|| CommandError::CommandMalformed)?)?.into();

            let reply_to: Option<String> = match split.next() {
                Some(v) => Some(std::str::from_utf8(v)?.into()),
                _ => None,
            };

            Ok(Message {
                subject,
                sid,
                payload,
                reply_to,
            })
        } else {
            Err(CommandError::CommandMalformed)
        }
    }
}

impl MessageBuilder {
    fn validate(&self) -> Result<(), String> {
        if let Some(ref subj) = self.subject {
            check_cmd_arg!(subj, "subject");
        }

        if let Some(ref reply_to_maybe) = self.reply_to {
            if let Some(ref reply_to) = reply_to_maybe {
                check_cmd_arg!(reply_to, "inbox");
            }
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::{Message, MessageBuilder};
    use crate::protocol::Command;

    static DEFAULT_MSG: &'static str = "MSG\tFOO\tpouet\t4\r\ntoto\r\n";

    #[test]
    fn it_parses() {
        let parse_res = Message::try_parse(DEFAULT_MSG.into());
        assert!(parse_res.is_ok());
        let cmd = parse_res.unwrap();
        assert!(cmd.reply_to.is_none());
        assert_eq!(cmd.subject, "FOO");
        assert_eq!(cmd.sid, "pouet");
        assert_eq!(cmd.payload, "toto");
    }

    #[test]
    fn it_stringifies() {
        let cmd = MessageBuilder::default()
            .subject("FOO")
            .sid("pouet")
            .payload("toto")
            .build()
            .unwrap();

        let cmd_bytes_res = cmd.into_vec();
        assert!(cmd_bytes_res.is_ok());
        let cmd_bytes = cmd_bytes_res.unwrap();

        assert_eq!(DEFAULT_MSG, cmd_bytes);
    }
}
