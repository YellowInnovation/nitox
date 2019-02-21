use bytes::Bytes;

/// Trait used to implement a common interface for implementing new commands
pub trait Command {
    /// Command name as a static byte slice
    const CMD_NAME: &'static [u8];
    /// Encodes the command into bytes
    fn into_vec(self) -> Result<Bytes, CommandError>;
    /// Tries to parse a buffer into a command
    fn try_parse(buf: Bytes) -> Result<Self, CommandError>
    where
        Self: Sized;
}

pub(crate) fn check_command_arg(s: &str) -> Result<(), ArgumentValidationError> {
    if s.contains(' ') {
        return Err(ArgumentValidationError::ContainsSpace);
    } else if s.contains('\t') {
        return Err(ArgumentValidationError::ContainsTab);
    }

    Ok(())
}

macro_rules! check_cmd_arg {
    ($val:ident, $part:expr) => {
        use crate::protocol::{check_command_arg, ArgumentValidationError};

        match check_command_arg($val) {
            Ok(_) => {}
            Err(ArgumentValidationError::ContainsSpace) => {
                return Err(format!("{} contains spaces", $part).into());
            }
            Err(ArgumentValidationError::ContainsTab) => {
                return Err(format!("{} contains tabs", $part).into());
            }
        }
    };
}

mod error;
pub use self::error::*;

mod client;
mod server;

mod op;
pub use self::op::*;

pub mod commands {
    pub use super::{
        client::{connect::*, pub_cmd::*, sub_cmd::*, unsub_cmd::*},
        server::{info::*, message::*, server_error::ServerError},
    };
    pub use crate::Command;
}

#[cfg(test)]
mod tests {
    use super::check_command_arg;

    #[test]
    #[should_panic]
    fn it_detects_spaces() {
        check_command_arg(&"foo bar").unwrap()
    }

    #[test]
    #[should_panic]
    fn it_detects_tabs() {
        check_command_arg(&"foo\tbar").unwrap()
    }

    #[test]
    fn it_works() {
        check_command_arg(&"foo.bar").unwrap()
    }
}
