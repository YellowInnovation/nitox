use std::fmt;

/// The -ERR message is used by the server indicate a protocol, authorization, or other runtime
/// connection error to the client. Most of these errors result in the server closing the connection.
///
/// Handling of these errors usually has to be done asynchronously.
#[derive(Debug, PartialEq, Clone)]
pub struct ServerError(pub String);

impl fmt::Display for ServerError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.debug_tuple("ServerError").field(&self.0).finish()
    }
}
