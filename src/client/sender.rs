use futures::{prelude::*, sync::mpsc, Future};
use tokio_executor;

use super::NatsSink;
use error::NatsError;
use protocol::Op;

/// Keep-alive for the sink, also supposed to take care of handling verbose messaging, but can't for now
#[derive(Clone, Debug)]
pub(crate) struct NatsClientSender {
    tx: mpsc::UnboundedSender<Op>,
    verbose: bool,
}

impl NatsClientSender {
    pub fn new(sink: NatsSink) -> Self {
        let (tx, rx) = mpsc::unbounded();
        let rx = rx.map_err(|_| NatsError::InnerBrokenChain);
        let work = sink.send_all(rx).map(|_| ()).map_err(|_| ());
        tokio_executor::spawn(work);

        NatsClientSender { tx, verbose: false }
    }

    #[allow(dead_code)]
    pub fn set_verbose(&mut self, verbose: bool) {
        self.verbose = verbose;
    }

    /// Sends an OP to the server
    pub fn send(&self, op: Op) -> impl Future<Item = (), Error = NatsError> {
        //let _verbose = self.verbose.clone();
        self.tx
            .unbounded_send(op)
            .map_err(|_| NatsError::InnerBrokenChain)
            .into_future()
    }
}
