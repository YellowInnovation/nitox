use futures::{future::Either, prelude::*, sync::mpsc, Future};
use tokio_executor;

use super::{AckTrigger, NatsSink};
use error::NatsError;
use protocol::Op;

/// Keep-alive for the sink, also takes care of handling verbose signaling
#[derive(Clone, Debug)]
pub(crate) struct NatsClientSender {
    tx: mpsc::UnboundedSender<Op>,
    verbose: bool,
    trigger: AckTrigger,
}

impl NatsClientSender {
    pub fn new(sink: NatsSink, trigger: AckTrigger) -> Self {
        let (tx, rx) = mpsc::unbounded();
        let rx = rx.map_err(|_| NatsError::InnerBrokenChain);
        let work = sink.send_all(rx).map(|_| ()).map_err(|_| ());
        tokio_executor::spawn(work);

        NatsClientSender {
            tx,
            verbose: false,
            trigger,
        }
    }

    pub fn set_verbose(&mut self, verbose: bool) {
        self.verbose = verbose;
    }

    /// Sends an OP to the server
    pub fn send(&self, op: Op) -> impl Future<Item = (), Error = NatsError> {
        debug!(target: "nitox", "Sending OP: {:?}", op);
        debug!(target: "nitox", "Sender is verbose: {}", self.verbose);

        let fut = self
            .tx
            .unbounded_send(op)
            .map_err(|_| NatsError::InnerBrokenChain)
            .into_future();

        if !self.verbose {
            return Either::A(fut);
        }

        debug!(target: "nitox", "Verbose mode is enabled, will try firing trigger");
        let trigger = self.trigger.clone();
        Either::B(fut.and_then(move |_| {
            debug!(target: "nitox", "Command sent, now pulling down and firing trigger");
            trigger.fire()
        }))
    }
}
