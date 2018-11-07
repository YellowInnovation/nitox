use futures::{future, prelude::*, sync::mpsc};
use parking_lot::RwLock;
use std::{collections::HashMap, sync::Arc};
use tokio_executor;

use super::NatsStream;
use super::NatsSubscriptionId;
use error::NatsError;
use protocol::{commands::*, Op};

#[derive(Debug)]
pub(crate) struct SubscriptionSink {
    tx: mpsc::UnboundedSender<Message>,
    pub(crate) max_count: Option<u32>,
    pub(crate) count: u32,
}

/// Internal multiplexer for incoming streams and subscriptions. Quite a piece of code, with almost no overhead yay
#[derive(Debug)]
pub(crate) struct NatsClientMultiplexer {
    pub(crate) other_tx: Arc<mpsc::UnboundedSender<Op>>,
    pub(crate) subs_tx: Arc<RwLock<HashMap<NatsSubscriptionId, SubscriptionSink>>>,
}

impl NatsClientMultiplexer {
    pub fn new(stream: NatsStream) -> (Self, mpsc::UnboundedReceiver<Op>) {
        let subs_tx: Arc<RwLock<HashMap<NatsSubscriptionId, SubscriptionSink>>> =
            Arc::new(RwLock::new(HashMap::default()));

        let (other_tx, other_rx) = mpsc::unbounded();
        let other_tx = Arc::new(other_tx);

        let stx_inner = Arc::clone(&subs_tx);
        let otx_inner = Arc::clone(&other_tx);

        // Here we filter the incoming TCP stream Messages by subscription ID and sending it to the appropriate Sender
        let work_tx = stream
            .for_each(move |op| {
                match op {
                    Op::MSG(msg) => {
                        debug!(target: "nitox", "Found MSG from global Stream {:?}", msg);
                        if let Some(s) = (*stx_inner.read()).get(&msg.sid) {
                            debug!(target: "nitox", "Found multiplexed receiver to send to {}", msg.sid);
                            let _ = s.tx.unbounded_send(msg);
                        }
                    }
                    // Forward the rest of the messages to the owning client
                    op => {
                        debug!(target: "nitox", "Sending OP to the rest of the queue: {:?}", op);
                        let _ = otx_inner.unbounded_send(op);
                    }
                }

                future::ok::<(), NatsError>(())
            }).map(|_| ())
            .map_err(|_| ());

        tokio_executor::spawn(work_tx);

        (NatsClientMultiplexer { subs_tx, other_tx }, other_rx)
    }

    pub fn for_sid(&self, sid: NatsSubscriptionId) -> impl Stream<Item = Message, Error = NatsError> + Send + Sync {
        let (tx, rx) = mpsc::unbounded();
        (*self.subs_tx.write()).insert(
            sid,
            SubscriptionSink {
                tx,
                max_count: None,
                count: 0,
            },
        );

        rx.map_err(|_| NatsError::InnerBrokenChain)
    }

    pub fn remove_sid(&self, sid: &str) {
        (*self.subs_tx.write()).remove(sid);
    }
}
