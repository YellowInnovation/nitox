use super::{client::NatsStreamingClientConfiguration, error::NatsStreamingError, streaming_protocol as streaming};
use bytes::BytesMut;
use client::NatsClient;
use futures::{
    future::{self, Either},
    prelude::*,
    sync::mpsc::UnboundedReceiver,
};
use parking_lot::RwLock;
use prost::Message;
use protocol::commands;
use std::sync::Arc;

#[derive(Debug, Clone, Builder, Default)]
pub(crate) struct StreamingSubscriptionSettings {
    sid: String,
    subject: String,
    ack_inbox: String,
    client_id: String,
}

impl StreamingSubscriptionSettings {
    pub fn builder() -> StreamingSubscriptionSettingsBuilder {
        StreamingSubscriptionSettingsBuilder::default()
    }
}

#[derive(Debug)]
pub struct StreamingSubscription {
    nats: Arc<NatsClient>,
    config: Arc<RwLock<NatsStreamingClientConfiguration>>,
    rx: UnboundedReceiver<StreamingMessage>,
    settings: StreamingSubscriptionSettings,
}

impl Stream for StreamingSubscription {
    type Item = StreamingMessage;
    type Error = NatsStreamingError;

    fn poll(&mut self) -> Poll<Option<Self::Item>, Self::Error> {
        self.rx.poll().map_err(|_| NatsStreamingError::SubscriptionError)
    }
}

impl StreamingSubscription {
    pub(crate) fn new(
        nats: Arc<NatsClient>,
        config: Arc<RwLock<NatsStreamingClientConfiguration>>,
        rx: UnboundedReceiver<StreamingMessage>,
        settings: StreamingSubscriptionSettings,
    ) -> Self {
        StreamingSubscription {
            nats,
            config,
            rx,
            settings,
        }
    }

    fn unsub_or_close(self, close: bool) -> impl Future<Item = (), Error = NatsStreamingError> {
        let subject = if close {
            self.config.read().sub_close_requests.clone()
        } else {
            self.config.read().unsub_requests.clone()
        };

        let unsub_cmd = commands::UnsubCommand::builder()
            .sid(self.settings.sid.clone())
            .build()
            .unwrap();

        self.nats
            .unsubscribe(unsub_cmd)
            .from_err()
            .and_then(move |_| {
                let unsub_req = streaming::UnsubscribeRequest {
                    client_id: self.settings.client_id,
                    subject: self.settings.subject,
                    inbox: self.settings.ack_inbox,
                    ..Default::default()
                };

                let mut buf = BytesMut::with_capacity(unsub_req.encoded_len());
                match unsub_req.encode(&mut buf) {
                    Err(encode_err) => {
                        return Either::B(future::err(encode_err.into()));
                    },
                    _ => ()
                }

                Either::A(self.nats.request(subject, buf.freeze()).from_err())
            }).and_then(|msg| {
                future::result(streaming::SubscriptionResponse::decode(&msg.payload).map_err(|e| e.into()))
            }).and_then(|sub_res| {
                if sub_res.error.len() > 0 {
                    future::err(NatsStreamingError::ServerError(sub_res.error))
                } else {
                    future::ok(())
                }
            })
    }

    pub fn unsubscribe(self) -> impl Future<Item = (), Error = NatsStreamingError> {
        self.unsub_or_close(false)
    }

    pub fn close(self) -> impl Future<Item = (), Error = NatsStreamingError> {
        self.unsub_or_close(true)
    }
}

/// A message coming from a subscription stream.
#[derive(Debug)]
pub struct StreamingMessage {
    /// The protobuf message from the Nats stream.
    pub proto: streaming::MsgProto,

    /// The data used for acking this message.
    ack: Option<(Arc<NatsClient>, commands::PubCommand)>,
}

impl StreamingMessage {
    pub fn new(proto: streaming::MsgProto, ack: Option<(Arc<NatsClient>, commands::PubCommand)>) -> Self {
        StreamingMessage{proto, ack}
    }

    /// Ack this message.
    ///
    /// If this message came from a stream configured with `SubscriptionAckMode::Auto`, then this
    /// will be a no-op returning an immediately resolved `future::ok(())`.
    pub fn ack(&mut self) -> impl Future<Item=(), Error=NatsStreamingError> {
        if let Some((client, ack_cmd)) = self.ack.take() {
            Either::A(client.publish(ack_cmd).from_err())
        } else {
            Either::B(future::ok(()))
        }
    }
}
