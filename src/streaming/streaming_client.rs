use bytes::{Bytes, BytesMut};
use futures::{
    future::{self, Either},
    prelude::*,
    sync::oneshot,
};
use parking_lot::RwLock;
use prost::Message;
use rand::Rng;
use std::collections::HashMap;
use std::sync::Arc;

use crate::{
    NatsError,
    client::NatsClient,
    protocol::commands,
    streaming::{
        client::{StreamingMessage, StreamingSubscription, StreamingSubscriptionSettings},
        error::NatsStreamingError,
        streaming_protocol as streaming,
    },
};

static DISCOVER_PREFIX: &'static str = "_STAN.discover";
static ACK_PREFIX: &'static str = "_NITOX.acks";

#[derive(Debug, Clone, Default)]
pub(crate) struct NatsStreamingClientConfiguration {
    pub(crate) pub_prefix: String,
    pub(crate) sub_requests: String,
    pub(crate) unsub_requests: String,
    pub(crate) close_requests: String,
    pub(crate) sub_close_requests: String,
    pub(crate) ping_requests: String,
    pub(crate) hb_subject: String,
    pub(crate) ack_subject: String,
}

impl NatsStreamingClientConfiguration {
    pub(crate) fn assign_from_connect_resp(&mut self, resp: streaming::ConnectResponse) {
        self.pub_prefix = resp.pub_prefix;
        self.sub_requests = resp.sub_requests;
        self.unsub_requests = resp.unsub_requests;
        self.close_requests = resp.close_requests;
        self.sub_close_requests = resp.sub_close_requests;
        self.ping_requests = resp.ping_requests;
    }
}

#[derive(Debug, Clone, Default, Builder)]
#[builder(default)]
pub struct SubscribeOptions {
    queue_group: Option<String>,
    #[builder(default = "16384")]
    max_in_flight: i32,
    #[builder(default = "30000")]
    ack_max_wait_in_secs: i32,
    ack_mode: SubscriptionAckMode,
    durable_name: Option<String>,
    start_position: streaming::StartPosition,
    start_sequence: Option<u64>,
    start_sequence_delta: Option<i64>,
}

/// Control whether a subscription will automatically or manually ack received messages.
///
/// By default, subscriptions will be setup to automatically ack messages which are received on
/// the stream. Use `Manual` mode to control the acks yourself.
#[derive(Debug, Clone)]
pub enum SubscriptionAckMode {
    Auto,
    Manual,
}

impl Default for SubscriptionAckMode {
    fn default() -> Self {
        SubscriptionAckMode::Auto
    }
}

#[derive(Debug)]
pub struct NatsStreamingClient {
    pub(crate) nats: Arc<NatsClient>,
    ack: Arc<RwLock<HashMap<String, oneshot::Sender<Option<String>>>>>,
    pub(crate) client_id: String,
    cluster_id: Option<String>,
    pub(crate) config: Arc<RwLock<NatsStreamingClientConfiguration>>,
}

impl From<NatsClient> for NatsStreamingClient {
    fn from(client: NatsClient) -> Self {
        NatsStreamingClient {
            nats: Arc::new(client),
            ack: Arc::new(RwLock::new(HashMap::new())),
            client_id: format!("nitox_streaming_{}", Self::generate_guid()),
            cluster_id: None,
            config: Arc::new(RwLock::new(NatsStreamingClientConfiguration {
                hb_subject: Self::generate_guid(),
                ack_subject: format!("{}.{}", ACK_PREFIX, Self::generate_guid()),
                ..Default::default()
            })),
        }
    }
}

impl NatsStreamingClient {
    pub(crate) fn encode_message<T: Message>(msg: T) -> Result<Bytes, NatsStreamingError> {
        let mut buf = BytesMut::with_capacity(msg.encoded_len());
        msg.encode(&mut buf)?;
        Ok(buf.freeze())
    }

    pub(crate) fn generate_guid() -> String {
        let mut rng = rand::thread_rng();
        rng.sample_iter(&rand::distributions::Alphanumeric).take(16).collect()
    }

    pub fn try_eject_streaming(mut self) -> Result<NatsClient, Self> {
        match Arc::try_unwrap(self.nats) {
            Ok(nats) => Ok(nats),
            Err(nats_arc) => {
                self.nats = nats_arc;
                Err(self)
            }
        }
    }

    pub fn cluster_id(mut self, cluster_id: String) -> Self {
        self.cluster_id = Some(cluster_id);
        self
    }

    fn setup_hb(&self) {
        let nats_hb = Arc::clone(&self.nats);
        tokio_executor::spawn(
            self.nats
                .subscribe(
                    commands::SubCommand::builder()
                        .subject((*self.config.read()).hb_subject.clone())
                        .build()
                        .unwrap(),
                ).from_err()
                .and_then(|hb_stream| {
                    hb_stream
                        .for_each(move |msg| {
                            if let Some(reply) = msg.reply_to {
                                Either::A(
                                    nats_hb.publish(commands::PubCommand::builder().subject(reply).build().unwrap()),
                                )
                            } else {
                                Either::B(future::err(NatsError::ServerDisconnected(None)))
                            }
                        }).into_future()
                }).map(|_| ())
                .map_err(|_| ()),
        );
    }

    fn setup_ack(&self) {
        let ack_arc = Arc::clone(&self.ack);
        tokio_executor::spawn(
            self.nats
                .subscribe(
                    commands::SubCommand::builder()
                        .subject((*self.config.read()).ack_subject.clone())
                        .build()
                        .unwrap(),
                ).from_err()
                .and_then(|ack_stream| {
                    ack_stream
                        .from_err()
                        .for_each(move |msg| match streaming::PubAck::decode(msg.payload) {
                            Ok(ack) => {
                                if let Some(inner_ack) = (*ack_arc.write()).remove(&ack.guid) {
                                    let err = if ack.error.len() > 0 {
                                        Some(ack.error.clone())
                                    } else {
                                        None
                                    };

                                    future::result(
                                        inner_ack.send(err).map_err(|_| NatsStreamingError::CannotAck(ack.guid)),
                                    )
                                } else {
                                    future::ok(())
                                }
                            }
                            Err(decode_err) => future::err(decode_err.into()),
                        }).into_future()
                }).map(|_| ())
                .map_err(|_| ()),
        );
    }

    pub fn connect(self) -> impl Future<Item = Self, Error = NatsStreamingError> {
        if self.cluster_id.is_none() {
            return Either::A(future::err(NatsStreamingError::MissingClusterId));
        }

        self.setup_hb();
        self.setup_ack();

        let connect_buf = match Self::encode_message(streaming::ConnectRequest {
            client_id: self.client_id.clone(),
            heartbeat_inbox: (*self.config.read()).hb_subject.clone(),
            ..Default::default()
        }) {
            Ok(buf) => buf,
            Err(e) => {
                return Either::A(future::err(e.into()));
            }
        };

        Either::B(
            self.nats
                .request(
                    format!("{}.{}", DISCOVER_PREFIX, self.cluster_id.clone().unwrap()),
                    connect_buf,
                ).from_err()
                .and_then(move |msg| {
                    future::result(streaming::ConnectResponse::decode(&msg.payload).map_err(|e| e.into()))
                }).map(move |resp| {
                    (*self.config.write()).assign_from_connect_resp(resp);

                    self
                }),
        )
    }

    pub fn publish(&self, subject: String, payload: Bytes) -> impl Future<Item = (), Error = NatsStreamingError> {
        let internal_subject = format!("{}.{}", (*self.config.read()).pub_prefix, subject);
        let guid = Self::generate_guid();

        let pub_buf = match Self::encode_message(streaming::PubMsg {
            client_id: self.client_id.clone(),
            guid: guid.clone(),
            subject,
            data: payload.to_vec(),
            ..Default::default()
        }) {
            Ok(buf) => buf,
            Err(e) => {
                return Either::A(future::err(e.into()));
            }
        };

        let (tx, rx) = oneshot::channel();

        (*self.ack.write()).insert(guid, tx);

        let pub_cmd = commands::PubCommand::builder()
            .subject(internal_subject)
            .payload(pub_buf)
            .reply_to(Some((*self.config.read()).ack_subject.clone()))
            .build()
            .unwrap();

        Either::B(
            self.nats
                .publish(pub_cmd)
                .and_then(|_| rx.into_future().map_err(|_| NatsError::InnerBrokenChain))
                .from_err()
                .and_then(|maybe_err| {
                    if let Some(err) = maybe_err {
                        future::err(NatsStreamingError::CannotAck(err))
                    } else {
                        future::ok(())
                    }
                }),
        )
    }

    pub fn subscribe(&self, subject: String, mut opts: SubscribeOptions)
        -> impl Future<Item = StreamingSubscription, Error = NatsStreamingError>
    {
        let sub_inbox = Self::generate_guid();
        let mut sub_request = streaming::SubscriptionRequest {
            client_id: self.client_id.clone(),
            subject: subject.clone(),
            inbox: sub_inbox.clone(),
            max_in_flight: opts.max_in_flight,
            ack_wait_in_secs: opts.ack_max_wait_in_secs,
            start_position: opts.start_position as i32,
            ..Default::default()
        };

        if let Some(qgroup) = opts.queue_group.take() {
            sub_request.q_group = qgroup;
        }

        if let Some(durable_name) = opts.durable_name.take() {
            sub_request.durable_name = durable_name;
        }

        match opts.start_position {
            streaming::StartPosition::TimeDeltaStart => {
                if let Some(start_sequence_delta) = opts.start_sequence_delta.take() {
                    sub_request.start_time_delta = start_sequence_delta;
                }
            }
            streaming::StartPosition::SequenceStart => {
                if let Some(start_sequence) = opts.start_sequence.take() {
                    sub_request.start_sequence = start_sequence;
                }
            }
            _ => (),
        }

        let sub_request_buf = match Self::encode_message(sub_request) {
            Ok(buf) => buf,
            Err(e) => {
                return Either::A(future::err(e.into()));
            }
        };

        let sub_command = commands::SubCommand::builder().subject(sub_inbox).build().unwrap();

        let sub_sid = sub_command.sid.clone();

        let nats = Arc::clone(&self.nats);
        let nats_ack = Arc::clone(&self.nats);
        let sub_requests = self.config.read().sub_requests.clone();
        let client_id = self.client_id.clone();
        let sub_config = Arc::clone(&self.config);
        let ack_mode = opts.ack_mode.clone();

        Either::B(self.nats.subscribe(sub_command).from_err().and_then(move |sub_stream| {
            nats.request(sub_requests, sub_request_buf)
                .from_err()
                .and_then(|msg| {
                    future::result(streaming::SubscriptionResponse::decode(&msg.payload).map_err(|e| e.into()))
                }).and_then(move |resp| {

                    // Setup sink for decoding received messages & auto acking if needed.
                    let (tx, rx) = futures::sync::mpsc::unbounded();
                    let ack_inbox_autoack = resp.ack_inbox.clone();
                    tokio_executor::spawn(sub_stream
                        .map_err(|e| NatsStreamingError::from(e))
                        .and_then(move |msg| {
                            let msg_pbuf = match streaming::MsgProto::decode(&msg.payload) {
                                Ok(msg) => msg,
                                Err(e) => {
                                    return Err(NatsStreamingError::from(e));
                                }
                            };

                            let sub_request_buf = match Self::encode_message(streaming::Ack {
                                subject: msg_pbuf.subject.clone(),
                                sequence: msg_pbuf.sequence.clone(),
                                ..Default::default()
                            }) {
                                Ok(buf) => buf,
                                Err(e) => {
                                    return Err(NatsStreamingError::from(e));
                                }
                            };

                            let ack_pub_msg = commands::PubCommand::builder()
                                .subject(ack_inbox_autoack.clone())
                                .payload(sub_request_buf)
                                .build()
                                .unwrap();

                            Ok((StreamingMessage::new(msg_pbuf, Some((nats_ack.clone(), ack_pub_msg))), ack_mode.clone()))
                        })
                        .and_then(|(mut stream_msg, ack_mode)| match ack_mode {
                            SubscriptionAckMode::Auto => Either::A(stream_msg.ack().map(move |_| stream_msg)),
                            SubscriptionAckMode::Manual => Either::B(future::ok(()).map(move |_| stream_msg)),
                        })
                        .forward(tx).map(|_| ()).map_err(|_| ())
                    );

                    let settings = StreamingSubscriptionSettings::builder()
                        .sid(sub_sid)
                        .subject(subject)
                        .ack_inbox(resp.ack_inbox)
                        .client_id(client_id)
                        .build()
                        .unwrap();
                    future::ok(StreamingSubscription::new(Arc::clone(&nats), sub_config, rx, settings))
                })
        }))
    }

    /*pub fn close(self) -> impl Future<Item = NatsClient, Error = NatsStreamingError> {

    }*/
}
