use bytes::Bytes;

use futures::{
    future::{self, Either},
    prelude::*,
    stream,
    sync::mpsc,
    Future,
};
use hashbrown::HashMap;
use parking_lot::RwLock;
use std::{
    net::{SocketAddr, ToSocketAddrs},
    str::FromStr,
    sync::Arc,
};
use tokio_executor;
use url::Url;

use error::NatsError;
use net::*;
use protocol::{commands::*, Op};

/// Sink (write) part of a TCP stream
type NatsSink = stream::SplitSink<NatsConnection>;
/// Stream (read) part of a TCP stream
type NatsStream = stream::SplitStream<NatsConnection>;
/// Useless pretty much, just for code semantics
type NatsSubscriptionId = String;

/// Keep-alive for the sink, also supposed to take care of handling verbose messaging, but can't for now
#[derive(Clone, Debug)]
struct NatsClientSender {
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

#[derive(Debug)]
struct SubscriptionSink {
    tx: mpsc::UnboundedSender<Message>,
    max_count: Option<u32>,
    count: u32,
}

/// Internal multiplexer for incoming streams and subscriptions. Quite a piece of code, with almost no overhead yay
#[derive(Debug)]
struct NatsClientMultiplexer {
    other_tx: Arc<mpsc::UnboundedSender<Op>>,
    subs_tx: Arc<RwLock<HashMap<NatsSubscriptionId, SubscriptionSink>>>,
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
            })
            .map(|_| ())
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
        if let Some(mut tx) = (*self.subs_tx.write()).remove(sid) {
            let _ = tx.tx.close();
            drop(tx);
        }
    }
}

/// Options that are to be given to the client for initialization
#[derive(Debug, Default, Clone, Builder)]
#[builder(setter(into))]
pub struct NatsClientOptions {
    /// CONNECT command that will be sent upon calling the `connect()` method
    pub connect_command: ConnectCommand,
    /// Cluster URI in the IP:PORT format
    pub cluster_uri: String,
}

impl NatsClientOptions {
    pub fn builder() -> NatsClientOptionsBuilder {
        NatsClientOptionsBuilder::default()
    }
}

/// The NATS Client. What you'll be using mostly. All the async handling is made internally except for
/// the system messages that are forwarded on the `Stream` that the client implements
pub struct NatsClient {
    /// Backup of options
    opts: NatsClientOptions,
    /// Server info
    server_info: Arc<RwLock<Option<ServerInfo>>>,
    /// Stream of the messages that are not caught for subscriptions (only system messages like PING/PONG should be here)
    other_rx: Box<dyn Stream<Item = Op, Error = NatsError> + Send + Sync>,
    /// Sink part to send commands
    tx: NatsClientSender,
    /// Subscription multiplexer
    rx: Arc<NatsClientMultiplexer>,
}

impl ::std::fmt::Debug for NatsClient {
    fn fmt(&self, f: &mut ::std::fmt::Formatter) -> ::std::fmt::Result {
        f.debug_struct("NatsClient")
            .field("opts", &self.opts)
            .field("tx", &self.tx)
            .field("rx", &self.rx)
            .field("other_rx", &"Box<Stream>...")
            .finish()
    }
}

impl Stream for NatsClient {
    type Error = NatsError;
    type Item = Op;

    fn poll(&mut self) -> Result<Async<Option<Self::Item>>, Self::Error> {
        self.other_rx.poll().map_err(|_| NatsError::InnerBrokenChain)
    }
}

impl NatsClient {
    /// Creates a client and initiates a connection to the server
    ///
    /// Returns `impl Future<Item = Self, Error = NatsError>`
    pub fn from_options(opts: NatsClientOptions) -> impl Future<Item = Self, Error = NatsError> + Send + Sync {
        let tls_required = opts.connect_command.tls_required;

        let cluster_uri = opts.cluster_uri.clone();
        let cluster_sa = if let Ok(sockaddr) = SocketAddr::from_str(&cluster_uri) {
            Ok(sockaddr)
        } else {
            match cluster_uri.to_socket_addrs() {
                Ok(mut ips_iter) => ips_iter.next().ok_or(NatsError::UriDNSResolveError(None)),
                Err(e) => Err(NatsError::UriDNSResolveError(Some(e))),
            }
        };

        future::result(cluster_sa)
            .from_err()
            .and_then(move |cluster_sa| {
                if tls_required {
                    match Url::parse(&cluster_uri) {
                        Ok(url) => match url.host_str() {
                            Some(host) => future::ok(Either::B(connect_tls(host.to_string(), cluster_sa))),
                            None => future::err(NatsError::TlsHostMissingError),
                        },
                        Err(e) => future::err(e.into()),
                    }
                } else {
                    future::ok(Either::A(connect(cluster_sa)))
                }
            })
            .and_then(|either| either)
            .and_then(move |connection| {
                let (sink, stream): (NatsSink, NatsStream) = connection.split();
                let (rx, other_rx) = NatsClientMultiplexer::new(stream);
                let tx = NatsClientSender::new(sink);

                let (tmp_other_tx, tmp_other_rx) = mpsc::unbounded();
                let tx_inner = tx.clone();
                let client = NatsClient {
                    tx,
                    server_info: Arc::new(RwLock::new(None)),
                    other_rx: Box::new(tmp_other_rx.map_err(|_| NatsError::InnerBrokenChain)),
                    rx: Arc::new(rx),
                    opts,
                };

                let server_info_arc = Arc::clone(&client.server_info);

                tokio_executor::spawn(
                    other_rx
                        .for_each(move |op| {
                            match op {
                                Op::PING => {
                                    tokio_executor::spawn(tx_inner.send(Op::PONG).map_err(|_| ()));
                                    let _ = tmp_other_tx.unbounded_send(op);
                                }
                                Op::INFO(server_info) => {
                                    *server_info_arc.write() = Some(server_info);
                                }
                                op => {
                                    let _ = tmp_other_tx.unbounded_send(op);
                                }
                            }

                            future::ok(())
                        })
                        .into_future()
                        .map_err(|_| ()),
                );

                future::ok(client)
            })
    }

    /// Sends the CONNECT command to the server to setup connection
    ///
    /// Returns `impl Future<Item = Self, Error = NatsError>`
    pub fn connect(self) -> impl Future<Item = Self, Error = NatsError> + Send + Sync {
        self.tx
            .send(Op::CONNECT(self.opts.connect_command.clone()))
            .and_then(move |_| future::ok(self))
    }

    /// Send a raw command to the server
    ///
    /// Returns `impl Future<Item = Self, Error = NatsError>`
    #[deprecated(
        since = "0.1.4",
        note = "Using this method prevents the library to track what you are sending to the server and causes memory leaks in case of subscriptions/unsubs, it'll be fully removed in v0.2.0"
    )]
    pub fn send(self, op: Op) -> impl Future<Item = Self, Error = NatsError> {
        self.tx.send(op).and_then(move |_| future::ok(self))
    }

    /// Send a PUB command to the server
    ///
    /// Returns `impl Future<Item = (), Error = NatsError>`
    pub fn publish(&self, cmd: PubCommand) -> impl Future<Item = (), Error = NatsError> + Send + Sync {
        if let Some(ref server_info) = *self.server_info.read() {
            if cmd.payload.len() > server_info.max_payload as usize {
                return Either::A(future::err(NatsError::MaxPayloadOverflow(server_info.max_payload)));
            }
        }

        Either::B(self.tx.send(Op::PUB(cmd)))
    }

    /// Send a UNSUB command to the server and de-register stream in the multiplexer
    ///
    /// Returns `impl Future<Item = (), Error = NatsError>`
    pub fn unsubscribe(&self, cmd: UnsubCommand) -> impl Future<Item = (), Error = NatsError> + Send + Sync {
        let mut unsub_now = true;
        if let Some(max) = cmd.max_msgs {
            if let Some(mut s) = (*self.rx.subs_tx.write()).get_mut(&cmd.sid) {
                s.max_count = Some(max);
                unsub_now = false;
            }
        }

        let sid = cmd.sid.clone();
        let rx_arc = Arc::clone(&self.rx);

        self.tx.send(Op::UNSUB(cmd)).and_then(move |_| {
            if unsub_now {
                rx_arc.remove_sid(&sid);
            }

            future::ok(())
        })
    }

    /// Send a SUB command and register subscription stream in the multiplexer and return that `Stream` in a future
    ///
    /// Returns `impl Future<Item = impl Stream<Item = Message, Error = NatsError>>`
    pub fn subscribe(
        &self,
        cmd: SubCommand,
    ) -> impl Future<Item = impl Stream<Item = Message, Error = NatsError> + Send + Sync, Error = NatsError> + Send + Sync
    {
        let inner_rx = self.rx.clone();
        let sid = cmd.sid.clone();
        self.tx.send(Op::SUB(cmd)).and_then(move |_| {
            let stream = inner_rx.for_sid(sid.clone()).and_then(move |msg| {
                {
                    let mut stx = inner_rx.subs_tx.write();
                    let mut delete = None;
                    debug!(target: "nitox", "Retrieving sink for sid {:?}", sid);
                    if let Some(s) = stx.get_mut(&sid) {
                        debug!(target: "nitox", "Checking if count exists");
                        if let Some(max_count) = s.max_count {
                            s.count += 1;
                            debug!(target: "nitox", "Max: {} / current: {}", max_count, s.count);
                            if s.count >= max_count {
                                debug!(target: "nitox", "Starting deletion");
                                delete = Some(max_count);
                            }
                        }
                    }

                    if let Some(count) = delete.take() {
                        debug!(target: "nitox", "Deleted stream for sid {} at count {}", sid, count);
                        stx.remove(&sid);
                    }
                }

                Ok(msg)
            });

            future::ok(stream)
        })
    }

    /// Performs a request to the server following the Request/Reply pattern. Returns a future containing the MSG that will be replied at some point by a third party
    ///
    /// Returns `impl Future<Item = Message, Error = NatsError>`
    fn internal_request(
        &self,
        subject: String,
        payload: Bytes,
        mut timeout: Option<std::time::Duration>,
    ) -> impl Future<Item = Message, Error = NatsError> + Send + Sync {
        if let Some(ref server_info) = *self.server_info.read() {
            if payload.len() > server_info.max_payload as usize {
                return Either::B(future::err(NatsError::MaxPayloadOverflow(server_info.max_payload)));
            }
        }

        let inbox = PubCommand::generate_reply_to();
        let pub_cmd = PubCommand {
            subject,
            payload,
            reply_to: Some(inbox.clone()),
        };

        let sub_cmd = SubCommand {
            queue_group: None,
            sid: SubCommand::generate_sid(),
            subject: inbox,
        };

        let sid_ok = sub_cmd.sid.clone();
        let sid_err = sub_cmd.sid.clone();

        let unsub_cmd = UnsubCommand {
            sid: sub_cmd.sid.clone(),
            max_msgs: Some(1),
        };

        let tx1 = self.tx.clone();
        let tx2 = self.tx.clone();
        let rx_arc = Arc::clone(&self.rx);
        let rx_arc_timeout = Arc::clone(&self.rx);

        let stream = self
            .rx
            .for_sid(sid_ok.clone())
            .inspect(|msg| debug!(target: "nitox", "Request saw msg in multiplexed stream {:#?}", msg))
            .take(1)
            .into_future()
            // This unwrap is safe because we take only one message from the stream which means
            // we'll always have one and only one message there
            .map(|(surely_message, _)| surely_message.unwrap())
            .map_err(|(e, _)| e)
            .and_then(move |msg| {
                rx_arc.remove_sid(&sid_ok);
                future::ok(msg)
            });

        let fut = self
            .tx
            .send(Op::SUB(sub_cmd))
            .and_then(move |_| tx1.send(Op::UNSUB(unsub_cmd)))
            .and_then(move |_| tx2.send(Op::PUB(pub_cmd)))
            .and_then(move |_| stream)
            .or_else(move |e| {
                rx_arc_timeout.remove_sid(&sid_err);
                future::err(e)
            });

        if let Some(timeout) = timeout.take() {
            Either::A(Either::B(tokio_timer::Timeout::new(fut, timeout).from_err()))
        } else {
            Either::A(Either::A(fut))
        }
    }

    /// Performs a request to the server following the Request/Reply pattern. Returns a future containing the MSG that will be replied at some point by a third party
    ///
    /// Returns `impl Future<Item = Message, Error = NatsError>`
    pub fn request(
        &self,
        subject: String,
        payload: Bytes,
    ) -> impl Future<Item = Message, Error = NatsError> + Send + Sync {
        self.internal_request(subject, payload, None)
    }

    /// Performs a request to the server following the Request/Reply pattern. Returns a future containing the MSG that will be replied at some point by a third party
    /// Requires a timeout after which the request will be canceled and the `Future` will resolve
    /// Returns `impl Future<Item = Message, Error = NatsError>`
    pub fn request_with_timeout(
        &self,
        subject: String,
        payload: Bytes,
        timeout: std::time::Duration,
    ) -> impl Future<Item = Message, Error = NatsError> + Send + Sync {
        self.internal_request(subject, payload, Some(timeout))
    }
}
