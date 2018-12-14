extern crate env_logger;
extern crate futures;
extern crate nitox;
extern crate tokio;
use futures::{future::ok, prelude::*};
use nitox::{commands::*, NatsClient, NatsClientOptions, NatsError};

fn connect_to_nats() -> impl Future<Item = NatsClient, Error = NatsError> {
    let connect_cmd = ConnectCommand::builder().build().unwrap();
    let options = NatsClientOptions::builder()
        .connect_command(connect_cmd)
        .cluster_uri("127.0.0.1:4222")
        .build()
        .unwrap();

    NatsClient::from_options(options)
        .and_then(|client| client.connect())
        .and_then(|client| ok(client))
}

fn handle_request(
    message_stream: impl Stream<Item = Message, Error = NatsError> + Send + 'static,
) -> impl Future<Item = (), Error = NatsError> {
    message_stream.for_each(|msg| {
        println!("Received: {:#?}", msg.payload);
        ok(())
    })
}

fn unsubscribe_after_10_messages(client: &NatsClient, sid: String) -> impl Future<Item = (), Error = NatsError> {
    client.unsubscribe(UnsubCommand {
        sid,
        max_msgs: Some(10),
    })
}

fn get_15_publish_tasks(client: &NatsClient) -> Vec<impl Future<Item = (), Error = NatsError>> {
    let mut fut_vec = vec![];
    for i in 1..=15 {
        let pub_command = PubCommand::builder()
            .subject("messages")
            .payload(format!("message #{}", i))
            .build()
            .unwrap();
        fut_vec.push(client.publish(pub_command).and_then(move |_| {
            println!("Sent message #{}", i);
            ok(())
        }));
    }
    fut_vec
}

fn main() {
    let mut runtime = tokio::runtime::Runtime::new().unwrap();

    let tasks_to_perform = connect_to_nats().and_then(|client| {
        let subscribe_command = SubCommand::builder().subject("messages").build().unwrap();
        let sc_id = subscribe_command.sid.clone();
        // Subscribe to the Messages topic
        client.subscribe(subscribe_command).and_then(move |message_stream| {
            unsubscribe_after_10_messages(&client, sc_id)
                .and_then(move |_| {
                    // The last 5 messages won't be received
                    let fut_vec = get_15_publish_tasks(&client);
                    futures::future::join_all(fut_vec)
                })
                // Map the message stream to the handler
                .and_then(move |_| handle_request(message_stream))
        })
    });

    let (tx, rx) = futures::sync::oneshot::channel();
    runtime.spawn(tasks_to_perform.then(|_| tx.send(()).map_err(|_| panic!("tx.send() failed!"))));

    // Block until the tasks are complete and tx is sent.
    let _ = rx.wait();
    let _ = runtime.shutdown_now().wait();
}
