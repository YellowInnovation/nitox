extern crate futures;
extern crate nitox;
extern crate tokio;
use futures::{future::ok, prelude::*};
use nitox::{commands::*, NatsClient, NatsClientOptions, NatsError};
use std::{thread::sleep, time};

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
    tokio::spawn({
        message_stream
            .for_each(move |msg| {
                // b"Hello world!""
                println!("Received message ! {:#?}", msg.payload);
                ok(())
            })
            .map_err(|_| ())
    });
    ok(())
}

fn main() {
    let mut runtime = tokio::runtime::Runtime::new().unwrap();

    let tasks_to_perform = connect_to_nats()
        .and_then(|client| {
            client
                .subscribe(SubCommand::builder().subject("topic").build().unwrap())
                .and_then(|message_stream| handle_request(message_stream).and_then(|_| ok(client)))
        })
        .and_then(|client| {
            let publish_command = PubCommand::builder()
                .subject("topic")
                .payload("Hello world!")
                .build()
                .unwrap();
            client.publish(publish_command).and_then(|_| {
                // Wait a little bit until the receiver logs the messae
                sleep(time::Duration::new(1, 0));
                ok(())
            })
        });

    let (tx, rx) = futures::sync::oneshot::channel();

    runtime.spawn(tasks_to_perform.then(|_| tx.send(()).map_err(|_| panic!("tx.send() failed!"))));

    // Block until the tasks are complete and tx is sent.
    let _ = rx.wait();
    let _ = runtime.shutdown_now().wait();
}
