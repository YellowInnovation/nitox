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
    request_stream: impl Stream<Item = Message, Error = NatsError> + Send + 'static,
) -> impl Future<Item = (), Error = NatsError> {
    tokio::spawn({
        connect_to_nats().map_err(|_| ()).and_then(|client| {
            request_stream
                .for_each(move |msg| {
                    if &msg.payload == "marco" {
                        // b"marco"
                        println!("{:#?}", msg.payload);
                        if let Some(reply_subject) = msg.reply_to {
                            let response = PubCommand::builder()
                                .subject(reply_subject)
                                .payload("polo")
                                .build()
                                .unwrap();
                            futures::future::Either::A(client.publish(response))
                        } else {
                            println!("The received messages is not a request {:?}", msg);
                            futures::future::Either::B(ok(()))
                        }
                    } else {
                        println!("Received {:?} instead of marco", &msg.payload);
                        futures::future::Either::B(ok(()))
                    }
                })
                .map_err(|_| ())
        })
    });
    ok(())
}

fn main() {
    let mut runtime = tokio::runtime::Runtime::new().unwrap();

    let tasks_to_perform = connect_to_nats()
        .and_then(|client| {
            client
                .subscribe(SubCommand::builder().subject("marcopolo").build().unwrap())
                .and_then(|message_stream| handle_request(message_stream).and_then(|_| ok(client)))
        })
        .and_then(|client| {
            client.request("marcopolo".into(), "marco".into()).and_then(|response| {
                // b"polo"
                println!("{:#?}", response.payload);
                ok(())
            })
        });

    let (tx, rx) = futures::sync::oneshot::channel();

    runtime.spawn(tasks_to_perform.then(|_| tx.send(()).map_err(|_| panic!("tx.send() failed!"))));

    // Block until the tasks are complete and tx is sent.
    let _ = rx.wait();
    let _ = runtime.shutdown_now().wait();
}
