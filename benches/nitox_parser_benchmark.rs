#[macro_use]
extern crate criterion;
extern crate bytes;
extern crate nitox;

use criterion::Criterion;
use nitox::commands::*;

fn benchmark_parser(c: &mut Criterion) {
    c.bench_function("connect_parse", |b| {
        let cmd = b"CONNECT\t{\"verbose\":false,\"pedantic\":false,\"tls_required\":false,\"name\":\"nitox\",\"lang\":\"rust\",\"version\":\"1.0.0\"}\r\n";
        b.iter(|| ConnectCommand::try_parse(cmd))
    });

    c.bench_function("connect_write", |b| b.iter(|| ConnectCommand::default().into_vec()));

    c.bench_function("pub_parse", |b| {
        let cmd = b"PUB\tFOO\t11\r\nHello NATS!\r\n";
        b.iter(|| PubCommand::try_parse(cmd))
    });

    c.bench_function("pub_write", |b| {
        b.iter(|| {
            PubCommand {
                subject: String::new(),
                payload: bytes::Bytes::new(),
                reply_to: None,
            }.into_vec()
        })
    });

    c.bench_function("sub_parse", |b| {
        let cmd = b"SUB\tFOO\tpouet\r\n";
        b.iter(|| SubCommand::try_parse(cmd))
    });

    c.bench_function("sub_write", |b| {
        b.iter(|| {
            SubCommand {
                queue_group: None,
                sid: String::new(),
                subject: String::new(),
            }.into_vec()
        })
    });

    c.bench_function("unsub_parse", |b| {
        let cmd = b"UNSUB\tpouet\r\n";
        b.iter(|| UnsubCommand::try_parse(cmd))
    });

    c.bench_function("unsub_write", |b| {
        b.iter(|| {
            UnsubCommand {
                max_msgs: None,
                sid: String::new(),
            }.into_vec()
        })
    });

    c.bench_function("info_parse", |b| {
        let cmd = b"INFO\t{\"server_id\":\"test\",\"version\":\"1.3.0\",\"go\":\"go1.10.3\",\"host\":\"0.0.0.0\",\"port\":4222,\"max_payload\":4000,\"proto\":1,\"client_id\":1337}\r\n";
        b.iter(|| ServerInfo::try_parse(cmd))
    });

    c.bench_function("info_write", |b| {
        b.iter(|| {
            ServerInfo::builder()
                .server_id("")
                .version("")
                .go("")
                .host("")
                .port(0u32)
                .max_payload(0u32)
                .build()
                .unwrap()
                .into_vec()
        })
    });

    c.bench_function("message_parse", |b| {
        let cmd = b"MSG\tFOO\tpouet\t4\r\ntoto\r\n";
        b.iter(|| Message::try_parse(cmd))
    });

    c.bench_function("message_write", |b| {
        b.iter(|| {
            Message {
                subject: String::new(),
                sid: String::new(),
                reply_to: None,
                payload: bytes::Bytes::new(),
            }.into_vec()
        })
    });
}

criterion_group!(benches, benchmark_parser);
criterion_main!(benches);
