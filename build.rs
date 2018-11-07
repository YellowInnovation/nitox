#[cfg(feature = "nats-streaming")]
extern crate prost_build;

fn main() -> Result<(), std::io::Error> {
    if cfg!(feature = "nats-streaming") == false {
        return Ok(());
    }

    prost_build::compile_protos(&["src/streaming/protocol/nats-streaming.proto"], &["src/"])?;

    println!("Protobuf translation done.");
    Ok(())
}
