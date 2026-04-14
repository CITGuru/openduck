//! OpenDuck execution gateway binary — forwards RPCs to workers (see `OPENDUCK_WORKER_ADDRS`).

use std::net::SocketAddr;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt::init();
    let addr: SocketAddr = "0.0.0.0:7878".parse()?;
    let workers = exec_gateway::worker_base_urls();
    println!(
        "openduck-gateway on {addr}; workers={workers:?}; max_in_flight={}",
        exec_gateway::max_in_flight()
    );
    exec_gateway::serve(addr, workers).await?;
    Ok(())
}
