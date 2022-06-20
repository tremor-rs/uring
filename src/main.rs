use std::{collections::HashSet, env, sync::Arc, time::Duration};

use async_std::{net::TcpListener, task};
use memstore::MemStore;
use network::{RaftRouter, TremorRaft};
use openraft::{declare_raft_types, Config, Raft};
use serde::{Deserialize, Serialize};
// We use anyhow::Result in our impl below.
use anyhow::Result;
use toy_rpc::Server;
use tracing::info;
mod memstore;
mod network;

declare_raft_types!(
    /// Dummy Raft types for the purpose of testing internal structures requiring
    /// `RaftTypeConfig`, like `MembershipConfig`.
    pub(crate) RaftConfig: D = u64, R = u64, NodeId = u64
);
// impl AppData for ClientRequest {}

type ClientError = String;
/// The application data response type which the `MemStore` works with.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ClientResponse(Result<Option<String>, ClientError>);

// impl AppDataResponse for ClientResponse {}

/// A concrete Raft type used during testing.
pub type MemRaft = Raft<memstore::Config, ClientResponse, MemStore>;

#[async_std::main]
async fn main() {
    tracing_subscriber::fmt().init();
    // Get our node's ID from stable storage.
    let node_id = get_id_from_storage().await;

    info!("Starting node {node_id}");
    // Build our Raft runtime config, then instantiate our
    // RaftNetwork & RaftStorage impls.
    let config = Arc::new(
        Config::build("primary-raft-group".into())
            .validate()
            .expect("failed to build Raft config"),
    );
    let network = Arc::new(RaftRouter::new(config.clone()).await);
    let storage = Arc::new(MemStore::new());

    // Create a new Raft node, which spawns an async task which
    // runs the Raft core logic. Keep this Raft instance around
    // for calling API methods based on events in your app.
    let raft = Raft::new(node_id, config, network, storage);

    run_app(node_id, raft).await;
}

async fn get_id_from_storage() -> u64 {
    env::args()
        .skip(1)
        .map(|a| a.parse::<u64>().expect("bad argument"))
        .next()
        .expect("bad argumet")
}

async fn run_app(node_id: u64, raft: MemRaft) {
    let addr = format!("127.0.0.1:{}", 10000 + node_id);
    info!("Node address: {addr}");

    let mut members = HashSet::new();

    let nodes: Vec<_> = env::args()
        .skip(1)
        .map(|a| a.parse::<u64>().expect("bad argument"))
        .collect();
    for nid in nodes {
        members.insert(nid);
    }

    info!("Peers: {members:?}");
    info!("delay startup ...");
    async_std::task::sleep(Duration::from_secs(5)).await;
    info!("... Starting");
    raft.initialize(members)
        .await
        .expect("oh no initialize failed");

    let mut metrics = raft.metrics();
    task::spawn(async move {
        loop {
            metrics.changed().await.unwrap();
            info!("Metrics: {:?}", metrics.borrow());
        }
    });
    let raft = TremorRaft::new(raft);
    let raft_server = Arc::new(raft);

    let server = Server::builder().register(raft_server).build();
    let listener = TcpListener::bind(addr).await.unwrap();

    let handle = task::spawn(async move {
        info!("Accept task started");
        server.accept(listener).await.unwrap();
    });
    handle.await;
}
