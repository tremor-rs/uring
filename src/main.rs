use std::{collections::HashSet, env, sync::Arc, time::Duration};

use async_raft::{AppData, AppDataResponse, Config, Raft};
use async_std::{net::TcpListener, task};
use memstore::MemStore;
use network::{RaftRouter, TremorRaft};
use serde::{Deserialize, Serialize};
// We use anyhow::Result in our impl below.
use anyhow::Result;
use toy_rpc::Server;
mod memstore;
mod network;

/// The application data request type which the `MemStore` works with.
///
/// Conceptually, for demo purposes, this represents an update to a client's status info,
/// returning the previously recorded status.
#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct ClientRequest {
    /// The ID of the client which has sent the request.
    pub client: String,
    /// The serial number of this request.
    pub serial: u64,
    /// A string describing the status of the client. For a real application, this should probably
    /// be an enum representing all of the various types of requests / operations which a client
    /// can perform.
    pub status: String,
}

impl AppData for ClientRequest {}

type ClientError = String;
/// The application data response type which the `MemStore` works with.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ClientResponse(Result<Option<String>, ClientError>);

impl AppDataResponse for ClientResponse {}

/// A concrete Raft type used during testing.
pub type MemRaft = Raft<ClientRequest, ClientResponse, RaftRouter, MemStore>;

#[async_std::main]
async fn main() {
    tracing_subscriber::fmt().init();
    // Get our node's ID from stable storage.
    let node_id = get_id_from_storage().await;

    // Build our Raft runtime config, then instantiate our
    // RaftNetwork & RaftStorage impls.
    let config = Arc::new(
        Config::build("primary-raft-group".into())
            .validate()
            .expect("failed to build Raft config"),
    );
    let network = Arc::new(RaftRouter::new(config.clone()).await);
    let storage = Arc::new(MemStore::new(node_id));

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
    let mut members = HashSet::new();

    let nodes: Vec<_> = env::args()
        .skip(1)
        .map(|a| a.parse::<u64>().expect("bad argument"))
        .collect();
    for nid in nodes {
        members.insert(nid);
    }
    async_std::task::sleep(Duration::from_secs(5)).await;
    raft.initialize(members)
        .await
        .expect("oh no initialize failed");

    let raft = TremorRaft::new(raft);
    let raft_server = Arc::new(raft);

    let server = Server::builder().register(raft_server).build();
    let listener = TcpListener::bind(addr).await.unwrap();

    let handle = task::spawn(async move {
        server.accept(listener).await.unwrap();
    });
    handle.await;
}
