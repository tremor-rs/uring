// Copyright 2018-2019, Wayfair GmbH
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#![recursion_limit = "2048"]

mod codec;
#[allow(unused)]
pub mod errors;
pub mod network;
mod pubsub;
pub mod raft_node;
pub mod service;
pub mod storage;
pub mod version;

use crate::network::{ws, Network, RaftNetworkMsg};
use crate::raft_node::*;
use crate::service::mring::{self, placement::continuous};
use crate::service::{kv, Service};
use crate::storage::URRocksStorage;
use async_std::task;
use clap::{App as ClApp, Arg};
use futures::{select, FutureExt, StreamExt};
use serde_derive::{Deserialize, Serialize};
use slog::{Drain, Logger};
use slog_json;
use std::time::{Duration, Instant};
pub use uring_common::*;
use ws_proto::PSURing;

const CHANNEL_SIZE: usize = 64usize;

#[macro_use]
extern crate slog;

#[derive(Deserialize, Serialize)]
pub struct KV {
    key: String,
    value: serde_json::Value,
}

#[derive(Deserialize, Serialize, Debug)]
pub struct Event {
    nid: Option<NodeId>,
    eid: EventId,
    sid: ServiceId,
    data: Vec<u8>,
}

#[derive(Deserialize, Serialize)]
pub struct KVs {
    scope: u16,
    key: Vec<u8>,
    value: Vec<u8>,
}

async fn raft_loop<N: Network>(
    id: NodeId,
    bootstrap: bool,
    ring_size: Option<u64>,
    pubsub: pubsub::Channel,
    network: N,
    logger: Logger,
) where
    N: 'static,
{
    // Tick the raft node per 100ms. So use an `Instant` to trace it.
    let mut node: RaftNode<URRocksStorage, _> = if bootstrap {
        RaftNode::create_raft_leader(&logger, id, pubsub, network).await
    } else {
        RaftNode::create_raft_follower(&logger, id, pubsub, network).await
    };
    node.set_raft_tick_duration(Duration::from_millis(100));
    node.log().await;
    let kv = kv::Service::new(&logger, 0);
    node.add_service(kv::ID, Box::new(kv));
    let mut vnode: mring::Service<continuous::Strategy> = mring::Service::new();

    if let Some(size) = ring_size {
        if bootstrap {
            vnode
                .execute(
                    node.raft_group.as_ref().unwrap(),
                    &mut node.pubsub,
                    service::mring::Event::set_size(size),
                )
                .await
                .unwrap();
        }
    }
    node.add_service(mring::ID, Box::new(vnode));

    let version = crate::service::version::Service::new(&logger);
    node.add_service(crate::service::version::ID, Box::new(version));

    let status = crate::service::status::Service::new(&logger);
    let status = Box::new(status);
    node.add_service(service::status::ID, status);

    node.node_loop().await.unwrap()
}

fn main() -> std::io::Result<()> {
    use version::VERSION;
    let matches = ClApp::new("cake")
        .version(VERSION)
        .author("The Treamor Team")
        .about("Uring Demo")
        .arg(
            Arg::with_name("id")
                .short("i")
                .long("id")
                .value_name("ID")
                .help("The Node ID")
                .takes_value(true),
        )
        .arg(
            Arg::with_name("bootstrap")
                .short("b")
                .long("bootstrap")
                .value_name("BOOTSTRAP")
                .help("Sets the node to bootstrap mode and become leader")
                .takes_value(false),
        )
        .arg(
            Arg::with_name("ring-size")
                .short("r")
                .long("ring-size")
                .value_name("RING_SIZE")
                .help("Initialized mring size, only has an effect when used together with --bootstrap")
                .takes_value(true),
        )
        .arg(
            Arg::with_name("http-endpoint")
                .long("http")
                .value_name("HTTP")
                .help("http endpoint to listen to")
                .takes_value(true),
        )
        .arg(
            Arg::with_name("no-json")
                .short("n")
                .long("no-json")
                .value_name("NOJSON")
                .help("don't log via json")
                .takes_value(false),
        )
        .arg(
            Arg::with_name("peers")
                .short("p")
                .long("peers")
                .value_name("PEERS")
                .multiple(true)
                .takes_value(true)
                .help("Peers to connet to"),
        )
        .arg(
            Arg::with_name("endpoint")
                .short("e")
                .long("endpoint")
                .value_name("ENDPOINT")
                .takes_value(true)
                .default_value("127.0.0.1:8080")
                .help("Peers to connet to"),
        )
        .get_matches();

    let logger = if matches.is_present("no-json") {
        let decorator = slog_term::TermDecorator::new().build();
        let drain = slog_term::FullFormat::new(decorator).build().fuse();
        let drain = slog_async::Async::new(drain).build().fuse();
        slog::Logger::root(drain, o!())
    } else {
        let drain = slog_json::Json::default(std::io::stderr()).map(slog::Fuse);
        let drain = slog_async::Async::new(drain).build().fuse();
        slog::Logger::root(drain, o!())
    };
    let peers = matches.values_of_lossy("peers").unwrap_or(vec![]);
    let ring_size: Option<u64> = matches.value_of("ring-size").map(|s| s.parse().unwrap());
    let bootstrap = matches.is_present("bootstrap");
    let endpoint = matches.value_of("endpoint").unwrap_or("127.0.0.1:8080");
    let id = NodeId(matches.value_of("id").unwrap_or("1").parse().unwrap());
    let loop_logger = logger.clone();
    let rest_endpoint = matches.value_of("http-endpoint");

    let ps_tx = pubsub::start(&logger);

    let network = ws::Network::new(&logger, id, endpoint, rest_endpoint, peers, ps_tx.clone());

    task::block_on(raft_loop(
        id,
        bootstrap,
        ring_size,
        ps_tx,
        network,
        loop_logger,
    ));
    Ok(())
}
