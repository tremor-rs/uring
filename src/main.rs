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

mod codec;
#[allow(unused)]
pub mod errors;
pub mod network;
mod pubsub;
pub mod raft_node;
pub mod service;
pub mod storage;

use crate::network::{ws, Network, RaftNetworkMsg};
use crate::raft_node::*;
use crate::service::kv::{Service as KVService, KV_SERVICE};
use crate::service::mring::{Service as MRingService, MRING_SERVICE};
use crate::storage::URRocksStorage;
use clap::{App as ClApp, Arg};
use serde::{Deserialize, Serialize};
use slog::{Drain, Logger};
use slog_json;
use std::thread;
use std::time::{Duration, Instant};
pub use uring_common::*;
use ws_proto::PSURing;

#[macro_use]
extern crate slog;

#[derive(Deserialize, Serialize)]
pub struct KV {
    key: String,
    value: String,
}

#[derive(Deserialize, Serialize)]
pub struct Event {
    sid: ServiceId,
    eid: EventId,
    data: Vec<u8>,
}

#[derive(Deserialize, Serialize)]
pub struct KVs {
    scope: u16,
    key: Vec<u8>,
    value: Vec<u8>,
}

fn raft_loop<N: Network>(
    id: NodeId,
    bootstrap: bool,
    pubsub: pubsub::Channel,
    network: N,
    logger: Logger,
) {
    // Tick the raft node per 100ms. So use an `Instant` to trace it.
    let mut t1 = Instant::now();
    let mut node: RaftNode<URRocksStorage, _> = if bootstrap {
        RaftNode::create_raft_leader(&logger, id, pubsub, network)
    } else {
        RaftNode::create_raft_follower(&logger, id, pubsub, network)
    };
    node.set_raft_tick_duration(Duration::from_millis(100));
    node.log();
    let kv = KVService::new(0);
    node.add_service(KV_SERVICE, Box::new(kv));
    let vnode: MRingService<service::mring::placement::continuous::Strategy> = MRingService::new();
    node.add_service(MRING_SERVICE, Box::new(vnode));

    loop {
        thread::sleep(Duration::from_millis(10));

        if t1.elapsed() >= Duration::from_secs(10) {
            // Tick the raft.
            node.log();
            t1 = Instant::now();
        }

        // Handle readies from the raft.
        node.tick().unwrap();
    }
}

fn main() -> std::io::Result<()> {
    let drain = slog_json::Json::default(std::io::stderr()).map(slog::Fuse);
    // let decorator = slog_term::TermDecorator::new().build();
    // let drain = slog_term::FullFormat::new(decorator).build().fuse();
    let drain = slog_async::Async::new(drain).build().fuse();
    let logger = slog::Logger::root(drain, o!());

    //std::env::set_var("RUST_LOG", "actix_server=info,actix_web=info");
    let matches = ClApp::new("cake")
        .version("1.0")
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
                .help("Sets the node to bootstrap and become leader")
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

    let peers = matches.values_of_lossy("peers").unwrap_or(vec![]);
    let bootstrap = matches.is_present("bootstrap");
    let endpoint = matches.value_of("endpoint").unwrap_or("127.0.0.1:8080");
    let id = NodeId(matches.value_of("id").unwrap_or("1").parse().unwrap());
    let loop_logger = logger.clone();

    let (_ps_handle, ps_tx) = pubsub::start(&logger);

    let (handle, network) = ws::Network::new(&logger, id, endpoint, peers, ps_tx.clone());

    thread::spawn(move || raft_loop(id, bootstrap, ps_tx, network, loop_logger));

    handle.join().unwrap()
}
