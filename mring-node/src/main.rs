// Copyright 2018-2020, Wayfair GmbH
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

mod handoff_listener;
mod mring_listener;
mod vnode_manager;
use handoff_listener::*;
use mring_listener::*;
use vnode_manager::*;

use async_std::net::SocketAddr;
use async_std::task;
use async_tungstenite::connect_async;
use futures::channel::mpsc::{channel, Receiver, Sender};
use serde_derive::{Deserialize, Serialize};
use slog::Drain;
use std::env;
use tungstenite::protocol::Message;

const CHANNEL_SIZE: usize = 64usize;

#[macro_use]
extern crate slog;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
enum Direction {
    Inbound,
    Outbound,
}
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
struct Handoff {
    partner: String,
    chunk: u64,
    direction: Direction,
}
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
struct VNode {
    id: u64,
    handoff: Option<Handoff>,
    data: Vec<String>,
}

enum Task {
    HandoffOut {
        target: String,
        vnode: u64,
    },
    Assign {
        vnodes: Vec<u64>,
    },
    HandoffInStart {
        src: String,
        vnode: u64,
    },
    HandoffIn {
        vnode: u64,
        chunk: u64,
        data: Vec<String>,
    },
    HandoffInEnd {
        vnode: u64,
    },
}

struct Connection {
    addr: SocketAddr,
    rx: Receiver<Message>,
    tx: Sender<Message>,
    tasks: Sender<Task>,
    vnode: Option<u64>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
enum HandoffMsg {
    Start {
        src: String,
        vnode: u64,
    },
    Data {
        vnode: u64,
        chunk: u64,
        data: Vec<String>,
    },
    Finish {
        vnode: u64,
    },
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
enum HandoffAck {
    Start { vnode: u64 },
    Data { chunk: u64 },
    Finish { vnode: u64 },
}

fn main() {
    let decorator = slog_term::TermDecorator::new().build();
    let drain = slog_term::FullFormat::new(decorator).build().fuse();
    let drain = slog_async::Async::new(drain).build().fuse();
    let logger = slog::Logger::root(drain, o!());

    let (tasks_tx, tasks_rx) = channel(crate::CHANNEL_SIZE);

    let local = env::args()
        .nth(1)
        .unwrap_or_else(|| panic!("this program requires at least two arguments"))
        .to_string();

    // Specify the server address to which the client will be connecting.
    let remote = env::args()
        .nth(2)
        .unwrap_or_else(|| panic!("this program requires at least two argument"))
        .to_string();

    task::spawn(tick_loop(
        logger.clone(),
        local.clone(),
        tasks_rx,
        tasks_tx.clone(),
    ));

    task::spawn(server_loop(
        logger.clone(),
        local.to_string(),
        tasks_tx.clone(),
    ));

    task::block_on(run(logger, local, remote, tasks_tx))
}
