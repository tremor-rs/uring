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

mod mring_listener;
mod vnode_manager;
use mring_listener::*;
use vnode_manager::*;

use async_std::net::{SocketAddr, ToSocketAddrs};
use async_std::net::{TcpListener, TcpStream};
use async_std::task;
use async_tungstenite::connect_async;
use futures::channel::mpsc::{UnboundedReceiver, UnboundedSender};
use futures::StreamExt;
use serde::{Deserialize, Serialize};
use slog::{Drain, Logger};
use std::env;
use tungstenite::protocol::Message;

#[macro_use]
extern crate slog;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
enum Direction {
    Inbound,
    Outbound,
}
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
struct Migration {
    partner: String,
    chunk: u64,
    direction: Direction,
}
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
struct VNode {
    id: u64,
    migration: Option<Migration>,
    data: Vec<String>,
}

enum Task {
    MigrateOut {
        target: String,
        vnode: u64,
    },
    Assign {
        vnodes: Vec<u64>,
    },
    MigrateInStart {
        src: String,
        vnode: u64,
    },
    MigrateIn {
        vnode: u64,
        chunk: u64,
        data: Vec<String>,
    },
    MigrateInEnd {
        vnode: u64,
    },
}

struct Connection {
    addr: SocketAddr,
    rx: UnboundedReceiver<Message>,
    tx: UnboundedSender<Message>,
    tasks: UnboundedSender<Task>,
    vnode: Option<u64>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
enum MigrationMsg {
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
enum MigrationAck {
    Start { vnode: u64 },
    Data { chunk: u64 },
    Finish { vnode: u64 },
}

async fn handle_connection(logger: Logger, mut connection: Connection) {
    while let Some(msg) = connection.rx.next().await {
        info!(
            logger,
            "Received a message from {}: {}", connection.addr, msg
        );
        match serde_json::from_slice(&msg.into_data()) {
            Ok(MigrationMsg::Start { src, vnode }) => {
                assert!(connection.vnode.is_none());
                connection.vnode = Some(vnode);
                connection
                    .tasks
                    .unbounded_send(Task::MigrateInStart { src, vnode })
                    .unwrap();
                info!(logger, "migration for node {} started", vnode);
                connection
                    .tx
                    .unbounded_send(Message::text(
                        serde_json::to_string(&MigrationAck::Start { vnode }).unwrap(),
                    ))
                    .expect("Failed to forward message");
            }
            Ok(MigrationMsg::Data { vnode, data, chunk }) => {
                if let Some(vnode_current) = connection.vnode {
                    assert_eq!(vnode, vnode_current);
                    connection
                        .tasks
                        .unbounded_send(Task::MigrateIn { data, vnode, chunk })
                        .unwrap();
                    connection
                        .tx
                        .unbounded_send(Message::text(
                            serde_json::to_string(&MigrationAck::Data { chunk }).unwrap(),
                        ))
                        .expect("Failed to forward message");
                }
            }
            Ok(MigrationMsg::Finish { vnode }) => {
                if let Some(node_id) = connection.vnode {
                    assert_eq!(node_id, vnode);
                    connection
                        .tasks
                        .unbounded_send(Task::MigrateInEnd { vnode })
                        .unwrap();
                    connection
                        .tx
                        .unbounded_send(Message::text(
                            serde_json::to_string(&MigrationAck::Finish { vnode }).unwrap(),
                        ))
                        .expect("Failed to forward message");
                }
                connection.vnode = None;
                info!(logger, "migration for node {} finished", vnode);
            }
            Err(e) => error!(logger, "failed to decode: {}", e),
        }
    }
}

async fn accept_connection(logger: Logger, stream: TcpStream, tasks: UnboundedSender<Task>) {
    let addr = stream
        .peer_addr()
        .expect("connected streams should have a peer address");
    info!(logger, "Peer address: {}", addr);

    let mut ws_stream = async_tungstenite::accept_async(stream)
        .await
        .expect("Error during the websocket handshake occurred");

    info!(logger, "New WebSocket connection: {}", addr);

    // Create a channel for our stream, which other sockets will use to
    // send us messages. Then register our address with the stream to send
    // data to us.
    let (msg_tx, msg_rx) = futures::channel::mpsc::unbounded();
    let (response_tx, mut response_rx) = futures::channel::mpsc::unbounded();
    let c = Connection {
        addr: addr,
        rx: msg_rx,
        tx: response_tx,
        vnode: None,
        tasks,
    };
    task::spawn(handle_connection(logger.clone(), c));

    while let Some(Ok(message)) = ws_stream.next().await {
        msg_tx
            .unbounded_send(message)
            .expect("Failed to forward request");
        if let Some(resp) = response_rx.next().await {
            if ws_stream.send(resp).await.is_err() {
                break;
            }
        }
    }
    info!(logger, "Closing WebSocket connection: {}", addr);
}

async fn server_loop(
    logger: Logger,
    addr: String,
    tasks: UnboundedSender<Task>,
) -> Result<(), std::io::Error> {
    let addr = addr
        .to_socket_addrs()
        .await
        .expect("Not a valid address")
        .next()
        .expect("Not a socket address");

    // Create the event loop and TCP listener we'll accept connections on.
    let try_socket = TcpListener::bind(&addr).await;
    let listener = try_socket.expect("Failed to bind");
    info!(logger, "Listening on: {}", addr);

    while let Ok((stream, _)) = listener.accept().await {
        task::spawn(accept_connection(logger.clone(), stream, tasks.clone()));
    }

    Ok(())
}

fn main() {
    let decorator = slog_term::TermDecorator::new().build();
    let drain = slog_term::FullFormat::new(decorator).build().fuse();
    let drain = slog_async::Async::new(drain).build().fuse();
    let logger = slog::Logger::root(drain, o!());

    let (tasks_tx, tasks_rx) = futures::channel::mpsc::unbounded();

    let local = env::args()
        .nth(1)
        .unwrap_or_else(|| panic!("this program requires at least two arguments"))
        .to_string();

    // Specify the server address to which the client will be connecting.
    let remote = env::args()
        .nth(2)
        .unwrap_or_else(|| panic!("this program requires at least two argument"))
        .to_string();

    task::spawn(tick_loop(logger.clone(), local.clone(), tasks_rx));

    task::spawn(server_loop(
        logger.clone(),
        local.to_string(),
        tasks_tx.clone(),
    ));

    task::block_on(run(logger, local, remote, tasks_tx))
}
