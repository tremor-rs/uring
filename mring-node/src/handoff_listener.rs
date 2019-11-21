
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

use async_std::net::{ ToSocketAddrs};
use async_std::net::{TcpListener, TcpStream};
use async_std::task;
use futures::channel::mpsc::{ UnboundedSender};
use futures::StreamExt;
use slog::{ Logger};
use tungstenite::protocol::Message;
use super::*;

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
                            serde_json::to_string(&MigrationAck::Data { chunk: chunk + 1 }).unwrap(),
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

pub(crate) async fn server_loop(
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