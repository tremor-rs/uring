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

use super::*;
use async_std::net::ToSocketAddrs;
use async_std::net::{TcpListener, TcpStream};
use async_std::task;
use futures::channel::mpsc::{Sender, channel};
use futures::StreamExt;
use slog::Logger;
use tungstenite::protocol::Message;
use futures::sink::SinkExt;

async fn handle_connection(logger: Logger, mut connection: Connection) {
    while let Some(msg) = connection.rx.next().await {
        info!(
            logger,
            "Received a message from {}: {}", connection.addr, msg
        );
        match serde_json::from_slice(&msg.into_data()) {
            Ok(HandoffMsg::Start { src, vnode }) => {
                assert!(connection.vnode.is_none());
                connection.vnode = Some(vnode);
                connection
                    .tasks
                    .send(Task::HandoffInStart { src, vnode }).await
                    .unwrap();
                info!(logger, "handoff for node {} started", vnode);
                connection
                    .tx
                    .send(Message::text(
                        serde_json::to_string(&HandoffAck::Start { vnode }).unwrap(),
                    )).await
                    .expect("Failed to forward message");
            }
            Ok(HandoffMsg::Data { vnode, data, chunk }) => {
                if let Some(vnode_current) = connection.vnode {
                    assert_eq!(vnode, vnode_current);
                    connection
                        .tasks
                        .send(Task::HandoffIn { data, vnode, chunk }).await
                        .unwrap();
                    connection
                        .tx
                        .send(Message::text(
                            serde_json::to_string(&HandoffAck::Data { chunk: chunk }).unwrap(),
                        )).await
                        .expect("Failed to forward message");
                }
            }
            Ok(HandoffMsg::Finish { vnode }) => {
                if let Some(node_id) = connection.vnode {
                    assert_eq!(node_id, vnode);
                    connection
                        .tasks
                        .send(Task::HandoffInEnd { vnode }).await
                        .unwrap();
                    connection
                        .tx
                        .send(Message::text(
                            serde_json::to_string(&HandoffAck::Finish { vnode }).unwrap(),
                        )).await
                        .expect("Failed to forward message");
                }
                connection.vnode = None;
                info!(logger, "handoff for node {} finished", vnode);
            }
            Err(e) => error!(logger, "failed to decode: {}", e),
        }
    }
}

async fn accept_connection(logger: Logger, stream: TcpStream, tasks: Sender<Task>) {
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
    let (mut msg_tx, msg_rx) = channel(64);
    let (response_tx, mut response_rx) = channel(64);
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
            .send(message).await
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
    tasks: Sender<Task>,
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
