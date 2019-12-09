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

use crate::vnode;
use async_std::net::SocketAddr;
use async_std::net::ToSocketAddrs;
use async_std::net::{TcpListener, TcpStream};
use async_std::task;
use async_tungstenite::connect_async;
use futures::channel::mpsc::{channel, Receiver, Sender};
use futures::{SinkExt, StreamExt};
use serde_derive::{Deserialize, Serialize};
use slog::Logger;
use tungstenite::protocol::Message as TungstenMessage;

type WSStream = async_tungstenite::WebSocketStream<
    async_tungstenite::stream::Stream<TcpStream, async_tls::client::TlsStream<TcpStream>>,
>;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub(crate) enum Direction {
    Inbound,
    Outbound,
}
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub(crate) struct Handoff {
    pub partner: String,
    pub chunk: u64,
    pub direction: Direction,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub(crate) enum Message {
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
pub(crate) enum Ack {
    Start { vnode: u64 },
    Data { chunk: u64 },
    Finish { vnode: u64 },
}

struct Connection {
    addr: SocketAddr,
    rx: Receiver<TungstenMessage>,
    tx: Sender<TungstenMessage>,
    tasks: Sender<vnode::Task>,
    vnode: Option<u64>,
}

async fn handle_connection(logger: Logger, mut connection: Connection) {
    while let Some(msg) = connection.rx.next().await {
        info!(
            logger,
            "Received a message from {}: {}", connection.addr, msg
        );
        match serde_json::from_slice(&msg.into_data()) {
            Ok(Message::Start { src, vnode }) => {
                assert!(connection.vnode.is_none());
                connection.vnode = Some(vnode);
                connection
                    .tasks
                    .send(vnode::Task::HandoffInStart { src, vnode })
                    .await
                    .unwrap();
                info!(logger, "handoff for node {} started", vnode);
                connection
                    .tx
                    .send(TungstenMessage::text(
                        serde_json::to_string(&Ack::Start { vnode }).unwrap(),
                    ))
                    .await
                    .expect("Failed to forward message");
            }
            Ok(Message::Data { vnode, data, chunk }) => {
                if let Some(vnode_current) = connection.vnode {
                    assert_eq!(vnode, vnode_current);
                    connection
                        .tasks
                        .send(vnode::Task::HandoffIn { data, vnode, chunk })
                        .await
                        .unwrap();
                    connection
                        .tx
                        .send(TungstenMessage::text(
                            serde_json::to_string(&Ack::Data { chunk: chunk }).unwrap(),
                        ))
                        .await
                        .expect("Failed to forward message");
                }
            }
            Ok(Message::Finish { vnode }) => {
                if let Some(node_id) = connection.vnode {
                    assert_eq!(node_id, vnode);
                    connection
                        .tasks
                        .send(vnode::Task::HandoffInEnd { vnode })
                        .await
                        .unwrap();
                    connection
                        .tx
                        .send(TungstenMessage::text(
                            serde_json::to_string(&Ack::Finish { vnode }).unwrap(),
                        ))
                        .await
                        .expect("Failed to forward message");
                }
                connection.vnode = None;
                info!(logger, "handoff for node {} finished", vnode);
            }
            Err(e) => error!(logger, "failed to decode: {}", e),
        }
    }
}

async fn accept_connection(logger: Logger, stream: TcpStream, tasks: Sender<vnode::Task>) {
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
    let (mut msg_tx, msg_rx) = channel(crate::CHANNEL_SIZE);
    let (response_tx, mut response_rx) = channel(crate::CHANNEL_SIZE);
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
            .send(message)
            .await
            .expect("Failed to forward request");
        if let Some(resp) = response_rx.next().await {
            if ws_stream.send(resp).await.is_err() {
                break;
            }
        }
    }
    info!(logger, "Closing WebSocket connection: {}", addr);
}

pub(crate) async fn listener(
    logger: Logger,
    addr: String,
    tasks: Sender<vnode::Task>,
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

pub(crate) struct Worker {
    logger: Logger,
    src: String,
    target: String,
    vnode: u64,
    cnc: Sender<vnode::Cmd>,
    ws_stream: WSStream,
    chunk: u64,
}

pub(crate) enum Error {
    ConnectionFailed,
    Canceld,
}

impl Worker {
    pub async fn new(
        logger: Logger,
        src: String,
        target: String,
        vnode: u64,
        mut cnc: Sender<vnode::Cmd>,
    ) -> Result<Self, Error> {
        let url = url::Url::parse(&format!("ws://{}", target)).unwrap();
        if let Ok((ws_stream, _)) = connect_async(url).await {
            Ok(Self {
                logger,
                target,
                vnode,
                cnc,
                ws_stream,
                chunk: 0,
                src,
            })
        } else {
            error!(
                logger,
                "Failed to connect to {} to transfair vnode {}", target, vnode
            );
            cnc.send(vnode::Cmd::CancelHandoff { vnode, target })
                .await
                .unwrap();
            Err(Error::ConnectionFailed)
        }
    }

    pub async fn handoff(mut self) -> Result<(), Error> {
        info!(self.logger, "Starting handoff for vnode {}", self.vnode);
        self.init().await?;
        while self.transfair_data().await? {}
        self.finish().await
    }

    async fn finish(&mut self) -> Result<(), Error> {
        let vnode = self.vnode;
        self.try_ack(Message::Finish { vnode }, Ack::Finish { vnode })
            .await?;
        self.cnc
            .send(vnode::Cmd::FinishHandoff { vnode })
            .await
            .unwrap();
        Ok(())
    }

    async fn init(&mut self) -> Result<(), Error> {
        let vnode = self.vnode;
        self.try_ack(
            Message::Start {
                src: self.src.clone(),
                vnode,
            },
            Ack::Start { vnode },
        )
        .await?;
        Ok(())
    }

    async fn transfair_data(&mut self) -> Result<bool, Error> {
        let vnode = self.vnode;
        let chunk = self.chunk;
        info!(
            self.logger,
            "requesting chunk {} from vnode {}", chunk, vnode
        );
        let (tx, mut rx) = channel(crate::CHANNEL_SIZE);
        if self
            .cnc
            .send(vnode::Cmd::GetHandoffData {
                vnode,
                chunk,
                reply: tx.clone(),
            })
            .await
            .is_err()
        {
            return self.cancel().await;
        };

        if let Some((r_chunk, data)) = rx.next().await {
            assert_eq!(chunk, r_chunk);
            if data.is_empty() {
                info!(self.logger, "transfer for vnode {} finished", vnode);
                return Ok(false);
            }
            info!(
                self.logger,
                "transfering chunk {} from vnode {}", chunk, vnode
            );

            self.try_ack(Message::Data { vnode, chunk, data }, Ack::Data { chunk })
                .await?;
            self.chunk += 1;
            Ok(true)
        } else {
            error!(
                self.logger,
                "error tranfairing chunk {} for vnode {}", chunk, vnode
            );
            self.cancel().await
        }
    }

    async fn try_ack(&mut self, msg: Message, ack: Ack) -> Result<(), Error> {
        if self
            .ws_stream
            .send(TungstenMessage::text(serde_json::to_string(&msg).unwrap()))
            .await
            .is_err()
        {
            error!(
                self.logger,
                "Failed to send handoff command {:?} for vnode {} to {}",
                msg,
                self.vnode,
                self.target
            );
            return self.cancel().await;
        };

        if let Some(Ok(data)) = self.ws_stream.next().await {
            let r: Ack = serde_json::from_slice(&data.into_data()).unwrap();
            if ack != r {
                error!(self.logger, "Bad reply for  handoff command {:?} for vnode {} to {}. Expected, {:?}, but gut {:?}", msg, self.vnode, self.target, ack, r);
                return self.cancel().await;
            }
        } else {
            error!(
                self.logger,
                "No reply for  handoff command {:?} for vnode {} to {}",
                msg,
                self.vnode,
                self.target
            );
            return self.cancel().await;
        }
        Ok(())
    }

    async fn cancel<T>(&mut self) -> Result<T, Error>
    where
        T: Default,
    {
        self.cnc
            .send(vnode::Cmd::CancelHandoff {
                vnode: self.vnode,
                target: self.target.clone(),
            })
            .await
            .unwrap();
        Err(Error::Canceld)
    }
}
