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
// use crate::{NodeId, KV};

use super::Reply as WsReply;
use super::*;
use crate::version::VERSION;
use crate::{pubsub, NodeId};
use async_std::net::TcpListener;
use async_std::net::ToSocketAddrs;
use async_std::task;
use futures::channel::mpsc::{channel, Receiver, Sender};
use futures::io::{AsyncRead, AsyncWrite};
use futures::{select, FutureExt, StreamExt};
use std::io::Error;
use tungstenite::protocol::Message;
use ws_proto::*;

/// websocket connection is long running connection, it easier
/// to handle with an actor
pub(crate) struct Connection {
    node: Node,
    remote_id: NodeId,
    protocol: Option<Protocol>,
    rx: Receiver<Message>,
    tx: Sender<Message>,
    ws_rx: Receiver<WsMessage>,
    ws_tx: Sender<WsMessage>,
    ps_rx: Receiver<SubscriberMsg>,
    ps_tx: Sender<SubscriberMsg>,
}

impl Connection {
    pub(crate) fn new(node: Node, rx: Receiver<Message>, tx: Sender<Message>) -> Self {
        let (ps_tx, ps_rx) = channel(crate::CHANNEL_SIZE);
        let (ws_tx, ws_rx) = channel(crate::CHANNEL_SIZE);
        Self {
            node,
            remote_id: NodeId(0),
            protocol: None,
            rx,
            tx,
            ps_tx,
            ps_rx,
            ws_tx,
            ws_rx,
        }
    }

    async fn handle_initial(&mut self, msg: Message) -> bool {
        if msg.is_text() {
            let text = msg.into_data();
            match serde_json::from_slice(&text) {
                Ok(ProtocolSelect::Status { rid }) => self
                    .node
                    .tx
                    .send(UrMsg::Status(rid, self.ws_tx.clone()))
                    .await
                    .is_ok(),
                Ok(ProtocolSelect::Version { .. }) => self
                    .tx
                    .send(Message::Text(serde_json::to_string(VERSION).unwrap()))
                    .await
                    .is_ok(),
                Ok(ProtocolSelect::Select { rid, protocol }) => {
                    self.protocol = Some(protocol);
                    self.tx
                        .send(Message::Text(
                            serde_json::to_string(&ProtocolSelect::Selected { rid, protocol })
                                .unwrap(),
                        ))
                        .await
                        .is_ok()
                }
                Ok(ProtocolSelect::Selected { .. }) => false,
                Ok(ProtocolSelect::As { protocol, cmd }) => match protocol {
                    Protocol::KV => self.handle_kv_msg(serde_json::from_value(cmd).unwrap()),
                    _ => false,
                },
                Ok(ProtocolSelect::Subscribe { channel }) => self
                    .node
                    .pubsub
                    .send(pubsub::Msg::Subscribe {
                        channel,
                        tx: self.ps_tx.clone(),
                    })
                    .await
                    .is_ok(),
                Err(e) => {
                    error!(
                        self.node.logger,
                        "Failed to decode ProtocolSelect message: {} => {}",
                        e,
                        String::from_utf8(text).unwrap_or_default()
                    );
                    true
                }
            }
        } else {
            true
        }
    }

    async fn handle_uring(&mut self, msg: Message) -> bool {
        if msg.is_text() {
            let text = msg.into_data();
            match serde_json::from_slice(&text) {
                Ok(CtrlMsg::Hello(id, peer)) => {
                    info!(self.node.logger, "Hello from {}", id);
                    self.remote_id = id;
                    self.node
                        .tx
                        .unbounded_send(UrMsg::RegisterRemote(id, peer, self.ws_tx.clone()))
                        .is_ok()
                }
                Ok(CtrlMsg::AckProposal(pid, success)) => self
                    .node
                    .tx
                    .unbounded_send(UrMsg::AckProposal(pid, success))
                    .is_ok(),
                Ok(CtrlMsg::ForwardProposal(from, pid, sid, eid, value)) => self
                    .node
                    .tx
                    .unbounded_send(UrMsg::ForwardProposal(from, pid, sid, eid, value))
                    .is_ok(),
                Ok(_) => true,
                Err(e) => {
                    error!(
                        self.node.logger,
                        "Failed to decode CtrlMsg message: {} => {}",
                        e,
                        String::from_utf8(text).unwrap_or_default()
                    );
                    true
                }
            }
        } else if msg.is_binary() {
            let bin = msg.into_data();
            let msg = decode_ws(&bin);
            self.node.tx.unbounded_send(UrMsg::RaftMsg(msg)).is_ok()
        } else {
            true
        }
    }

    async fn handle_kv(&mut self, msg: Message) -> bool {
        if msg.is_text() {
            let text = msg.into_data();
            match serde_json::from_slice(&text) {
                Ok(msg) => self.handle_kv_msg(msg),
                Err(e) => {
                    error!(
                        self.node.logger,
                        "Failed to decode KVRequest message: {} => {}",
                        e,
                        String::from_utf8(text).unwrap_or_default()
                    );
                    true
                }
            }
        } else {
            true
        }
    }

    fn handle_kv_msg(&mut self, msg: KVRequest) -> bool {
        match msg {
            KVRequest::Get { rid, key } => self
                .node
                .tx
                .unbounded_send(UrMsg::Get(
                    key.into_bytes(),
                    WsReply(rid, self.ws_tx.clone()),
                ))
                .is_ok(),
            KVRequest::Put { rid, key, store } => self
                .node
                .tx
                .unbounded_send(UrMsg::Put(
                    key.into_bytes(),
                    store.into_bytes(),
                    WsReply(rid, self.ws_tx.clone()),
                ))
                .is_ok(),
            KVRequest::Delete { rid, key } => self
                .node
                .tx
                .unbounded_send(UrMsg::Delete(
                    key.into_bytes(),
                    WsReply(rid, self.ws_tx.clone()),
                ))
                .is_ok(),
            KVRequest::Cas {
                rid,
                key,
                check,
                store,
            } => self
                .node
                .tx
                .unbounded_send(UrMsg::Cas(
                    key.into_bytes(),
                    check.map(String::into_bytes),
                    store.into_bytes(),
                    WsReply(rid, self.ws_tx.clone()),
                ))
                .is_ok(),
        }
    }

    async fn handle_mring(&mut self, msg: Message) -> bool {
        if msg.is_text() {
            let text = msg.into_data();
            match serde_json::from_slice(&text) {
                Ok(msg) => self.handle_mring_msg(msg).await,
                Err(e) => {
                    error!(
                        self.node.logger,
                        "Failed to decode MRRequest message: {} => {}",
                        e,
                        String::from_utf8(text).unwrap_or_default()
                    );
                    true
                }
            }
        } else {
            true
        }
    }

    async fn handle_mring_msg(&mut self, msg: MRRequest) -> bool {
        match msg {
            MRRequest::GetSize { rid } => self
                .node
                .tx
                .unbounded_send(UrMsg::MRingGetSize(WsReply(rid, self.ws_tx.clone())))
                .is_ok(),

            MRRequest::SetSize { rid, size } => self
                .node
                .tx
                .unbounded_send(UrMsg::MRingSetSize(size, WsReply(rid, self.ws_tx.clone())))
                .is_ok(),
            MRRequest::GetNodes { rid } => self
                .node
                .tx
                .unbounded_send(UrMsg::MRingGetNodes(WsReply(rid, self.ws_tx.clone())))
                .is_ok(),
            MRRequest::AddNode { rid, node } => self
                .node
                .tx
                .unbounded_send(UrMsg::MRingAddNode(node, WsReply(rid, self.ws_tx.clone())))
                .is_ok(),
            MRRequest::RemoveNode { rid, node } => self
                .node
                .tx
                .unbounded_send(UrMsg::MRingRemoveNode(
                    node,
                    WsReply(rid, self.ws_tx.clone()),
                ))
                .is_ok(),
        }
    }

    async fn handle_version(&mut self, msg: Message) -> bool {
        if msg.is_text() {
            let text = msg.into_data();
            match serde_json::from_slice(&text) {
                Ok(msg) => self.handle_version_msg(msg),
                Err(e) => {
                    error!(
                        self.node.logger,
                        "Failed to decode VRequest message: {} => {}",
                        e,
                        String::from_utf8(text).unwrap_or_default()
                    );
                    true
                }
            }
        } else {
            true
        }
    }

    fn handle_version_msg(&mut self, msg: VRequest) -> bool {
        match msg {
            VRequest::Get { rid } => {
                self.node
                    .tx
                    .unbounded_send(UrMsg::Version(rid, self.ws_tx.clone()))
                    .unwrap();
                true
            }
        }
    }

    async fn handle_status(&mut self, msg: Message) -> bool {
        if msg.is_text() {
            let text = msg.into_data();
            match serde_json::from_slice(&text) {
                Ok(msg) => self.handle_status_msg(msg),
                Err(e) => {
                    error!(
                        self.node.logger,
                        "Failed to decode SRequest message: {} => {}",
                        e,
                        String::from_utf8(text).unwrap_or_default()
                    );
                    true
                }
            }
        } else {
            true
        }
    }

    fn handle_status_msg(&mut self, msg: SRequest) -> bool {
        match msg {
            SRequest::Get { rid } => {
                self.node
                    .tx
                    .unbounded_send(UrMsg::Status(rid, self.ws_tx.clone()))
                    .unwrap();
                true
            }
        }
    }

    pub async fn msg_loop(mut self, logger: Logger) {
        loop {
            let cont = select! {
                msg = self.rx.next() => {
                    if let Some(msg) = msg {
                        match self.protocol {
                            None => self.handle_initial(msg).await,
                            Some(Protocol::KV) => self.handle_kv(msg).await,
                            Some(Protocol::URing) => self.handle_uring(msg).await,
                            Some(Protocol::MRing) => self.handle_mring(msg).await,
                            Some(Protocol::Version) => self.handle_version(msg).await,
                            Some(Protocol::Status) => self.handle_status(msg).await,
                        }
                    } else {
                        false
                    }

                }
                msg = self.ws_rx.next() => {
                    match self.protocol {
                        None | Some(Protocol::Status) | Some(Protocol::Version) | Some(Protocol::KV) | Some(Protocol::MRing) => match msg {
                            Some(WsMessage::Ctrl(msg)) =>self.tx.send(Message::Text(serde_json::to_string(&msg).unwrap())).await.is_ok(),
                            Some(WsMessage::Reply(_, msg)) =>self.tx.send(Message::Text(serde_json::to_string(&msg).unwrap())).await.is_ok(),
                            None | Some(WsMessage::Raft(_)) => false,
                        }
                        Some(Protocol::URing) => match msg {
                            Some(WsMessage::Ctrl(msg)) =>self.tx.send(Message::Text(serde_json::to_string(&msg).unwrap())).await.is_ok(),
                            Some(WsMessage::Raft(msg)) => self.tx.send(Message::Binary(encode_ws(msg).to_vec())).await.is_ok(),
                            Some(WsMessage::Reply(_, msg)) =>self.tx.send(Message::Text(serde_json::to_string(&msg).unwrap())).await.is_ok(),
                            None => false,
                        },
                    }
                }
                msg = self.ps_rx.next() => {
                    if let Some(msg) = msg {
                        self.tx.send(Message::Text(serde_json::to_string(&msg).unwrap())).await.is_ok()
                    } else {
                        false
                    }
                }
                complete => false
            };
            if !cont {
                error!(logger, "Client connection to {} down.", self.remote_id);
                self.node
                    .tx
                    .unbounded_send(UrMsg::DownRemote(self.remote_id))
                    .unwrap();
                break;
            }
        }
    }
}

pub(crate) async fn accept_connection<S>(logger: Logger, node: Node, stream: S)
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    let mut ws_stream = if let Ok(ws_stream) = async_tungstenite::accept_async(stream).await {
        ws_stream
    } else {
        error!(logger, "Error during the websocket handshake occurred");
        return;
    };

    // Create a channel for our stream, which other sockets will use to
    // send us messages. Then register our address with the stream to send
    // data to us.
    let (mut msg_tx, msg_rx) = channel(crate::CHANNEL_SIZE);
    let (response_tx, mut response_rx) = channel(crate::CHANNEL_SIZE);
    let c = Connection::new(node, msg_rx, response_tx);
    task::spawn(c.msg_loop(logger.clone()));

    loop {
        select! {
            message = ws_stream.next().fuse() => {
                if let Some(Ok(message)) = message {
                    msg_tx
                    .send(message).await
                    .expect("Failed to forward request");
                } else {
                    error!(logger, "Client connection down.", );
                    break;
                }
            }
            resp = response_rx.next() => {
                if let Some(resp) = resp {
                    ws_stream.send(resp).await.expect("Failed to send response");
                } else {
                    error!(logger, "Client connection down.", );
                    break;
                }

            }
            complete => {
                error!(logger, "Client connection down.", );
                break;
            }
        }
    }
}

pub(crate) async fn run(logger: Logger, node: Node, addr: String) -> Result<(), Error> {
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
        task::spawn(accept_connection(logger.clone(), node.clone(), stream));
    }

    Ok(())
}
