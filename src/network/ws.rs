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

mod client;
mod rest;
mod server;
mod server2;
use crate::network::{
    Error, EventId, Network as NetworkTrait, ProposalId, RaftNetworkMsg, ServiceId,
};
use crate::pubsub;
use crate::service::{kv, mring};
use crate::{NodeId, RequestId};
use async_std::task;
use async_trait::async_trait;
use bytes::Bytes;
use futures::channel::mpsc::{unbounded, Sender, UnboundedReceiver, UnboundedSender};
use futures::{SinkExt, StreamExt};
use raft::eraftpb::Message as RaftMessage;
use serde_derive::{Deserialize, Serialize};
use slog::Logger;
use std::collections::HashMap;
use std::io::{self, Cursor};
use ws_proto::Reply as ProtoReply;

type LocalMailboxes = HashMap<NodeId, Sender<WsMessage>>;
type RemoteMailboxes = HashMap<NodeId, Sender<WsMessage>>;

#[derive(Clone)]
pub(crate) struct Node {
    _id: NodeId,
    tx: UnboundedSender<UrMsg>,
    logger: Logger,
    pubsub: pubsub::Channel,
}

pub struct Network {
    id: NodeId,
    local_mailboxes: LocalMailboxes,
    remote_mailboxes: RemoteMailboxes,
    known_peers: HashMap<NodeId, String>,
    endpoint: String,
    logger: Logger,
    rx: UnboundedReceiver<UrMsg>,
    tx: UnboundedSender<UrMsg>,
    next_eid: u64,
    pending: HashMap<EventId, Reply>,
    prot_pending: HashMap<EventId, (RequestId, protocol_driver::HandlerOutboundChannelSender)>,
}

pub(crate) struct Reply(RequestId, Sender<WsMessage>);

#[derive(Serialize, Deserialize, Debug)]
pub enum CtrlMsg {
    Hello(NodeId, String),
    HelloAck(NodeId, String, Vec<(NodeId, String)>),
    AckProposal(ProposalId, bool),
    ForwardProposal(NodeId, ProposalId, ServiceId, EventId, Vec<u8>),
}

pub(crate) enum UrMsg {
    // Network related
    InitLocal(Sender<WsMessage>),
    RegisterLocal(NodeId, String, Sender<WsMessage>, Vec<(NodeId, String)>),
    RegisterRemote(NodeId, String, Sender<WsMessage>),
    DownLocal(NodeId),
    DownRemote(NodeId),
    Status(RequestId, Sender<WsMessage>),
    Version(RequestId, Sender<WsMessage>),

    // Raft related
    AckProposal(ProposalId, bool),
    ForwardProposal(NodeId, ProposalId, ServiceId, EventId, Vec<u8>),
    RaftMsg(RaftMessage),
    GetNode(NodeId, Sender<bool>),
    AddNode(NodeId, Sender<bool>),

    // KV related
    Get(Vec<u8>, Reply),
    Put(Vec<u8>, Vec<u8>, Reply),
    Cas(Vec<u8>, Option<Vec<u8>>, Vec<u8>, Reply),
    Delete(Vec<u8>, Reply),

    // VNode
    MRingSetSize(u64, Reply),
    MRingGetSize(Reply),
    MRingGetNodes(Reply),
    MRingAddNode(String, Reply),
    MRingRemoveNode(String, Reply),

    Protocol(ProtocolMessage),
}

pub(crate) enum ProtocolMessage {
    Event {
        id: RequestId,
        service_id: ServiceId,
        event: Vec<u8>,
        reply: protocol_driver::HandlerOutboundChannelSender,
    },
}

#[async_trait]
impl NetworkTrait for Network {
    async fn event_reply(&mut self, id: EventId, code: u16, data: Vec<u8>) -> Result<(), Error> {
        if let Some(Reply(rid, mut sender)) = self.pending.remove(&id) {
            let data: serde_json::Value = serde_json::from_slice(&data).unwrap();
            sender
                .send(ProtoReply { code, rid, data }.into())
                .await
                .unwrap()
        } else if let Some((rid, mut sender)) = self.prot_pending.remove(&id) {
            sender
                .send(protocol_driver::HandlerOutboundMessage::ok(rid, data))
                .await
                .unwrap()
        } else {
            error!(self.logger, "Uknown event id {} for reply: {:?}", id, data)
        };

        Ok(())
    }

    async fn next(&mut self) -> Option<RaftNetworkMsg> {
        use RaftNetworkMsg::*;
        let msg = if let Some(msg) = self.rx.next().await {
            msg
        } else {
            return None;
        };
        match msg {
            UrMsg::Protocol(msg) => match msg {
                ProtocolMessage::Event {
                    id,
                    service_id,
                    event,
                    reply,
                } => {
                    let eid = self.register_prot_reply(id, reply);
                    Some(RaftNetworkMsg::Event(eid, service_id, event))
                }
            },
            UrMsg::MRingSetSize(size, reply) => {
                let eid = self.register_reply(reply);
                Some(RaftNetworkMsg::Event(
                    eid,
                    mring::ID,
                    mring::Event::set_size(size),
                ))
            }
            UrMsg::MRingGetSize(reply) => {
                let eid = self.register_reply(reply);
                Some(RaftNetworkMsg::Event(
                    eid,
                    mring::ID,
                    mring::Event::get_size(),
                ))
            }
            UrMsg::MRingGetNodes(reply) => {
                let eid = self.register_reply(reply);
                Some(RaftNetworkMsg::Event(
                    eid,
                    mring::ID,
                    mring::Event::get_nodes(),
                ))
            }
            UrMsg::MRingAddNode(node, reply) => {
                let eid = self.register_reply(reply);
                Some(RaftNetworkMsg::Event(
                    eid,
                    mring::ID,
                    mring::Event::add_node(node),
                ))
            }
            UrMsg::MRingRemoveNode(node, reply) => {
                let eid = self.register_reply(reply);
                Some(RaftNetworkMsg::Event(
                    eid,
                    mring::ID,
                    mring::Event::remove_node(node),
                ))
            }
            UrMsg::Status(rid, reply) => Some(Status(rid, reply)),
            UrMsg::Version(rid, reply) => Some(Version(rid, reply)),
            UrMsg::GetNode(id, reply) => Some(GetNode(id, reply)),
            UrMsg::AddNode(id, reply) => Some(AddNode(id, reply)),
            UrMsg::Get(key, reply) => {
                let eid = self.register_reply(reply);
                Some(RaftNetworkMsg::Event(eid, kv::ID, kv::Event::get(key)))
            }
            UrMsg::Put(key, value, reply) => {
                let eid = self.register_reply(reply);
                Some(RaftNetworkMsg::Event(
                    eid,
                    kv::ID,
                    kv::Event::put(key, value),
                ))
            }
            UrMsg::Cas(key, check_value, store_value, reply) => {
                let eid = self.register_reply(reply);
                Some(RaftNetworkMsg::Event(
                    eid,
                    kv::ID,
                    kv::Event::cas(key, check_value, store_value),
                ))
            }
            UrMsg::Delete(key, reply) => {
                let eid = self.register_reply(reply);
                Some(RaftNetworkMsg::Event(eid, kv::ID, kv::Event::delete(key)))
            }
            UrMsg::AckProposal(pid, success) => Some(AckProposal(pid, success)),
            UrMsg::ForwardProposal(from, pid, sid, eid, data) => {
                Some(ForwardProposal(from, pid, sid, eid, data))
            }
            UrMsg::RaftMsg(msg) => Some(RaftMsg(msg)),
            // Connection handling of websocket connections
            // partially based on the problem that actix ws client
            // doens't reconnect
            UrMsg::InitLocal(mut endpoint) => {
                info!(self.logger, "Initializing local endpoint");
                endpoint
                    .send(WsMessage::Ctrl(CtrlMsg::Hello(
                        self.id,
                        self.endpoint.clone(),
                    )))
                    .await
                    .unwrap();
                self.next().await
            }
            UrMsg::RegisterLocal(id, peer, endpoint, peers) => {
                if id != self.id {
                    info!(self.logger, "register(local)"; "remote-id" => id, "remote-peer" => peer, "discovered-peers" => format!("{:?}", peers));
                    self.local_mailboxes.insert(id, endpoint.clone());
                    for (peer_id, peer) in peers {
                        if !self.known_peers.contains_key(&peer_id) {
                            self.known_peers.insert(peer_id, peer.clone());
                            let tx = self.tx.clone();
                            let logger = self.logger.clone();
                            task::spawn(client::remote_endpoint(peer, tx, logger));
                        }
                    }
                }
                self.next().await
            }

            // Reply to hello => sends RegisterLocal
            UrMsg::RegisterRemote(id, peer, endpoint) => {
                if id != self.id {
                    info!(self.logger, "register(remote)"; "remote-id" => id, "remote-peer" => &peer);
                    if !self.known_peers.contains_key(&id) {
                        self.known_peers.insert(id, peer.clone());
                        let tx = self.tx.clone();
                        let logger = self.logger.clone();
                        task::spawn(client::remote_endpoint(peer, tx, logger));
                    }
                    endpoint
                        .clone()
                        .send(WsMessage::Ctrl(CtrlMsg::HelloAck(
                            self.id,
                            self.endpoint.clone(),
                            self.known_peers
                                .clone()
                                .into_iter()
                                .collect::<Vec<(NodeId, String)>>(),
                        )))
                        .await
                        .unwrap();
                    self.remote_mailboxes.insert(id, endpoint.clone());
                }
                self.next().await
            }
            UrMsg::DownLocal(id) => {
                warn!(self.logger, "down(local)"; "id" => id);
                self.local_mailboxes.remove(&id);
                if !self.remote_mailboxes.contains_key(&id) {
                    self.known_peers.remove(&id);
                }
                self.next().await
            }
            UrMsg::DownRemote(id) => {
                warn!(self.logger, "down(remote)"; "id" => id);
                self.remote_mailboxes.remove(&id);
                if !self.local_mailboxes.contains_key(&id) {
                    self.known_peers.remove(&id);
                }
                self.next().await
            }
        }
    }

    async fn ack_proposal(
        &mut self,
        to: NodeId,
        pid: ProposalId,
        success: bool,
    ) -> Result<(), Error> {
        if let Some(remote) = self.local_mailboxes.get_mut(&to) {
            remote
                .send(WsMessage::Ctrl(CtrlMsg::AckProposal(pid, success)))
                .await
                .map_err(|e| Error::Io(io::Error::new(io::ErrorKind::ConnectionAborted, e)))
        } else if let Some(remote) = self.remote_mailboxes.get_mut(&to) {
            remote
                .send(WsMessage::Ctrl(CtrlMsg::AckProposal(pid, success)))
                .await
                .map_err(|e| Error::Io(io::Error::new(io::ErrorKind::ConnectionAborted, e)))
        } else {
            Err(Error::Io(io::Error::new(
                io::ErrorKind::ConnectionAborted,
                format!("send ack proposla to {} fail, let Raft retry it", to),
            )))
        }
    }

    async fn send_msg(&mut self, msg: RaftMessage) -> Result<(), Error> {
        let to = NodeId(msg.to);
        if let Some(remote) = self.local_mailboxes.get_mut(&to) {
            remote
                .send(WsMessage::Raft(msg))
                .await
                .map_err(|e| Error::Io(io::Error::new(io::ErrorKind::ConnectionAborted, e)))
        } else if let Some(remote) = self.remote_mailboxes.get_mut(&to) {
            remote
                .send(WsMessage::Raft(msg))
                .await
                .map_err(|e| Error::Io(io::Error::new(io::ErrorKind::ConnectionAborted, e)))
        } else {
            // Err(Error::NotConnected(to)) this is not an error we'll retry
            Ok(())
        }
    }

    fn connections(&self) -> Vec<NodeId> {
        let mut k1: Vec<NodeId> = self.local_mailboxes.keys().copied().collect();
        let mut k2: Vec<NodeId> = self.remote_mailboxes.keys().copied().collect();
        k1.append(&mut k2);
        k1.sort();
        k1.dedup();
        k1
    }

    async fn forward_proposal(
        &mut self,
        from: NodeId,
        to: NodeId,
        pid: ProposalId,
        sid: ServiceId,
        eid: EventId,
        data: Vec<u8>,
    ) -> Result<(), Error> {
        let msg = WsMessage::Ctrl(CtrlMsg::ForwardProposal(from, pid, sid, eid, data));
        if let Some(remote) = self.local_mailboxes.get_mut(&to) {
            remote
                .send(msg)
                .await
                .map_err(|e| Error::Generic(format!("{}", e)))
        } else if let Some(remote) = self.remote_mailboxes.get_mut(&to) {
            remote
                .send(msg)
                .await
                .map_err(|e| Error::Generic(format!("{}", e)))
        } else {
            Err(Error::NotConnected(to))
        }
    }
}

/// do websocket handshake and start `client::Connection` actor

#[derive(Debug)]
pub enum WsMessage {
    Ctrl(CtrlMsg),
    Raft(RaftMessage),
    Reply(u16, ws_proto::Reply),
}

impl From<CtrlMsg> for WsMessage {
    fn from(m: CtrlMsg) -> Self {
        Self::Ctrl(m)
    }
}

impl From<RaftMessage> for WsMessage {
    fn from(m: RaftMessage) -> Self {
        Self::Raft(m)
    }
}

impl From<ws_proto::Reply> for WsMessage {
    fn from(m: ws_proto::Reply) -> Self {
        Self::Reply(m.code, m)
    }
}

use protobuf::{CodedInputStream, CodedOutputStream, Message};
fn decode_ws(bin: Vec<u8>) -> RaftMessage {
    let mut msg = RaftMessage::default();
    let mut c = Cursor::new(bin);
    let mut is = CodedInputStream::new(&mut c);
    msg.merge_from(&mut is).unwrap();
    msg
}

fn encode_ws(msg: RaftMessage) -> Bytes {
    let mut bs = Vec::new();
    let mut os = CodedOutputStream::new(&mut bs);

    msg.write_to_with_cached_sizes(&mut os).unwrap();
    bs.into()
}

impl Network {
    pub fn new(
        logger: &Logger,
        id: NodeId,
        ws_endpoint: &str,
        rest_endpoint: Option<&str>,
        peers: Vec<String>,
        pubsub: pubsub::Channel,
    ) -> Self {
        let (tx, rx) = unbounded();

        for peer in peers {
            let logger = logger.clone();
            let tx = tx.clone();
            task::spawn(client::remote_endpoint(peer, tx, logger));
        }

        let node = Node {
            tx: tx.clone(),
            _id: id,
            logger: logger.clone(),
            pubsub: pubsub.clone(),
        };

        let endpoint = ws_endpoint.to_string();

        task::spawn(server::run(logger.clone(), node.clone(), endpoint.clone()));

        if let Some(rest_endpoint) = rest_endpoint {
            error!(logger, "ENDPOINT: {}", rest_endpoint);
            let rest_endpoint = rest_endpoint.to_string();
            task::spawn(rest::run(logger.clone(), node, rest_endpoint));
        }

        let net_handler = crate::protocol::network::Handler::new(tx.clone());
        let mut net_interceptor = protocol_driver::Interceptor::new(net_handler);

        let kv_handler = crate::protocol::kv::Handler::default();
        let mut kv_interceptor = protocol_driver::Interceptor::new(kv_handler);
        kv_interceptor.connect_next(&mut net_interceptor);
        let status_handler = crate::protocol::status::Handler::default();
        let mut status_interceptor = protocol_driver::Interceptor::new(status_handler);
        status_interceptor.connect_next(&mut net_interceptor);

        let ps_handler = crate::protocol::pubsub::Handler::new(pubsub);
        let ps_interceptor = protocol_driver::Interceptor::new(ps_handler);

        let mut driver = protocol_driver::Driver::default();

        driver.register_handler("kv", kv_interceptor.tx.clone());
        driver.register_handler("pubsub", ps_interceptor.tx.clone());
        driver.register_handler("status", status_interceptor.tx.clone());

        task::spawn(net_interceptor.run_loop());
        task::spawn(kv_interceptor.run_loop());
        task::spawn(ps_interceptor.run_loop());
        task::spawn(status_interceptor.run_loop());

        let driver_tx = driver.transport_tx.clone();
        task::spawn(driver.run_loop());

        task::spawn(server2::run(
            logger.clone(),
            driver_tx,
            "localhost:1234".to_string(),
        ));

        Self {
            id,
            endpoint,
            logger: logger.clone(),
            local_mailboxes: HashMap::new(),
            remote_mailboxes: HashMap::new(),
            known_peers: HashMap::new(),
            rx,
            tx,
            next_eid: 1,
            pending: HashMap::new(),
            prot_pending: HashMap::new(),
        }
    }
    fn register_reply(&mut self, reply: Reply) -> EventId {
        let eid = EventId(self.next_eid);
        self.next_eid += 1;
        self.pending.insert(eid, reply);
        eid
    }

    fn register_prot_reply(
        &mut self,
        rid: RequestId,
        reply: protocol_driver::HandlerOutboundChannelSender,
    ) -> EventId {
        let eid = EventId(self.next_eid);
        self.next_eid += 1;
        self.prot_pending.insert(eid, (rid, reply));
        eid
    }
}
