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

mod client;
mod rest;
mod server;
use super::{Error, EventId, Network as NetworkTrait, ProposalId, RaftNetworkMsg, ServiceId};
use crate::raft_node::RaftNodeStatus;
use crate::service::kv::{Event as KVEvent, KV_SERVICE};
use crate::service::vnode::{Event as VNodeEvent, VNODE_SERVICE};
use crate::NodeId;
use actix::prelude::*;
use actix_web::{middleware, web, App, HttpServer};
use bytes::Bytes;
use crossbeam_channel::{bounded, Receiver, Sender, TryRecvError};
use futures::Future;
use raft::eraftpb::Message as RaftMessage;
use serde::{Deserialize, Serialize};
use slog::Logger;
use std::collections::HashMap;
use std::io;
use std::thread::{self, JoinHandle};
use std::time::{Duration, Instant};

type LocalMailboxes = HashMap<NodeId, Addr<client::Connection>>;
type RemoteMailboxes = HashMap<NodeId, Addr<server::Connection>>;

/// How often heartbeat pings are sent
const HEARTBEAT_INTERVAL: Duration = Duration::from_secs(5);
/// How long before lack of client response causes a timeout
const CLIENT_TIMEOUT: Duration = Duration::from_secs(10);

#[derive(Clone)]
pub(crate) struct Node {
    id: NodeId,
    tx: Sender<UrMsg>,
    logger: Logger,
}

pub struct Network {
    id: NodeId,
    local_mailboxes: LocalMailboxes,
    remote_mailboxes: RemoteMailboxes,
    known_peers: HashMap<NodeId, String>,
    endpoint: String,
    logger: Logger,
    rx: Receiver<UrMsg>,
    tx: Sender<UrMsg>,
    next_eid: u64,
    pending: HashMap<EventId, Sender<Option<Vec<u8>>>>,
}

#[derive(Serialize, Deserialize, Debug)]
pub enum CtrlMsg {
    Hello(NodeId, String),
    HelloAck(NodeId, String, Vec<(NodeId, String)>),
    AckProposal(ProposalId, bool),
    ForwardProposal(NodeId, ProposalId, ServiceId, Vec<u8>),
}

#[derive(Serialize, Deserialize, Debug, Clone, Copy)]
pub enum Protocol {
    URing,
    KV,
}

#[derive(Serialize, Deserialize, Debug, Clone, Copy)]
pub enum ProtocolSelect {
    Select(u64, Protocol),
    Selected(u64, Protocol),
}

pub(crate) enum UrMsg {
    // Network related
    InitLocal(Addr<client::Connection>),
    RegisterLocal(
        NodeId,
        String,
        Addr<client::Connection>,
        Vec<(NodeId, String)>,
    ),
    RegisterRemote(NodeId, String, Addr<server::Connection>),
    DownLocal(NodeId),
    DownRemote(NodeId),
    Status(Sender<RaftNodeStatus>),

    // Raft related
    AckProposal(ProposalId, bool),
    ForwardProposal(NodeId, ProposalId, ServiceId, Vec<u8>),
    RaftMsg(RaftMessage),
    GetNode(NodeId, Sender<bool>),
    AddNode(NodeId, Sender<bool>),

    // KV related
    Get(Vec<u8>, Sender<Option<Vec<u8>>>),
    Post(Vec<u8>, Vec<u8>, Sender<Option<Vec<u8>>>),
    Cas(Vec<u8>, Vec<u8>, Vec<u8>, Sender<Option<Vec<u8>>>),
    Delete(Vec<u8>, Sender<Option<Vec<u8>>>),

    // VNode
    MRingSetSize(u64, Sender<Option<Vec<u8>>>),
    MRingGetSize(Sender<Option<Vec<u8>>>),
    MRingGetNodes(Sender<Option<Vec<u8>>>),
    MRingAddNode(String, Sender<Option<Vec<u8>>>),
    MRingRemoveNode(String, Sender<Option<Vec<u8>>>),
}

impl Network {
    pub fn new(
        logger: &Logger,
        id: NodeId,
        endpoint: &str,
        peers: Vec<String>,
    ) -> (JoinHandle<Result<(), io::Error>>, Self) {
        let (tx, rx) = bounded(100);

        for peer in peers {
            let logger = logger.clone();
            let tx = tx.clone();
            thread::spawn(move || client::remote_endpoint(peer, tx, logger));
        }

        let node = Node {
            tx: tx.clone(),
            id,
            logger: logger.clone(),
        };

        let endpoint = endpoint.to_string();
        let t_endpoint = endpoint.clone();

        let handle = thread::spawn(move || {
            HttpServer::new(move || {
                App::new()
                    .data(node.clone())
                    // enable logger
                    .wrap(middleware::Logger::default())
                    .service(web::resource("/status").route(web::get().to(rest::status)))
                    .service(
                        web::resource("/data/{id}")
                            .route(web::get().to(rest::get))
                            .route(web::post().to(rest::post))
                            .route(web::delete().to(rest::delete)),
                    )
                    .service(
                        web::resource("/data/{id}/cas") // FIXME use query param instead
                            .route(web::post().to(rest::cas)),
                    )
                    .service(
                        web::resource("/node/{id}")
                            .route(web::get().to(rest::get_node))
                            .route(web::post().to(rest::post_node)),
                    )
                    .service(
                        web::resource("/mring")
                            .route(web::get().to(rest::get_mring_size))
                            .route(web::post().to(rest::set_mring_size)),
                    )
                    .service(
                        web::resource("/mring/node")
                            .route(web::get().to(rest::get_mring_nodes))
                            .route(web::post().to(rest::add_mring_node)),
                    )
                    // websocket route
                    .service(web::resource("/uring").route(web::get().to(rest::uring_index)))
            })
            // start http server on 127.0.0.1:8080
            .bind(t_endpoint)?
            .run()
        });

        (
            handle,
            Self {
                id,
                endpoint,
                logger: logger.clone(),
                local_mailboxes: HashMap::new(),
                remote_mailboxes: HashMap::new(),
                known_peers: HashMap::new(),
                rx,
                tx,
                next_eid: 0,
                pending: HashMap::new(),
            },
        )
    }
    fn register_reply(&mut self, reply: Sender<Option<Vec<u8>>>) -> EventId {
        let eid = EventId(self.next_eid);
        self.next_eid += 1;
        self.pending.insert(eid, reply);
        eid
    }
}
#[derive(Message)]
pub(crate) struct RaftMsg(RaftMessage);

impl NetworkTrait for Network {
    fn event_reply(&mut self, id: EventId, reply: Option<Vec<u8>>) -> Result<(), Error> {
        if let Some(sender) = self.pending.remove(&id) {
            sender.send(reply).unwrap();
        }
        Ok(())
    }

    fn try_recv(&mut self) -> Result<RaftNetworkMsg, TryRecvError> {
        use RaftNetworkMsg::*;
        match self.rx.try_recv() {
            Ok(UrMsg::MRingSetSize(size, reply)) => {
                let eid = self.register_reply(reply);
                Ok(RaftNetworkMsg::Event(
                    eid,
                    VNODE_SERVICE,
                    VNodeEvent::set_size(size),
                ))
            }
            Ok(UrMsg::MRingGetSize(reply)) => {
                let eid = self.register_reply(reply);
                Ok(RaftNetworkMsg::Event(
                    eid,
                    VNODE_SERVICE,
                    VNodeEvent::get_size(),
                ))
            }
            Ok(UrMsg::MRingGetNodes(reply)) => {
                let eid = self.register_reply(reply);
                Ok(RaftNetworkMsg::Event(
                    eid,
                    VNODE_SERVICE,
                    VNodeEvent::get_nodes(),
                ))
            }
            Ok(UrMsg::MRingAddNode(node, reply)) => {
                let eid = self.register_reply(reply);
                Ok(RaftNetworkMsg::Event(
                    eid,
                    VNODE_SERVICE,
                    VNodeEvent::add_node(node),
                ))
            }
            Ok(UrMsg::MRingRemoveNode(node, reply)) => {
                let eid = self.register_reply(reply);
                Ok(RaftNetworkMsg::Event(
                    eid,
                    VNODE_SERVICE,
                    VNodeEvent::remove_node(node),
                ))
            }
            Ok(UrMsg::Status(reply)) => Ok(Status(reply)),
            Ok(UrMsg::GetNode(id, reply)) => Ok(GetNode(id, reply)),
            Ok(UrMsg::AddNode(id, reply)) => Ok(AddNode(id, reply)),
            Ok(UrMsg::Get(key, reply)) => {
                let eid = self.register_reply(reply);
                Ok(RaftNetworkMsg::Event(eid, KV_SERVICE, KVEvent::get(key)))
            }
            Ok(UrMsg::Post(key, value, reply)) => {
                let eid = self.register_reply(reply);
                Ok(RaftNetworkMsg::Event(
                    eid,
                    KV_SERVICE,
                    KVEvent::post(key, value),
                ))
            }
            Ok(UrMsg::Cas(key, check_value, store_value, reply)) => {
                let eid = self.register_reply(reply);
                Ok(RaftNetworkMsg::Event(
                    eid,
                    KV_SERVICE,
                    KVEvent::cas(key, check_value, store_value),
                ))
            }
            Ok(UrMsg::Delete(key, reply)) => {
                let eid = EventId(self.next_eid);
                self.next_eid += 1;
                self.pending.insert(eid, reply);
                Ok(RaftNetworkMsg::Event(eid, KV_SERVICE, KVEvent::delete(key)))
            }
            Ok(UrMsg::AckProposal(pid, success)) => Ok(AckProposal(pid, success)),
            Ok(UrMsg::ForwardProposal(from, pid, sid, data)) => {
                Ok(ForwardProposal(from, pid, sid, data))
            }
            Ok(UrMsg::RaftMsg(msg)) => Ok(RaftMsg(msg)),
            // Connection handling of websocket connections
            // partially based on the problem that actix ws client
            // doens't reconnect
            Ok(UrMsg::InitLocal(endpoint)) => {
                info!(self.logger, "Initializing local endpoint");
                endpoint
                    .send(WsMessage::Msg(CtrlMsg::Hello(
                        self.id,
                        self.endpoint.clone(),
                    )))
                    .wait()
                    .unwrap();
                self.try_recv()
            }
            Ok(UrMsg::RegisterLocal(id, peer, endpoint, peers)) => {
                if id != self.id {
                    info!(self.logger, "register(local)"; "remote-id" => id, "remote-peer" => peer, "discovered-peers" => format!("{:?}", peers));
                    self.local_mailboxes.insert(id, endpoint.clone());
                    for (peer_id, peer) in peers {
                        if !self.known_peers.contains_key(&peer_id) {
                            self.known_peers.insert(peer_id, peer.clone());
                            let tx = self.tx.clone();
                            let logger = self.logger.clone();
                            thread::spawn(move || client::remote_endpoint(peer, tx, logger));
                        }
                    }
                }
                self.try_recv()
            }

            // Reply to hello => sends RegisterLocal
            Ok(UrMsg::RegisterRemote(id, peer, endpoint)) => {
                if id != self.id {
                    info!(self.logger, "register(remote)"; "remote-id" => id, "remote-peer" => &peer);
                    if !self.known_peers.contains_key(&id) {
                        self.known_peers.insert(id, peer.clone());
                        let tx = self.tx.clone();
                        let logger = self.logger.clone();
                        thread::spawn(move || client::remote_endpoint(peer, tx, logger));
                    }
                    endpoint
                        .clone()
                        .send(WsMessage::Msg(CtrlMsg::HelloAck(
                            self.id,
                            self.endpoint.clone(),
                            self.known_peers
                                .clone()
                                .into_iter()
                                .collect::<Vec<(NodeId, String)>>(),
                        )))
                        .wait()
                        .unwrap();
                    self.remote_mailboxes.insert(id, endpoint.clone());
                }
                self.try_recv()
            }
            Ok(UrMsg::DownLocal(id)) => {
                warn!(self.logger, "down(local)"; "id" => id);
                self.local_mailboxes.remove(&id);
                if !self.remote_mailboxes.contains_key(&id) {
                    self.known_peers.remove(&id);
                }
                self.try_recv()
            }
            Ok(UrMsg::DownRemote(id)) => {
                warn!(self.logger, "down(remote)"; "id" => id);
                self.remote_mailboxes.remove(&id);
                if !self.local_mailboxes.contains_key(&id) {
                    self.known_peers.remove(&id);
                }
                self.try_recv()
            }
            Err(e) => Err(e),
        }
    }
    fn ack_proposal(&self, to: NodeId, pid: ProposalId, success: bool) -> Result<(), Error> {
        if let Some(remote) = self.local_mailboxes.get(&to) {
            remote
                .send(WsMessage::Msg(CtrlMsg::AckProposal(pid, success)))
                .wait()
                .map_err(|e| Error::Io(io::Error::new(io::ErrorKind::ConnectionAborted, e)))
        } else if let Some(remote) = self.remote_mailboxes.get(&to) {
            remote
                .send(WsMessage::Msg(CtrlMsg::AckProposal(pid, success)))
                .wait()
                .map_err(|e| Error::Io(io::Error::new(io::ErrorKind::ConnectionAborted, e)))
        } else {
            Err(Error::Io(io::Error::new(
                io::ErrorKind::ConnectionAborted,
                format!("send ack proposla to {} fail, let Raft retry it", to),
            )))
        }
    }

    fn send_msg(&self, msg: RaftMessage) -> Result<(), Error> {
        let to = NodeId(msg.to);
        if let Some(remote) = self.local_mailboxes.get(&to) {
            remote
                .send(RaftMsg(msg))
                .wait()
                .map_err(|e| Error::Io(io::Error::new(io::ErrorKind::ConnectionAborted, e)))
        } else if let Some(remote) = self.remote_mailboxes.get(&to) {
            remote
                .send(RaftMsg(msg))
                .wait()
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

    fn forward_proposal(
        &self,
        from: NodeId,
        to: NodeId,
        pid: ProposalId,
        sid: ServiceId,
        data: Vec<u8>,
    ) -> Result<(), Error> {
        let msg = WsMessage::Msg(CtrlMsg::ForwardProposal(from, pid, sid, data));
        if let Some(remote) = self.local_mailboxes.get(&to) {
            remote
                .send(msg)
                .wait()
                .map_err(|e| Error::Generic(format!("{}", e)))
        } else if let Some(remote) = self.remote_mailboxes.get(&to) {
            remote
                .send(msg)
                .wait()
                .map_err(|e| Error::Generic(format!("{}", e)))
        } else {
            Err(Error::NotConnected(to))
        }
    }
}

/// do websocket handshake and start `client::Connection` actor

#[derive(Message)]
pub enum WsMessage {
    Msg(CtrlMsg),
}

#[cfg(feature = "json-proto")]
fn decode_ws(bin: &[u8]) -> RaftMessage {
    let msg: crate::codec::json::Event = serde_json::from_slice(bin).unwrap();
    msg.into()
}

#[cfg(not(feature = "json-proto"))]
fn decode_ws(bin: &[u8]) -> RaftMessage {
    use protobuf::Message;
    let mut msg = RaftMessage::default();
    msg.merge_from_bytes(bin).unwrap();
    msg
}

#[cfg(feature = "json-proto")]
fn encode_ws(msg: RaftMsg) -> Bytes {
    let data: crate::codec::json::Event = msg.0.clone().into();
    let data = serde_json::to_string_pretty(&data);
    data.unwrap().into()
}

#[cfg(not(feature = "json-proto"))]
fn encode_ws(msg: RaftMsg) -> Bytes {
    use protobuf::Message;
    msg.0.write_to_bytes().unwrap().into()
}
