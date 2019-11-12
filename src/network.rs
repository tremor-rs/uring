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

use crate::*;
use actix::Addr;
use raft::eraftpb::Message;
use std::collections::HashMap;
use std::fmt;
use std::io;

type LocalMailboxes = HashMap<NodeId, Addr<WsOfframpWorker>>;
type RemoteMailboxes = HashMap<NodeId, Addr<UrSocket>>;

#[derive(Debug)]
pub enum Error {
    Io(io::Error),
    Generic(String),
    NotConnected(NodeId),
}
impl std::error::Error for Error {}
impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{:?}", self)
    }
}

/*
    // Network related
    InitLocal(Addr<WsOfframpWorker>),
    RegisterLocal(NodeId, String, Addr<WsOfframpWorker>, Vec<(NodeId, String)>),
    RegisterRemote(NodeId, String, Addr<UrSocket>),
    DownLocal(NodeId),
    DownRemote(NodeId),

*/
pub enum RaftNetworkMsg {
    // Raft related
    AckProposal(u64, bool),
    ForwardProposal(NodeId, u64, Vec<u8>, Vec<u8>),
    GetNode(NodeId, Sender<bool>),
    AddNode(NodeId, Sender<bool>),

    Get(Vec<u8>, Sender<Option<Vec<u8>>>),
    Post(Vec<u8>, Vec<u8>, Sender<bool>),

    RaftMsg(RaftMessage),
}

pub trait Network {
    fn try_recv(&mut self) -> Result<RaftNetworkMsg, TryRecvError>;
    fn ack_proposal(&self, to: NodeId, pid: u64, success: bool) -> Result<(), Error>;
    fn send_msg(&self, msg: Message) -> Result<(), Error>;
    fn connections(&self) -> Vec<NodeId>;
    fn forward_proposal(
        &self,
        from: NodeId,
        to: NodeId,
        pid: u64,
        key: Vec<u8>,
        value: Vec<u8>,
    ) -> Result<(), Error>;
}

pub struct WsNetwork {
    id: NodeId,
    pub local_mailboxes: LocalMailboxes,
    pub remote_mailboxes: RemoteMailboxes,
    pub known_peers: HashMap<NodeId, String>,
    endpoint: String,
    logger: Logger,
    rx: Receiver<UrMsg>,
    tx: Sender<UrMsg>,
}

impl WsNetwork {
    pub fn new(
        logger: &Logger,
        id: NodeId,
        endpoint: &str,
        rx: Receiver<UrMsg>,
        tx: Sender<UrMsg>,
    ) -> Self {
        Self {
            id,
            endpoint: endpoint.to_string(),
            logger: logger.clone(),
            local_mailboxes: HashMap::new(),
            remote_mailboxes: HashMap::new(),
            known_peers: HashMap::new(),
            rx,
            tx,
        }
    }
}

impl Network for WsNetwork {
    fn try_recv(&mut self) -> Result<RaftNetworkMsg, TryRecvError> {
        use RaftNetworkMsg::*;
        match self.rx.try_recv() {
            Ok(UrMsg::GetNode(id, reply)) => Ok(GetNode(id, reply)),
            Ok(UrMsg::AddNode(id, reply)) => Ok(AddNode(id, reply)),
            Ok(UrMsg::Get(key, reply)) => Ok(Get(key, reply)),
            Ok(UrMsg::Post(key, value, reply)) => Ok(Post(key, value, reply)),
            Ok(UrMsg::AckProposal(pid, success)) => Ok(AckProposal(pid, success)),
            Ok(UrMsg::ForwardProposal(from, pid, key, value)) => {
                Ok(ForwardProposal(from, pid, key, value))
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
                            thread::spawn(move || remote_endpoint(peer, tx, logger));
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
                        thread::spawn(move || remote_endpoint(peer, tx, logger));
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
    fn ack_proposal(&self, to: NodeId, pid: u64, success: bool) -> Result<(), Error> {
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

    fn send_msg(&self, msg: Message) -> Result<(), Error> {
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
        pid: u64,
        key: Vec<u8>,
        value: Vec<u8>,
    ) -> Result<(), Error> {
        let msg = WsMessage::Msg(CtrlMsg::ForwardProposal(from, pid, key, value));
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
