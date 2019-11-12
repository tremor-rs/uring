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

pub trait Network: Default {
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
    pub local_mailboxes: LocalMailboxes,
    pub remote_mailboxes: RemoteMailboxes,
}

impl Default for WsNetwork {
    fn default() -> Self {
        Self {
            local_mailboxes: HashMap::new(),
            remote_mailboxes: HashMap::new(),
        }
    }
}

impl Network for WsNetwork {
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
            Err(Error::NotConnected(to))
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
