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

pub mod ws;

use crate::network::ws::WsMessage;
use crate::*;
use async_trait::async_trait;
use futures::channel::mpsc::{Sender, TryRecvError};
use raft::eraftpb::Message as RaftMessage;
use std::{fmt, io};

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

pub enum RaftNetworkMsg {
    Status(RequestId, Sender<WsMessage>),
    Version(RequestId, Sender<WsMessage>), // FIXME normalize to WS for now to work with both rest/ws

    // Raft related
    AckProposal(ProposalId, bool),
    ForwardProposal(NodeId, ProposalId, ServiceId, EventId, Vec<u8>),
    GetNode(NodeId, Sender<bool>),
    AddNode(NodeId, Sender<bool>),

    Event(EventId, ServiceId, Vec<u8>),
    RaftMsg(RaftMessage),
}

pub enum TryNextError {
    Done,
    Error(TryRecvError),
}

#[async_trait]
pub trait Network: Send + Sync {
    async fn next(&mut self) -> Option<RaftNetworkMsg>;
    async fn ack_proposal(
        &mut self,
        to: NodeId,
        pid: ProposalId,
        success: bool,
    ) -> Result<(), Error>;
    async fn event_reply(&mut self, id: EventId, code: u16, reply: Vec<u8>) -> Result<(), Error>;
    async fn send_msg(&mut self, msg: RaftMessage) -> Result<(), Error>;
    fn connections(&self) -> Vec<NodeId>;
    async fn forward_proposal(
        &mut self,
        from: NodeId,
        to: NodeId,
        pid: ProposalId,
        sid: ServiceId,
        eid: EventId,
        data: Vec<u8>,
    ) -> Result<(), Error>;
}

#[derive(Default)]
pub struct NullNetwork {}

#[async_trait]
impl Network for NullNetwork {
    async fn next(&mut self) -> Option<RaftNetworkMsg> {
        unimplemented!()
    }
    async fn ack_proposal(
        &mut self,
        _to: NodeId,
        _pid: ProposalId,
        _success: bool,
    ) -> Result<(), network::Error> {
        unimplemented!()
    }
    async fn event_reply(
        &mut self,
        _id: EventId,
        _code: u16,
        _reply: Vec<u8>,
    ) -> Result<(), network::Error> {
        unimplemented!()
    }
    async fn send_msg(&mut self, _msg: RaftMessage) -> Result<(), network::Error> {
        unimplemented!()
    }
    fn connections(&self) -> Vec<NodeId> {
        unimplemented!()
    }
    async fn forward_proposal(
        &mut self,
        _from: NodeId,
        _to: NodeId,
        _pid: ProposalId,
        _sid: ServiceId,
        _eid: EventId,
        _data: Vec<u8>,
    ) -> Result<(), network::Error> {
        unimplemented!()
    }
}
