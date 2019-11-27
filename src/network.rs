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

use crate::*;
use async_trait::async_trait;
use futures::channel::mpsc::{TryRecvError, UnboundedSender};
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
    Status(UnboundedSender<raft_node::RaftNodeStatus>),
    // Raft related
    AckProposal(ProposalId, bool),
    ForwardProposal(NodeId, ProposalId, ServiceId, EventId, Vec<u8>),
    GetNode(NodeId, UnboundedSender<bool>),
    AddNode(NodeId, UnboundedSender<bool>),

    Event(EventId, ServiceId, Vec<u8>),
    RaftMsg(RaftMessage),
}

pub enum TryNextError {
    Done,
    Error(TryRecvError),
}

#[async_trait]
pub trait Network {
    async fn next(&mut self) -> Option<RaftNetworkMsg>;
    async fn ack_proposal(&self, to: NodeId, pid: ProposalId, success: bool) -> Result<(), Error>;
    async fn event_reply(&mut self, id: EventId, reply: Option<Vec<u8>>) -> Result<(), Error>;
    async fn send_msg(&self, msg: RaftMessage) -> Result<(), Error>;
    fn connections(&self) -> Vec<NodeId>;
    async fn forward_proposal(
        &self,
        from: NodeId,
        to: NodeId,
        pid: ProposalId,
        sid: ServiceId,
        eid: EventId,
        data: Vec<u8>,
    ) -> Result<(), Error>;
}
