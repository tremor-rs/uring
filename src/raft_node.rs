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

use super::*;
use crate::network::ws::WsMessage;
use crate::storage::*;
use crate::version::VERSION;
use async_std::sync::Mutex;
use futures::SinkExt;
use protobuf::Message as PBMessage;
use raft::eraftpb::ConfState;
use raft::eraftpb::Message;
use raft::{prelude::*, Error, Result, StateRole};
use serde_derive::{Deserialize, Serialize};
use slog::Logger;
use std::collections::{HashMap, VecDeque};
use std::fmt;
use std::io::{Error as IoError, ErrorKind as IoErrorKind};

fn example_config() -> Config {
    Config {
        election_tick: 10,
        heartbeat_tick: 3,
        pre_vote: true,
        ..Default::default()
    }
}

// The message can be used to initialize a raft node or not.
fn is_initial_msg(msg: &Message) -> bool {
    let msg_type = msg.get_msg_type();
    msg_type == MessageType::MsgRequestVote
        || msg_type == MessageType::MsgRequestPreVote
        || (msg_type == MessageType::MsgHeartbeat && msg.commit == 0)
}

#[derive(Debug, Serialize, Deserialize, PartialEq, Clone)]
pub struct RaftNodeStatus {
    id: u64,
    role: String,
    promotable: bool,
    pass_election_timeout: bool,
    election_elapsed: usize,
    randomized_election_timeout: usize,
    term: u64,
    last_index: u64,
}

// unsafe impl Send for RaftNodeStatus {}
// unsafe impl Sync for RaftNodeStatus {}

pub struct RaftNode<Storage, Network>
where
    Storage: storage::Storage,
    Network: network::Network,
{
    logger: Logger,
    // None if the raft is not initialized.
    id: NodeId,
    pub raft_group: Option<Mutex<RawNode<Storage>>>,
    network: Network,
    proposals: VecDeque<Proposal>,
    pending_proposals: HashMap<ProposalId, Proposal>,
    pending_acks: HashMap<ProposalId, EventId>,
    proposal_id: u64,
    tick_duration: Duration,
    services: HashMap<ServiceId, Box<dyn Service<Storage>>>,
    pub pubsub: pubsub::Channel,
    last_state: StateRole,
}

impl<Storage, Network> fmt::Debug for RaftNode<Storage, Network>
where
    Storage: storage::Storage + Send + Sync,
    Network: network::Network,
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if let Some(g) = self.raft_group.as_ref() {
            let role = self.role();
            let g = g.try_lock().unwrap();
            write!(
                f,
                r#"node id: {}
role: {:?}
promotable: {}
pass election timeout: {}
election elapsed: {}
randomized election timeout: {}
leader id: {}
term: {}
last index: {}
voted for: {}
votes: {:?}
nodes: {:?}
connections: {:?}
            "#,
                &g.raft.id,
                role,
                g.raft.promotable(),
                g.raft.pass_election_timeout(),
                g.raft.election_elapsed,
                g.raft.randomized_election_timeout(),
                &g.raft.leader_id,
                &g.raft.term,
                &g.raft.store().last_index().unwrap_or(0),
                &g.raft.vote,
                &g.raft.prs().votes(),
                &g.raft.prs().conf().voters(),
                &self.network.connections(),
            )
        } else {
            write!(f, "UNINITIALIZED")
        }
    }
}

impl<Storage, Network> RaftNode<Storage, Network>
where
    Storage: storage::Storage + Send + Sync,
    Network: network::Network,
{
    pub fn add_service(&mut self, sid: ServiceId, service: Box<dyn Service<Storage>>) {
        self.services.insert(sid, service);
    }
    pub fn pubsub(&mut self) -> &mut pubsub::Channel {
        &mut self.pubsub
    }
    pub async fn node_loop(&mut self) -> Result<()> {
        let mut ticks = async_std::stream::interval(self.tick_duration);
        let mut i = Instant::now();

        loop {
            select! {
                msg = self.network.next().fuse() => {
                    let msg = if let Some(msg) = msg {
                        msg
                    } else {
                        break;
                    };
                    let raft = self.raft_group.as_ref().unwrap();
                    match msg {
                        RaftNetworkMsg::Status(rid, mut reply) => {
                            info!(self.logger, "Getting node status");
                            reply.send(WsMessage::Reply(200, ws_proto::Reply { code: 200, rid, data: serde_json::to_value(status(raft).await.unwrap()).unwrap() })).await.unwrap();
                        }
                        RaftNetworkMsg::Version(rid, mut reply) => {
                            info!(self.logger, "Getting version");
                            reply.send(WsMessage::Reply(200, ws_proto::Reply { code: 200, rid, data: serde_json::to_value(self.version()).unwrap() })).await.unwrap();
                        }
                        RaftNetworkMsg::Event(eid, sid, data) => {
                            if let Some(service) = self.services.get_mut(&sid) {
                                if service.is_local(&data).unwrap() {
                                    let (code, value) = service.execute(raft, &mut self.pubsub, data).await.unwrap();
                                    self.network.event_reply(eid, code, value).await.unwrap();
                                } else {
                                    let pid = self.next_pid();
                                    let from = self.id;
                                    if let Err(e) = self.propose_event(from, pid, sid, eid, data).await {
                                        error!(self.logger, "Post forward error: {}", e);
                                        self.network.event_reply(eid, 500, serde_json::to_vec(&format!("{}", e)).unwrap()).await.unwrap();
                                    } else {
                                        self.pending_acks.insert(pid, eid);
                                    }
                                }
                            } else {
                                error!(self.logger, "Unknown Service: {}", sid);
                                self.network.event_reply(eid, 500, serde_json::to_vec(&format!("Service {} not known", sid)).unwrap()).await.unwrap();
                            }
                        }
                        RaftNetworkMsg::GetNode(id, mut reply) => {
                            info!(self.logger, "Getting node status"; "id" => id);
                            reply.send(self.node_known(id).await).await.unwrap();
                        }
                        RaftNetworkMsg::AddNode(id, mut reply) => {
                            info!(self.logger, "Adding node"; "id" => id);
                            reply.send(self.add_node(id).await).await.unwrap();
                        }
                        RaftNetworkMsg::AckProposal(pid, success) => {
                            info!(self.logger, "proposal acknowledged"; "pid" => pid);
                            if let Some(proposal) = self.pending_proposals.remove(&pid) {
                                if !success {
                                    self.proposals.push_back(proposal)
                                }
                            }
                            if let Some(_eid) = self.pending_acks.remove(&pid) {
                                //self.network.event_reply(eid, Some(vec![skilled])).await.unwrap();
                            }
                        }
                        RaftNetworkMsg::ForwardProposal(from, pid, sid, eid, data) => {
                            if let Err(e) = self.propose_event(from, pid, sid, eid, data).await {
                                error!(self.logger, "Proposal forward error: {}", e);
                            }
                        }

                        // RAFT
                        RaftNetworkMsg::RaftMsg(msg) => {
                            if let Err(e) = self.step(msg).await {
                                error!(self.logger, "step error"; "error" => format!("{}", e));
                            }
                        }
                    }
                },
                _tick = ticks.next().fuse() => {
                    if !self.is_running() {
                        continue
                    }
                    if i.elapsed() >= Duration::from_secs(10) {
                        self.log().await;
                        i = Instant::now();
                    }


                    let this_state = self.role();
                    if this_state != self.last_state {
                        let prev_state = format!("{:?}", self.last_state);
                        let next_state = format!("{:?}", this_state);
                        debug!(&self.logger, "State transition"; "last-state" => prev_state.clone(), "next-state" => next_state.clone());
                        self.pubsub
                            .send(pubsub::Msg::new(
                                "uring",
                                PSURing::StateChange {
                                    prev_state,
                                    next_state,
                                    node: self.id,
                                },
                            )).await
                            .unwrap();
                        self.last_state = this_state;
                    }
                    self.raft_group.as_mut().unwrap().try_lock().unwrap().tick();
                    self.on_ready().await.unwrap();
                    if self.is_leader() {
                        // Handle new proposals.
                        self.propose_all().await?;
                    }
                }
            }
        }
        Ok(())
    }

    pub async fn propose_event(
        &mut self,
        from: NodeId,
        pid: ProposalId,
        sid: ServiceId,
        eid: EventId,
        data: Vec<u8>,
    ) -> Result<()> {
        self.pubsub
            .send(pubsub::Msg::new(
                "uring",
                PSURing::ProposalReceived {
                    from,
                    pid,
                    sid,
                    eid,
                    node: self.id,
                },
            ))
            .await
            .unwrap();
        if self.is_leader() {
            self.proposals
                .push_back(Proposal::normal(pid, from, eid, sid, data));
            Ok(())
        } else {
            self.network
                .forward_proposal(from, self.leader(), pid, sid, eid, data)
                .await
                .map_err(|e| {
                    Error::Io(IoError::new(
                        IoErrorKind::ConnectionAborted,
                        format!("{}", e),
                    ))
                })
        }
    }

    pub async fn add_node(&mut self, id: NodeId) -> bool {
        if self.is_leader() && !self.node_known(id).await {
            self.pubsub
                .send(pubsub::Msg::new(
                    "uring",
                    PSURing::AddNode {
                        new_node: id,
                        node: self.id,
                    },
                ))
                .await
                .unwrap();
            let mut conf_change = ConfChange::default();
            conf_change.node_id = id.0;
            conf_change.set_change_type(ConfChangeType::AddNode);
            let pid = self.next_pid();
            let proposal = Proposal::conf_change(pid, self.id, &conf_change);

            self.proposals.push_back(proposal);
            true
        } else {
            false
        }
    }

    pub fn next_pid(&mut self) -> ProposalId {
        let pid = self.proposal_id;
        self.proposal_id += 1;
        ProposalId(pid)
    }

    pub fn version(&self) -> String {
        VERSION.to_string()
    }

    pub async fn node_known(&self, id: NodeId) -> bool {
        if let Some(ref g) = self.raft_group {
            g.try_lock()
                .unwrap()
                .raft
                .prs()
                .conf()
                .voters()
                .contains(id.0)
        } else {
            false
        }
    }
    pub async fn log(&self) {
        if let Some(g) = self.raft_group.as_ref() {
            let role = self.role();
            let g = g.try_lock().unwrap();
            let raft = &g.raft;
            info!(
                self.logger,
                "NODE STATE";
                "node-id" => &raft.id,
                "role" => format!("{:?}", role),

                "leader-id" => raft.leader_id,
                "term" => raft.term,
                "first-index" => raft.raft_log.store.first_index().unwrap_or(0),
                "last-index" => raft.raft_log.store.last_index().unwrap_or(0),

                "vote" => raft.vote,
                "votes" => format!("{:?}", raft.prs().votes()),

                "voters" => format!("{:?}", raft.prs().conf().voters()),

                "promotable" => raft.promotable(),
                "pass-election-timeout" => raft.pass_election_timeout(),
                "election-elapsed" => raft.election_elapsed,
                "randomized-election-timeout" => raft.randomized_election_timeout(),

                "connections" => format!("{:?}", &self.network.connections()),
            )
        } else {
            error!(self.logger, "UNINITIALIZED NODE {}", self.id)
        }
    }

    pub fn is_running(&self) -> bool {
        self.raft_group.is_some()
    }
    pub fn role(&self) -> StateRole {
        self.raft_group
            .as_ref()
            .map(|g| g.try_lock().unwrap().raft.state)
            .unwrap_or(StateRole::PreCandidate)
    }
    pub fn is_leader(&self) -> bool {
        self.raft_group
            .as_ref()
            .map(|g| g.try_lock().unwrap().raft.state == StateRole::Leader)
            .unwrap_or_default()
    }

    pub fn leader(&self) -> NodeId {
        NodeId(
            self.raft_group
                .as_ref()
                .map(|g| g.try_lock().unwrap().raft.leader_id)
                .unwrap_or_default(),
        )
    }

    // Create a raft leader only with itself in its configuration.
    pub async fn create_raft_leader(
        logger: &Logger,
        id: NodeId,
        pubsub: pubsub::Channel,
        network: Network,
    ) -> Self {
        let mut cfg = example_config();
        cfg.id = id.0;

        let storage = Storage::new_with_conf_state(id, ConfState::from((vec![id.0], vec![]))).await;
        let raft_group = Some(Mutex::new(RawNode::new(&cfg, storage, logger).unwrap()));
        Self {
            logger: logger.clone(),
            id,
            raft_group,
            proposals: VecDeque::new(),
            network,
            pending_proposals: HashMap::new(),
            pending_acks: HashMap::new(),
            proposal_id: 0,
            tick_duration: Duration::from_millis(100),
            services: HashMap::new(),
            pubsub,
            last_state: StateRole::PreCandidate,
        }
    }

    pub fn set_raft_tick_duration(&mut self, d: Duration) {
        self.tick_duration = d;
    }

    // Create a raft follower.
    pub async fn create_raft_follower(
        logger: &Logger,
        id: NodeId,
        pubsub: pubsub::Channel,
        network: Network,
    ) -> Self {
        let storage = Storage::new(id).await;
        Self {
            logger: logger.clone(),
            id,
            raft_group: if storage.last_index().unwrap() == 1 {
                None
            } else {
                let mut cfg = example_config();
                cfg.id = id.0;
                Some(Mutex::new(RawNode::new(&cfg, storage, logger).unwrap()))
            },
            proposals: VecDeque::new(),
            network,
            pending_proposals: HashMap::new(),
            pending_acks: HashMap::new(),
            proposal_id: 0,
            tick_duration: Duration::from_millis(100),
            services: HashMap::new(),
            pubsub,
            last_state: StateRole::PreCandidate,
        }
    }

    // Initialize raft for followers.
    pub async fn initialize_raft_from_message(&mut self, msg: &Message) {
        if !is_initial_msg(msg) {
            return;
        }
        let mut cfg = example_config();
        cfg.id = msg.to;
        let storage = Storage::new(self.id).await;
        self.raft_group = Some(Mutex::new(
            RawNode::new(&cfg, storage, &self.logger).unwrap(),
        ));
    }

    // Step a raft message, initialize the raft if need.
    pub async fn step(&mut self, msg: Message) -> Result<()> {
        if self.raft_group.is_none() {
            if is_initial_msg(&msg) {
                self.initialize_raft_from_message(&msg).await;
            } else {
                return Ok(());
            }
        }
        let mut raft_group = self.raft_group.as_mut().unwrap().try_lock().unwrap();
        raft_group.step(msg)
    }
    async fn append(&self, entries: &[Entry]) -> Result<()> {
        let raft_node = self.raft_group.as_ref().unwrap().try_lock().unwrap();
        raft_node.store().append(entries).await
    }
    async fn apply_snapshot(&mut self, snapshot: Snapshot) -> Result<()> {
        let mut raft_node = self.raft_group.as_ref().unwrap().try_lock().unwrap();
        raft_node.mut_store().apply_snapshot(snapshot).await
    }

    // interface for raft-rs
    async fn set_conf_state(&mut self, cs: ConfState) -> Result<()> {
        let mut raft_node = self.raft_group.as_ref().unwrap().try_lock().unwrap();
        raft_node.mut_store().set_conf_state(cs).await
    }

    // interface for raft-rs
    async fn set_hard_state(&mut self, commit: u64, term: u64) -> Result<()> {
        let mut raft_node = self.raft_group.as_ref().unwrap().try_lock().unwrap();
        raft_node.mut_store().set_hard_state(commit, term).await
    }

    pub(crate) async fn on_ready(&mut self) -> Result<()> {
        if self.raft_group.as_ref().is_none() {
            return Ok(());
        };

        if !self
            .raft_group
            .as_ref()
            .unwrap()
            .try_lock()
            .unwrap()
            .has_ready()
        {
            return Ok(());
        }

        // Get the `Ready` with `RawNode::ready` interface.
        let ready = self
            .raft_group
            .as_mut()
            .unwrap()
            .try_lock()
            .unwrap()
            .ready();

        // Persistent raft logs. It's necessary because in `RawNode::advance` we stabilize
        // raft logs to the latest position.
        if let Err(e) = self.append(ready.entries().as_slice()).await {
            println!("persist raft log fail: {:?}, need to retry or panic", e);
            return Err(e);
        }

        // Apply the snapshot. It's necessary because in `RawNode::advance` we stabilize the snapshot.
        if *ready.snapshot() != Snapshot::default() {
            let s = ready.snapshot().clone();
            if let Err(e) = self.apply_snapshot(s).await {
                println!("apply snapshot fail: {:?}, need to retry or panic", e);
                return Err(e);
            }
        }

        // Send out the messages come from the node.
        for msg in ready.messages() {
            self.network.send_msg(msg.clone()).await.unwrap()
        }

        // Apply all committed proposals.
        for entry in ready.committed_entries() {
            if entry.data.is_empty() {
                // From new elected leaders.
                continue;
            }
            if let EntryType::EntryConfChange = entry.get_entry_type() {
                // For conf change messages, make them effective.
                let mut cc = ConfChange::default();
                cc.merge_from_bytes(&entry.data).unwrap();
                let _node_id = cc.node_id;

                let cs: ConfState = self
                    .raft_group
                    .as_mut()
                    .unwrap()
                    .try_lock()
                    .unwrap()
                    .apply_conf_change(&cc)
                    .unwrap();
                self.set_conf_state(cs).await?;
            } else {
                // For normal proposals, extract the key-value pair and then
                // insert them into the kv engine.
                if let Ok(event) = serde_json::from_slice::<Event>(&entry.data) {
                    if let Some(service) = self.services.get_mut(&event.sid) {
                        // let _store = &self
                        //     .raft_group
                        //     .as_ref()
                        //     .unwrap()
                        //     .lock()
                        //     .await
                        //     .raft
                        //     .raft_log
                        //     .store;
                        let (code, value) = service
                            .execute(
                                self.raft_group.as_ref().unwrap(),
                                &mut self.pubsub,
                                event.data,
                            )
                            .await
                            .unwrap();
                        if event.nid == Some(self.id) {
                            self.network
                                .event_reply(event.eid, code, value)
                                .await
                                .unwrap();
                        }
                    }
                }
            }
            if self
                .raft_group
                .as_ref()
                .unwrap()
                .try_lock()
                .unwrap()
                .raft
                .state
                == StateRole::Leader
            {
                // The leader should response to the clients, tell them if their proposals
                // succeeded or not.
                if let Some(proposal) = self.proposals.pop_front() {
                    if proposal.proposer == self.id {
                        info!(self.logger, "Handling proposal(local)"; "proposal-id" => proposal.id);
                        self.pending_proposals.remove(&proposal.id);
                    } else {
                        info!(self.logger, "Handling proposal(remote)"; "proposal-id" => proposal.id, "proposer" => proposal.proposer);
                        self.network
                            .ack_proposal(proposal.proposer, proposal.id, true)
                            .await
                            .map_err(|e| {
                                Error::Io(IoError::new(
                                    IoErrorKind::ConnectionAborted,
                                    format!("{}", e),
                                ))
                            })?;
                    }
                }
            }
        }
        if let Some(last_committed) = ready.committed_entries().last() {
            self.set_hard_state(last_committed.index, last_committed.term)
                .await?;
        }

        // Call `RawNode::advance` interface to update position flags in the raft.
        self.raft_group
            .as_mut()
            .unwrap()
            .try_lock()
            .unwrap()
            .advance(ready);
        Ok(())
    }

    pub(crate) async fn propose_all(&mut self) -> Result<()> {
        let mut raft_group = self.raft_group.as_mut().unwrap().try_lock().unwrap();
        let mut pending = Vec::new();
        for p in self.proposals.iter_mut().skip_while(|p| p.proposed > 0) {
            if propose_and_check_failed_proposal(&mut *raft_group, p)? {
                // if propose_and_check_failed_proposal(&mut *raft_group.lock().await, p)? {
                if p.proposer == self.id {
                    if let Some(prop) = self.pending_proposals.remove(&p.id) {
                        pending.push(prop);
                    }
                } else {
                    self.network
                        .ack_proposal(p.proposer, p.id, false)
                        .await
                        .map_err(|e| {
                            Error::Io(IoError::new(
                                IoErrorKind::ConnectionAborted,
                                format!("{}", e),
                            ))
                        })?;
                }
            }
        }
        for p in pending.drain(..) {
            self.proposals.push_back(p)
        }
        Ok(())
    }
}

pub async fn status<Storage>(node: &Mutex<raft::RawNode<Storage>>) -> Result<RaftNodeStatus>
where
    Storage: storage::Storage,
{
    let node = node.try_lock().unwrap();
    Ok(RaftNodeStatus {
        id: node.raft.id,
        role: format!("{:?}", node.raft.state),
        promotable: node.raft.promotable(),
        pass_election_timeout: node.raft.pass_election_timeout(),
        election_elapsed: node.raft.election_elapsed,
        randomized_election_timeout: node.raft.randomized_election_timeout(),
        term: node.raft.term,
        last_index: node.raft.raft_log.store.last_index().unwrap_or(0),
    })
}

pub(crate) fn propose_and_check_failed_proposal<Storage>(
    raft_group: &mut RawNode<Storage>,
    proposal: &mut Proposal,
) -> Result<bool>
where
    Storage: ReadStorage,
{
    let last_index1 = raft_group.raft.raft_log.last_index() + 1;
    if let Some(ref event) = proposal.normal {
        let data = serde_json::to_vec(&event).unwrap();
        raft_group.propose(vec![], data)?;
    } else if let Some(ref cc) = proposal.conf_change {
        raft_group.propose_conf_change(vec![], cc.clone())?;
    } else if let Some(_transferee) = proposal.transfer_leader {
        // TODO 0.8
        // TODO: implement transfer leader.
        unimplemented!();
    } else {
    }

    let last_index2 = raft_group.raft.raft_log.last_index() + 1;
    if last_index2 == last_index1 {
        // Propose failed, don't forget to respond to the client.
        Ok(true)
    } else {
        proposal.proposed = last_index1;
        Ok(false)
    }
}

pub struct Proposal {
    id: ProposalId,
    proposer: NodeId, // node id of the proposer
    normal: Option<Event>,
    conf_change: Option<ConfChange>, // conf change.
    transfer_leader: Option<u64>,
    // If it's proposed, it will be set to the index of the entry.
    pub(crate) proposed: u64,
}

impl Proposal {
    pub fn conf_change(id: ProposalId, proposer: NodeId, cc: &ConfChange) -> Self {
        Self {
            id,
            proposer,
            normal: None,
            conf_change: Some(cc.clone()),
            transfer_leader: None,
            proposed: 0,
        }
    }
    #[allow(dead_code)]
    pub fn normal(
        id: ProposalId,
        proposer: NodeId,
        eid: EventId,
        sid: ServiceId,
        data: Vec<u8>,
    ) -> Self {
        Self {
            id,
            proposer,
            normal: Some(Event {
                nid: Some(proposer),
                eid,
                sid,
                data,
            }),
            conf_change: None,
            transfer_leader: None,
            proposed: 0,
        }
    }
}
