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
use crate::storage::*;
use protobuf::Message as PBMessage;
use raft::eraftpb::ConfState;
use raft::eraftpb::Message;
use raft::{prelude::*, StateRole};
use raft::{Error, Result};
use slog::Logger;
use std::collections::VecDeque;
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

impl<Storage, Network> fmt::Debug for RaftNode<Storage, Network>
where
    Storage: storage::Storage,
    Network: network::Network,
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if let Some(g) = self.raft_group.as_ref() {
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
                self.role(),
                g.raft.promotable(),
                g.raft.pass_election_timeout(),
                g.raft.election_elapsed,
                g.raft.randomized_election_timeout(),
                &g.raft.leader_id,
                &g.raft.term,
                &g.raft.raft_log.store.last_index().unwrap_or(0),
                &g.raft.vote,
                &g.raft.votes,
                &g.raft.prs().configuration().voters(),
                &self.network.connections(),
            )
        } else {
            write!(f, "UNINITIALIZED")
        }
    }
}

pub struct RaftNode<Storage, Network>
where
    Storage: storage::Storage,
    Network: network::Network,
{
    pub logger: Logger,
    // None if the raft is not initialized.
    pub id: NodeId,
    pub raft_group: Option<RawNode<Storage>>,
    pub my_mailbox: Receiver<UrMsg>,
    pub network: Network,
    pub proposals: VecDeque<Proposal>,
    pub pending_proposals: HashMap<u64, Proposal>,
    pub pending_acks: HashMap<u64, Sender<bool>>,
    pub proposal_id: u64,
    pub timer: Instant,
}

impl<Storage, Network> RaftNode<Storage, Network>
where
    Storage: storage::Storage,
    Network: network::Network,
{
    pub fn tick(&mut self) -> Result<()> {
        if !self.is_running() {
            return Ok(());
        }

        if self.timer.elapsed() >= Duration::from_millis(100) {
            // Tick the raft.

            self.raft_group.as_mut().unwrap().tick();
            self.timer = Instant::now();
        }

        if self.is_leader() {
            // Handle new proposals.
            self.propose_all()?;
        }

        self.on_ready()
    }
    pub fn propose_kv(
        &mut self,
        from: NodeId,
        pid: u64,
        key: Vec<u8>,
        value: Vec<u8>,
    ) -> Result<()> {
        if self.is_leader() {
            self.proposals
                .push_back(Proposal::normal(pid, from, key, value));
            Ok(())
        } else {
            self.network
                .forward_proposal(from, self.leader(), pid, key, value)
                .map_err(|e| {
                    Error::Io(IoError::new(
                        IoErrorKind::ConnectionAborted,
                        format!("{}", e),
                    ))
                })
        }
    }
    pub fn add_node(&mut self, id: NodeId) -> bool {
        if self.is_leader() && !self.node_known(id) {
            let mut conf_change = ConfChange::default();
            conf_change.node_id = id.0;
            conf_change.set_change_type(ConfChangeType::AddNode);
            let proposal = Proposal::conf_change(self.proposal_id, self.id, &conf_change);
            self.proposal_id += 1;

            self.proposals.push_back(proposal);
            true
        } else {
            false
        }
    }
    pub fn next_pid(&mut self) -> u64 {
        let pid = self.proposal_id;
        self.proposal_id += 1;
        pid
    }
    pub fn node_known(&self, id: NodeId) -> bool {
        self.raft_group
            .as_ref()
            .map(|g| g.raft.prs().configuration().contains(id.0))
            .unwrap_or_default()
    }
    pub fn log(&self) {
        if let Some(g) = self.raft_group.as_ref() {
            info!(
                self.logger,
                "NODE STATE";
                "node-id" => &g.raft.id,
                "role" => format!("{:?}", self.role()),

                "leader-id" => &g.raft.leader_id,
                "term" => &g.raft.term,
                "first-index" => &g.raft.raft_log.store.first_index().unwrap_or(0),
                "last-index" => &g.raft.raft_log.store.last_index().unwrap_or(0),

                "vote" => &g.raft.vote,
                "votes" => format!("{:?}", &g.raft.votes),

                "voters" => format!("{:?}", &g.raft.prs().configuration().voters()),

                "promotable" => g.raft.promotable(),
                "pass-election-timeout" => g.raft.pass_election_timeout(),
                "election-elapsed" => g.raft.election_elapsed,
                "randomized-election-timeout" => g.raft.randomized_election_timeout(),

                "connections" => format!("{:?}", &self.network.connections()),
            )
        } else {
            error!(self.logger, "UNINITIALIZED NODE {}", self.id)
        }
    }
    pub fn get_key(&self, key: &[u8]) -> Option<Vec<u8>> {
        self.raft_group
            .as_ref()
            .unwrap()
            .raft
            .raft_log
            .store
            .get(key)
    }
    pub fn put_key(&mut self, key: &[u8], value: &[u8]) -> bool {
        self.raft_group
            .as_ref()
            .unwrap()
            .raft
            .raft_log
            .store
            .put(&key, value);
        true
    }
    pub fn is_running(&self) -> bool {
        self.raft_group.is_some()
    }
    pub fn role(&self) -> &StateRole {
        self.raft_group
            .as_ref()
            .map(|g| &g.raft.state)
            .unwrap_or(&StateRole::PreCandidate)
    }
    pub fn is_leader(&self) -> bool {
        self.raft_group
            .as_ref()
            .map(|g| g.raft.state == StateRole::Leader)
            .unwrap_or_default()
    }

    pub fn leader(&self) -> NodeId {
        NodeId(
            self.raft_group
                .as_ref()
                .map(|g| g.raft.leader_id)
                .unwrap_or_default(),
        )
    }

    // Create a raft leader only with itself in its configuration.
    pub fn create_raft_leader(id: NodeId, my_mailbox: Receiver<UrMsg>, logger: &Logger) -> Self {
        let mut cfg = example_config();
        cfg.id = id.0;

        let storage = Storage::new_with_conf_state(id, ConfState::from((vec![id.0], vec![])));
        let raft_group = Some(RawNode::new(&cfg, storage, logger).unwrap());
        Self {
            logger: logger.clone(),
            id,
            raft_group,
            my_mailbox,
            proposals: VecDeque::new(),
            network: Network::default(),
            pending_proposals: HashMap::new(),
            pending_acks: HashMap::new(),
            proposal_id: 0,
            timer: Instant::now(),
        }
    }

    // Create a raft follower.
    pub fn create_raft_follower(id: NodeId, my_mailbox: Receiver<UrMsg>, logger: &Logger) -> Self {
        let storage = Storage::new(id);
        Self {
            logger: logger.clone(),
            id,
            raft_group: if storage.last_index().unwrap() == 1 {
                None
            } else {
                let mut cfg = example_config();
                cfg.id = id.0;
                Some(RawNode::new(&cfg, storage, logger).unwrap())
            },
            my_mailbox,
            proposals: VecDeque::new(),
            network: Network::default(),
            pending_proposals: HashMap::new(),
            pending_acks: HashMap::new(),
            proposal_id: 0,
            timer: Instant::now(),
        }
    }

    // Initialize raft for followers.
    pub fn initialize_raft_from_message(&mut self, msg: &Message) {
        if !is_initial_msg(msg) {
            return;
        }
        let mut cfg = example_config();
        cfg.id = msg.to;
        let storage = Storage::new(self.id);
        self.raft_group = Some(RawNode::new(&cfg, storage, &self.logger).unwrap());
    }

    // Step a raft message, initialize the raft if need.
    pub fn step(&mut self, msg: Message) -> Result<()> {
        if self.raft_group.is_none() {
            if is_initial_msg(&msg) {
                self.initialize_raft_from_message(&msg);
            } else {
                return Ok(());
            }
        }
        let raft_group = self.raft_group.as_mut().unwrap();
        raft_group.step(msg)
    }
    fn append(&self, entries: &[Entry]) -> Result<()> {
        self.raft_group
            .as_ref()
            .unwrap()
            .raft
            .raft_log
            .store
            .append(entries)
    }
    fn apply_snapshot(&mut self, snapshot: Snapshot) -> Result<()> {
        self.raft_group
            .as_mut()
            .unwrap()
            .raft
            .raft_log
            .store
            .apply_snapshot(snapshot)
    }

    // interface for raft-rs
    fn set_conf_state(&mut self, cs: ConfState) -> Result<()> {
        self.raft_group
            .as_mut()
            .unwrap()
            .raft
            .raft_log
            .store
            .set_conf_state(cs)
    }

    // interface for raft-rs
    fn set_hard_state(&mut self, commit: u64, term: u64) -> Result<()> {
        self.raft_group
            .as_mut()
            .unwrap()
            .raft
            .raft_log
            .store
            .set_hard_state(commit, term)
    }

    pub(crate) fn on_ready(&mut self) -> Result<()> {
        if self.raft_group.as_ref().is_none() {
            return Ok(());
        };

        if !self.raft_group.as_ref().unwrap().has_ready() {
            return Ok(());
        }

        // Get the `Ready` with `RawNode::ready` interface.
        let mut ready = self.raft_group.as_mut().unwrap().ready();

        // Persistent raft logs. It's necessary because in `RawNode::advance` we stabilize
        // raft logs to the latest position.
        if let Err(e) = self.append(ready.entries()) {
            println!("persist raft log fail: {:?}, need to retry or panic", e);
            return Err(e);
        }

        // Apply the snapshot. It's necessary because in `RawNode::advance` we stabilize the snapshot.
        if *ready.snapshot() != Snapshot::default() {
            let s = ready.snapshot().clone();
            if let Err(e) = self.apply_snapshot(s) {
                println!("apply snapshot fail: {:?}, need to retry or panic", e);
                return Err(e);
            }
        }

        // Send out the messages come from the node.
        for msg in ready.messages.drain(..) {
            self.network.send_msg(msg).unwrap()
        }

        // Apply all committed proposals.
        if let Some(committed_entries) = ready.committed_entries.take() {
            for entry in &committed_entries {
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
                        .apply_conf_change(&cc)
                        .unwrap();
                    self.set_conf_state(cs)?;
                } else {
                    // For normal proposals, extract the key-value pair and then
                    // insert them into the kv engine.
                    if let Ok(kv) = serde_json::from_slice::<KV>(&entry.data) {
                        let k = base64::decode(&kv.key).unwrap();
                        let v = base64::decode(&kv.value).unwrap();
                        self.put_key(&k, &v);
                    }
                }
                if self.raft_group.as_ref().unwrap().raft.state == StateRole::Leader {
                    // The leader should response to the clients, tell them if their proposals
                    // succeeded or not.
                    if let Some(proposal) = self.proposals.pop_front() {
                        if proposal.proposer == self.id {
                            info!(self.logger, "Handling proposal(local)"; "proposal-id" => proposal.id);
                            if let Some(reply) = self.pending_acks.remove(&proposal.id) {
                                reply.send(true).unwrap();
                            }
                            self.pending_proposals.remove(&proposal.id);
                        } else {
                            info!(self.logger, "Handling proposal(remopte)"; "proposal-id" => proposal.id, "proposer" => proposal.proposer);
                            self.network
                                .ack_proposal(proposal.proposer, proposal.id, true)
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
            if let Some(last_committed) = committed_entries.last() {
                self.set_hard_state(last_committed.index, last_committed.term)?;
            }
        }
        // Call `RawNode::advance` interface to update position flags in the raft.
        self.raft_group.as_mut().unwrap().advance(ready);
        Ok(())
    }

    pub(crate) fn propose_all(&mut self) -> Result<()> {
        let raft_group = self.raft_group.as_mut().unwrap();
        let mut pending = Vec::new();
        for p in self.proposals.iter_mut().skip_while(|p| p.proposed > 0) {
            if propose_and_check_failed_proposal(raft_group, p)? {
                if p.proposer == self.id {
                    if let Some(prop) = self.pending_proposals.remove(&p.id) {
                        pending.push(prop);
                    }
                } else {
                    self.network
                        .ack_proposal(p.proposer, p.id, false)
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

pub(crate) fn propose_and_check_failed_proposal<Storage>(
    raft_group: &mut RawNode<Storage>,
    proposal: &mut Proposal,
) -> Result<bool>
where
    Storage: ReadStorage,
{
    let last_index1 = raft_group.raft.raft_log.last_index() + 1;
    if let Some((ref key, ref value)) = proposal.normal {
        let data = serde_json::to_vec(&KV {
            key: base64::encode(key),
            value: base64::encode(value),
        })
        .unwrap();
        raft_group.propose(vec![], data)?;
    } else if let Some(ref cc) = proposal.conf_change {
        raft_group.propose_conf_change(vec![], cc.clone())?;
    } else if let Some(_transferee) = proposal.transfer_leader {
        // TODO 0.8
        // TODO: implement transfer leader.
        unimplemented!();
    } else {
        dbg!("snot");
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
    id: u64,
    proposer: NodeId, // node id of the proposer
    normal: Option<(Vec<u8>, Vec<u8>)>,
    conf_change: Option<ConfChange>, // conf change.
    transfer_leader: Option<u64>,
    // If it's proposed, it will be set to the index of the entry.
    pub(crate) proposed: u64,
}

impl Proposal {
    pub fn conf_change(id: u64, proposer: NodeId, cc: &ConfChange) -> Self {
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
    pub fn normal(id: u64, proposer: NodeId, key: Vec<u8>, value: Vec<u8>) -> Self {
        Self {
            id,
            proposer,
            normal: Some((key, value)),
            conf_change: None,
            transfer_leader: None,
            proposed: 0,
        }
    }
}
