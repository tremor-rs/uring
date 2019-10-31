use super::*;
use crate::storage::*;
use protobuf::Message as PBMessage;
use raft::eraftpb::ConfState;
use raft::eraftpb::Message;
use raft::storage::Storage as RaftStorage;
use raft::{prelude::*, StateRole};
use raft::{Error, Result};
use regex::Regex;
use slog::Logger;
use std::collections::VecDeque;
use std::fmt;

//type UsedStorage = URMemStorage;
type UsedStorage = URRocksStorage;

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

type LocalMailboxes = HashMap<u64, Addr<WsOfframpWorker>>;
type RemoteMailboxes = HashMap<u64, Addr<UrSocket>>;

impl fmt::Debug for RaftNode {
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
local  connections: {:?}
remote connections: {:?}
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
                &self.local_mailboxes.keys(),
                &self.remote_mailboxes.keys(),
            )
        } else {
            write!(f, "UNINITIALIZED")
        }
    }
}

pub struct RaftNode {
    // None if the raft is not initialized.
    pub id: u64,
    pub endpoint: String,
    pub raft_group: Option<RawNode<UsedStorage>>,
    pub my_mailbox: Receiver<UrMsg>,
    pub local_mailboxes: LocalMailboxes,
    pub remote_mailboxes: RemoteMailboxes,
    // Key-value pairs after applied. `MemStorage` only contains raft logs,
    // so we need an additional storage engine.
    pub kv_pairs: HashMap<String, String>,
    pub proposals: VecDeque<Proposal>,
    pub pending_proposals: HashMap<u64, Proposal>,
    pub pending_acks: HashMap<u64, Sender<bool>>,
    pub proposal_id: u64,
}

impl RaftNode {
    pub fn log(&self, logger: &Logger) {
        if let Some(g) = self.raft_group.as_ref() {
            debug!(
                logger,
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

                "local-mailboxes" => format!("{:?}", &self.local_mailboxes.keys()),
                "remote-mailboxes" => format!("{:?}", &self.remote_mailboxes.keys()),
            )
        } else {
            error!(logger, "UNINITIALIZED NODE {}", self.id)
        }
    }
    pub fn get_key(&self, key: String) -> String {
        self.kv_pairs
            .get(&key)
            .cloned()
            .unwrap_or_else(|| "<NOT FOUND>".into())
    }
    pub fn put_key(&mut self, key: String, value: String) -> bool {
        self.kv_pairs.insert(key, value);
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

    pub fn leader(&self) -> u64 {
        self.raft_group
            .as_ref()
            .map(|g| g.raft.leader_id)
            .unwrap_or_default()
    }

    // Create a raft leader only with itself in its configuration.
    pub fn create_raft_leader(
        id: u64,
        endpoint: String,
        my_mailbox: Receiver<UrMsg>,
        logger: &Logger,
    ) -> Self {
        let mut cfg = example_config();
        cfg.id = id;

        let storage = UsedStorage::new_with_conf_state(id, ConfState::from((vec![id], vec![])));
        let raft_group = Some(RawNode::new(&cfg, storage, logger).unwrap());
        Self {
            id,
            endpoint,
            raft_group,
            my_mailbox,
            proposals: VecDeque::new(),
            local_mailboxes: HashMap::new(),
            remote_mailboxes: HashMap::new(),
            kv_pairs: Default::default(),
            pending_proposals: HashMap::new(),
            pending_acks: HashMap::new(),
            proposal_id: 0,
        }
    }

    // Create a raft follower.
    pub fn create_raft_follower(
        id: u64,
        endpoint: String,
        my_mailbox: Receiver<UrMsg>,
        logger: &Logger,
    ) -> Self {
        let storage = UsedStorage::new(id);
        Self {
            id,
            endpoint,
            raft_group: if storage.last_index().unwrap() == 1 {
                None
            } else {
                let mut cfg = example_config();
                cfg.id = id;
                Some(RawNode::new(&cfg, storage, logger).unwrap())
            },
            my_mailbox,
            proposals: VecDeque::new(),
            local_mailboxes: HashMap::new(),
            remote_mailboxes: HashMap::new(),
            kv_pairs: Default::default(),
            pending_proposals: HashMap::new(),
            pending_acks: HashMap::new(),
            proposal_id: 0,
        }
    }

    // Initialize raft for followers.
    pub fn initialize_raft_from_message(&mut self, msg: &Message) {
        if !is_initial_msg(msg) {
            return;
        }
        let mut cfg = example_config();
        cfg.id = msg.to;
        let storage = UsedStorage::new(self.id);
        self.raft_group = Some(RawNode::with_default_logger(&cfg, storage).unwrap());
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
            .set_conf_state(cs, None)
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
            self.send_msg(msg).unwrap();
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
                    let node_id = cc.node_id;

                    let cs: ConfState = self
                        .raft_group
                        .as_mut()
                        .unwrap()
                        .apply_conf_change(dbg!(&cc))
                        .unwrap();
                    self.set_conf_state(cs)?;
                } else {
                    // For normal proposals, extract the key-value pair and then
                    // insert them into the kv engine.
                    let data = std::str::from_utf8(&entry.data).unwrap();
                    let reg = Regex::new("put (.+) (.+)").unwrap();
                    if let Some(caps) = reg.captures(&data) {
                        self.kv_pairs
                            .insert(caps[1].to_string(), caps[2].to_string());
                    }
                }
                if self.raft_group.as_ref().unwrap().raft.state == StateRole::Leader {
                    // The leader should response to the clients, tell them if their proposals
                    // succeeded or not.
                    if let Some(proposal) = self.proposals.pop_front() {
                        if proposal.proposer == self.id {
                            self.pending_proposals.remove(&proposal.id);
                        } else {
                            ack_proposal(
                                &self.local_mailboxes,
                                &self.remote_mailboxes,
                                &proposal,
                                true,
                            )?;
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
        let raft_group = match self.raft_group {
            Some(ref mut r) => r,
            // When Node::raft_group is `None` it means the node is not initialized.
            _ => return Err(Error::ViolatesContract("raft_group is None".into())),
        };
        let mut pending = Vec::new();
        for p in self.proposals.iter_mut().skip_while(|p| p.proposed > 0) {
            if propose_and_check_failed_proposal(raft_group, p)? {
                if p.proposer == self.id {
                    if let Some(prop) = self.pending_proposals.remove(&p.id) {
                        pending.push(prop);
                    }
                } else {
                    ack_proposal(&self.local_mailboxes, &self.remote_mailboxes, p, false)?;
                }
            }
        }
        for p in pending.drain(..) {
            self.proposals.push_back(p)
        }
        Ok(())
    }
    fn send_msg(&self, msg: Message) -> Result<()> {
        println!("sending raft message {:?}", msg);
        if let Some(remote) = self.local_mailboxes.get(&msg.to) {
            remote
                .send(RaftMsg(msg))
                .wait()
                .map_err(|e| Error::ViolatesContract(format!("failed to send: {}", e)))
        } else if let Some(remote) = self.remote_mailboxes.get(&msg.to) {
            remote
                .send(RaftMsg(msg))
                .wait()
                .map_err(|e| Error::ViolatesContract(format!("failed to send: {}", e)))
        } else {
            println!("send raft message to {} fail, let Raft retry it", msg.to);
            /* don't error or we crash)
            Err(Error::ViolatesContract(format!(
                "send raft message to {} fail, let Raft retry it",
                msg.to
            )));
            */
            Ok(())
        }
    }
}

pub(crate) fn propose_and_check_failed_proposal(
    raft_group: &mut RawNode<UsedStorage>,
    proposal: &mut Proposal,
) -> Result<bool> {
    let last_index1 = dbg!(raft_group.raft.raft_log.last_index() + 1);
    if let Some((ref key, ref value)) = proposal.normal {
        let data = format!("put {} {}", key, value).into_bytes();
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

fn ack_proposal(
    local_mailboxes: &LocalMailboxes,
    remote_mailboxes: &RemoteMailboxes,
    proposal: &Proposal,
    success: bool,
) -> Result<()> {
    if let Some(remote) = local_mailboxes.get(&proposal.proposer) {
        remote
            .send(WsMessage::Msg(HandshakeMsg::AckProposal(
                proposal.id,
                success,
            )))
            .wait()
            .map_err(|e| Error::ViolatesContract(format!("failed to send: {}", e)))
    } else if let Some(remote) = remote_mailboxes.get(&proposal.proposer) {
        remote
            .send(WsMessage::Msg(HandshakeMsg::AckProposal(
                proposal.id,
                success,
            )))
            .wait()
            .map_err(|e| Error::ViolatesContract(format!("failed to send: {}", e)))
    } else {
        println!(
            "send ack proposla to {} fail, let Raft retry it",
            proposal.proposer
        );
        Err(Error::ViolatesContract(format!(
            "send ack proposla to {} fail, let Raft retry it",
            proposal.proposer
        )))
    }
}

pub struct Proposal {
    id: u64,
    proposer: u64,                    // node id of the proposer
    normal: Option<(String, String)>, // key is an u16 integer, and value is a string.
    conf_change: Option<ConfChange>,  // conf change.
    transfer_leader: Option<u64>,
    // If it's proposed, it will be set to the index of the entry.
    pub(crate) proposed: u64,
}

impl Proposal {
    pub fn conf_change(id: u64, proposer: u64, cc: &ConfChange) -> Self {
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
    pub fn normal(id: u64, proposer: u64, key: String, value: String) -> Self {
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
