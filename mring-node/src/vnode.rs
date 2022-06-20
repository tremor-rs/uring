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

use crate::handoff;
use async_std::task;
use futures::channel::mpsc::{channel, Receiver, Sender};
use futures::{select, FutureExt, SinkExt, StreamExt};
use serde_derive::{Deserialize, Serialize};
use slog::Logger;
use std::collections::HashMap;
use std::time::Duration;
use uring_common::MRingNodes;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Default)]
struct VNode {
    id: u64,
    handoff: Option<handoff::Handoff>,
    data: Vec<String>,
}

pub(crate) enum Task {
    HandoffOut {
        target: String,
        vnode: u64,
    },
    Assign {
        vnodes: Vec<u64>,
    },
    Update {
        next: MRingNodes,
    },
    HandoffInStart {
        src: String,
        vnode: u64,
    },
    HandoffIn {
        vnode: u64,
        chunk: u64,
        data: Vec<String>,
    },
    HandoffInEnd {
        vnode: u64,
    },
}

pub(crate) enum Cmd {
    GetHandoffData {
        vnode: u64,
        chunk: u64,
        reply: Sender<(u64, Vec<String>)>,
    },
    FinishHandoff {
        vnode: u64,
    },
    CancelHandoff {
        vnode: u64,
        target: String,
    },
}

#[derive(Default)]
struct State {
    vnodes: HashMap<u64, VNode>,
    mappings: HashMap<u64, String>,
}

impl State {
    pub fn update_ring(&mut self, mapping: MRingNodes) {
        for node in mapping.into_iter() {
            for vnode in &node.vnodes {
                self.mappings.insert(*vnode, node.id.clone());
            }
        }
    }
}

async fn handle_cmd(
    logger: &Logger,
    cmd: Option<Cmd>,
    state: &mut State,
    tasks_tx: &mut Sender<Task>,
) {
    match cmd {
        Some(Cmd::GetHandoffData {
            vnode,
            chunk,
            mut reply,
        }) => {
            if let Some(vnode) = state.vnodes.get_mut(&vnode) {
                if let Some(ref mut handoff) = vnode.handoff {
                    assert_eq!(chunk, handoff.chunk);
                    assert_eq!(handoff.direction, handoff::Direction::Outbound);
                    if let Some(data) = vnode.data.get(chunk as usize) {
                        reply.send((chunk, vec![data.clone()])).await.unwrap();
                    } else {
                        reply.send((chunk, vec![])).await.unwrap();
                    };
                    handoff.chunk += 1;
                } else {
                    info!(logger, "Not in a migraiton");
                }
            } else {
                info!(logger, "Unknown vnode");
            }
        }
        Some(Cmd::CancelHandoff { vnode, target }) => {
            if let Some(node) = state.vnodes.get_mut(&vnode) {
                assert!(node.handoff.is_some());
                node.handoff = None;
                warn!(
                    logger,
                    "Canceling handoff of vnode {} to {} - requeueing to restart", vnode, target
                );
                tasks_tx
                    .send(Task::HandoffOut { target, vnode })
                    .await
                    .unwrap();
            } else {
                info!(logger, "Unknown vnode");
            }
        }
        Some(Cmd::FinishHandoff { vnode }) => {
            let v = state.vnodes.remove(&vnode).unwrap();
            let m = v.handoff.unwrap();
            assert_eq!(m.direction, handoff::Direction::Outbound);
        }
        None => (),
    }
}

async fn handle_tick(
    logger: &Logger,
    id: &str,
    state: &mut State,
    tasks: &mut Receiver<Task>,
    cnc_tx: &Sender<Cmd>,
) -> bool {
    select! {
        task = tasks.next() =>
        match task {
            Some(Task::Update{next}) => {
                state.update_ring(next);
            }
            Some(Task::HandoffOut { target, vnode }) => {
                if let Some(vnode) = state.vnodes.get_mut(&vnode){
                    info!(
                        logger,
                        "relocating vnode {} to node {}", vnode.id, target
                    );
                    assert!(vnode.handoff.is_none());
                        vnode.handoff = Some(handoff::Handoff {
                            partner: target.clone(),
                            chunk: 0,
                            direction: handoff::Direction::Outbound

                        });
                        if let Ok(worker) = handoff::Worker::new(logger.clone(), id.to_string(), target, vnode.id, cnc_tx.clone()).await {
                            task::spawn(worker.handoff());
                    }
                }
            },
            Some(Task::HandoffInStart { vnode, src }) => {
                state.vnodes.remove(&vnode);
                state.vnodes.insert(vnode, VNode{id: vnode, data: vec![], handoff: Some(handoff::Handoff{partner: src, chunk: 0, direction: handoff::Direction::Inbound})});
            }
            Some(Task::HandoffIn { mut data, vnode, chunk }) => {
                info!(
                    logger,
                    "accepting vnode {} with: {:?}", vnode, data
                );
                if let Some(node) = state.vnodes.get_mut(&vnode) {
                    if let Some(ref mut handoff) = &mut node.handoff {
                        assert_eq!(handoff.chunk, chunk);
                        assert_eq!(handoff.direction, handoff::Direction::Inbound);
                        node.data.append(&mut data);
                        handoff.chunk = chunk + 1;
                    } else {
                        error!(logger, "no handoff in progress for data vnode {}", vnode)

                    }
                } else {
                    error!(logger, "no handoff in progress for data vnode {}", vnode)
                }
            },
            Some(Task::HandoffInEnd { vnode }) => {
                if let Some(node) = state.vnodes.get_mut(&vnode) {
                    if let Some(ref mut handoff) = &mut node.handoff {
                        assert_eq!(handoff.direction, handoff::Direction::Inbound);
                    } else {
                        error!(logger, "no handoff in progress for data vnode {}", vnode)
                    }
                    node.handoff = None;
                    node.data.push(id.to_string());
                } else {
                    error!(logger, "no handoff in progress for data vnode {}", vnode)
                }
            }
            Some(Task::Assign{vnodes: ids}) => {
                info!(logger, "Initializing with {:?}", ids);
                let my_id = &id;
                for id in ids {
                    state.vnodes.insert(
                        id,
                        VNode {
                            handoff: None,
                            id,
                            data: vec![my_id.to_string()],
                        },
                    );
                }
            }
            None => return false,
        },
        complete => return false,
        default => ()
    }
    true
}

pub(crate) async fn run(
    logger: Logger,
    id: String,
    mut tasks: Receiver<Task>,
    mut tasks_tx: Sender<Task>,
) {
    let mut state = State::default();

    let (cnc_tx, mut cnc_rx) = channel(crate::CHANNEL_SIZE);

    let mut ticks = async_std::stream::interval(Duration::from_secs(1));
    loop {
        select! {
            cmd = cnc_rx.next() => handle_cmd(&logger, cmd, &mut state, &mut tasks_tx).await,
            _tick = ticks.next().fuse() =>  if ! handle_tick(&logger, &id, &mut state, &mut tasks, &cnc_tx).await {
                break
            },
        }
    }
}
