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
use async_std::net::TcpStream;
use futures::channel::mpsc::{channel, Receiver, Sender};
use futures::{select, FutureExt, SinkExt, StreamExt};
use slog::Logger;
use std::collections::HashMap;
use std::time::Duration;
use tungstenite::protocol::Message;
use uring_common::MRingNodes;

type WSStream = async_tungstenite::WebSocketStream<
    async_tungstenite::stream::Stream<TcpStream, async_tls::client::TlsStream<TcpStream>>,
>;

struct HandoffWorker {
    logger: Logger,
    src: String,
    target: String,
    vnode: u64,
    cnc: Sender<Cmd>,
    ws_stream: WSStream,
    chunk: u64,
}

enum HandoffError {
    ConnectionFailed,
    Canceld,
}

impl HandoffWorker {
    pub async fn new(
        logger: Logger,
        src: String,
        target: String,
        vnode: u64,
        mut cnc: Sender<Cmd>,
    ) -> Result<Self, HandoffError> {
        let url = url::Url::parse(&format!("ws://{}", target)).unwrap();
        if let Ok((ws_stream, _)) = connect_async(url).await {
            Ok(Self {
                logger,
                target,
                vnode,
                cnc,
                ws_stream,
                chunk: 0,
                src,
            })
        } else {
            error!(
                logger,
                "Failed to connect to {} to transfair vnode {}", target, vnode
            );
            cnc.send(Cmd::CancelHandoff { vnode, target })
                .await
                .unwrap();
            Err(HandoffError::ConnectionFailed)
        }
    }

    pub async fn handoff(mut self) -> Result<(), HandoffError> {
        info!(self.logger, "Starting handoff for vnode {}", self.vnode);
        self.init().await?;
        while self.transfair_data().await? {}
        self.finish().await
    }

    async fn finish(&mut self) -> Result<(), HandoffError> {
        let vnode = self.vnode;
        self.try_ack(HandoffMsg::Finish { vnode }, HandoffAck::Finish { vnode })
            .await?;
        self.cnc.send(Cmd::FinishHandoff { vnode }).await.unwrap();
        Ok(())
    }

    async fn init(&mut self) -> Result<(), HandoffError> {
        let vnode = self.vnode;
        self.try_ack(
            HandoffMsg::Start {
                src: self.src.clone(),
                vnode,
            },
            HandoffAck::Start { vnode },
        )
        .await?;
        Ok(())
    }

    async fn transfair_data(&mut self) -> Result<bool, HandoffError> {
        let vnode = self.vnode;
        let chunk = self.chunk;
        info!(
            self.logger,
            "requesting chunk {} from vnode {}", chunk, vnode
        );
        let (tx, mut rx) = channel(crate::CHANNEL_SIZE);
        if self
            .cnc
            .send(Cmd::GetHandoffData {
                vnode,
                chunk,
                reply: tx.clone(),
            })
            .await
            .is_err()
        {
            return self.cancel().await;
        };

        if let Some((r_chunk, data)) = rx.next().await {
            assert_eq!(chunk, r_chunk);
            if data.is_empty() {
                info!(self.logger, "transfer for vnode {} finished", vnode);
                return Ok(false);
            }
            info!(
                self.logger,
                "transfering chunk {} from vnode {}", chunk, vnode
            );

            self.try_ack(
                HandoffMsg::Data { vnode, chunk, data },
                HandoffAck::Data { chunk },
            )
            .await?;
            self.chunk += 1;
            Ok(true)
        } else {
            error!(
                self.logger,
                "error tranfairing chunk {} for vnode {}", chunk, vnode
            );
            self.cancel().await
        }
    }

    async fn try_ack(&mut self, msg: HandoffMsg, ack: HandoffAck) -> Result<(), HandoffError> {
        if self
            .ws_stream
            .send(Message::text(serde_json::to_string(&msg).unwrap()))
            .await
            .is_err()
        {
            error!(
                self.logger,
                "Failed to send handoff command {:?} for vnode {} to {}",
                msg,
                self.vnode,
                self.target
            );
            return self.cancel().await;
        };

        if let Some(Ok(data)) = self.ws_stream.next().await {
            let r: HandoffAck = serde_json::from_slice(&data.into_data()).unwrap();
            if ack != r {
                error!(self.logger, "Bad reply for  handoff command {:?} for vnode {} to {}. Expected, {:?}, but gut {:?}", msg, self.vnode, self.target, ack, r);
                return self.cancel().await;
            }
        } else {
            error!(
                self.logger,
                "No reply for  handoff command {:?} for vnode {} to {}",
                msg,
                self.vnode,
                self.target
            );
            return self.cancel().await;
        }
        Ok(())
    }

    async fn cancel<T>(&mut self) -> Result<T, HandoffError>
    where
        T: Default,
    {
        self.cnc
            .send(Cmd::CancelHandoff {
                vnode: self.vnode,
                target: self.target.clone(),
            })
            .await
            .unwrap();
        Err(HandoffError::Canceld)
    }
}

enum Cmd {
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
                &self.mappings.insert(*vnode, node.id.clone());
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
                    assert_eq!(handoff.direction, Direction::Outbound);
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
            assert_eq!(m.direction, Direction::Outbound);
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
                        vnode.handoff = Some(Handoff {
                            partner: target.clone(),
                            chunk: 0,
                            direction: Direction::Outbound

                        });
                        if let Ok(worker) = HandoffWorker::new(logger.clone(), id.to_string(), target, vnode.id, cnc_tx.clone()).await {
                            task::spawn(worker.handoff());
                    }
                }
            },
            Some(Task::HandoffInStart { vnode, src }) => {
                state.vnodes.remove(&vnode);
                state.vnodes.insert(vnode, VNode{id: vnode, data: vec![], handoff: Some(Handoff{partner: src, chunk: 0, direction: Direction::Inbound})});
            }
            Some(Task::HandoffIn { mut data, vnode, chunk }) => {
                info!(
                    logger,
                    "accepting vnode {} with: {:?}", vnode, data
                );
                if let Some(node) = state.vnodes.get_mut(&vnode) {
                    if let Some(ref mut handoff) = &mut node.handoff {
                        assert_eq!(handoff.chunk, chunk);
                        assert_eq!(handoff.direction, Direction::Inbound);
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
                        assert_eq!(handoff.direction, Direction::Inbound);
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

pub(crate) async fn tick_loop(
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
            tick = ticks.next().fuse() =>  if ! handle_tick(&logger, &id, &mut state, &mut tasks, &cnc_tx).await {
                break
            },
        }
    }
}
