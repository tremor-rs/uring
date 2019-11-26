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
use async_std::net::TcpStream;
use futures::channel::mpsc::{UnboundedReceiver, UnboundedSender};
use futures::{select, FutureExt, StreamExt};
use slog::Logger;
use std::collections::HashMap;
use std::time::Duration;
use tungstenite::protocol::Message;

type WSStream = async_tungstenite::WebSocketStream<
    async_tungstenite::stream::Stream<TcpStream, async_tls::client::TlsStream<TcpStream>>,
>;

struct HandoffWorker {
    logger: Logger,
    src: String,
    target: String,
    vnode: u64,
    cnc: UnboundedSender<Cmd>,
    ws_stream: WSStream,
    chunk: u64,
}

enum HandoffError {
    ConnectionFailed,
    Cancled,
}

impl HandoffWorker {
    pub async fn new(
        logger: Logger,
        src: String,
        target: String,
        vnode: u64,
        cnc: UnboundedSender<Cmd>,
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
            cnc.unbounded_send(Cmd::CancleHandoff { vnode, target })
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
        self.cnc
            .unbounded_send(Cmd::FinishHandoff { vnode })
            .unwrap();
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
        let (tx, mut rx) = futures::channel::mpsc::unbounded();
        if self
            .cnc
            .unbounded_send(Cmd::GetHandoffData {
                vnode,
                chunk,
                reply: tx.clone(),
            })
            .is_err()
        {
            return self.cancle();
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
            self.cancle()
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
            return self.cancle();
        };

        if let Some(Ok(data)) = self.ws_stream.next().await {
            let r: HandoffAck = serde_json::from_slice(&data.into_data()).unwrap();
            if ack != r {
                error!(self.logger, "Bad reply for  handoff command {:?} for vnode {} to {}. Expected, {:?}, but gut {:?}", msg, self.vnode, self.target, ack, r);
                return self.cancle();
            }
        } else {
            error!(
                self.logger,
                "No reply for  handoff command {:?} for vnode {} to {}",
                msg,
                self.vnode,
                self.target
            );
            return self.cancle();
        }
        Ok(())
    }

    fn cancle<T>(&self) -> Result<T, HandoffError>
    where
        T: Default,
    {
        self.cnc
            .unbounded_send(Cmd::CancleHandoff {
                vnode: self.vnode,
                target: self.target.clone(),
            })
            .unwrap();
        Err(HandoffError::Cancled)
    }
}

enum Cmd {
    GetHandoffData {
        vnode: u64,
        chunk: u64,
        reply: UnboundedSender<(u64, Vec<String>)>,
    },
    FinishHandoff {
        vnode: u64,
    },
    CancleHandoff {
        vnode: u64,
        target: String,
    },
}

fn handle_cmd(
    logger: &Logger,
    cmd: Option<Cmd>,
    vnodes: &mut HashMap<u64, VNode>,
    tasks_tx: &UnboundedSender<Task>,
) {
    match cmd {
        Some(Cmd::GetHandoffData {
            vnode,
            chunk,
            reply,
        }) => {
            if let Some(vnode) = vnodes.get_mut(&vnode) {
                if let Some(ref mut handoff) = vnode.handoff {
                    assert_eq!(chunk, handoff.chunk);
                    assert_eq!(handoff.direction, Direction::Outbound);
                    if let Some(data) = vnode.data.get(chunk as usize) {
                        reply.unbounded_send((chunk, vec![data.clone()])).unwrap();
                    } else {
                        reply.unbounded_send((chunk, vec![])).unwrap();
                    };
                    handoff.chunk += 1;
                } else {
                    info!(logger, "Not in a migraiton");
                }
            } else {
                info!(logger, "Unknown vnode");
            }
        }
        Some(Cmd::CancleHandoff { vnode, target }) => {
            if let Some(node) = vnodes.get_mut(&vnode) {
                assert!(node.handoff.is_some());
                node.handoff = None;
                warn!(
                    logger,
                    "Canceling handoff of vnode {} to {} - requeing to restart", vnode, target
                );
                tasks_tx
                    .unbounded_send(Task::HandoffOut { target, vnode })
                    .unwrap();
            } else {
                info!(logger, "Unknown vnode");
            }
        }
        Some(Cmd::FinishHandoff { vnode }) => {
            let v = vnodes.remove(&vnode).unwrap();
            let m = v.handoff.unwrap();
            assert_eq!(m.direction, Direction::Outbound);
        }
        None => (),
    }
}

async fn handle_tick(
    logger: &Logger,
    id: &str,
    vnodes: &mut HashMap<u64, VNode>,
    tasks: &mut UnboundedReceiver<Task>,
    cnc_tx: &UnboundedSender<Cmd>,
) -> bool {
    select! {
        task = tasks.next() =>
        match task {
            Some(Task::HandoffOut { target, vnode }) => {
                if let Some(vnode) = vnodes.get_mut(&vnode){
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
                vnodes.remove(&vnode);
                vnodes.insert(vnode, VNode{id: vnode, data: vec![], handoff: Some(Handoff{partner: src, chunk: 0, direction: Direction::Inbound})});
            }
            Some(Task::HandoffIn { mut data, vnode, chunk }) => {
                info!(
                    logger,
                    "accepting vnode {} with: {:?}", vnode, data
                );
                if let Some(node) = vnodes.get_mut(&vnode) {
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
                if let Some(node) = vnodes.get_mut(&vnode) {
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
                    vnodes.insert(
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
    mut tasks: UnboundedReceiver<Task>,
    tasks_tx: UnboundedSender<Task>,
) {
    let mut vnodes: HashMap<u64, VNode> = HashMap::new();
    let (cnc_tx, mut cnc_rx) = futures::channel::mpsc::unbounded();

    let mut ticks = async_std::stream::interval(Duration::from_secs(1));
    loop {
        select! {
            cmd = cnc_rx.next() => handle_cmd(&logger, cmd, &mut vnodes, &tasks_tx),
            tick = ticks.next().fuse() =>  if ! handle_tick(&logger, &id, &mut vnodes, &mut tasks, &cnc_tx).await {
                break
            },
        }
    }
}
