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
use futures::channel::mpsc::{UnboundedReceiver, UnboundedSender};
use futures::{select, FutureExt, StreamExt};
use slog::Logger;
use std::collections::HashMap;
use std::time::Duration;
use tungstenite::protocol::Message;

async fn do_migrate(logger: Logger, target: String, vnode: u64, cnc: UnboundedSender<Cmd>) {
    let url = url::Url::parse(&format!("ws://{}", target)).unwrap();
    let (mut ws_stream, _) = connect_async(url).await.expect("Failed to connect");
    info!(logger, "Starting migration for vnode {}", vnode);
    ws_stream
        .send(Message::text(
            serde_json::to_string(&MigrationMsg::Start {
                src: "".into(),
                vnode,
            })
            .unwrap(),
        ))
        .await
        .unwrap();
    if let Some(Ok(data)) = ws_stream.next().await {
        let r: MigrationAck = serde_json::from_slice(&data.into_data()).unwrap();
        assert_eq!(r, MigrationAck::Start { vnode })
    } else {
        error!(logger, "failed to start transfair for vnode {}", vnode);
        // FIXME error handling / rety
        panic!();
    };

    let mut chunk = 0;
    loop {
        info!(logger, "requesting chunk {} from vnode {}", chunk, vnode);
        let (tx, mut rx) = futures::channel::mpsc::unbounded();
        cnc.unbounded_send(Cmd::GetMigrationData {
            vnode,
            chunk,
            reply: tx.clone(),
        })
        .unwrap();
        if let Some((r_chunk, data)) = rx.next().await {
            assert_eq!(chunk, r_chunk);
            if data.is_empty() {
                info!(logger, "transfer for vnode {} finished", vnode);
                break;
            }
            info!(logger, "transfering chunk {} from vnode {}", chunk, vnode);
            ws_stream
                .send(Message::text(
                    serde_json::to_string(&MigrationMsg::Data {
                        vnode,
                        chunk,
                        data,
                    })
                    .unwrap(),
                ))
                .await
                .unwrap();
            if let Some(Ok(data)) = ws_stream.next().await {
                let r: MigrationAck = serde_json::from_slice(&data.into_data()).unwrap();
                assert_eq!(r, MigrationAck::Data { chunk })
            } else {
                error!(logger, "failed to start transfair for vnode {}", vnode);
                // FIXME error handling / rety
                panic!();
            };
            chunk += 1;
        } else {
            error!(
                logger,
                "error tranfairing chunk {} for vnode {}", chunk, vnode
            );
            break;
        }
    }
    info!(logger, "finalizing trnsfer for vnode {}", vnode);
    ws_stream
        .send(Message::text(
            serde_json::to_string(&MigrationMsg::Finish { vnode: vnode }).unwrap(),
        ))
        .await
        .unwrap();
        cnc.unbounded_send(Cmd::FinishMigration { vnode }).unwrap();
    if let Some(Ok(data)) = ws_stream.next().await {
        let r: MigrationAck = serde_json::from_slice(&data.into_data()).unwrap();
        assert_eq!(r, MigrationAck::Finish { vnode })
    } else {
        error!(logger, "failed to start transfair for vnode {}", vnode);
        // FIXME error handling / rety
        panic!();
    };
}

enum Cmd {
    GetMigrationData {
        vnode: u64,
        chunk: u64,
        reply: UnboundedSender<(u64, Vec<String>)>,
    },
    FinishMigration {
        vnode: u64,
    },
}
pub(crate) async fn tick_loop(logger: Logger, id: String, mut tasks: UnboundedReceiver<Task>) {
    let mut vnodes: HashMap<u64, VNode> = HashMap::new();
    let (cnc_tx, mut cnc_rx) = futures::channel::mpsc::unbounded();

    let mut ticks = async_std::stream::interval(Duration::from_secs(1));
    loop {
        select! {
            cmd = cnc_rx.next() => {
                match cmd  {
                    Some(Cmd::GetMigrationData{vnode, chunk, reply}) => {
                        if let Some(vnode) = vnodes.get_mut(&vnode) {
                            if let Some(ref mut migration) = vnode.migration {
                                assert_eq!(chunk, migration.chunk);
                                assert_eq!(migration.direction, Direction::Outbound);
                                if let Some(data) = vnode.data.pop() {
                                    reply.unbounded_send((chunk, vec![data])).unwrap();
                                } else {
                                    reply.unbounded_send((chunk, vec![])).unwrap();
                                };
                                migration.chunk += 1;
                            } else {
                                info!(logger, "Not in a migraiton");
                            }
                        } else {
                            info!(logger, "Unknown vnode");
                        }
                    },
                    Some(Cmd::FinishMigration{vnode}) => { let v= vnodes.remove(&vnode).unwrap(); let m = v.migration.unwrap(); assert_eq!(m.direction, Direction::Outbound);},
                    None => ()
                }
            }
            tick = ticks.next().fuse() =>  select! {
                task = tasks.next() =>
                match task {
                    Some(Task::MigrateOut { target, vnode }) => {
                        if let Some(vnode) = vnodes.get_mut(&vnode){
                            info!(
                                logger,
                                "relocating vnode {} to node {}", vnode.id, target
                            );
                            assert!(vnode.migration.is_none());
                            vnode.migration = Some(Migration {
                                partner: target.clone(),
                                chunk: 0,
                                direction: Direction::Outbound

                            });
                            task::spawn(do_migrate(logger.clone(), target, vnode.id, cnc_tx.clone()));
                        }
                    },

                    Some(Task::MigrateInStart { vnode, src }) => {
                        if vnodes.contains_key(&vnode) {
                            error!(logger, "vnode {} already known", vnode)
                        } else {
                            vnodes.insert(vnode, VNode{id: vnode, data: vec![], migration: Some(Migration{partner: src, chunk: 0, direction: Direction::Inbound})});

                        }
                    }
                    Some(Task::MigrateIn { mut data, vnode, chunk }) => {
                        info!(
                            logger,
                            "accepting vnode {} with: {:?}", vnode, data
                        );
                        if let Some(node) = vnodes.get_mut(&vnode) {
                            if let Some(ref mut migration) = &mut node.migration {
                                assert_eq!(migration.chunk, chunk);
                                assert_eq!(migration.direction, Direction::Inbound);
                                node.data.append(&mut data);
                                migration.chunk = chunk + 1;
                            } else {
                                error!(logger, "no migration in progress for data vnode {}", vnode)

                            }
                        } else {
                            error!(logger, "no migration in progress for data vnode {}", vnode)
                        }
                    },
                    Some(Task::MigrateInEnd { vnode }) => {
                        if let Some(node) = vnodes.get_mut(&vnode) {
                            if let Some(ref mut migration) = &mut node.migration {
                                assert_eq!(migration.direction, Direction::Inbound);
                            } else {
                                error!(logger, "no migration in progress for data vnode {}", vnode)
                            }
                            node.migration = None;
                            node.data.push(id.clone());
                        } else {
                            error!(logger, "no migration in progress for data vnode {}", vnode)
                        }
                    }
                    Some(Task::Assign{vnodes: ids}) => {
                        info!(logger, "Initializing with {:?}", ids);
                        let my_id = &id;
                        for id in ids {
                            vnodes.insert(
                                id,
                                VNode {
                                    migration: None,
                                    id,
                                    data: vec![my_id.to_string()],
                                },
                            );
                        }
                    }
                    None => break,
                },
                complete => break,
                default => ()
            }
        }
    }
}
