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
use futures::channel::mpsc::{UnboundedReceiver};
use futures::{select, StreamExt};
use slog::Logger;
use std::collections::HashMap;
use std::time::Duration;
use tungstenite::protocol::Message;

async fn do_migrate(logger: Logger, target: String, vnode: VNode) {
    let url = url::Url::parse(&format!("ws://{}", target)).unwrap();
    let (mut ws_stream, _) = connect_async(url).await.expect("Failed to connect");
    ws_stream
        .send(Message::text(
            serde_json::to_string(&MigrationMsg::Start {
                src: "".into(),
                vnode: vnode.id,
            })
            .unwrap(),
        ))
        .await
        .unwrap();

    ws_stream
        .send(Message::text(
            serde_json::to_string(&MigrationMsg::Data {
                vnode: vnode.id,
                id: 0,
                data: vnode.history,
            })
            .unwrap(),
        ))
        .await
        .unwrap();

    ws_stream
        .send(Message::text(
            serde_json::to_string(&MigrationMsg::Finish { vnode: vnode.id }).unwrap(),
        ))
        .await
        .unwrap();
}

pub(crate) async fn tick_loop(logger: Logger, id: String, mut tasks: UnboundedReceiver<Task>) {
    let mut vnodes: HashMap<u64, VNode> = HashMap::new();

    let mut ticks = async_std::stream::interval(Duration::from_secs(1));
    while ticks.next().await.is_some() {
        select! {
            task = tasks.next() =>
            match task {
                Some(Task::MigrateOut { target, vnode }) => {
                    if let Some(vnode) = vnodes.remove(&vnode){
                        info!(
                            logger,
                            "relocating vnode {} to node {}", vnode.id, target
                        );
                        task::spawn(do_migrate(logger.clone(), target, vnode));
                    }
                },

                Some(Task::MigrateInStart { vnode, src }) => {
                    if vnodes.contains_key(&vnode) {
                        error!(logger, "vnode {} already known", vnode)
                    } else {
                        vnodes.insert(vnode, VNode{id: vnode, history: vec![], migration: Some(Migration{partner: src, chunk: 0, direction: Direction::Inbound})});

                    }
                }
                Some(Task::MigrateIn { mut data, vnode, id }) => {
                    info!(
                        logger,
                        "accepting vnode {} with: {:?}", vnode, data
                    );
                    if let Some(node) = vnodes.get_mut(&vnode) {
                        if let Some(ref mut migration) = &mut node.migration {
                            assert_eq!(migration.chunk, id);
                            assert_eq!(migration.direction, Direction::Inbound);
                            node.history.append(&mut data);
                            migration.chunk = id + 1;
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
                                history: vec![my_id.to_string()],
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
