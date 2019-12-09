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

use crate::vnode::Task;
use async_tungstenite::connect_async;
use futures::channel::mpsc::Sender;
use futures::{select, FutureExt, SinkExt, StreamExt};
use slog::Logger;
use tungstenite::protocol::Message;
use uring_common::{MRingNodes, Relocations, RequestId};
use ws_proto::{MRRequest, PSMRing, Protocol, ProtocolSelect, Reply, SubscriberMsg};

pub(crate) async fn run(logger: Logger, id: String, connect_addr: String, mut tasks: Sender<Task>) {
    let url = url::Url::parse(&connect_addr).unwrap();

    // After the TCP connection has been established, we set up our client to
    // start forwarding data.
    //
    // First we do a WebSocket handshake on a TCP stream, i.e. do the upgrade
    // request.
    //
    // Half of the work we're going to do is to take all data we receive on
    // stdin (`stdin_rx`) and send that along the WebSocket stream (`sink`).
    // The second half is to take all the data we receive (`stream`) and then
    // write that to stdout. Currently we just write to stdout in a synchronous
    // fashion.
    //
    // Finally we set the client to terminate once either half of this work
    // finishes. If we don't have any more data to read or we won't receive any
    // more work from the remote then we can exit.
    let (mut ws_stream, _) = connect_async(url).await.expect("Failed to connect");
    info!(
        logger,
        "WebSocket handshake has been successfully completed"
    );

    // subscribe to uring messages
    //
    ws_stream
        .send(Message::text(
            serde_json::to_string(&ProtocolSelect::Subscribe {
                channel: "mring".into(),
            })
            .unwrap(),
        ))
        .await
        .unwrap();

    ws_stream
        .send(Message::text(
            serde_json::to_string(&ProtocolSelect::Select {
                protocol: Protocol::MRing,
                rid: RequestId(1),
            })
            .unwrap(),
        ))
        .await
        .unwrap();

    ws_stream
        .send(Message::text(
            serde_json::to_string(&MRRequest::AddNode {
                node: id.to_string(),
                rid: RequestId(2),
            })
            .unwrap(),
        ))
        .await
        .unwrap();

    loop {
        select! {
            msg = ws_stream.next().fuse() => match msg {
                Some(Ok(msg)) =>{
                    if msg.is_text() {
                        handle_msg(&logger, &id, &mut tasks, msg.into_data()).await;
                    }
                },
                Some(Err(_e)) => break,
                None => break,
            },
            complete => break
        }
    }
}

async fn handle_msg(logger: &Logger, id: &str, tasks: &mut Sender<Task>, msg: Vec<u8>) {
    match serde_json::from_slice(&msg) {
        Ok(SubscriberMsg::Msg { msg, .. }) => match serde_json::from_value(msg) {
            Ok(PSMRing::SetSize { size, .. }) => info!(logger, "Size set to {}", size),
            Ok(PSMRing::NodeAdded {
                node,
                next,
                relocations,
                ..
            }) => {
                info!(logger, "Node '{}' added", node);
                handle_change(logger, &id, tasks, relocations, next).await;
            }
            Ok(PSMRing::NodeRemoved {
                node,
                next,
                relocations,
                ..
            }) => {
                info!(logger, "Node '{}' removed", node,);
                handle_change(logger, &id, tasks, relocations, next).await;
            }
            Err(e) => error!(logger, "failed to decode: {}", e),
        },
        Err(e) => {
            if serde_json::from_slice::<Reply>(&msg).is_err()
                && serde_json::from_slice::<ProtocolSelect>(&msg).is_err()
            {
                error!(
                    logger,
                    "failed to decode: {} for '{:?}'",
                    e,
                    String::from_utf8(msg)
                )
            }
        }
    }
}

async fn handle_change(
    _logger: &Logger,
    id: &str,
    tasks: &mut Sender<Task>,
    mut relocations: Relocations,
    next: MRingNodes,
) {
    // This is an initial assignment
    if relocations.is_empty() {
    if let Some(vnode) = next.iter().filter(|v| v.id == id).next() {
                tasks
                .send(Task::Assign {
                    vnodes: vnode.vnodes.clone(),
                })
                .await
                .unwrap();

        };
        tasks
        .send(Task::Update {
            next: next.clone(),
        })
        .await            .unwrap();

    } else {
        if let Some(relocations) = relocations.remove(id) {
            tasks
            .send(Task::Update {
                next,
            })
            .await
            .unwrap();
            for (target, ids) in relocations.destinations.into_iter() {
                for vnode in ids {
                    let target = target.clone();
                    tasks
                        .send(Task::HandoffOut { target, vnode})
                        .await
                        .unwrap();
                }
            }
        }
    }
}
