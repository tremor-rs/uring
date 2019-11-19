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
#![recursion_limit = "512"]

use async_std::prelude::*;
use async_std::sync::{Arc, RwLock};
use async_std::{io, task};
use async_tungstenite::connect_async;
use futures::stream::Stream;
use futures::task::Poll;
use futures::{select, FutureExt, StreamExt};
use slog::{Drain, Logger};
use std::collections::{HashMap, VecDeque};
use std::env;
use std::time::Duration;
use tungstenite::protocol::Message;
use uring_common::{MRingNodes, Relocations, RequestId};
use ws_proto::{MRRequest, PSMRing, Protocol, ProtocolSelect, Reply, SubscriberMsg};

#[macro_use]
extern crate slog;

#[derive(Debug, Clone)]
struct VNode {
    id: u64,
    history: Vec<String>,
}

enum Task {
    Relocate { target: String, vnode: u64 },
}

async fn run(logger: Logger) {
    let id = env::args()
        .nth(1)
        .unwrap_or_else(|| panic!("this program requires at least two arguments"));
    // Specify the server address to which the client will be connecting.
    let connect_addr = env::args()
        .nth(2)
        .unwrap_or_else(|| panic!("this program requires at least two argument"));

    dbg!(&connect_addr);
    let url = url::Url::parse(&connect_addr).unwrap();

    // Spawn a new task that will will read data from stdin and then send it to the event loop over
    // a standard futures channel.
    let (stdin_tx, mut stdin_rx) = futures::channel::mpsc::channel(64);
    task::spawn(read_stdin(stdin_tx));

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
    let mut stdout = io::stdout();
    let (mut ws_stream, _) = connect_async(url).await.expect("Failed to connect");
    info!(
        logger,
        "WebSocket handshake has been successfully completed"
    );

    let vnodes: Arc<RwLock<HashMap<u64, VNode>>> = Arc::new(RwLock::new(HashMap::new()));

    let (mut tasks_tx, tasks_rx) = futures::channel::mpsc::unbounded();

    task::spawn(tick_loop(
        logger.clone(),
        id.to_string(),
        vnodes.clone(),
        tasks_rx,
    ));

    // subscribe to uring messages
    //
    ws_stream
        .send(Message::text(
            serde_json::to_string(&ProtocolSelect::Subscribe {
                channel: "mring".into(),
            })
            .unwrap(),
        ))
        .await;

    ws_stream
        .send(Message::text(
            serde_json::to_string(&ProtocolSelect::Select {
                protocol: Protocol::MRing,
                rid: RequestId(1),
            })
            .unwrap(),
        ))
        .await;

    ws_stream
        .send(Message::text(
            serde_json::to_string(&MRRequest::AddNode {
                node: id.to_string(),
                rid: RequestId(2),
            })
            .unwrap(),
        ))
        .await;

    loop {
        select! {
            txt = stdin_rx.next().fuse() => match txt {
                Some(line) => ws_stream.send(line).await.expect("failed to send"),
                None => break,
            },
            msg = ws_stream.next().fuse() => match msg {
                Some(Ok(msg)) =>{
                    if msg.is_text() {
                        handle_msg(&logger, &id, & vnodes, &mut tasks_tx, msg.into_data()).await;
                    }
                },
                Some(Err(_e)) => break,
                None => break,
            },
        }
    }
}

async fn tick_loop(
    logger: Logger,
    id: String,
    vnodes: Arc<RwLock<HashMap<u64, VNode>>>,
    mut tasks: futures::channel::mpsc::UnboundedReceiver<Task>,
) {
    let mut ticks = async_std::stream::interval(Duration::from_secs(1));
    while ticks.next().await.is_some() {
        select! {
            task = tasks.next() =>
            match task {
                Some(Task::Relocate { target, vnode }) => {
                    let mut vnodes = vnodes.write().await;
                    let success = vnodes.remove(&vnode).is_some();
                    info!(
                        logger,
                        "relocating vnode {} to node {} => success: {}", vnode, target, success
                    );
                },
                None => break,
            },
            complete => break,
            default => ()
        }
    }
}

async fn handle_msg(
    logger: &Logger,
    id: &str,
    vnodes: &Arc<RwLock<HashMap<u64, VNode>>>,
    tasks: &mut futures::channel::mpsc::UnboundedSender<Task>,
    msg: Vec<u8>,
) {
    match serde_json::from_slice(&msg) {
        Ok(SubscriberMsg::Msg { channel, msg }) => match serde_json::from_value(msg) {
            Ok(PSMRing::SetSize { size, .. }) => info!(logger, "Size set to {}", size),
            Ok(PSMRing::NodeAdded {
                node,
                next,
                relocations,
                ..
            }) => {
                info!(logger, "Node '{}' added", node);
                handle_change(logger, &id, vnodes, tasks, relocations, next).await;
            }
            Ok(PSMRing::NodeRemoved {
                node,
                next,
                relocations,
                ..
            }) => {
                info!(logger, "Node '{}' removed", node,);
                handle_change(logger, &id, vnodes, tasks, relocations, next).await;
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
    logger: &Logger,
    id: &str,
    vnodes: &Arc<RwLock<HashMap<u64, VNode>>>,
    tasks: &mut futures::channel::mpsc::UnboundedSender<Task>,
    mut relocations: Relocations,
    next: MRingNodes,
) {
    // This is an initial assignment
    if relocations.is_empty() {
        if let Some(vnode) = next.into_iter().filter(|v| v.id == id).next() {
            let is_empty = {
                let vnodes = vnodes.read().await;
                vnodes.is_empty()
            };
            if is_empty {
                info!(logger, "Initializing with {:?}", vnode.vnodes);
                let my_id = id;
                let mut vnodes = vnodes.write().await;
                for id in vnode.vnodes {
                    vnodes.insert(
                        id,
                        VNode {
                            id,
                            history: vec![my_id.to_string()],
                        },
                    );
                }
            }
        }
    } else {
        if let Some(relocations) = relocations.remove(id) {
            for (target, ids) in relocations.destinations.into_iter() {
                for vnode in ids {
                    let target = target.clone();
                    tasks.unbounded_send(Task::Relocate { target, vnode });
                }
            }
        }
    }
}

async fn handle_tick(
    logger: &Logger,
    id: &str,
    vnodes: &mut HashMap<u64, VNode>,
    tasks: &mut VecDeque<Task>,
) {
    match tasks.pop_front() {
        Some(Task::Relocate { target, vnode }) => {
            let success = vnodes.remove(&vnode).is_some();
            info!(
                logger,
                "relocating vnode {} to node {} => success: {}", vnode, target, success
            );
        }
        None => (),
    }
}

async fn read_stdin(mut tx: futures::channel::mpsc::Sender<Message>) {
    let mut stdin = io::stdin();
    loop {
        let mut buf = vec![0; 1024];
        let n = match stdin.read(&mut buf).await {
            Err(_) | Ok(0) => break,
            Ok(n) => n,
        };
        buf.truncate(n);
        tx.try_send(Message::text(String::from_utf8(buf).unwrap()))
            .unwrap();
    }
}

fn main() {
    let decorator = slog_term::TermDecorator::new().build();
    let drain = slog_term::FullFormat::new(decorator).build().fuse();
    let drain = slog_async::Async::new(drain).build().fuse();
    let logger = slog::Logger::root(drain, o!());

    task::block_on(run(logger))
}
