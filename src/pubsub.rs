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
// use crate::{NodeId, KV};
use async_std::task;
use futures::channel::mpsc::{channel, Receiver, Sender};
use futures::{SinkExt, StreamExt};
use serde::Serialize;
use slog::Logger;
use std::collections::HashMap;
use ws_proto::SubscriberMsg;

pub type Channel = Sender<Msg>;

pub enum Msg {
    Subscribe {
        channel: String,
        tx: Sender<SubscriberMsg>,
    },
    Msg {
        channel: String,
        msg: serde_json::Value,
    },
}

impl Msg {
    pub fn new<T>(channel: &str, msg: T) -> Self
    where
        T: Serialize,
    {
        let msg = serde_json::to_value(&msg).unwrap();
        let channel = channel.to_string();
        Self::Msg { channel, msg }
    }
}

async fn pubsub_loop(logger: Logger, mut rx: Receiver<Msg>) {
    let mut subscriptions: HashMap<String, Vec<Sender<SubscriberMsg>>> = HashMap::new();
    while let Some(msg) = rx.next().await {
        match msg {
            Msg::Subscribe { channel, tx } => {
                info!(logger, "Sub {}", channel);
                let subscriptions = subscriptions.entry(channel).or_default();
                subscriptions.push(tx);
            }
            Msg::Msg { channel, msg } => {
                info!(logger, "Msg: {} >> {}", channel, msg);
                let subscriptions = subscriptions.entry(channel.clone()).or_default();
                let mut s1 = Vec::with_capacity(subscriptions.len());
                for mut tx in subscriptions.drain(..) {
                    let channel = channel.clone();
                    let msg = msg.clone();
                    if tx.send(SubscriberMsg::Msg { channel, msg }).await.is_ok() {
                        s1.push(tx)
                    }
                }
                std::mem::swap(subscriptions, &mut s1);
            }
        }
    }
}

pub(crate) fn start(logger: &Logger) -> Channel {
    let logger = logger.clone();
    let (tx, rx) = channel(crate::CHANNEL_SIZE);

    task::spawn(pubsub_loop(logger, rx));
    tx
}
