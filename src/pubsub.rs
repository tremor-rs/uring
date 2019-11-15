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
// use crate::{NodeId, KV};
use crossbeam_channel::{bounded, Receiver, Sender};
use slog::Logger;
use std::collections::HashMap;
use std::thread::{self, JoinHandle};

pub(crate) enum PubSubSubscriberMsg {
    Msg {
        channel: String,
        msg: serde_json::Value,
    },
}
pub(crate) enum PubSubMsg {
    Subscrube {
        channel: String,
        tx: Sender<PubSubSubscriberMsg>,
    },
    Msg {
        channel: String,
        msg: serde_json::Value,
    },
}

fn pubsub_loop(logger: Logger, rx: Receiver<PubSubMsg>) {
    let mut subscriptions: HashMap<String, Vec<Sender<PubSubSubscriberMsg>>> = HashMap::new();
    for msg in rx {
        match msg {
            PubSubMsg::Subscrube { channel, tx } => {
                debug!(logger, "Sub {}", channel);
                let subscriptions = subscriptions.entry(channel).or_default();
                subscriptions.push(tx);
            }
            PubSubMsg::Msg { channel, msg } => {
                debug!(logger, "Msg: {} >> {}", channel, msg);
                let subscriptions = subscriptions.entry(channel.clone()).or_default();
                *subscriptions = subscriptions
                    .iter()
                    .cloned()
                    .filter_map(|tx| {
                        let channel = channel.clone();
                        let msg = msg.clone();
                        if tx.send(PubSubSubscriberMsg::Msg { channel, msg }).is_ok() {
                            Some(tx)
                        } else {
                            None
                        }
                    })
                    .collect();
            }
        }
    }
}

pub(crate) fn start(logger: &Logger) -> (JoinHandle<()>, Sender<PubSubMsg>) {
    let logger = logger.clone();
    let (tx, rx) = bounded(100);

    let h = thread::spawn(move || pubsub_loop(logger, rx));
    (h, tx)
}
