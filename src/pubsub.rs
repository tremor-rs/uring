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
use crossbeam_channel::{bounded, Receiver, Sender, TryRecvError};
use futures::{Async, Poll};
use serde::{Deserialize, Serialize};
use slog::Logger;
use std::collections::HashMap;
use std::thread::{self, JoinHandle};

pub type Channel = Sender<Msg>;

#[derive(Serialize, Deserialize, Debug, Clone)]
pub enum SubscriberMsg {
    Msg {
        channel: String,
        msg: serde_json::Value,
    },
}
pub enum Msg {
    Subscrube {
        channel: String,
        tx: Sender<SubscriberMsg>,
    },
    Msg {
        channel: String,
        msg: serde_json::Value,
    },
}

pub enum Error {
    Disconnected,
}

pub struct Stream(Receiver<SubscriberMsg>);

impl Stream {
    pub fn new(e: Receiver<SubscriberMsg>) -> Self {
        Self(e)
    }
}

impl futures::Stream for Stream {
    type Item = SubscriberMsg;
    type Error = Error;
    fn poll(&mut self) -> Poll<Option<Self::Item>, Self::Error> {
        match self.0.try_recv() {
            Ok(e) => Poll::Ok(Async::Ready(Some(e))),
            Err(TryRecvError::Empty) => Poll::Ok(Async::NotReady),
            Err(TryRecvError::Disconnected) => Poll::Err(Error::Disconnected),
        }
    }
}
fn pubsub_loop(logger: Logger, rx: Receiver<Msg>) {
    let mut subscriptions: HashMap<String, Vec<Sender<SubscriberMsg>>> = HashMap::new();
    for msg in rx {
        match msg {
            Msg::Subscrube { channel, tx } => {
                debug!(logger, "Sub {}", channel);
                let subscriptions = subscriptions.entry(channel).or_default();
                subscriptions.push(tx);
            }
            Msg::Msg { channel, msg } => {
                debug!(logger, "Msg: {} >> {}", channel, msg);
                let subscriptions = subscriptions.entry(channel.clone()).or_default();
                *subscriptions = subscriptions
                    .iter()
                    .cloned()
                    .filter_map(|tx| {
                        let channel = channel.clone();
                        let msg = msg.clone();
                        if tx.send(SubscriberMsg::Msg { channel, msg }).is_ok() {
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

pub(crate) fn start(logger: &Logger) -> (JoinHandle<()>, Sender<Msg>) {
    let logger = logger.clone();
    let (tx, rx) = bounded(100);

    let h = thread::spawn(move || pubsub_loop(logger, rx));
    (h, tx)
}
