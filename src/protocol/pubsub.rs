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

use crate::pubsub;
use async_std::task;
use async_trait::async_trait;
use futures::channel::mpsc::channel;
use futures::{SinkExt, StreamExt};
use protocol_driver::{
    interceptor, DriverErrorType, HandlerInboundMessage, HandlerOutboundMessage,
};
use serde_derive::{Deserialize, Serialize};

#[derive(Deserialize, Serialize, Debug)]
enum Request {
    Subscribe { channel: String },
}

#[derive(Deserialize, Serialize, Debug)]
enum Reply {
    Subscribed { channel: String },
}

// FIXME: collect dead destinations
// FIXME: guarantee unique ids
pub struct Handler {
    pubsub: pubsub::Channel,
}

impl Handler {
    pub fn new(pubsub: pubsub::Channel) -> Self {
        Self { pubsub }
    }
}

#[async_trait]
impl interceptor::Intercept for Handler {
    async fn inbound(&mut self, msg: HandlerInboundMessage) -> interceptor::Reply {
        match dbg!(serde_json::from_slice(&msg.data)) {
            Ok(Request::Subscribe { channel: c }) => {
                let (tx, mut rx) = channel(64);
                let mut outbound = msg.outbound_channel;
                let id = msg.id;
                let data = serde_json::to_vec(&Reply::Subscribed { channel: c.clone() }).unwrap();
                outbound
                    .send(HandlerOutboundMessage::partial(id, data))
                    .await
                    .unwrap();

                task::spawn(async move {
                    while let Some(msg) = dbg!(rx.next().await) {
                        let data = serde_json::to_vec(&msg).unwrap();
                        outbound
                            .send(HandlerOutboundMessage::partial(id, data))
                            .await
                            .unwrap();
                    }
                });
                self.pubsub
                    .send(pubsub::Msg::Subscribe { channel: c, tx })
                    .await
                    .unwrap();

                interceptor::Reply::Terminate
            }
            Err(_) => interceptor::Reply::Err(DriverErrorType::BadInput),
        }
    }
}
