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
use crate::{pubsub, storage, ServiceId};
use async_std::sync::Mutex;
use async_trait::async_trait;
use raft::RawNode;
use serde_derive::{Deserialize, Serialize};
use slog::Logger;

pub const ID: ServiceId = ServiceId(3);

pub struct Service {
    logger: Logger,
}

impl Service {
    pub fn new(logger: &Logger) -> Self {
        Self {
            logger: logger.clone(),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum Event {
    Get,
}

impl Event {
    pub fn get() -> Vec<u8> {
        serde_json::to_vec(&Event::Get).unwrap()
    }
}

#[async_trait]
impl<S> super::Service<S> for Service
where
    S: storage::Storage + Send + Sync + 'static,
{
    async fn execute(
        &mut self,
        node: &Mutex<RawNode<S>>,
        _pubsub: &mut pubsub::Channel,
        event: Vec<u8>,
    ) -> Result<(u16, Vec<u8>), Error> {
        match serde_json::from_slice(&event) {
            Ok(Event::Get) => {
                debug!(self.logger, "GET",);
                let status = crate::raft_node::status(node).await.unwrap();
                let val = serde_json::to_vec(&status).unwrap().clone();
                Ok((200, val))
            }
            _ => Err(Error::UnknownEvent),
        }
    }

    fn is_local(&self, _event: &[u8]) -> Result<bool, Error> {
        Ok(true)
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::network;
    use crate::pubsub;
    use crate::raft_node;
    use crate::service::Service;
    use crate::NodeId;
    use crate::RaftNode;
    use crate::RaftNodeStatus;
    use futures::channel::mpsc::{channel, Sender};
    use futures::executor::block_on;

    #[test]
    fn test_get_status() {
        let logger = &slog::Logger::root(slog::Discard, o!());
        let id = NodeId(42);
        let mut s = super::Service::new(&logger);
        let get = serde_json::to_vec(&Event::Get).ok().unwrap();
        let network = network::NullNetwork::default();
        let fake = channel(1);
        let (tx, _rx) = fake;
        let topic = Sender::<pubsub::Msg>::from(tx);

        {
            block_on(async {
                let mut node: RaftNode<storage::NullStorage, _> =
                    RaftNode::create_raft_leader(&logger, id, topic, network).await;
                let raw_node = node.raft_group.unwrap();
                let status_check: RaftNodeStatus = raft_node::status(&raw_node).await.unwrap();
                let status_response: RaftNodeStatus = serde_json::from_slice(
                    &s.execute(&raw_node, &mut node.pubsub, get)
                        .await
                        .ok()
                        .unwrap()
                        .1,
                )
                .unwrap();
                assert_eq!(status_response, status_check);
            });
        }
    }
}
