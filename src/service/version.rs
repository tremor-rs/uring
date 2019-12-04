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
use crate::version::VERSION;
use crate::{storage, ServiceId};
use async_std::sync::Mutex;
use async_trait::async_trait;
use raft::RawNode;
use serde_derive::{Deserialize, Serialize};
use slog::Logger;

pub const ID: ServiceId = ServiceId(2);

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
impl<Storage> super::Service<Storage> for Service
where
    Storage: storage::Storage + Send + Sync + 'static,
{
    async fn execute(
        &mut self,
        _node: &Mutex<RawNode<Storage>>,
        _pubsub: &mut pubsub::Channel,
        event: Vec<u8>,
    ) -> Result<(u16, Vec<u8>), Error> {
        match serde_json::from_slice(&event) {
            Ok(Event::Get) => {
                debug!(self.logger, "GET",);
                Ok((
                    200,
                    serde_json::to_vec(&serde_json::Value::String(VERSION.to_string())).unwrap(),
                ))
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
    use crate::service::Service;
    use crate::storage;
    use crate::NodeId;
    use crate::RaftNode;
    use futures::channel::mpsc::{channel, Sender};
    use futures::executor::block_on;

    #[test]
    fn test_get_version() {
        let logger = &slog::Logger::root(slog::Discard, o!());
        let mut s = super::Service::new(&logger);
        let id = NodeId(42);
        let get = serde_json::to_vec(&Event::Get).ok().unwrap();
        let network = network::NullNetwork::default();

        let fake = channel(1);
        let (tx, _rx) = fake;
        let topic = Sender::<pubsub::Msg>::from(tx);

        block_on(async {
            let mut node: RaftNode<storage::NullStorage, _> =
                RaftNode::create_raft_leader(&logger, id, topic, network).await;
            let version = s
                .execute(node.raft_group.as_ref().unwrap(), &mut node.pubsub, get)
                .await
                .ok()
                .unwrap()
                .1;
            assert_eq!(
                format!("\"{}\"", VERSION),
                String::from_utf8(version).ok().unwrap()
            );
        });
    }
}
