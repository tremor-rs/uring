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
use crate::{pubsub, storage, ServiceId};
use async_std::sync::Mutex;
use async_trait::async_trait;
use futures::SinkExt;
use raft::RawNode;
use serde_derive::{Deserialize, Serialize};
use slog::Logger;

pub const ID: ServiceId = ServiceId(0);

#[derive(Serialize, Deserialize, Debug, Clone)]
pub(crate) enum PSEvent {
    Put {
        scope: u16,
        key: String,
        new: String,
        old: Option<String>,
    },
    Cas {
        scope: u16,
        key: String,
        new: String,
        old: Option<String>,
    },
    CasConflict {
        scope: u16,
        key: String,
        new: String,
        conflict: Option<String>,
    },
    Delete {
        scope: u16,
        key: String,
        old: Option<String>,
    },
}
pub struct Service {
    scope: u16,
    logger: Logger,
}

impl Service {
    pub fn new(logger: &Logger, scope: u16) -> Self {
        Self {
            scope,
            logger: logger.clone(),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum Event {
    Get {
        key: Vec<u8>,
    },
    Put {
        key: Vec<u8>,
        value: Vec<u8>,
    },
    Cas {
        key: Vec<u8>,
        check_value: Option<Vec<u8>>,
        store_value: Vec<u8>,
    },
    Delete {
        key: Vec<u8>,
    },
}

impl Event {
    pub fn get(key: Vec<u8>) -> Vec<u8> {
        serde_json::to_vec(&Event::Get { key }).unwrap()
    }
    pub fn put(key: Vec<u8>, value: Vec<u8>) -> Vec<u8> {
        serde_json::to_vec(&Event::Put { key, value }).unwrap()
    }
    pub fn cas(key: Vec<u8>, check_value: Option<Vec<u8>>, store_value: Vec<u8>) -> Vec<u8> {
        serde_json::to_vec(&Event::Cas {
            key,
            check_value,
            store_value,
        })
        .unwrap()
    }
    pub fn delete(key: Vec<u8>) -> Vec<u8> {
        serde_json::to_vec(&Event::Delete { key }).unwrap()
    }
}

#[async_trait]
impl<Storage> super::Service<Storage> for Service
where
    Storage: storage::Storage + Send + Sync + 'static,
{
    async fn execute(
        &mut self,
        node: &Mutex<RawNode<Storage>>,
        pubsub: &mut pubsub::Channel,
        event: Vec<u8>,
    ) -> Result<(u16, Vec<u8>), Error> {
        let raft_node = node.try_lock().unwrap();
        let storage = raft_node.store();
        match serde_json::from_slice(&event) {
            Ok(Event::Get { key }) => {
                debug!(
                    self.logger,
                    "READ {:?}",
                    String::from_utf8(key.clone()).ok()
                );
                if let Some(s) = storage
                    .get(self.scope, &key)
                    .await
                    .and_then(|v| String::from_utf8(v).ok())
                {
                    Ok((200u16, serde_json::to_vec(&s).unwrap()))
                } else {
                    Ok((404u16, serde_json::to_vec(&"not found").ok().unwrap()))
                }
            }
            Ok(Event::Put { key, value }) => {
                debug!(
                    self.logger,
                    "WROTE {:?}: {:?}",
                    String::from_utf8(key.clone()).ok(),
                    String::from_utf8(value.clone()).ok()
                );
                let old = storage
                    .get(self.scope, &key)
                    .await
                    .and_then(|value| String::from_utf8(value).ok());
                storage.put(self.scope, &key, &value).await;
                let msg = serde_json::to_value(&PSEvent::Put {
                    scope: self.scope,
                    key: String::from_utf8(key).unwrap_or_default(),
                    new: String::from_utf8(value).unwrap_or_default(),
                    old: old.clone(),
                })
                .unwrap();
                pubsub
                    .send(pubsub::Msg::Msg {
                        channel: "kv".into(),
                        msg: msg,
                    })
                    .await
                    .unwrap();

                if let Some(old) = old {
                    Ok((201, serde_json::to_vec(&old).unwrap()))
                } else {
                    Ok((201, serde_json::to_vec(&serde_json::Value::Null).unwrap()))
                }
            }
            Ok(Event::Cas {
                key,
                check_value,
                store_value,
            }) => {
                if let Some(conflict) = storage
                    .cas(
                        self.scope,
                        &key,
                        check_value.as_ref().map(|v| v.as_slice()),
                        &store_value,
                    )
                    .await
                {
                    let conflict = conflict.and_then(|o| String::from_utf8(o).ok());
                    let msg = serde_json::to_value(&PSEvent::CasConflict {
                        scope: self.scope,
                        key: String::from_utf8(key).unwrap_or_default(),
                        new: String::from_utf8(store_value).unwrap_or_default(),
                        conflict: conflict.clone(),
                    })
                    .unwrap();
                    pubsub
                        .send(pubsub::Msg::Msg {
                            channel: "kv".into(),
                            msg: msg,
                        })
                        .await
                        .unwrap();
                    if let Some(conflict) = conflict {
                        Ok((
                            409,
                            serde_json::to_vec(&serde_json::Value::String(conflict)).unwrap(),
                        ))
                    } else {
                        Ok((409, serde_json::to_vec(&serde_json::Value::Null).unwrap()))
                    }
                } else {
                    let old = check_value.and_then(|c| String::from_utf8(c).ok());
                    let new = String::from_utf8(store_value).ok();
                    let msg = serde_json::to_value(&PSEvent::Cas {
                        scope: self.scope,
                        key: String::from_utf8(key).unwrap_or_default(),
                        new: new.clone().unwrap_or_default(),
                        old,
                    })
                    .unwrap();
                    pubsub
                        .send(pubsub::Msg::Msg {
                            channel: "kv".into(),
                            msg: msg,
                        })
                        .await
                        .unwrap();
                    Ok((201, serde_json::to_vec(&"set").unwrap()))
                }
            }
            Ok(Event::Delete { key }) => Ok((
                200u16,
                storage
                    .delete(self.scope, &key)
                    .await
                    .and_then(|v| String::from_utf8(v).ok())
                    .and_then(|s| serde_json::to_vec(&serde_json::Value::String(s)).ok())
                    .unwrap(),
            )),
            _ => Err(Error::UnknownEvent),
        }
    }
    fn is_local(&self, event: &[u8]) -> Result<bool, Error> {
        match serde_json::from_slice(&event) {
            Ok(Event::Get { .. }) => Ok(true),
            Ok(Event::Put { .. }) => Ok(false),
            Ok(Event::Cas { .. }) => Ok(false),
            Ok(Event::Delete { .. }) => Ok(false),
            _ => Err(Error::UnknownEvent),
        }
    }
}
