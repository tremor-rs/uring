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
use crate::{storage, ServiceId};
use serde::{Deserialize, Serialize};

pub const KV_SERVICE: ServiceId = ServiceId(0);

pub struct Service {
    scope: u16,
}

impl Service {
    pub fn new(scope: u16) -> Self {
        Self { scope }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum Event {
    Get {
        key: Vec<u8>,
    },
    Post {
        key: Vec<u8>,
        value: Vec<u8>,
    },
    Cas {
        key: Vec<u8>,
        check_value: Vec<u8>,
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
    pub fn post(key: Vec<u8>, value: Vec<u8>) -> Vec<u8> {
        serde_json::to_vec(&Event::Post { key, value }).unwrap()
    }
    pub fn cas(key: Vec<u8>, check_value: Vec<u8>, store_value: Vec<u8>) -> Vec<u8> {
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

impl<Storage> super::Service<Storage> for Service
where
    Storage: storage::Storage,
{
    fn execute(&mut self, storage: &Storage, event: Vec<u8>) -> Result<Option<Vec<u8>>, Error> {
        match serde_json::from_slice(&event) {
            Ok(Event::Get { key }) => Ok(storage.get(self.scope, &key)),
            Ok(Event::Post { key, value }) => {
                storage.put(self.scope, &key, &value);
                Ok(Some(value))
            }
            Ok(Event::Cas {
                key,
                check_value,
                store_value,
            }) => {
                storage.cas(self.scope, &key, &check_value, &store_value);
                Ok(Some(store_value))
            }
            Ok(Event::Delete { key }) => Ok(storage.delete(self.scope, &key)),
            _ => Err(Error::UnknownEvent),
        }
    }
    fn is_local(&self, event: &[u8]) -> Result<bool, Error> {
        match serde_json::from_slice(&event) {
            Ok(Event::Get { .. }) => Ok(true),
            Ok(Event::Post { .. }) => Ok(false),
            Ok(Event::Cas { .. }) => Ok(false),
            Ok(Event::Delete { .. }) => Ok(false),
            _ => Err(Error::UnknownEvent),
        }
    }
}
