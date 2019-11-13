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

pub mod placement;
use super::*;
use crate::{storage, ServiceId};
use bytes::BufMut;
use serde::{Deserialize, Serialize};
use std::io::Cursor;

pub const VNODE_SERVICE: ServiceId = ServiceId(1);

pub struct Service {}

impl Service {
    pub fn new() -> Self {
        Self {}
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum Event {
    GetSize,
    SetSize { size: u64 },
}

impl Event {
    pub fn get_size() -> Vec<u8> {
        serde_json::to_vec(&Event::GetSize).unwrap()
    }
    pub fn set_size(size: u64) -> Vec<u8> {
        serde_json::to_vec(&Event::SetSize { size }).unwrap()
    }
}

pub const RING_SIZE: &[u8; 9] = b"ring-size";

impl<Storage> super::Service<Storage> for Service
where
    Storage: storage::Storage,
{
    fn execute(&mut self, storage: &Storage, event: Vec<u8>) -> Result<Option<Vec<u8>>, Error> {
        match serde_json::from_slice(&event) {
            Ok(Event::GetSize) => Ok(storage.get(VNODE_SERVICE.0 as u16, RING_SIZE)),
            Ok(Event::SetSize { size }) => {
                let mut data = vec![0; 8];
                {
                    let mut data = Cursor::new(&mut data[..]);
                    data.put_u64_be(size);
                }
                storage.put(VNODE_SERVICE.0 as u16, RING_SIZE, &data);

                Ok(Some(vec![]))
            }
            _ => Err(Error::UnknownEvent),
        }
    }
    fn is_local(&self, event: &[u8]) -> Result<bool, Error> {
        match serde_json::from_slice(&event) {
            Ok(Event::GetSize) => Ok(true),
            Ok(Event::SetSize { .. }) => Ok(false),
            _ => Err(Error::UnknownEvent),
        }
    }
}
