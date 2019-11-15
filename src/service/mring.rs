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
use crate::{pubsub, storage, ServiceId};
use byteorder::{BigEndian, ReadBytesExt};
use bytes::BufMut;
use serde::{Deserialize, Serialize};
use std::io::Cursor;
use std::marker::PhantomData;

pub const MRING_SERVICE: ServiceId = ServiceId(1);

#[derive(Serialize, Deserialize, Debug, Clone)]
pub(crate) enum PSEvent {
    SetSize {
        size: u64,
    },
    NodeAdded {
        node: String,
        next: placement::Nodes,
        relocations: placement::Relocations,
    },
    NodeRemoved {
        node: String,
        next: placement::Nodes,
        relocations: placement::Relocations,
    },
}

pub struct Service<Placement>
where
    Placement: placement::Placement,
{
    marker: PhantomData<Placement>,
}

impl<Placement> Service<Placement>
where
    Placement: placement::Placement,
{
    pub fn new() -> Self {
        Self {
            marker: PhantomData::default(),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum Event {
    GetSize,
    SetSize { size: u64 },
    GetNodes,
    AddNode { node: String },
    RemoveNode { node: String },
}

impl Event {
    pub fn get_size() -> Vec<u8> {
        serde_json::to_vec(&Event::GetSize).unwrap()
    }
    pub fn set_size(size: u64) -> Vec<u8> {
        serde_json::to_vec(&Event::SetSize { size }).unwrap()
    }
    pub fn get_nodes() -> Vec<u8> {
        serde_json::to_vec(&Event::GetNodes).unwrap()
    }
    pub fn add_node(node: String) -> Vec<u8> {
        serde_json::to_vec(&Event::AddNode { node }).unwrap()
    }
    pub fn remove_node(node: String) -> Vec<u8> {
        serde_json::to_vec(&Event::RemoveNode { node }).unwrap()
    }
}

pub const RING_SIZE: &[u8; 9] = b"ring-size";
pub const NODES: &[u8; 10] = b"ring-nodes";

impl<Placement> Service<Placement>
where
    Placement: placement::Placement,
{
    fn size<Storage>(&self, storage: &Storage) -> Option<u64>
    where
        Storage: storage::Storage,
    {
        storage
            .get(MRING_SERVICE.0 as u16, RING_SIZE)
            .and_then(|v| {
                let mut rdr = Cursor::new(v);
                rdr.read_u64::<BigEndian>().ok()
            })
    }

    fn nodes<Storage>(&self, storage: &Storage) -> Option<placement::Nodes>
    where
        Storage: storage::Storage,
    {
        storage
            .get(MRING_SERVICE.0 as u16, NODES)
            .and_then(|v| serde_json::from_slice(&v).ok())
    }
}

impl<Storage, Placement> super::Service<Storage> for Service<Placement>
where
    Storage: storage::Storage,
    Placement: placement::Placement,
{
    fn execute(
        &mut self,
        storage: &Storage,
        pubsub: &pubsub::Channel,
        event: Vec<u8>,
    ) -> Result<Option<Vec<u8>>, Error> {
        match serde_json::from_slice(&event) {
            Ok(Event::GetSize) => Ok(storage.get(MRING_SERVICE.0 as u16, RING_SIZE)),
            Ok(Event::SetSize { size }) => {
                if let Some(data) = storage.get(MRING_SERVICE.0 as u16, RING_SIZE) {
                    return Ok(Some(data));
                }
                let mut data = vec![0; 8];
                {
                    let mut data = Cursor::new(&mut data[..]);
                    data.put_u64_be(size);
                }
                storage.put(MRING_SERVICE.0 as u16, RING_SIZE, &data);

                Ok(Some(data))
            }
            Ok(Event::GetNodes) => Ok(storage.get(MRING_SERVICE.0 as u16, NODES)),
            Ok(Event::AddNode { node }) => {
                let size = if let Some(size) = self.size(storage) {
                    size
                } else {
                    return Ok(None);
                };
                let next = if let Some(current) = self.nodes(storage) {
                    let (next, relocations) = Placement::add_node(size, current, node.clone());
                    let msg = serde_json::to_value(&PSEvent::NodeAdded {
                        node,
                        next: next.clone(),
                        relocations,
                    })
                    .unwrap();
                    pubsub
                        .send(pubsub::Msg::Msg {
                            channel: "mring".into(),
                            msg: msg,
                        })
                        .unwrap();
                    next
                } else {
                    let next = Placement::new(size, node.clone());

                    let msg = serde_json::to_value(&PSEvent::NodeAdded {
                        node,
                        next: next.clone(),
                        relocations: placement::Relocations::new(),
                    })
                    .unwrap();
                    pubsub
                        .send(pubsub::Msg::Msg {
                            channel: "mring".into(),
                            msg: msg,
                        })
                        .unwrap();

                    next
                };
                let next = serde_json::to_vec(&next).unwrap();
                storage.put(MRING_SERVICE.0 as u16, NODES, &next);
                Ok(Some(next))
            }
            Ok(Event::RemoveNode { node }) => {
                let size = if let Some(size) = self.size(storage) {
                    size
                } else {
                    return Ok(None);
                };

                if let Some(current) = self.nodes(storage) {
                    let (next, relocations) = Placement::remove_node(size, current, node.clone());
                    let msg = serde_json::to_value(&PSEvent::NodeRemoved {
                        node,
                        next: next.clone(),
                        relocations,
                    })
                    .unwrap();
                    pubsub
                        .send(pubsub::Msg::Msg {
                            channel: "mring".into(),
                            msg: msg,
                        })
                        .unwrap();
                    let next = serde_json::to_vec(&next).unwrap();
                    storage.put(MRING_SERVICE.0 as u16, NODES, &next);
                    Ok(Some(next))
                } else {
                    Ok(None)
                }
            }
            Err(_) => Err(Error::UnknownEvent),
        }
    }
    fn is_local(&self, event: &[u8]) -> Result<bool, Error> {
        match serde_json::from_slice(&event) {
            Ok(Event::GetSize) => Ok(true),
            Ok(Event::GetNodes) => Ok(true),
            Ok(Event::SetSize { .. }) => Ok(false),
            Ok(Event::AddNode { .. }) => Ok(false),
            Ok(Event::RemoveNode { .. }) => Ok(false),
            Err(_) => Err(Error::UnknownEvent),
        }
    }
}
