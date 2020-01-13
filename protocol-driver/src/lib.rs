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
#![recursion_limit = "1024"]

pub mod interceptor;

use futures::channel::mpsc::{channel, Receiver, SendError, Sender};
use futures::{select, SinkExt, StreamExt};
use serde_derive::{Deserialize, Serialize};
use std::{collections::HashMap, hash::Hash};
pub use uring_common::{RequestId, ServiceId};

pub type CustomProtocol = String;

#[derive(Deserialize, Serialize, PartialEq, Eq, Hash)]
pub enum Protocol {
    Connect,
    None,
    Custom(CustomProtocol),
}

#[derive(Deserialize, Serialize)]
pub enum DriverInboundData {
    Message(Vec<u8>),
    Select(CustomProtocol),
    As(CustomProtocol, Vec<u8>),
    Connect(Vec<CustomProtocol>),
    Disconnect,
}

#[derive(Deserialize, Serialize, PartialEq, Eq)]
pub enum DriverErrorType {
    SystemError,  // 500
    LogicalError, // 412
    Conflict,     // 409
    BadInput,     // 406
    NotFound,     // 404
    BadProtocol,  //
    InvalidRequest,
}

#[derive(Deserialize, Serialize, PartialEq, Eq)]
pub struct DriverError {
    pub error: DriverErrorType,
    pub message: String,
}

pub enum DriverOutboundData {
    Ok(Vec<u8>),
    Error(DriverError),
}

pub type ClientId = u64;
pub type CorrelationId = u64;

pub struct ClientConnection {
    protocol: Protocol,
    enabled_protocols: Vec<CustomProtocol>,
}

impl Default for ClientConnection {
    fn default() -> Self {
        ClientConnection {
            protocol: Protocol::Connect,
            enabled_protocols: vec![],
        }
    }
}

#[derive(Hash, PartialEq, Eq, Clone, Copy)]
pub struct MessageId {
    client: ClientId,
    correlation: CorrelationId,
}
impl MessageId {
    pub fn new(client: ClientId, correlation: CorrelationId) -> Self {
        Self {
            client,
            correlation,
        }
    }
}

pub struct DriverInboundMessage {
    pub data: DriverInboundData,
    pub outbound_channel: DriverOutboundChannelSender,
    pub id: MessageId,
}
pub type DriverInboundChannelReceiver = Receiver<DriverInboundMessage>;
pub type DriverInboundChannelSender = Sender<DriverInboundMessage>;

pub struct DriverOutboundMessage {
    pub data: DriverOutboundData,
    pub id: MessageId,
}
impl DriverOutboundMessage {
    fn ok(id: MessageId, data: Vec<u8>) -> Self {
        Self {
            id,
            data: DriverOutboundData::Ok(data),
        }
    }
    fn error<T>(id: MessageId, error: DriverErrorType, message: T) -> Self
    where
        T: ToString,
    {
        Self {
            id,
            data: DriverOutboundData::Error(DriverError {
                error,
                message: message.to_string(),
            }),
        }
    }
}
pub type DriverOutboundChannelReceiver = Receiver<DriverOutboundMessage>;
pub type DriverOutboundChannelSender = Sender<DriverOutboundMessage>;

pub type HandlerOutboundData = DriverOutboundData;

pub struct HandlerInboundMessage {
    pub data: Vec<u8>,
    pub outbound_channel: HandlerOutboundChannelSender,
    pub service_id: Option<ServiceId>,
    pub id: RequestId,
}

pub struct HandlerOutboundMessage {
    pub data: HandlerOutboundData,
    pub id: RequestId,
}

impl HandlerOutboundMessage {
    pub fn ok(id: RequestId, data: Vec<u8>) -> Self {
        Self {
            id,
            data: DriverOutboundData::Ok(data),
        }
    }
    pub fn error<T>(id: RequestId, error: DriverErrorType, message: T) -> Self
    where
        T: ToString,
    {
        Self {
            id,
            data: DriverOutboundData::Error(DriverError {
                error,
                message: message.to_string(),
            }),
        }
    }
}
pub type HandlerOutboundChannelReceiver = Receiver<HandlerOutboundMessage>;
pub type HandlerOutboundChannelSender = Sender<HandlerOutboundMessage>;

pub type HandlerInboundChannelReceiver = Receiver<HandlerInboundMessage>;
pub type HandlerInboundChannelSender = Sender<HandlerInboundMessage>;

pub struct Driver {
    transport_rx: DriverInboundChannelReceiver,
    pub transport_tx: DriverInboundChannelSender,

    handler_rx: HandlerOutboundChannelReceiver,
    handler_tx: HandlerOutboundChannelSender,

    pending: HashMap<RequestId, (MessageId, DriverOutboundChannelSender)>,
    clients: HashMap<ClientId, ClientConnection>,
    protocol_handlers: HashMap<CustomProtocol, HandlerInboundChannelSender>,

    next_rid: u64,
}

impl Default for Driver {
    fn default() -> Self {
        let (transport_tx, transport_rx) = channel(64);
        let (handler_tx, handler_rx) = channel(64);
        Self {
            transport_rx,
            transport_tx,
            handler_rx,
            handler_tx,
            pending: HashMap::new(),
            clients: HashMap::new(),
            protocol_handlers: HashMap::new(),
            next_rid: 0,
        }
    }
}

impl Driver {
    pub async fn run_loop(&mut self) -> Result<(), SendError> {
        loop {
            select! {
                msg = self.transport_rx.next() => {
                    if let Some(msg) = msg {
                        self.inbound_handler(msg).await?;
                    } else {
                        // ARGH! errro
                        break;
                    };
                },
                msg = self.handler_rx.next() => {
                    if let Some(msg) = msg {
                        self.outbound_handler(msg).await?;
                    } else {
                        // ARGH! errro
                        break;
                    };

                },
            };
        }
        Ok(())
    }

    async fn outbound_handler(&mut self, msg: HandlerOutboundMessage) -> Result<(), SendError> {
        let HandlerOutboundMessage { data, id } = msg;
        if let Some((id, mut transport)) = self.pending.remove(&id) {
            let msg = DriverOutboundMessage { id, data };
            transport.send(msg).await?;
        };
        Ok(())
    }

    #[allow(mutable_transmutes)]
    async fn inbound_handler(&mut self, msg: DriverInboundMessage) -> Result<(), SendError> {
        let DriverInboundMessage {
            data,
            id,
            mut outbound_channel,
        } = msg;

        let client = self.clients.entry(id.client).or_default();
        let keep_client = match (&client.protocol, &data) {
            // When we're in connect
            (Protocol::Connect, DriverInboundData::Connect(protos)) => {
                //TODO: Validate protocols
                client.protocol = Protocol::None;
                client.enabled_protocols = protos.clone();
                outbound_channel
                    .send(DriverOutboundMessage::ok(id, vec![]))
                    .await
                    .is_ok()
            }
            (Protocol::Connect, _) => outbound_channel
                .send(DriverOutboundMessage::error(
                    id,
                    DriverErrorType::InvalidRequest,
                    "Can not call Connect twice",
                ))
                .await
                .is_ok(),
            // When we've not selected a protocol
            (Protocol::Custom(_), DriverInboundData::As(proto, data))
            | (Protocol::None, DriverInboundData::As(proto, data)) => {
                if let Some(handler) = self.protocol_handlers.get(proto) {
                    // Rust does not recognize that the mutable borrow of handler never will relocate
                    // when we register pending so we got to transmute the hell out of it.
                    let handler: &mut Sender<_> = unsafe { std::mem::transmute(handler) };
                    let rid = self.register_pending(id, outbound_channel);
                    let msg = HandlerInboundMessage {
                        id: rid,
                        data: data.clone(),
                        outbound_channel: self.handler_tx.clone(),
                        service_id: None,
                    };
                    handler.send(msg).await?;
                    true
                } else {
                    outbound_channel
                        .send(DriverOutboundMessage::error(
                            id,
                            DriverErrorType::BadProtocol,
                            format!("Invalid protocol {}", proto),
                        ))
                        .await
                        .is_ok()
                }
            }
            (Protocol::None, DriverInboundData::Select(proto))
            | (Protocol::Custom(_), DriverInboundData::Select(proto)) => {
                if client.enabled_protocols.contains(proto) {
                    client.protocol = Protocol::Custom(proto.clone());
                    outbound_channel
                        .send(DriverOutboundMessage::ok(id, vec![]))
                        .await
                        .is_ok()
                } else {
                    outbound_channel
                        .send(DriverOutboundMessage::error(
                            id,
                            DriverErrorType::BadProtocol,
                            format!("Invalid protocol {}", proto),
                        ))
                        .await
                        .is_ok()
                }
            }
            (Protocol::None, _) => outbound_channel
                .send(DriverOutboundMessage::error(
                    id,
                    DriverErrorType::InvalidRequest,
                    "No protocol specified",
                ))
                .await
                .is_ok(),
            // we have a default
            (Protocol::Custom(_), DriverInboundData::Connect(_)) => outbound_channel
                .send(DriverOutboundMessage::error(
                    id,
                    DriverErrorType::InvalidRequest,
                    "Can not call Connect twice",
                ))
                .await
                .is_ok(),
            (Protocol::Custom(proto), DriverInboundData::Message(data)) => {
                if !client.enabled_protocols.contains(&proto) {
                    outbound_channel
                        .send(DriverOutboundMessage::error(
                            id,
                            DriverErrorType::BadProtocol,
                            format!("Protocol {} is not enabled.", proto),
                        ))
                        .await
                        .is_ok()
                } else if let Some(handler) = self.protocol_handlers.get(proto) {
                    // Rust does not recognize that the mutable borrow of handler never will relocate
                    // when we register pending so we got to transmute the hell out of it.
                    let handler: &mut Sender<_> = unsafe { std::mem::transmute(handler) };
                    let rid = self.register_pending(id, outbound_channel);
                    let msg = HandlerInboundMessage {
                        id: rid,
                        data: data.clone(),
                        outbound_channel: self.handler_tx.clone(),
                        service_id: None,
                    };
                    handler.send(msg).await?;
                    true
                } else {
                    outbound_channel
                        .send(DriverOutboundMessage::error(
                            id,
                            DriverErrorType::BadProtocol,
                            format!("Protocol {} is not known.", proto),
                        ))
                        .await
                        .is_ok()
                }
            }
            (_, DriverInboundData::Disconnect) => false,
        };
        if !keep_client {
            self.clients.remove(&id.client);
        }
        Ok(())
    }

    fn register_pending(
        &mut self,
        mid: MessageId,
        sender: DriverOutboundChannelSender,
    ) -> RequestId {
        let rid = RequestId(self.next_rid);
        self.next_rid += 1;
        self.pending.insert(rid, (mid, sender));
        rid
    }
}

#[cfg(test)]
mod tests {
    #[test]
    fn it_works() {
        assert_eq!(2 + 2, 4);
    }
}
