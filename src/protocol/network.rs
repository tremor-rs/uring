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

use crate::network::ws::{ProtocolMessage, UrMsg};
use async_trait::async_trait;
use futures::channel::mpsc::Sender;
use futures::SinkExt;
use protocol_driver::{interceptor, DriverErrorType, HandlerInboundMessage};

pub struct Handler {
    network: Sender<UrMsg>,
}

#[async_trait]
impl interceptor::Intercept for Handler {
    async fn inbound(&mut self, msg: HandlerInboundMessage) -> interceptor::Reply {
        if let Some(service_id) = msg.service_id {
            if self
                .network
                .send(UrMsg::Protocol(ProtocolMessage::Event {
                    id: msg.id,
                    service_id,
                    event: msg.data,
                    reply: msg.outbound_channel,
                }))
                .await
                .is_ok()
            {
                interceptor::Reply::Terminate
            } else {
                interceptor::Reply::Err(DriverErrorType::SystemError)
            }
        } else {
            interceptor::Reply::Err(DriverErrorType::LogicalError)
        }
    }
}
