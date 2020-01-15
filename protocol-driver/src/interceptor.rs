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

use crate::{
    DriverErrorType, HandlerInboundChannelReceiver, HandlerInboundChannelSender,
    HandlerInboundMessage, HandlerOutboundChannelReceiver, HandlerOutboundChannelSender,
    HandlerOutboundMessage,
};
use async_trait::async_trait;
use futures::channel::mpsc::{channel, SendError};
use futures::{select, SinkExt, StreamExt};
use std::collections::HashMap;
use uring_common::RequestId;

#[async_trait]
pub trait Intercept {
    async fn inbound(&mut self, msg: HandlerInboundMessage) -> Reply;
    async fn outbound(&mut self, id: RequestId, data: Vec<u8>) -> Result<Vec<u8>, DriverErrorType> {
        let _id = id;
        Ok(data)
    }
}

pub struct Interceptor<Handler>
where
    Handler: Intercept,
{
    // interceptor In Tx
    pub tx: HandlerInboundChannelSender,
    rx: HandlerInboundChannelReceiver,

    // interceptor In Rx
    //network: Sender<UrMsg>,
    next_tx: Option<HandlerInboundChannelSender>,

    // Interceptor Out Rx
    service_reply_tx: HandlerOutboundChannelSender,
    service_reply_rx: HandlerOutboundChannelReceiver,

    // Interceptor Out Tx
    pending: HashMap<RequestId, HandlerOutboundChannelSender>,
    handler: Handler,
}

pub enum Reply {
    Ok(HandlerInboundMessage),
    Err(DriverErrorType),
    Terminate,
}

impl<T> Interceptor<T>
where
    T: Intercept,
{
    pub fn new(handler: T) -> Self {
        let (tx, rx) = channel(64);
        let (service_reply_tx, service_reply_rx) = channel(64);
        Self {
            handler,
            rx,
            tx,
            next_tx: None,
            service_reply_rx,
            service_reply_tx,
            pending: HashMap::new(),
        }
    }

    pub fn connect_next<TNext: Intercept>(&mut self, next: &mut Interceptor<TNext>) {
        self.next_tx = Some(next.tx.clone());
    }

    pub async fn run_loop(mut self) -> Result<(), SendError> {
        loop {
            select! {
                msg = self.rx.next() => {
                    if let Some(msg) = msg {
                        self.inbound_handler(msg).await?;
                    } else {
                        // ARGH! errro
                        break;
                    };
                },
                msg = self.service_reply_rx.next() => {
                    if let Some(msg) = msg {
                        self.outbound_handler(msg).await?;
                    } else {
                        // ARGH! errro
                        break;
                    };

                }
            }
        }
        Ok(())
    }
    async fn outbound_handler(&mut self, msg: HandlerOutboundMessage) -> Result<(), SendError> {
        if let Some(mut tx) = self.pending.remove(&msg.id) {
            tx.send(msg).await
        } else {
            // TODO: handle error
            Ok(())
        }
    }
    async fn inbound_handler(&mut self, mut msg: HandlerInboundMessage) -> Result<(), SendError> {
        let id = msg.id;
        let mut outbound_channel = msg.outbound_channel;
        msg.outbound_channel = self.service_reply_tx.clone();
        match self.handler.inbound(msg).await {
            Reply::Ok(msg) => {
                self.pending.insert(id, outbound_channel);
                if let Some(ref mut next) = self.next_tx {
                    next.send(msg).await?;
                };
            }
            Reply::Terminate => {
                self.pending.insert(id, outbound_channel);
            }
            Reply::Err(e) => {
                outbound_channel
                    .send(HandlerOutboundMessage::error(id, e, "Interceptor failed"))
                    .await?;
            }
        }
        Ok(())
    }
}
