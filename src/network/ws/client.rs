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
// use crate::{NodeId, KV};

use super::*;
//use async_std::io;
use async_std::net::TcpStream;
//use async_std::prelude::*;
//use async_std::task;
use async_tungstenite::connect_async;
use futures::channel::mpsc::{unbounded, UnboundedReceiver, UnboundedSender};
use futures::{select, FutureExt, StreamExt};
use slog::Logger;
//use std::env;
use tungstenite::protocol::Message;
use uring_common::NodeId;
use ws_proto::{Protocol, ProtocolSelect};

type WSStream = async_tungstenite::WebSocketStream<
    async_tungstenite::stream::Stream<TcpStream, async_tls::client::TlsStream<TcpStream>>,
>;

macro_rules! eat_error_and_blow {
    ($l:expr, $e:expr) => {
        match $e {
            Err(e) => {
                error!($l, "[WS Error] {}", e);
                panic!(format!("{}: {:?}", e, e));
            }
            Ok(v) => v,
        }
    };
}

pub(crate) struct Connection {
    //    my_id: u64,
    remote_id: NodeId,
    master: UnboundedSender<UrMsg>,
    tx: UnboundedSender<WsMessage>,
    rx: UnboundedReceiver<WsMessage>,
    logger: Logger,
    ws_stream: WSStream,
    handshake_done: bool,
}

/// Handle server websocket messages
impl Connection {
    fn handle(&mut self, msg: Message) -> bool {
        if self.handshake_done {
            if msg.is_binary() {
                let msg = decode_ws(&msg.into_data());
                self.master.unbounded_send(UrMsg::RaftMsg(msg)).unwrap();
            } else if msg.is_text() {
                let msg: CtrlMsg =
                    eat_error_and_blow!(self.logger, serde_json::from_slice(&msg.into_data()));
                match msg {
                    CtrlMsg::HelloAck(id, peer, peers) => {
                        self.remote_id = id;
                        eat_error_and_blow!(
                            self.logger,
                            self.master.unbounded_send(UrMsg::RegisterLocal(
                                id,
                                peer,
                                self.tx.clone(),
                                peers
                            ))
                        );
                    }
                    CtrlMsg::AckProposal(pid, success) => {
                        eat_error_and_blow!(
                            self.logger,
                            self.master.unbounded_send(UrMsg::AckProposal(pid, success))
                        );
                    }
                    _ => (),
                }
            } else {
                //
                ()
            }
        } else {
            if msg.is_text() {
                let msg: ProtocolSelect =
                    eat_error_and_blow!(self.logger, serde_json::from_slice(&msg.into_data()));
                match msg {
                    ProtocolSelect::Selected {
                        rid: RequestId(1),
                        protocol: Protocol::URing,
                    } => {
                        self.handshake_done = true;
                        self.master
                            .unbounded_send(UrMsg::InitLocal(self.tx.clone()))
                            .unwrap();
                    }
                    ProtocolSelect::Selected { rid, protocol } => {
                        error!(
                            self.logger,
                            "Wrong protocol select response: {} / {:?}", rid, protocol
                        );
                        return false;
                    }
                    ProtocolSelect::Select { rid, protocol } => {
                        error!(
                            self.logger,
                            "Select response not selected response for: {} / {:?}", rid, protocol
                        );
                        return false;
                    }

                    ProtocolSelect::As { protocol, .. } => {
                        error!(
                            self.logger,
                            "as response not selected response for:  {:?}", protocol
                        );
                        return false;
                    }
                    ProtocolSelect::Subscribe { .. } => {
                        error!(self.logger, "subscribe response not selected response for");
                        return false;
                    }
                }
            } else {
                //
                ()
            }
        }
        true
    }
}

async fn worker(
    logger: Logger,
    endpoint: String,
    master: UnboundedSender<UrMsg>,
) -> std::io::Result<()> {
    let url = url::Url::parse(&format!("ws://{}", endpoint)).unwrap();
    loop {
        let logger = logger.clone();
        let mut ws_stream = if let Ok((ws_stream, _)) = connect_async(url.clone()).await {
            ws_stream
        } else {
            error!(logger, "Failed to connect to {}", endpoint);
            break;
        };
        let (tx, rx) = unbounded::<WsMessage>();
        ws_stream
            .send(Message::Text(
                serde_json::to_string(&ProtocolSelect::Select {
                    rid: RequestId(1),
                    protocol: Protocol::URing,
                })
                .unwrap(),
            ))
            .await
            .unwrap();
        let mut c = Connection {
            logger,
            remote_id: NodeId(0),
            master: master.clone(),
            ws_stream,
            handshake_done: false,
            rx,
            tx,
        };
        loop {
            select! {
                msg = c.rx.next().fuse() =>
                    match msg {
                        Some(WsMessage::Raft(msg)) => {c.ws_stream.send(Message::Binary(encode_ws(msg).to_vec())).await.unwrap();},
                        Some(WsMessage::Ctrl(msg)) => {c.ws_stream.send(Message::Text(serde_json::to_string(&msg).unwrap())).await.unwrap();},
                        Some(WsMessage::Reply(msg)) => {c.ws_stream.send(Message::Text(serde_json::to_string(&msg).unwrap())).await.unwrap();},
                        None => break
                },
                msg = c.ws_stream.next().fuse() => {
                    if let Some(Ok(msg)) = msg {
                        if ! c.handle(msg) {
                            break
                        }
                    }

                }
            }
        }
        c.master
            .unbounded_send(UrMsg::DownLocal(c.remote_id))
            .unwrap();
    }

    Ok(())
}

pub(crate) async fn remote_endpoint(
    endpoint: String,
    master: UnboundedSender<UrMsg>,
    logger: Logger,
) -> std::io::Result<()> {
    worker(logger, endpoint, master).await
}
