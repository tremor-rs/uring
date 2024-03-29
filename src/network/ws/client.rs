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
use async_std::net::TcpStream;
use async_tungstenite::async_std::connect_async;
use futures::channel::mpsc::{channel, Receiver, Sender, UnboundedSender};
use futures::{select, FutureExt, StreamExt};
use slog::Logger;
use tungstenite::protocol::Message;
use uring_common::NodeId;
use ws_proto::{Protocol, ProtocolSelect};

type WSStream = async_tungstenite::WebSocketStream<TcpStream>;

macro_rules! eat_error_and_blow {
    ($l:expr, $e:expr) => {
        match $e {
            Err(e) => {
                error!($l, "[WS Error] {}", e);
                panic!("{}: {:?}", e, e);
            }
            Ok(v) => v,
        }
    };
}

pub(crate) struct Connection {
    //    my_id: u64,
    remote_id: NodeId,
    handler: UnboundedSender<UrMsg>,
    tx: Sender<WsMessage>,
    rx: Receiver<WsMessage>,
    logger: Logger,
    ws_stream: WSStream,
    handshake_done: bool,
}

/// Handle server websocket messages
impl Connection {
    async fn handle(&mut self, msg: Message) -> bool {
        if self.handshake_done {
            if msg.is_binary() {
                let msg = decode_ws(msg.into_data());
                self.handler.unbounded_send(UrMsg::RaftMsg(msg)).unwrap();
            } else if msg.is_text() {
                let msg: CtrlMsg =
                    eat_error_and_blow!(self.logger, serde_json::from_slice(&msg.into_data()));
                match msg {
                    CtrlMsg::HelloAck(id, peer, peers) => {
                        self.remote_id = id;
                        eat_error_and_blow!(
                            self.logger,
                            self.handler
                                .send(UrMsg::RegisterLocal(id, peer, self.tx.clone(), peers))
                                .await
                        );
                    }
                    CtrlMsg::AckProposal(pid, success) => {
                        eat_error_and_blow!(
                            self.logger,
                            self.handler
                                .unbounded_send(UrMsg::AckProposal(pid, success))
                        );
                    }
                    _ => (),
                }
            } else {
                dbg!(("unknown message type", msg));
                // Nothing to do
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
                        self.handler
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
                    ProtocolSelect::Version { .. } => {
                        error!(self.logger, "version response not selected response for");
                        return false;
                    }
                    ProtocolSelect::Status { .. } => {
                        error!(self.logger, "status response not selected response for");
                        return false;
                    }
                }
            } else {
                dbg!(("unknown message type", msg));
                ()
            }
        }
        true
    }
}

async fn worker(
    logger: Logger,
    endpoint: String,
    handler: UnboundedSender<UrMsg>,
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
        let (tx, rx) = channel::<WsMessage>(crate::CHANNEL_SIZE);
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
            handler: handler.clone(),
            ws_stream,
            handshake_done: false,
            rx,
            tx,
        };
        loop {
            let cont = select! {
                msg = c.rx.next().fuse() =>
                    match msg {
                        Some(WsMessage::Raft(msg)) => {c.ws_stream.send(Message::Binary(encode_ws(msg).to_vec())).await.is_ok()},
                        Some(WsMessage::Ctrl(msg)) => {c.ws_stream.send(Message::Text(serde_json::to_string(&msg).unwrap())).await.is_ok()},
                        Some(WsMessage::Reply(_, msg)) => {c.ws_stream.send(Message::Text(serde_json::to_string(&msg).unwrap())).await.is_ok()},
                        None => false
                },
                msg = c.ws_stream.next().fuse() => {
                    if let Some(Ok(msg)) = msg {
                        c.handle(msg).await
                    } else {
                        false
                    }
                },
                complete => false
            };
            if !cont {
                break;
            }
        }
        c.handler
            .unbounded_send(UrMsg::DownLocal(c.remote_id))
            .unwrap();
    }

    Ok(())
}

pub(crate) async fn remote_endpoint(
    endpoint: String,
    handler: UnboundedSender<UrMsg>,
    logger: Logger,
) -> std::io::Result<()> {
    worker(logger, endpoint, handler).await
}
