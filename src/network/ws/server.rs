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

use super::Reply;
use super::*;
use crate::{pubsub, NodeId};
use actix::prelude::*;
use actix_web_actors::ws;
use serde::Serialize;
use ws_proto::*;

#[derive(Message, Serialize)]
pub(crate) struct WsReply(pub ws_proto::Reply);

/// websocket connection is long running connection, it easier
/// to handle with an actor
pub(crate) struct Connection {
    /// Client must send ping at least once per 10 seconds (CLIENT_TIMEOUT),
    /// otherwise we drop connection.
    hb: Instant,
    node: Node,
    remote_id: NodeId,
    protocol: Option<Protocol>,
}

impl Actor for Connection {
    type Context = ws::WebsocketContext<Self>;

    /// Method is called on actor start. We start the heartbeat process here.
    fn started(&mut self, ctx: &mut Self::Context) {
        self.hb(ctx);
    }

    fn stopped(&mut self, _ctx: &mut Self::Context) {
        self.node
            .tx
            .send(UrMsg::DownRemote(self.remote_id))
            .unwrap();
    }
}

/// Handler for `ws::Message`
/// Handler for `ws::Message`
impl StreamHandler<ws::Message, ws::ProtocolError> for Connection {
    fn handle(&mut self, msg: ws::Message, ctx: &mut Self::Context) {
        match self.protocol {
            None => self.handle_initial(msg, ctx),
            Some(Protocol::KV) => self.handle_kv(msg, ctx),
            Some(Protocol::URing) => self.handle_uring(msg, ctx),
            Some(Protocol::MRing) => self.handle_mring(msg, ctx),
        }
    }
}

impl StreamHandler<SubscriberMsg, pubsub::Error> for Connection {
    fn handle(&mut self, msg: SubscriberMsg, ctx: &mut Self::Context) {
        ctx.text(serde_json::to_string(&msg).unwrap());
    }
}

impl Handler<WsReply> for Connection {
    type Result = ();
    fn handle(&mut self, msg: WsReply, ctx: &mut Self::Context) {
        match self.protocol {
            None => ctx.text(serde_json::to_string(&msg).unwrap()),
            Some(Protocol::KV) => ctx.text(serde_json::to_string(&msg).unwrap()),
            Some(Protocol::MRing) => ctx.text(serde_json::to_string(&msg).unwrap()),
            Some(Protocol::URing) => ctx.stop(),
        }
    }
}

/// Handle stdin commands
impl Handler<WsMessage> for Connection {
    type Result = ();

    fn handle(&mut self, msg: WsMessage, ctx: &mut Self::Context) {
        match msg {
            WsMessage::Msg(data) => ctx.text(serde_json::to_string(&data).unwrap()),
        }
    }
}

/// Handle stdin commands
impl Handler<RaftMsg> for Connection {
    type Result = ();
    fn handle(&mut self, msg: RaftMsg, ctx: &mut Self::Context) {
        ctx.binary(encode_ws(msg));
    }
}

impl Connection {
    pub(crate) fn new(node: Node) -> Self {
        Self {
            hb: Instant::now(),
            node,
            remote_id: NodeId(0),
            protocol: None,
        }
    }

    fn handle_initial(&mut self, msg: ws::Message, ctx: &mut ws::WebsocketContext<Self>) {
        match msg {
            ws::Message::Ping(msg) => {
                self.hb = Instant::now();
                ctx.pong(&msg);
            }
            ws::Message::Pong(_) => {
                self.hb = Instant::now();
            }
            ws::Message::Text(text) => {
                match serde_json::from_str(&text) {
                    Ok(ProtocolSelect::Select { rid, protocol }) => {
                        self.protocol = Some(protocol);
                        ctx.text(
                            serde_json::to_string(&ProtocolSelect::Selected { rid, protocol })
                                .unwrap(),
                        );
                    }
                    Ok(ProtocolSelect::Selected { .. }) => {
                        ctx.stop();
                    }
                    Ok(ProtocolSelect::As { protocol, cmd }) => match protocol {
                        Protocol::KV => {
                            self.handle_kv_msg(serde_json::from_value(cmd).unwrap(), ctx)
                        }
                        _ => ctx.stop(),
                    },
                    Ok(ProtocolSelect::Subscribe { channel }) => {
                        let (tx, rx) = bounded(10);
                        self.node
                            .pubsub
                            .send(pubsub::Msg::Subscribe { channel, tx })
                            .unwrap();

                        let stream = pubsub::Stream::new(rx);
                        Self::add_stream(stream, ctx);
                    }
                    Err(e) => error!(
                        self.node.logger,
                        "Failed to decode ProtocolSelect message: {} => {}", e, text
                    ),
                }

                //ctx.text(text)
            }
            ws::Message::Binary(_) => {
                ctx.stop();
            }
            ws::Message::Close(_) => {
                ctx.stop();
            }
            ws::Message::Nop => (),
        }
    }

    fn handle_uring(&mut self, msg: ws::Message, ctx: &mut ws::WebsocketContext<Self>) {
        match msg {
            ws::Message::Ping(msg) => {
                self.hb = Instant::now();
                ctx.pong(&msg);
            }
            ws::Message::Pong(_) => {
                self.hb = Instant::now();
            }
            ws::Message::Text(text) => {
                match serde_json::from_str(&text) {
                    Ok(CtrlMsg::Hello(id, peer)) => {
                        self.remote_id = id;
                        self.node
                            .tx
                            .send(UrMsg::RegisterRemote(id, peer, ctx.address()))
                            .unwrap();
                    }
                    Ok(CtrlMsg::AckProposal(pid, success)) => {
                        self.node.tx.send(UrMsg::AckProposal(pid, success)).unwrap();
                    }
                    Ok(CtrlMsg::ForwardProposal(from, pid, key, value)) => {
                        self.node
                            .tx
                            .send(UrMsg::ForwardProposal(from, pid, key, value))
                            .unwrap();
                    }
                    Ok(_) => (),
                    Err(e) => error!(
                        self.node.logger,
                        "Failed to decode CtrlMsg message: {} => {}", e, text
                    ),
                }
                //ctx.text(text)
            }
            ws::Message::Binary(bin) => {
                let msg = decode_ws(&bin);
                self.node.tx.send(UrMsg::RaftMsg(msg)).unwrap();
            }
            ws::Message::Close(_) => {
                ctx.stop();
            }
            ws::Message::Nop => (),
        }
    }

    fn handle_kv(&mut self, msg: ws::Message, ctx: &mut ws::WebsocketContext<Self>) {
        match msg {
            ws::Message::Ping(msg) => {
                self.hb = Instant::now();
                ctx.pong(&msg);
            }
            ws::Message::Pong(_) => {
                self.hb = Instant::now();
            }
            ws::Message::Text(text) => match serde_json::from_str(&text) {
                Ok(msg) => self.handle_kv_msg(msg, ctx),
                Err(e) => error!(
                    self.node.logger,
                    "Failed to decode KVRequest message: {} => {}", e, text
                ),
            },
            ws::Message::Binary(_) => {
                ctx.stop();
            }
            ws::Message::Close(_) => {
                ctx.stop();
            }
            ws::Message::Nop => (),
        }
    }

    fn handle_kv_msg(&mut self, msg: KVRequest, ctx: &mut ws::WebsocketContext<Self>) {
        match msg {
            KVRequest::Get { rid, key } => {
                self.node
                    .tx
                    .send(UrMsg::Get(key.into_bytes(), Reply::WS(rid, ctx.address())))
                    .unwrap();
            }
            KVRequest::Put { rid, key, store } => {
                self.node
                    .tx
                    .send(UrMsg::Put(
                        key.into_bytes(),
                        store.into_bytes(),
                        Reply::WS(rid, ctx.address()),
                    ))
                    .unwrap();
            }
            KVRequest::Delete { rid, key } => {
                self.node
                    .tx
                    .send(UrMsg::Delete(
                        key.into_bytes(),
                        Reply::WS(rid, ctx.address()),
                    ))
                    .unwrap();
            }
            KVRequest::Cas {
                rid,
                key,
                check,
                store,
            } => {
                self.node
                    .tx
                    .send(UrMsg::Cas(
                        key.into_bytes(),
                        check.into_bytes(),
                        store.into_bytes(),
                        Reply::WS(rid, ctx.address()),
                    ))
                    .unwrap();
            }
        }
    }

    fn handle_mring(&mut self, msg: ws::Message, ctx: &mut ws::WebsocketContext<Self>) {
        match msg {
            ws::Message::Ping(msg) => {
                self.hb = Instant::now();
                ctx.pong(&msg);
            }
            ws::Message::Pong(_) => {
                self.hb = Instant::now();
            }
            ws::Message::Text(text) => match serde_json::from_str(&text) {
                Ok(msg) => self.handle_mring_msg(msg, ctx),
                Err(e) => error!(
                    self.node.logger,
                    "Failed to decode MRRequest message: {} => {}", e, text
                ),
            },
            ws::Message::Binary(_) => {
                ctx.stop();
            }
            ws::Message::Close(_) => {
                ctx.stop();
            }
            ws::Message::Nop => (),
        }
    }

    fn handle_mring_msg(&mut self, msg: MRRequest, ctx: &mut ws::WebsocketContext<Self>) {
        match msg {
            MRRequest::GetSize { rid } => {
                self.node
                    .tx
                    .send(UrMsg::MRingGetSize(Reply::WS(rid, ctx.address())))
                    .unwrap();
            }

            MRRequest::SetSize { rid, size } => {
                self.node
                    .tx
                    .send(UrMsg::MRingSetSize(size, Reply::WS(rid, ctx.address())))
                    .unwrap();
            }
            MRRequest::GetNodes { rid } => {
                self.node
                    .tx
                    .send(UrMsg::MRingGetNodes(Reply::WS(rid, ctx.address())))
                    .unwrap();
            }
            MRRequest::AddNode { rid, node } => {
                self.node
                    .tx
                    .send(UrMsg::MRingAddNode(node, Reply::WS(rid, ctx.address())))
                    .unwrap();
            }
            MRRequest::RemoveNode { rid, node } => {
                self.node
                    .tx
                    .send(UrMsg::MRingRemoveNode(node, Reply::WS(rid, ctx.address())))
                    .unwrap();
            }
        }
    }

    /// helper method that sends ping to client every second.
    ///
    /// also this method checks heartbeats from client
    fn hb(&self, ctx: &mut <Self as Actor>::Context) {
        ctx.run_interval(HEARTBEAT_INTERVAL, |act, ctx| {
            // check client heartbeats
            if Instant::now().duration_since(act.hb) > CLIENT_TIMEOUT {
                // heartbeat timed out
                error!(
                    act.node.logger,
                    "Websocket Client heartbeat failed, disconnecting!"
                );

                // stop actor
                ctx.stop();

                // don't try to send a ping
                return;
            }

            ctx.ping("");
        });
    }
}
