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
use crate::NodeId;
use actix::prelude::*;
use actix_web_actors::ws;

/// websocket connection is long running connection, it easier
/// to handle with an actor
pub(crate) struct Connection {
    /// Client must send ping at least once per 10 seconds (CLIENT_TIMEOUT),
    /// otherwise we drop connection.
    hb: Instant,
    node: Node,
    remote_id: NodeId,
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
impl StreamHandler<ws::Message, ws::ProtocolError> for Connection {
    fn handle(&mut self, msg: ws::Message, ctx: &mut Self::Context) {
        // process websocket messages
        match msg {
            ws::Message::Ping(msg) => {
                self.hb = Instant::now();
                ctx.pong(&msg);
            }
            ws::Message::Pong(_) => {
                self.hb = Instant::now();
            }
            ws::Message::Text(text) => {
                let msg: CtrlMsg = serde_json::from_str(&text).unwrap();
                match msg {
                    CtrlMsg::Hello(id, peer) => {
                        self.remote_id = id;
                        self.node
                            .tx
                            .send(UrMsg::RegisterRemote(id, peer, ctx.address()))
                            .unwrap();
                    }
                    CtrlMsg::AckProposal(pid, success) => {
                        self.node.tx.send(UrMsg::AckProposal(pid, success)).unwrap();
                    }
                    CtrlMsg::ForwardProposal(from, pid, key, value) => {
                        self.node
                            .tx
                            .send(UrMsg::ForwardProposal(from, pid, key, value))
                            .unwrap();
                    }
                    _ => (),
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
