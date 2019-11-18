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
use actix::io::SinkWrite;
use actix::prelude::*;
use actix_codec::Framed;
use awc::{
    error::WsProtocolError,
    ws::{CloseCode, CloseReason, Codec as AxCodec, Frame, Message},
    BoxedSocket, Client,
};
use crossbeam_channel::Sender;
use futures::{
    lazy,
    stream::{SplitSink, Stream},
    Future,
};
use slog::Logger;
use ws_proto::{Protocol, ProtocolSelect};

macro_rules! eat_error {
    ($l:expr, $e:expr) => {
        if let Err(e) = $e {
            error!($l, "[WS Error] {}", e)
        }
    };
}

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
    tx: Sender<UrMsg>,
    logger: Logger,
    sink: SinkWrite<SplitSink<Framed<BoxedSocket, AxCodec>>>,
    handshake_done: bool,
}

impl Connection {
    fn hb(&self, ctx: &mut Context<Self>) {
        ctx.run_later(HEARTBEAT_INTERVAL, |act, ctx| {
            act.sink
                .write(Message::Ping(String::from("Snot badger!")))
                .unwrap();
            act.hb(ctx);
        });
    }
}

impl actix::io::WriteHandler<WsProtocolError> for Connection {}

impl Actor for Connection {
    type Context = Context<Self>;

    fn started(&mut self, ctx: &mut Context<Self>) {
        self.hb(ctx)
    }

    fn stopped(&mut self, _: &mut Context<Self>) {
        eat_error_and_blow!(self.logger, self.tx.send(UrMsg::DownLocal(self.remote_id)));
        info!(self.logger, "system stopped");
        System::current().stop();
    }
}

/// Handle server websocket messages
impl StreamHandler<Frame, WsProtocolError> for Connection {
    fn handle(&mut self, msg: Frame, ctx: &mut Context<Self>) {
        if self.handshake_done {
            match msg {
                Frame::Close(_) => {
                    eat_error!(self.logger, self.sink.write(Message::Close(None)));
                }
                Frame::Ping(data) => {
                    eat_error!(self.logger, self.sink.write(Message::Pong(data)));
                }
                Frame::Text(Some(data)) => {
                    let msg: CtrlMsg =
                        eat_error_and_blow!(self.logger, serde_json::from_slice(&data));
                    match msg {
                        CtrlMsg::HelloAck(id, peer, peers) => {
                            self.remote_id = id;
                            eat_error_and_blow!(
                                self.logger,
                                self.tx
                                    .send(UrMsg::RegisterLocal(id, peer, ctx.address(), peers))
                            );
                        }
                        CtrlMsg::AckProposal(pid, success) => {
                            eat_error_and_blow!(
                                self.logger,
                                self.tx.send(UrMsg::AckProposal(pid, success))
                            );
                        }
                        _ => (),
                    }
                }
                Frame::Text(None) => {
                    eat_error!(self.logger, self.sink.write(Message::Text(String::new())));
                }
                Frame::Binary(Some(bin)) => {
                    let msg = decode_ws(&bin);
                    println!("received raft message {:?}", msg);
                    self.tx.send(UrMsg::RaftMsg(msg)).unwrap();
                }
                Frame::Binary(None) => (),
                Frame::Pong(_) => (),
            }
        } else {
            match msg {
                Frame::Close(_) => {
                    eat_error!(self.logger, self.sink.write(Message::Close(None)));
                }
                Frame::Ping(data) => {
                    eat_error!(self.logger, self.sink.write(Message::Pong(data)));
                }
                Frame::Text(Some(data)) => {
                    let msg: ProtocolSelect =
                        eat_error_and_blow!(self.logger, serde_json::from_slice(&data));
                    match msg {
                        ProtocolSelect::Selected {
                            rid: RequestId(1),
                            protocol: Protocol::URing,
                        } => {
                            self.handshake_done = true;
                            self.tx.send(UrMsg::InitLocal(ctx.address())).unwrap();
                        }
                        ProtocolSelect::Selected { rid, protocol } => {
                            error!(
                                self.logger,
                                "Wrong protocol select response: {} / {:?}", rid, protocol
                            );
                            ctx.stop();
                        }
                        ProtocolSelect::Select { rid, protocol } => {
                            error!(
                                self.logger,
                                "Select response not selected response for: {} / {:?}",
                                rid,
                                protocol
                            );
                            ctx.stop();
                        }

                        ProtocolSelect::As { protocol, .. } => {
                            error!(
                                self.logger,
                                "as response not selected response for:  {:?}", protocol
                            );
                            ctx.stop();
                        }
                        ProtocolSelect::Subscribe { .. } => {
                            error!(self.logger, "subscribe response not selected response for");
                            ctx.stop();
                        }
                    }
                }
                Frame::Text(None) => {
                    ctx.stop();
                }
                Frame::Binary(_) => {
                    ctx.stop();
                }
                Frame::Pong(_) => (),
            }
        }
    }

    fn error(&mut self, error: WsProtocolError, _ctx: &mut Context<Self>) -> Running {
        match error {
            _ => eat_error!(
                self.logger,
                self.sink.write(Message::Close(Some(CloseReason {
                    code: CloseCode::Protocol,
                    description: None,
                })))
            ),
        };
        // Reconnect
        Running::Continue
    }

    fn started(&mut self, _ctx: &mut Context<Self>) {
        info!(self.logger, "[WS Onramp] Connection established.");
    }

    fn finished(&mut self, ctx: &mut Context<Self>) {
        info!(self.logger, "[WS Onramp] Connection terminated.");
        eat_error!(self.logger, self.sink.write(Message::Close(None)));
        ctx.stop()
    }
}

pub(crate) fn remote_endpoint(
    endpoint: String,
    master: Sender<UrMsg>,
    logger: Logger,
) -> std::io::Result<()> {
    loop {
        let sys = actix::System::new("ws-example");
        let endpoint1 = endpoint.clone();
        let master1 = master.clone();
        // This clones the logger passed in from the function and makes it local for each loop
        let logger = logger.clone();
        Arbiter::spawn(lazy(move || {
            let err_logger = logger.clone();
            Client::new()
                .ws(&format!("ws://{}/uring", endpoint1))
                .connect()
                .map_err(move |e| {
                    error!(err_logger, "Error: {}", e);
                    ()
                })
                .map(move |(_response, framed)| {
                    let (sink, stream) = framed.split();
                    let master2 = master1.clone();
                    Connection::create(move |ctx| {
                        Connection::add_stream(stream, ctx);
                        let mut sink = SinkWrite::new(sink, ctx);
                        sink.write(Message::Text(
                            serde_json::to_string(&ProtocolSelect::Select {
                                rid: RequestId(1),
                                protocol: Protocol::URing,
                            })
                            .unwrap(),
                        ))
                        .unwrap();
                        Connection {
                            logger,
                            remote_id: NodeId(0),
                            tx: master2,
                            sink,
                            handshake_done: false,
                        }
                    });
                })
        }));
        sys.run().unwrap();
        //        dbg!(r);
    }
}

/// Handle stdin commands
impl Handler<RaftMsg> for Connection {
    type Result = ();

    fn handle(&mut self, msg: RaftMsg, _ctx: &mut Context<Self>) {
        self.sink.write(Message::Binary(encode_ws(msg))).unwrap();
    }
}

/// Handle stdin commands
impl Handler<WsMessage> for Connection {
    type Result = ();

    fn handle(&mut self, msg: WsMessage, _ctx: &mut Context<Self>) {
        match msg {
            WsMessage::Msg(data) => eat_error!(
                self.logger,
                self.sink
                    .write(Message::Text(serde_json::to_string(&data).unwrap()))
            ),
        }
    }
}
