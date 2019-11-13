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

use super::{Error, EventId, Network as NetworkTrait, ProposalId, RaftNetworkMsg, ServiceId};
use crate::raft_node::RaftNodeStatus;
use crate::service::kv::{Event as KVEvent, KV_SERVICE};
use crate::{NodeId, KV};
use actix::io::SinkWrite;
use actix::prelude::*;
use actix_codec::Framed;
use actix_web::{
    http::StatusCode, middleware, web, App, Error as ActixError, HttpRequest, HttpResponse,
    HttpServer,
};
use actix_web_actors::ws;
use awc::{
    error::WsProtocolError,
    ws::{CloseCode, CloseReason, Codec as AxCodec, Frame, Message},
    BoxedSocket, Client,
};
use bytes::Bytes;
use crossbeam_channel::{bounded, Receiver, Sender, TryRecvError};
use futures::{
    lazy,
    stream::{SplitSink, Stream},
    Future,
};
use raft::eraftpb::Message as RaftMessage;
use serde::{Deserialize, Serialize};
use slog::Logger;
use std::collections::HashMap;
use std::io;
use std::thread::{self, JoinHandle};
use std::time::{Duration, Instant};

type LocalMailboxes = HashMap<NodeId, Addr<WsOfframpWorker>>;
type RemoteMailboxes = HashMap<NodeId, Addr<UrSocket>>;

/// How often heartbeat pings are sent
const HEARTBEAT_INTERVAL: Duration = Duration::from_secs(5);
/// How long before lack of client response causes a timeout
const CLIENT_TIMEOUT: Duration = Duration::from_secs(10);

#[derive(Clone)]
struct Node {
    id: NodeId,
    tx: Sender<UrMsg>,
    logger: Logger,
}

macro_rules! eat_error {
    ($l:expr, $e:expr) => {
        if let Err(e) = $e {
            error!($l, "[WS Error] {}", e)
        }
    };
}
pub struct Network {
    id: NodeId,
    local_mailboxes: LocalMailboxes,
    remote_mailboxes: RemoteMailboxes,
    known_peers: HashMap<NodeId, String>,
    endpoint: String,
    logger: Logger,
    rx: Receiver<UrMsg>,
    tx: Sender<UrMsg>,
    next_eid: u64,
    pending: HashMap<EventId, Sender<Option<Vec<u8>>>>,
}

impl Network {
    pub fn new(
        logger: &Logger,
        id: NodeId,
        endpoint: &str,
        peers: Vec<String>,
    ) -> (JoinHandle<Result<(), io::Error>>, Self) {
        let (tx, rx) = bounded(100);

        for peer in peers {
            let logger = logger.clone();
            let tx = tx.clone();
            thread::spawn(move || remote_endpoint(peer, tx, logger));
        }

        let node = Node {
            tx: tx.clone(),
            id,
            logger: logger.clone(),
        };

        let endpoint = endpoint.to_string();
        let t_endpoint = endpoint.clone();

        let handle = thread::spawn(move || {
            HttpServer::new(move || {
                App::new()
                    .data(node.clone())
                    // enable logger
                    .wrap(middleware::Logger::default())
                    .service(web::resource("/status").route(web::get().to(status)))
                    .service(
                        web::resource("/data/{id}")
                            .route(web::get().to(get))
                            .route(web::post().to(post)),
                    )
                    .service(
                        web::resource("/node/{id}")
                            .route(web::get().to(get_node))
                            .route(web::post().to(post_node)),
                    )
                    // websocket route
                    .service(web::resource("/uring").route(web::get().to(uring_index)))
            })
            // start http server on 127.0.0.1:8080
            .bind(t_endpoint)?
            .run()
        });

        (
            handle,
            Self {
                id,
                endpoint,
                logger: logger.clone(),
                local_mailboxes: HashMap::new(),
                remote_mailboxes: HashMap::new(),
                known_peers: HashMap::new(),
                rx,
                tx,
                next_eid: 0,
                pending: HashMap::new(),
            },
        )
    }
}
#[derive(Message)]
pub(crate) struct RaftMsg(RaftMessage);

impl NetworkTrait for Network {
    fn event_reply(&mut self, id: EventId, reply: Option<Vec<u8>>) -> Result<(), Error> {
        if let Some(sender) = self.pending.remove(&id) {
            sender.send(reply).unwrap();
        }
        Ok(())
    }

    fn try_recv(&mut self) -> Result<RaftNetworkMsg, TryRecvError> {
        use RaftNetworkMsg::*;
        match self.rx.try_recv() {
            Ok(UrMsg::Status(reply)) => Ok(Status(reply)),
            Ok(UrMsg::GetNode(id, reply)) => Ok(GetNode(id, reply)),
            Ok(UrMsg::AddNode(id, reply)) => Ok(AddNode(id, reply)),
            Ok(UrMsg::Get(key, reply)) => {
                let eid = EventId(self.next_eid);
                self.next_eid += 1;
                self.pending.insert(eid, reply);
                Ok(RaftNetworkMsg::Event(eid, KV_SERVICE, KVEvent::get(key)))
            }
            Ok(UrMsg::Post(key, value, reply)) => {
                let eid = EventId(self.next_eid);
                self.next_eid += 1;
                self.pending.insert(eid, reply);
                Ok(RaftNetworkMsg::Event(
                    eid,
                    KV_SERVICE,
                    KVEvent::post(key, value),
                ))
            }
            Ok(UrMsg::AckProposal(pid, success)) => Ok(AckProposal(pid, success)),
            Ok(UrMsg::ForwardProposal(from, pid, sid, data)) => {
                Ok(ForwardProposal(from, pid, sid, data))
            }
            Ok(UrMsg::RaftMsg(msg)) => Ok(RaftMsg(msg)),
            // Connection handling of websocket connections
            // partially based on the problem that actix ws client
            // doens't reconnect
            Ok(UrMsg::InitLocal(endpoint)) => {
                info!(self.logger, "Initializing local endpoint");
                endpoint
                    .send(WsMessage::Msg(CtrlMsg::Hello(
                        self.id,
                        self.endpoint.clone(),
                    )))
                    .wait()
                    .unwrap();
                self.try_recv()
            }
            Ok(UrMsg::RegisterLocal(id, peer, endpoint, peers)) => {
                if id != self.id {
                    info!(self.logger, "register(local)"; "remote-id" => id, "remote-peer" => peer, "discovered-peers" => format!("{:?}", peers));
                    self.local_mailboxes.insert(id, endpoint.clone());
                    for (peer_id, peer) in peers {
                        if !self.known_peers.contains_key(&peer_id) {
                            self.known_peers.insert(peer_id, peer.clone());
                            let tx = self.tx.clone();
                            let logger = self.logger.clone();
                            thread::spawn(move || remote_endpoint(peer, tx, logger));
                        }
                    }
                }
                self.try_recv()
            }

            // Reply to hello => sends RegisterLocal
            Ok(UrMsg::RegisterRemote(id, peer, endpoint)) => {
                if id != self.id {
                    info!(self.logger, "register(remote)"; "remote-id" => id, "remote-peer" => &peer);
                    if !self.known_peers.contains_key(&id) {
                        self.known_peers.insert(id, peer.clone());
                        let tx = self.tx.clone();
                        let logger = self.logger.clone();
                        thread::spawn(move || remote_endpoint(peer, tx, logger));
                    }
                    endpoint
                        .clone()
                        .send(WsMessage::Msg(CtrlMsg::HelloAck(
                            self.id,
                            self.endpoint.clone(),
                            self.known_peers
                                .clone()
                                .into_iter()
                                .collect::<Vec<(NodeId, String)>>(),
                        )))
                        .wait()
                        .unwrap();
                    self.remote_mailboxes.insert(id, endpoint.clone());
                }
                self.try_recv()
            }
            Ok(UrMsg::DownLocal(id)) => {
                warn!(self.logger, "down(local)"; "id" => id);
                self.local_mailboxes.remove(&id);
                if !self.remote_mailboxes.contains_key(&id) {
                    self.known_peers.remove(&id);
                }
                self.try_recv()
            }
            Ok(UrMsg::DownRemote(id)) => {
                warn!(self.logger, "down(remote)"; "id" => id);
                self.remote_mailboxes.remove(&id);
                if !self.local_mailboxes.contains_key(&id) {
                    self.known_peers.remove(&id);
                }
                self.try_recv()
            }
            Err(e) => Err(e),
        }
    }
    fn ack_proposal(&self, to: NodeId, pid: ProposalId, success: bool) -> Result<(), Error> {
        if let Some(remote) = self.local_mailboxes.get(&to) {
            remote
                .send(WsMessage::Msg(CtrlMsg::AckProposal(pid, success)))
                .wait()
                .map_err(|e| Error::Io(io::Error::new(io::ErrorKind::ConnectionAborted, e)))
        } else if let Some(remote) = self.remote_mailboxes.get(&to) {
            remote
                .send(WsMessage::Msg(CtrlMsg::AckProposal(pid, success)))
                .wait()
                .map_err(|e| Error::Io(io::Error::new(io::ErrorKind::ConnectionAborted, e)))
        } else {
            Err(Error::Io(io::Error::new(
                io::ErrorKind::ConnectionAborted,
                format!("send ack proposla to {} fail, let Raft retry it", to),
            )))
        }
    }

    fn send_msg(&self, msg: RaftMessage) -> Result<(), Error> {
        let to = NodeId(msg.to);
        if let Some(remote) = self.local_mailboxes.get(&to) {
            remote
                .send(RaftMsg(msg))
                .wait()
                .map_err(|e| Error::Io(io::Error::new(io::ErrorKind::ConnectionAborted, e)))
        } else if let Some(remote) = self.remote_mailboxes.get(&to) {
            remote
                .send(RaftMsg(msg))
                .wait()
                .map_err(|e| Error::Io(io::Error::new(io::ErrorKind::ConnectionAborted, e)))
        } else {
            // Err(Error::NotConnected(to)) this is not an error we'll retry
            Ok(())
        }
    }

    fn connections(&self) -> Vec<NodeId> {
        let mut k1: Vec<NodeId> = self.local_mailboxes.keys().copied().collect();
        let mut k2: Vec<NodeId> = self.remote_mailboxes.keys().copied().collect();
        k1.append(&mut k2);
        k1.sort();
        k1.dedup();
        k1
    }

    fn forward_proposal(
        &self,
        from: NodeId,
        to: NodeId,
        pid: ProposalId,
        sid: ServiceId,
        data: Vec<u8>,
    ) -> Result<(), Error> {
        let msg = WsMessage::Msg(CtrlMsg::ForwardProposal(from, pid, sid, data));
        if let Some(remote) = self.local_mailboxes.get(&to) {
            remote
                .send(msg)
                .wait()
                .map_err(|e| Error::Generic(format!("{}", e)))
        } else if let Some(remote) = self.remote_mailboxes.get(&to) {
            remote
                .send(msg)
                .wait()
                .map_err(|e| Error::Generic(format!("{}", e)))
        } else {
            Err(Error::NotConnected(to))
        }
    }
}

/// do websocket handshake and start `UrSocket` actor
fn uring_index(
    r: HttpRequest,
    stream: web::Payload,
    srv: web::Data<Node>,
) -> Result<HttpResponse, ActixError> {
    let res = ws::start(UrSocket::new(srv.get_ref().clone()), &r, stream);
    res
}

#[derive(Deserialize)]
pub struct NoParams {}

fn status(
    _r: HttpRequest,
    _params: web::Path<NoParams>,
    srv: web::Data<Node>,
) -> Result<HttpResponse, ActixError> {
    let (tx, rx) = bounded(1);
    srv.get_ref().tx.send(UrMsg::Status(tx)).unwrap();
    if let Some(value) = rx.recv().ok() {
        Ok(HttpResponse::Ok().json(value))
    } else {
        Ok(HttpResponse::new(StatusCode::from_u16(404).unwrap()))
    }
}

#[derive(Deserialize)]
pub struct GetParams {
    id: String,
}

/// do websocket handshake and start `UrSocket` actor
fn get(
    _r: HttpRequest,
    params: web::Path<GetParams>,
    srv: web::Data<Node>,
) -> Result<HttpResponse, ActixError> {
    let (tx, rx) = bounded(1);
    let key = params.id.clone();
    srv.get_ref()
        .tx
        .send(UrMsg::Get(key.clone().into_bytes(), tx))
        .unwrap();
    if let Some(value) = rx
        .recv()
        .ok()
        .and_then(|v| v)
        .and_then(|v| String::from_utf8(v).ok())
    {
        Ok(HttpResponse::Ok().json(KV { key, value }))
    } else {
        Ok(HttpResponse::new(StatusCode::from_u16(404).unwrap()))
    }
}

#[derive(Deserialize)]
pub struct PostBody {
    value: String,
}

/// do websocket handshake and start `UrSocket` actor
fn post(
    _r: HttpRequest,
    params: web::Path<GetParams>,
    body: web::Json<PostBody>,
    srv: web::Data<Node>,
) -> Result<HttpResponse, ActixError> {
    let (tx, rx) = bounded(1);
    srv.get_ref()
        .tx
        .send(UrMsg::Post(
            params.id.clone().into_bytes(),
            body.value.clone().into_bytes(),
            tx,
        ))
        .unwrap();
    if rx.recv().unwrap().is_some() {
        Ok(HttpResponse::new(StatusCode::from_u16(201).unwrap()))
    } else {
        Ok(HttpResponse::new(StatusCode::from_u16(500).unwrap()))
    }
}

#[derive(Deserialize)]
pub struct GetNodeParams {
    id: NodeId,
}
fn get_node(
    _r: HttpRequest,
    params: web::Path<GetNodeParams>,
    srv: web::Data<Node>,
) -> Result<HttpResponse, ActixError> {
    let (tx, rx) = bounded(1);
    srv.get_ref()
        .tx
        .send(UrMsg::GetNode(params.id, tx))
        .unwrap();
    if rx.recv().unwrap() {
        Ok(HttpResponse::new(StatusCode::from_u16(200).unwrap()))
    } else {
        Ok(HttpResponse::new(StatusCode::from_u16(404).unwrap()))
    }
}

fn post_node(
    _r: HttpRequest,
    params: web::Path<GetNodeParams>,
    _body: web::Payload,
    srv: web::Data<Node>,
) -> Result<HttpResponse, ActixError> {
    let (tx, rx) = bounded(1);
    srv.get_ref()
        .tx
        .send(UrMsg::AddNode(params.id, tx))
        .unwrap();
    if rx.recv().unwrap() {
        Ok(HttpResponse::new(StatusCode::from_u16(200).unwrap()))
    } else {
        Ok(HttpResponse::new(StatusCode::from_u16(500).unwrap()))
    }
}

/// websocket connection is long running connection, it easier
/// to handle with an actor
pub struct UrSocket {
    /// Client must send ping at least once per 10 seconds (CLIENT_TIMEOUT),
    /// otherwise we drop connection.
    hb: Instant,
    node: Node,
    remote_id: NodeId,
}

impl Actor for UrSocket {
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
impl StreamHandler<ws::Message, ws::ProtocolError> for UrSocket {
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
impl Handler<WsMessage> for UrSocket {
    type Result = ();

    fn handle(&mut self, msg: WsMessage, ctx: &mut Self::Context) {
        match msg {
            WsMessage::Msg(data) => ctx.text(serde_json::to_string(&data).unwrap()),
        }
    }
}

/// Handle stdin commands
impl Handler<RaftMsg> for UrSocket {
    type Result = ();
    fn handle(&mut self, msg: RaftMsg, ctx: &mut Self::Context) {
        ctx.binary(encode_ws(msg));
    }
}

impl UrSocket {
    fn new(node: Node) -> Self {
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

pub struct WsOfframpWorker {
    //    my_id: u64,
    remote_id: NodeId,
    tx: Sender<UrMsg>,
    logger: Logger,
    sink: SinkWrite<SplitSink<Framed<BoxedSocket, AxCodec>>>,
}

impl WsOfframpWorker {
    fn hb(&self, ctx: &mut Context<Self>) {
        ctx.run_later(HEARTBEAT_INTERVAL, |act, ctx| {
            act.sink
                .write(Message::Ping(String::from("Snot badger!")))
                .unwrap();
            act.hb(ctx);
        });
    }
}

impl actix::io::WriteHandler<WsProtocolError> for WsOfframpWorker {}

impl Actor for WsOfframpWorker {
    type Context = Context<Self>;

    fn started(&mut self, ctx: &mut Context<Self>) {
        self.hb(ctx)
    }

    fn stopped(&mut self, _: &mut Context<Self>) {
        self.tx.send(UrMsg::DownLocal(self.remote_id)).unwrap();
        info!(self.logger, "system stopped");
        System::current().stop();
    }
}

#[derive(Message)]
pub enum WsMessage {
    Msg(CtrlMsg),
}

/// Handle stdin commands
impl Handler<WsMessage> for WsOfframpWorker {
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

#[cfg(feature = "json-proto")]
fn decode_ws(bin: &[u8]) -> RaftMessage {
    let msg: crate::codec::json::Event = serde_json::from_slice(bin).unwrap();
    msg.into()
}

#[cfg(not(feature = "json-proto"))]
fn decode_ws(bin: &[u8]) -> RaftMessage {
    use protobuf::Message;
    let mut msg = RaftMessage::default();
    msg.merge_from_bytes(bin).unwrap();
    msg
}

#[cfg(feature = "json-proto")]
fn encode_ws(msg: RaftMsg) -> Bytes {
    let data: crate::codec::json::Event = msg.0.clone().into();
    let data = serde_json::to_string_pretty(&data);
    data.unwrap().into()
}

#[cfg(not(feature = "json-proto"))]
fn encode_ws(msg: RaftMsg) -> Bytes {
    use protobuf::Message;
    msg.0.write_to_bytes().unwrap().into()
}

/// Handle server websocket messages
impl StreamHandler<Frame, WsProtocolError> for WsOfframpWorker {
    fn handle(&mut self, msg: Frame, ctx: &mut Context<Self>) {
        match msg {
            Frame::Close(_) => {
                eat_error!(self.logger, self.sink.write(Message::Close(None)));
            }
            Frame::Ping(data) => {
                eat_error!(self.logger, self.sink.write(Message::Pong(data)));
            }
            Frame::Text(Some(data)) => {
                let msg: CtrlMsg = serde_json::from_slice(&data).unwrap();
                match msg {
                    CtrlMsg::HelloAck(id, peer, peers) => {
                        self.remote_id = id;
                        self.tx
                            .send(UrMsg::RegisterLocal(id, peer, ctx.address(), peers))
                            .unwrap();
                    }
                    CtrlMsg::AckProposal(pid, success) => {
                        self.tx.send(UrMsg::AckProposal(pid, success)).unwrap();
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

#[derive(Serialize, Deserialize, Debug)]
pub enum CtrlMsg {
    Hello(NodeId, String),
    HelloAck(NodeId, String, Vec<(NodeId, String)>),
    AckProposal(ProposalId, bool),
    ForwardProposal(NodeId, ProposalId, ServiceId, Vec<u8>),
    Put(Vec<u8>, Vec<u8>),
}

pub enum UrMsg {
    // Network related
    InitLocal(Addr<WsOfframpWorker>),
    RegisterLocal(NodeId, String, Addr<WsOfframpWorker>, Vec<(NodeId, String)>),
    RegisterRemote(NodeId, String, Addr<UrSocket>),
    DownLocal(NodeId),
    DownRemote(NodeId),
    Status(Sender<RaftNodeStatus>),

    // Raft related
    AckProposal(ProposalId, bool),
    ForwardProposal(NodeId, ProposalId, ServiceId, Vec<u8>),
    RaftMsg(RaftMessage),
    GetNode(NodeId, Sender<bool>),
    AddNode(NodeId, Sender<bool>),

    Get(Vec<u8>, Sender<Option<Vec<u8>>>),
    Post(Vec<u8>, Vec<u8>, Sender<Option<Vec<u8>>>),
}

fn remote_endpoint(endpoint: String, master: Sender<UrMsg>, logger: Logger) -> std::io::Result<()> {
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
                    let addr = WsOfframpWorker::create(move |ctx| {
                        WsOfframpWorker::add_stream(stream, ctx);
                        WsOfframpWorker {
                            logger,
                            remote_id: NodeId(0),
                            tx: master2,
                            sink: SinkWrite::new(sink, ctx),
                        }
                    });
                    master1.send(UrMsg::InitLocal(addr)).unwrap();
                    ()
                })
        }));
        sys.run().unwrap();
        //        dbg!(r);
    }
}

/// Handle stdin commands
impl Handler<RaftMsg> for WsOfframpWorker {
    type Result = ();

    fn handle(&mut self, msg: RaftMsg, _ctx: &mut Context<Self>) {
        self.sink.write(Message::Binary(encode_ws(msg))).unwrap();
    }
}
