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

mod codec;
#[allow(unused)]
pub mod errors;
mod network;
mod raft_node;
mod storage;
use crate::network::{RaftNetworkMsg, WsNetwork};
use crate::storage::URRocksStorage;
use actix::io::SinkWrite;
use actix::prelude::*;
use actix_codec::Framed;
use actix_web::{
    http::StatusCode, middleware, web, App, Error, HttpRequest, HttpResponse, HttpServer,
};
use actix_web_actors::ws;
use awc::{
    error::WsProtocolError,
    ws::{CloseCode, CloseReason, Codec as AxCodec, Frame, Message},
    BoxedSocket, Client,
};
use clap::{App as ClApp, Arg};
use crossbeam_channel::{bounded, Receiver, Sender, TryRecvError};
use futures::{
    lazy,
    stream::{SplitSink, Stream},
    Future,
};
use raft::eraftpb::Message as RaftMessage;
use raft::StateRole;
use raft_node::*;
use serde::{Deserialize, Serialize};
use slog::Drain;
use slog::{Key, Logger, Record, Value};
use std::collections::HashMap;
use std::fmt;
use std::thread;
use std::time::{Duration, Instant};

#[macro_use]
extern crate slog;

/// How often heartbeat pings are sent
const HEARTBEAT_INTERVAL: Duration = Duration::from_secs(5);
/// How long before lack of client response causes a timeout
const CLIENT_TIMEOUT: Duration = Duration::from_secs(10);

#[derive(Serialize, Deserialize, Debug, Hash, PartialEq, Eq, Ord, PartialOrd, Clone, Copy)]
pub struct NodeId(u64);

impl Value for NodeId {
    fn serialize(
        &self,
        _rec: &Record,
        key: Key,
        serializer: &mut dyn slog::Serializer,
    ) -> slog::Result {
        serializer.emit_u64(key, self.0)
    }
}

impl fmt::Display for NodeId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "Node({:?})", self)
    }
}

macro_rules! eat_error {
    ($l:expr, $e:expr) => {
        if let Err(e) = $e {
            error!($l, "[WS Error] {}", e)
        }
    };
}

/// do websocket handshake and start `UrSocket` actor
fn uring_index(
    r: HttpRequest,
    stream: web::Payload,
    srv: web::Data<Node>,
) -> Result<HttpResponse, Error> {
    let res = ws::start(UrSocket::new(srv.get_ref().clone()), &r, stream);
    res
}

#[derive(Deserialize)]
pub struct GetParams {
    id: String,
}
#[derive(Deserialize, Serialize)]
pub struct KV {
    key: String,
    value: String,
}

/// do websocket handshake and start `UrSocket` actor
fn get(
    _r: HttpRequest,
    params: web::Path<GetParams>,
    srv: web::Data<Node>,
) -> Result<HttpResponse, Error> {
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
) -> Result<HttpResponse, Error> {
    let (tx, rx) = bounded(1);
    srv.get_ref()
        .tx
        .send(UrMsg::Post(
            params.id.clone().into_bytes(),
            body.value.clone().into_bytes(),
            tx,
        ))
        .unwrap();
    if rx.recv().unwrap() {
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
) -> Result<HttpResponse, Error> {
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
) -> Result<HttpResponse, Error> {
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
                let msg: codec::json::Event = serde_json::from_slice(&bin).unwrap();
                let msg: RaftMessage = msg.into();
                // TODO FIXME feature flag pb / json
                // let mut msg = RaftMessage::default();
                // msg.merge_from_bytes(&bin).unwrap();
                //println!("recived raft message {:?}", msg);
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
        // let data = msg.0.write_to_bytes().unwrap();
        // TODO FIXME Allow switching from pb <-> json by feature flag
        let data: codec::json::Event = msg.0.clone().into();
        /*
        if data.kind != codec::json::EventType::Heartbeat
            && data.kind != codec::json::EventType::HeartbeatResponse
        {
            info!(self.node.logger, "{:?} => {:?}", msg.0, &data);
        }
        */
        let data = serde_json::to_string_pretty(&data);
        let data: bytes::Bytes = data.unwrap().into();
        ctx.binary(data);
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

#[derive(Clone)]
struct Node {
    id: NodeId,
    tx: Sender<UrMsg>,
    logger: Logger,
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

#[derive(Message)]
pub(crate) struct RaftMsg(RaftMessage);
/// Handle stdin commands
impl Handler<RaftMsg> for WsOfframpWorker {
    type Result = ();

    fn handle(&mut self, msg: RaftMsg, _ctx: &mut Context<Self>) {
        // TODO FIXME Allow switching from pb <-> json by feature flag
        let data: codec::json::Event = msg.0.clone().into();
        /*
        if data.kind != codec::json::EventType::Heartbeat
            && data.kind != codec::json::EventType::HeartbeatResponse
        {
            info!(self.logger, "{:?} => {:?}", msg.0, &data);
        }
        */
        let data = serde_json::to_string_pretty(&data);
        self.sink
            .write(Message::Binary(data.unwrap().into()))
            .unwrap();
    }
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
                let msg: codec::json::Event = serde_json::from_slice(&bin).unwrap();
                let msg: RaftMessage = msg.into();
                // TODO FIXME feature flag pb / json
                // msg.merge_from_bytes(&bin).unwrap();
                println!("received raft message {:?}", msg);
                self.tx.send(UrMsg::RaftMsg(msg)).unwrap();
            }
            Frame::Binary(None) => {
                //eat_error!(self.2.write(Message::Binary(Bytes::new())));
                ()
            }
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
    AckProposal(u64, bool),
    ForwardProposal(NodeId, u64, Vec<u8>, Vec<u8>),
    Put(Vec<u8>, Vec<u8>),
}

pub enum UrMsg {
    // Network related
    InitLocal(Addr<WsOfframpWorker>),
    RegisterLocal(NodeId, String, Addr<WsOfframpWorker>, Vec<(NodeId, String)>),
    RegisterRemote(NodeId, String, Addr<UrSocket>),
    DownLocal(NodeId),
    DownRemote(NodeId),

    // Raft related
    AckProposal(u64, bool),
    ForwardProposal(NodeId, u64, Vec<u8>, Vec<u8>),
    RaftMsg(RaftMessage),
    GetNode(NodeId, Sender<bool>),
    AddNode(NodeId, Sender<bool>),

    Get(Vec<u8>, Sender<Option<Vec<u8>>>),
    Post(Vec<u8>, Vec<u8>, Sender<bool>),
}

fn loopy_thing(
    id: NodeId,
    my_endpoint: String,
    bootstrap: bool,
    rx: Receiver<UrMsg>,
    tx: Sender<UrMsg>,
    logger: Logger,
) {
    // Tick the raft node per 100ms. So use an `Instant` to trace it.
    let mut t1 = Instant::now();
    let mut last_state = StateRole::PreCandidate;
    let network = WsNetwork::new(&logger, id, &my_endpoint, rx, tx);
    let mut node: RaftNode<URRocksStorage, _> = if bootstrap {
        RaftNode::create_raft_leader(&logger, id, network)
    } else {
        RaftNode::create_raft_follower(&logger, id, network)
    };
    node.log();

    loop {
        thread::sleep(Duration::from_millis(10));

        let this_state = node.role().clone();

        if t1.elapsed() >= Duration::from_secs(10) {
            // Tick the raft.
            node.log();
            t1 = Instant::now();
        }

        if this_state != last_state {
            debug!(&logger, "State transition"; "last-state" => format!("{:?}", last_state), "next-state" => format!("{:?}", this_state));
            last_state = this_state
        }

        // Handle readies from the raft.
        node.tick().unwrap();
    }
}

fn main() -> std::io::Result<()> {
    env_logger::init();
    let decorator = slog_term::TermDecorator::new().build();
    let drain = slog_term::FullFormat::new(decorator).build().fuse();
    let drain = slog_async::Async::new(drain).build().fuse();

    let logger = slog::Logger::root(drain, o!());

    //std::env::set_var("RUST_LOG", "actix_server=info,actix_web=info");
    let matches = ClApp::new("cake")
        .version("1.0")
        .author("The Treamor Team")
        .about("Uring Demo")
        .arg(
            Arg::with_name("id")
                .short("i")
                .long("id")
                .value_name("ID")
                .help("The Node ID")
                .takes_value(true),
        )
        .arg(
            Arg::with_name("bootstrap")
                .short("b")
                .long("bootstrap")
                .value_name("BOOTSTRAP")
                .help("Sets the node to bootstrap and become leader")
                .takes_value(false),
        )
        .arg(
            Arg::with_name("peers")
                .short("p")
                .long("peers")
                .value_name("PEERS")
                .multiple(true)
                .takes_value(true)
                .help("Peers to connet to"),
        )
        .arg(
            Arg::with_name("endpoint")
                .short("e")
                .long("endpoint")
                .value_name("ENDPOINT")
                .takes_value(true)
                .default_value("127.0.0.1:8080")
                .help("Peers to connet to"),
        )
        .get_matches();

    let peers = matches.values_of_lossy("peers").unwrap_or(vec![]);
    let bootstrap = matches.is_present("bootstrap");
    let (tx, rx) = bounded(100);
    let endpoint = matches.value_of("endpoint").unwrap_or("127.0.0.1:8080");
    let id = NodeId(matches.value_of("id").unwrap_or("1").parse().unwrap());
    let my_endpoint = endpoint.to_string();
    let tx1 = tx.clone();
    let loop_logger = logger.clone();
    thread::spawn(move || loopy_thing(id, my_endpoint, bootstrap, rx, tx1, loop_logger));

    for peer in peers {
        let logger = logger.clone();
        let tx = tx.clone();
        thread::spawn(move || remote_endpoint(peer, tx, logger));
    }

    let node = Node {
        tx,
        id,
        logger: logger.clone(),
    };

    HttpServer::new(move || {
        App::new()
            .data(node.clone())
            // enable logger
            .wrap(middleware::Logger::default())
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
    .bind(endpoint)?
    .run()
}
