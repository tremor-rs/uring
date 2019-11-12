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
mod raft_node;
mod storage;
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
use raft::{prelude::*, StateRole};
use raft_node::*;
use serde::{Deserialize, Serialize};
use slog::Drain;
use slog::Logger;
use std::collections::HashMap;
use std::thread;
use std::time::{Duration, Instant};

#[macro_use]
extern crate slog;

/// How often heartbeat pings are sent
const HEARTBEAT_INTERVAL: Duration = Duration::from_secs(5);
/// How long before lack of client response causes a timeout
const CLIENT_TIMEOUT: Duration = Duration::from_secs(10);

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
    srv.get_ref().tx.send(UrMsg::Get(key.clone(), tx)).unwrap();
    if let Some(value) = rx.recv().unwrap() {
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
        .send(UrMsg::Post(params.id.clone(), body.value.clone(), tx))
        .unwrap();
    if rx.recv().unwrap() {
        Ok(HttpResponse::new(StatusCode::from_u16(201).unwrap()))
    } else {
        Ok(HttpResponse::new(StatusCode::from_u16(500).unwrap()))
    }
}

#[derive(Deserialize)]
pub struct GetNodeParams {
    id: u64,
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
    remote_id: u64,
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
                let msg: HandshakeMsg = serde_json::from_str(&text).unwrap();
                match msg {
                    HandshakeMsg::Hello(id, peer) => {
                        self.remote_id = id;
                        self.node
                            .tx
                            .send(UrMsg::RegisterRemote(id, peer, ctx.address()))
                            .unwrap();
                    }
                    HandshakeMsg::AckProposal(pid, success) => {
                        self.node.tx.send(UrMsg::AckProposal(pid, success)).unwrap();
                    }
                    HandshakeMsg::ForwardProposal(from, pid, key, value) => {
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
            remote_id: 0,
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
    id: u64,
    tx: Sender<UrMsg>,
    logger: Logger,
}

fn remote_endpoint(
    _id: u64,
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
                    let addr = WsOfframpWorker::create(move |ctx| {
                        WsOfframpWorker::add_stream(stream, ctx);
                        WsOfframpWorker {
                            logger,
                            remote_id: 0,
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
    remote_id: u64,
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
    Msg(HandshakeMsg),
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
                let msg: HandshakeMsg = serde_json::from_slice(&data).unwrap();
                match msg {
                    HandshakeMsg::Welcome(id, peer, peers) => {
                        self.remote_id = id;
                        self.tx
                            .send(UrMsg::RegisterLocal(id, peer, ctx.address(), peers))
                            .unwrap();
                    }
                    HandshakeMsg::AckProposal(pid, success) => {
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
pub enum HandshakeMsg {
    Hello(u64, String),
    Welcome(u64, String, Vec<(u64, String)>),
    AckProposal(u64, bool),
    ForwardProposal(u64, u64, String, String),
}

pub enum UrMsg {
    InitLocal(Addr<WsOfframpWorker>),
    RegisterLocal(u64, String, Addr<WsOfframpWorker>, Vec<(u64, String)>),
    RegisterRemote(u64, String, Addr<UrSocket>),
    DownLocal(u64),
    DownRemote(u64),
    AckProposal(u64, bool),
    ForwardProposal(u64, u64, String, String),
    RaftMsg(RaftMessage),
    Get(String, Sender<Option<String>>),
    Post(String, String, Sender<bool>),
    GetNode(u64, Sender<bool>),
    AddNode(u64, Sender<bool>),
}

fn loopy_thing(
    id: u64,
    my_endpoint: String,
    bootstrap: bool,
    rx: Receiver<UrMsg>,
    tx: Sender<UrMsg>,
    logger: Logger,
) {
    // Tick the raft node per 100ms. So use an `Instant` to trace it.
    let mut t = Instant::now();
    let mut t1 = Instant::now();
    let mut last_state = StateRole::PreCandidate;
    let mut node: RaftNode<URRocksStorage> = if bootstrap {
        RaftNode::create_raft_leader(id, my_endpoint, rx.clone(), &logger)
    } else {
        RaftNode::create_raft_follower(id, my_endpoint, rx.clone(), &logger)
    };
    node.log();

    let mut known_peers: HashMap<u64, String> = HashMap::new();
    loop {
        thread::sleep(Duration::from_millis(10));
        loop {
            match rx.try_recv() {
                Ok(UrMsg::GetNode(id, reply)) => {
                    info!(logger, "Getting node status"; "id" => id);
                    let g = node.raft_group.as_ref().unwrap();
                    reply
                        .send(g.raft.prs().configuration().contains(id))
                        .unwrap();
                }
                Ok(UrMsg::AddNode(id, reply)) => {
                    info!(logger, "Adding node"; "id" => id);
                    let g = node.raft_group.as_ref().unwrap();
                    if node.is_leader() && !g.raft.prs().configuration().contains(id) {
                        let mut conf_change = ConfChange::default();
                        conf_change.node_id = id;
                        conf_change.set_change_type(ConfChangeType::AddNode);
                        let proposal =
                            Proposal::conf_change(node.proposal_id, node.id, &conf_change);
                        node.proposal_id += 1;

                        // FIXME might not be the leader
                        node.proposals.push_back(proposal);
                        reply.send(true).unwrap();
                    } else {
                        reply.send(false).unwrap();
                    }
                }
                Ok(UrMsg::Get(key, reply)) => {
                    info!(logger, "Reading key"; "key" => &key);
                    let value = node
                        .get_key(key.as_bytes())
                        .and_then(|v| String::from_utf8(v).ok());
                    info!(logger, "Found value"; "value" => &value);
                    reply.send(value).unwrap();
                }
                Ok(UrMsg::Post(key, value, reply)) => {
                    let pid = node.proposal_id;
                    node.proposal_id += 1;
                    let from = node.id;
                    if node.is_leader() {
                        info!(logger, "Writing key (on leader)"; "key" => &key, "proposal-id" => pid, "from" => from);
                        node.pending_acks.insert(pid, reply.clone());
                        node.proposals
                            .push_back(Proposal::normal(pid, from, key, value));
                    } else {
                        let leader = node.leader();
                        info!(logger, "Writing key (forwarded)"; "key" => &key, "proposal-id" => pid, "forwarded-to" => leader);
                        let msg =
                            WsMessage::Msg(HandshakeMsg::ForwardProposal(from, pid, key, value));
                        if let Some(remote) = node.local_mailboxes.get(&leader) {
                            node.pending_acks.insert(pid, reply.clone());
                            remote.send(msg).wait().unwrap();
                        } else if let Some(remote) = node.remote_mailboxes.get(&leader) {
                            node.pending_acks.insert(pid, reply.clone());
                            remote.send(msg).wait().unwrap();
                        } else {
                            error!(&logger, "not connected to leader"; "leader" => leader, "state" => format!("{:?}", node.role()));
                            reply.send(false).unwrap();
                        }
                    }
                }
                Ok(UrMsg::AckProposal(pid, success)) => {
                    info!(&logger, "proposal acknowledged"; "pid" => pid);
                    if let Some(proposal) = node.pending_proposals.remove(&pid) {
                        if !success {
                            node.proposals.push_back(proposal)
                        }
                    }
                    if let Some(reply) = node.pending_acks.remove(&pid) {
                        reply.send(success).unwrap();
                    }
                }
                Ok(UrMsg::ForwardProposal(from, pid, key, value)) => {
                    if node.is_leader() {
                        node.proposals
                            .push_back(Proposal::normal(pid, from, key, value));
                    } else {
                        let leader = node.leader();
                        let msg =
                            WsMessage::Msg(HandshakeMsg::ForwardProposal(from, pid, key, value));
                        if let Some(remote) = node.local_mailboxes.get(&leader) {
                            remote.send(msg).wait().unwrap();
                        } else if let Some(remote) = node.remote_mailboxes.get(&leader) {
                            remote.send(msg).wait().unwrap();
                        } else {
                            error!(&logger, "not connected to leader"; "leader" => leader, "state" => format!("{:?}", node.role()))
                        }
                    }
                }
                // Connection handling of websocket connections
                // partially based on the problem that actix ws client
                // doens't reconnect
                Ok(UrMsg::InitLocal(endpoint)) => {
                    info!(&logger, "Initializing local endpoint");
                    endpoint
                        .send(WsMessage::Msg(HandshakeMsg::Hello(
                            node.id,
                            node.endpoint.clone(),
                        )))
                        .wait()
                        .unwrap();
                }
                Ok(UrMsg::RegisterLocal(id, peer, endpoint, peers)) => {
                    if id != node.id {
                        info!(&logger, "register(local)"; "remote-id" => id, "remote-peer" => peer, "discovered-peers" => format!("{:?}", peers));
                        node.local_mailboxes.insert(id, endpoint.clone());
                        for (peer_id, peer) in peers {
                            if !known_peers.contains_key(&peer_id) {
                                known_peers.insert(peer_id, peer.clone());
                                let tx = tx.clone();
                                let node_id = node.id;
                                let logger = logger.clone();
                                thread::spawn(move || remote_endpoint(node_id, peer, tx, logger));
                            }
                        }
                    }
                }

                // Reply to hello => sends RegisterLocal
                Ok(UrMsg::RegisterRemote(id, peer, endpoint)) => {
                    if id != node.id {
                        info!(&logger, "register(remote)"; "remote-id" => id, "remote-peer" => &peer);
                        if !known_peers.contains_key(&id) {
                            known_peers.insert(id, peer.clone());
                            let tx = tx.clone();
                            let node_id = node.id;
                            let logger = logger.clone();
                            thread::spawn(move || remote_endpoint(node_id, peer, tx, logger));
                        }
                        endpoint
                            .clone()
                            .send(WsMessage::Msg(HandshakeMsg::Welcome(
                                node.id,
                                node.endpoint.clone(),
                                known_peers
                                    .clone()
                                    .into_iter()
                                    .collect::<Vec<(u64, String)>>(),
                            )))
                            .wait()
                            .unwrap();
                        node.remote_mailboxes.insert(id, endpoint.clone());
                    }
                }
                Ok(UrMsg::DownLocal(id)) => {
                    warn!(&logger, "down(local)"; "id" => id);
                    node.local_mailboxes.remove(&id);
                    if !node.remote_mailboxes.contains_key(&id) {
                        known_peers.remove(&id);
                    }
                }
                Ok(UrMsg::DownRemote(id)) => {
                    warn!(&logger, "down(remote)"; "id" => id);
                    node.remote_mailboxes.remove(&id);
                    if !node.local_mailboxes.contains_key(&id) {
                        known_peers.remove(&id);
                    }
                }

                // RAFT
                Ok(UrMsg::RaftMsg(msg)) => {
                    if let Err(e) = node.step(msg) {
                        error!(&logger, "step error"; "error" => format!("{}", e));
                    }
                }
                Err(TryRecvError::Empty) => break,
                Err(TryRecvError::Disconnected) => return,
            }
        }
        if !node.is_running() {
            continue;
        }

        if t.elapsed() >= Duration::from_millis(100) {
            // Tick the raft.

            node.raft_group.as_mut().unwrap().tick();
            t = Instant::now();
        }

        let this_state = node.raft_group.as_ref().unwrap().raft.state.clone();

        if t1.elapsed() >= Duration::from_secs(10) {
            // Tick the raft.
            node.log();
            t1 = Instant::now();
        }

        // Let the leader pick pending proposals from the global queue.
        if node.is_leader() {
            // Handle new proposals.
            node.propose_all().unwrap();
        }

        if this_state != last_state {
            debug!(&logger, "State transition"; "last-state" => format!("{:?}", last_state), "next-state" => format!("{:?}", this_state));
            last_state = this_state
        }

        // Handle readies from the raft.
        node.on_ready().unwrap();

        // Check control signals from
        //        if check_signals(&rx_stop_clone) {
        //           return;
        //        };
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
    let id: u64 = matches.value_of("id").unwrap_or("1").parse().unwrap();
    let my_endpoint = endpoint.to_string();
    let tx1 = tx.clone();
    let loop_logger = logger.clone();
    thread::spawn(move || loopy_thing(id, my_endpoint, bootstrap, rx, tx1, loop_logger));

    for peer in peers {
        let logger = logger.clone();
        let tx = tx.clone();
        thread::spawn(move || remote_endpoint(id, peer, tx, logger));
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
