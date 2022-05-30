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
// use crate::{NodeId, KV};

mod kv;
mod mring;
use std::num::ParseIntError;

use super::Node;
use super::*;
use crate::NodeId;
use futures::channel::mpsc::{channel, Receiver, TrySendError};
use http::StatusCode;
use serde::Serialize;
use tide::{Error as ParamError, Request, Response};

#[derive(Debug)]
pub enum Error {
    HTTP(StatusCode),
    Tide(tide::Error),
    JSON(serde_json::Error),
    Param(String),
    ParseInt(ParseIntError),
    SendError,
}
impl Into<Response> for Error {
    fn into(self) -> Response {
        match self {
            Error::HTTP(code) => response_json(code.as_u16(), code.canonical_reason()).unwrap(),
            Error::SendError => {
                response_json(505, "Internal communication to raft core failed").unwrap()
            }
            Error::JSON(e) => response_json(400, format!("Invalid JSON: {}", e)).unwrap(),
            Error::Param(e) => response_json(400, format!("Invalid Param: {}", e)).unwrap(),
            Error::Tide(e) => e.into(),
            Error::ParseInt(e) => response_json(400, format!("Invalid Integer: {}", e)).unwrap(),
        }
    }
}

impl From<ParseIntError> for Error {
    fn from(s: ParseIntError) -> Self {
        Self::ParseInt(s)
    }
}
impl From<StatusCode> for Error {
    fn from(s: StatusCode) -> Self {
        Self::HTTP(s)
    }
}
impl From<tide::Error> for Error {
    fn from(s: tide::Error) -> Self {
        Self::Tide(s)
    }
}
impl From<serde_json::Error> for Error {
    fn from(s: serde_json::Error) -> Self {
        Self::JSON(s)
    }
}

impl<T> From<TrySendError<T>> for Error {
    fn from(_s: TrySendError<T>) -> Self {
        Self::SendError
    }
}

type Result<T> = std::result::Result<T, Error>;

fn unerror(r: Result<Response>) -> tide::Result {
    Ok(match r {
        Ok(r) => r,
        Err(e) => e.into(),
    })
}

fn reply(tx: Sender<WsMessage>) -> Reply {
    Reply(RequestId(666), tx)
}

async fn request(cx: Request<Node>, req: UrMsg, mut rx: Receiver<WsMessage>) -> Result<Response> {
    cx.state().tx.unbounded_send(req)?;
    rx.next()
        .await
        .ok_or(StatusCode::INTERNAL_SERVER_ERROR.into())
        .and_then(|msg| match msg {
            WsMessage::Reply(code, r) => response_json(code, r.data),
            _ => unreachable!(),
        })
}

fn response_json<S: Serialize>(c: u16, v: S) -> Result<Response> {
    let mut r = Response::new(c);
    r.set_body(serde_json::to_vec(&v)?);
    Ok(r)
}

fn response_json_200<S: Serialize>(v: S) -> Result<Response> {
    response_json(200, v)
}

async fn version(cx: Request<Node>) -> Result<Response> {
    let (tx, rx) = channel(crate::CHANNEL_SIZE);
    request(cx, UrMsg::Version(RequestId(666), tx), rx).await
}

async fn status(cx: Request<Node>) -> Result<Response> {
    let (tx, rx) = channel(crate::CHANNEL_SIZE);
    request(cx, UrMsg::Status(RequestId(666), tx), rx).await
}

pub(crate) fn param_err(e: ParamError) -> Error {
    Error::Param(format!("{:?}", e))
}
async fn uring_get(cx: Request<Node>) -> Result<Response> {
    let (tx, mut rx) = channel(crate::CHANNEL_SIZE);
    let id: u64 = cx.param("id").map_err(param_err)?.parse()?;
    cx.state()
        .tx
        .unbounded_send(UrMsg::GetNode(NodeId(id), tx))?;

    rx.next()
        .await
        .ok_or(StatusCode::NOT_FOUND.into())
        .and_then(response_json_200)
}

async fn uring_post(cx: Request<Node>) -> Result<Response> {
    let (tx, mut rx) = channel(crate::CHANNEL_SIZE);
    let id: u64 = cx.param("id").map_err(param_err)?.parse()?;
    cx.state()
        .tx
        .unbounded_send(UrMsg::AddNode(NodeId(id), tx))?;
    rx.next()
        .await
        .ok_or(StatusCode::NOT_FOUND.into())
        .and_then(response_json_200)
}

pub(crate) async fn run(logger: Logger, node: Node, addr: String) -> std::io::Result<()> {
    use async_std::net::{SocketAddr, ToSocketAddrs};
    let addr: SocketAddr = addr
        .to_socket_addrs()
        .await
        .expect("Not a valid address")
        .next()
        .expect("Not a socket address");

    let mut app = tide::with_state(node);

    app.at("/version")
        .get(|c| async { unerror(version(c).await) });
    app.at("/status")
        .get(|c| async { unerror(status(c).await) });
    app.at("/kv/:id")
        .get(|c| async { unerror(kv::get(c).await) })
        .post(|c| async { unerror(kv::post(c).await) })
        .delete(|c| async { unerror(kv::delete(c).await) })
        .at("/cas")
        .post(|c| async { unerror(kv::cas(c).await) });
    app.at("/uring/:id")
        .get(|c| async { unerror(uring_get(c).await) })
        .post(|c| async { unerror(uring_post(c).await) });
    app.at("/mring")
        .get(|c| async { unerror(mring::get_size(c).await) })
        .post(|c| async { unerror(mring::set_size(c).await) })
        .at("/node")
        .post(|c| async { unerror(mring::get_nodes(c).await) })
        .post(|c| async { unerror(mring::add_node(c).await) });
    info!(logger, "Starting server on {}", addr);
    app.listen(addr).await?;
    Ok(())
}
