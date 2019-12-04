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

use super::Node;
use super::*;
use crate::{NodeId, KV};
use futures::channel::mpsc::{unbounded, channel};
use http::StatusCode;
use tide::{ResultExt, Request, Response, IntoResponse};
use serde::Serialize;

const CHANNEL_SIZE: usize = 64usize;
type Result<T> = std::result::Result<T, tide::Error>;

fn unerror(r: Result<Response>) -> Response{
    match r {
        Ok(r) => r,
        Err(e) => e.into_response()
    }
}


async fn version(cx: Request<Node>) -> Result<Response> {
    let (tx, mut rx) = channel(CHANNEL_SIZE);
    cx.state()
        .tx
        .unbounded_send(UrMsg::Version(RequestId(666), tx))
        .unwrap();
    rx.next()
        .await
        .ok_or(StatusCode::NOT_FOUND.into())
        // FIXME TODO refactor REST ( direct ) vs WS (WS ) support => make protocol agnostic by design
        .map(|msg| match msg {
            WsMessage::Reply(r) => response_json_200(r.data),
            _ => unreachable!(),
        })
}

async fn status(cx: Request<Node>) -> Result<Response> {
    let (tx, mut rx) = channel(CHANNEL_SIZE);
    cx.state()
        .tx
        .unbounded_send(UrMsg::Status(RequestId(666), tx))
        .unwrap();
    rx.next()
        .await
        .ok_or(StatusCode::NOT_FOUND.into())
        .map(|msg| match msg {
            WsMessage::Reply(r) => response_json_200(r.data),
            _ => unreachable!(),
        })
}

async fn kv_get(cx: Request<Node>) -> Result<Response> {
    let (tx, mut rx) = unbounded();
    let key: String = cx.param("id").client_err()?;
    let id = key.clone().into_bytes();
    info!(cx.state().logger, "GET /kv/{}", key);
    cx.state()
        .tx
        .unbounded_send(UrMsg::Get(id.clone(), Reply::Direct(tx)))
        .unwrap();

    rx.next()
        .await
        .and_then(|v| v)
        .and_then(|v| serde_json::from_slice::<serde_json::Value>(&v).ok())
        .ok_or(StatusCode::NOT_FOUND.into())
        .map(|value| KV { key, value }).map(response_json_200)


}

#[derive(Deserialize, Debug)]
struct PostBody {
    value: String,
}

async fn kv_post(mut cx: Request<Node>) -> Result<Response> {
    let (tx, mut rx) = unbounded();
    let key: String = cx.param("id").client_err()?;
    let id = key.clone().into_bytes();
    let body: PostBody = cx.body_json().await.client_err()?;
    info!(cx.state().logger, "POST /kv/{} -> {}", key, body.value);

    cx.state()
        .tx
        .unbounded_send(UrMsg::Put(
            id,
            body.value.clone().into_bytes(),
            Reply::Direct(tx),
        ))
        .unwrap();
    rx.next()
        .await
        .ok_or(StatusCode::INTERNAL_SERVER_ERROR.into())
        .map(|_| response_json(201, "created"))
}

#[derive(Deserialize)]
struct CasBody {
    check: Option<String>,
    store: String,
}

fn response_json<S: Serialize>(c: u16,  v: S) -> Response {
    Response::new(c).body_json(&v).unwrap()
}

fn response_json_200<S: Serialize>(v: S) -> Response {
    response_json(200, v)
}


async fn kv_cas(mut cx: Request<Node>) -> Result<Response> {
    let (tx, mut rx) = unbounded();
    let id: String = cx.param("id").client_err()?;
    let id = id.into_bytes();
    let body: CasBody = cx.body_json().await.client_err()?;
    cx.state()
        .tx
        .unbounded_send(UrMsg::Cas(
            id,
            body.check.clone().map(String::into_bytes),
            body.store.clone().into_bytes(),
            Reply::Direct(tx),
        ))
        .unwrap();
    if let Some(result) = rx.next().await {
        if let Some(conflict) = result {
             Ok(if conflict.is_empty() {
                response_json(409, serde_json::Value::Null)
            } else {
                response_json(409, serde_json::from_slice::<serde_json::Value>(&conflict).unwrap())
            })
        } else {
            Ok(response_json(201, "set"))
        }
    } else {
        Err(StatusCode::INTERNAL_SERVER_ERROR)?
    }
}

async fn kv_delete(cx: Request<Node>) -> Result<Response> {
    let (tx, mut rx) = unbounded();
    let key: String = cx.param("id").client_err()?;
    let id = key.clone().into_bytes();
    cx.state()
        .tx
        .unbounded_send(UrMsg::Delete(id.clone(), Reply::Direct(tx)))
        .unwrap();
    rx.next()
        .await
        .and_then(|v| v)
        .and_then(|v| serde_json::from_slice(&v).ok())
        .ok_or(StatusCode::NOT_FOUND.into())
        .map(|value| KV { key, value })
        .map(response_json_200)
}

async fn uring_get(cx: Request<Node>) -> Result<Response> {
    let (tx, mut rx) = unbounded();
    let id: u64 = cx.param("id").client_err()?;
    cx.state()
        .tx
        .unbounded_send(UrMsg::GetNode(NodeId(id), tx))
        .unwrap();

    rx.next()
        .await
        .ok_or(StatusCode::NOT_FOUND.into())
        .map(response_json_200)
}

async fn uring_post(cx: Request<Node>) -> Result<Response> {
    let (tx, mut rx) = unbounded();
    let id: u64 = cx.param("id").client_err()?;
    cx.state()
        .tx
        .unbounded_send(UrMsg::AddNode(NodeId(id), tx))
        .unwrap();
    rx.next()
        .await
        .ok_or(StatusCode::NOT_FOUND.into())
        .map(response_json_200)
}

#[derive(Deserialize, Serialize)]
pub struct MRingSize {
    size: u64,
}
async fn mring_get_size(cx: Request<Node>) -> Result<Response> {
    let (tx, mut rx) = unbounded();
    cx.state()
        .tx
        .unbounded_send(UrMsg::MRingGetSize(Reply::Direct(tx)))
        .unwrap();
    rx.next()
        .await
        .and_then(|d| d)
        .and_then(|data| serde_json::from_slice::<serde_json::Value>(&data).ok())
        .ok_or(StatusCode::INTERNAL_SERVER_ERROR.into())
        .map(response_json_200)
}

async fn mring_set_size(mut cx: Request<Node>) -> Result<Response> {
    let (tx, mut rx) = unbounded();
    let body: MRingSize = cx.body_json().await.client_err()?;
    cx.state()
        .tx
        .unbounded_send(UrMsg::MRingSetSize(body.size, Reply::Direct(tx)))
        .unwrap();
    rx.next()
        .await
        .and_then(|d| d)
        .and_then(|data| serde_json::from_slice::<serde_json::Value>(&data).ok())
        .ok_or(StatusCode::INTERNAL_SERVER_ERROR.into())
        .map(response_json_200)
}

async fn mring_get_nodes(cx: Request<Node>) -> Result<Response> {
    let (tx, mut rx) = unbounded();
    cx.state()
        .tx
        .unbounded_send(UrMsg::MRingGetNodes(Reply::Direct(tx)))
        .unwrap();
    rx.next()
        .await
        .and_then(|d| d)
        .and_then(|data| serde_json::from_slice::<serde_json::Value>(&data).ok())
        .ok_or(StatusCode::INTERNAL_SERVER_ERROR.into())
        .map(response_json_200)
}

#[derive(Deserialize, Serialize)]
pub struct MRingNode {
    node: String,
}

async fn mring_add_node(mut cx: Request<Node>) -> Result<Response> {
    let (tx, mut rx) = unbounded();
    let body: MRingNode = cx.body_json().await.client_err()?;
    cx.state()
        .tx
        .unbounded_send(UrMsg::MRingAddNode(body.node.clone(), Reply::Direct(tx)))
        .unwrap();
    rx.next()
        .await
        .and_then(|d| d)
        .and_then(|data| serde_json::from_slice::<serde_json::Value>(&data).ok())
        .ok_or(StatusCode::INTERNAL_SERVER_ERROR.into())
        .map(response_json_200)
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

    app.at("/version").get(|c| async {unerror(version(c).await)});
    app.at("/status").get(|c| async {unerror(status(c).await)});
    app.at("/kv/:id")
        .get(|c| async {unerror(kv_get(c).await)})
        .post(|c| async {unerror(kv_post(c).await)})
        .delete(|c| async {unerror(kv_delete(c).await)})
        .at("/cas")
        .post(|c| async {unerror(kv_cas(c).await)});
    app.at("/uring/:id").get(|c| async {unerror(uring_get(c).await)}).post(|c| async {unerror(uring_post(c).await)});
    app.at("/mring")
        .get(|c| async {unerror(mring_get_size(c).await)})
        .post(|c| async {unerror(mring_set_size(c).await)})
        .at("/node")
        .post(|c| async {unerror(mring_get_nodes(c).await)})
        .post(|c| async {unerror(mring_add_node(c).await)});
    info!(logger, "Starting server on {}", addr);
    app.listen(addr).await?;
    Ok(())
}
