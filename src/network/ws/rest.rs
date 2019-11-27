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
use futures::channel::mpsc::unbounded;
use http::StatusCode;
use tide::{error::ResultExt, response, App, Context, EndpointResult};

async fn status(cx: Context<Node>) -> EndpointResult {
    let (tx, mut rx) = unbounded();
    cx.state().tx.unbounded_send(UrMsg::Status(tx)).unwrap();
    rx.next()
        .await
        .ok_or(StatusCode::NOT_FOUND.into())
        .map(response::json)
}

async fn kv_get(cx: Context<Node>) -> EndpointResult {
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
        .map(|value| KV { key, value })
        .map(response::json)
}

#[derive(Deserialize, Debug)]
struct PostBody {
    value: String,
}

async fn kv_post(mut cx: Context<Node>) -> EndpointResult {
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
        .map(|_| response::json("created"))
        .map(|mut r| {
            *r.status_mut() = StatusCode::CREATED;
            r
        })
}

#[derive(Deserialize)]
struct CasBody {
    check: Option<String>,
    store: String,
}

async fn kv_cas(mut cx: Context<Node>) -> EndpointResult {
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
            let mut r = if conflict.is_empty() {
                response::json(serde_json::Value::Null)
            } else {
                response::json(serde_json::from_slice::<serde_json::Value>(&conflict).unwrap())
            };
            *r.status_mut() = StatusCode::CONFLICT;
            Ok(r)
        } else {
            let mut r = response::json("set");
            *r.status_mut() = StatusCode::CREATED;
            Ok(r)
        }
    } else {
        Err(StatusCode::INTERNAL_SERVER_ERROR)?
    }
}

async fn kv_delete(cx: Context<Node>) -> EndpointResult {
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
        .map(response::json)
}

async fn uring_get(cx: Context<Node>) -> EndpointResult {
    let (tx, mut rx) = unbounded();
    let id: u64 = cx.param("id").client_err()?;
    cx.state()
        .tx
        .unbounded_send(UrMsg::GetNode(NodeId(id), tx))
        .unwrap();

    rx.next()
        .await
        .ok_or(StatusCode::NOT_FOUND.into())
        .map(response::json)
}

async fn uring_post(cx: Context<Node>) -> EndpointResult {
    let (tx, mut rx) = unbounded();
    let id: u64 = cx.param("id").client_err()?;
    cx.state()
        .tx
        .unbounded_send(UrMsg::AddNode(NodeId(id), tx))
        .unwrap();
    rx.next()
        .await
        .ok_or(StatusCode::NOT_FOUND.into())
        .map(response::json)
}

#[derive(Deserialize, Serialize)]
pub struct MRingSize {
    size: u64,
}
async fn mring_get_size(cx: Context<Node>) -> EndpointResult {
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
        .map(response::json)
}

async fn mring_set_size(mut cx: Context<Node>) -> EndpointResult {
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
        .map(response::json)
}

async fn mring_get_nodes(cx: Context<Node>) -> EndpointResult {
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
        .map(response::json)
}

#[derive(Deserialize, Serialize)]
pub struct MRingNode {
    node: String,
}

async fn mring_add_node(mut cx: Context<Node>) -> EndpointResult {
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
        .map(response::json)
}

pub(crate) async fn run(logger: Logger, node: Node, addr: String) -> std::io::Result<()> {
    use async_std::net::{SocketAddr, ToSocketAddrs};
    let addr: SocketAddr = addr
        .to_socket_addrs()
        .await
        .expect("Not a valid address")
        .next()
        .expect("Not a socket address");

    let mut app = App::with_state(node);

    app.at("/status").get(status);
    app.at("/kv/:id")
        .get(kv_get)
        .post(kv_post)
        .delete(kv_delete)
        .at("/cas")
        .post(kv_cas);
    app.at("/uring/:id").get(uring_get).post(uring_post);
    app.at("/mring")
        .get(mring_get_size)
        .post(mring_set_size)
        .at("/node")
        .post(mring_get_nodes)
        .post(mring_add_node);
    info!(logger, "Starting server on {}", addr);

    std::thread::spawn(move || {
        error!(logger, "Starting server tread for {}", addr);
        app.serve(addr).unwrap();
    });
    Ok(())
}
