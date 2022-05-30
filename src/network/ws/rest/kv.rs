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

use super::rest::param_err;
use super::*;
use futures::channel::mpsc::channel;
use tide::{Request, Response};

pub(crate) async fn get(cx: Request<Node>) -> Result<Response> {
    let (tx, rx) = channel(crate::CHANNEL_SIZE);
    let key: String = cx.param("id").map_err(param_err)?.to_string();
    let id = key.clone().into_bytes();
    info!(cx.state().logger, "GET /kv/{}", key);
    request(cx, UrMsg::Get(id.clone(), reply(tx)), rx).await
}

#[derive(Deserialize, Debug)]
struct PostBody {
    value: String,
}

pub(crate) async fn post(mut cx: Request<Node>) -> Result<Response> {
    let (tx, rx) = channel(crate::CHANNEL_SIZE);
    let key = cx.param("id").map_err(param_err)?.to_string();
    let id = key.clone().into_bytes();
    let body: PostBody = cx.body_json().await?;
    info!(cx.state().logger, "POST /kv/{} -> {}", key, body.value);
    request(
        cx,
        UrMsg::Put(id, body.value.clone().into_bytes(), reply(tx)),
        rx,
    )
    .await
}

#[derive(Deserialize)]
struct CasBody {
    check: Option<String>,
    store: String,
}

pub(crate) async fn cas(mut cx: Request<Node>) -> Result<Response> {
    let (tx, rx) = channel(crate::CHANNEL_SIZE);
    let id: String = cx.param("id").map_err(param_err)?.to_string();
    let id = id.into_bytes();
    let body: CasBody = cx.body_json().await?;

    request(
        cx,
        UrMsg::Cas(
            id,
            body.check.clone().map(String::into_bytes),
            body.store.clone().into_bytes(),
            reply(tx),
        ),
        rx,
    )
    .await
}

pub(crate) async fn delete(cx: Request<Node>) -> Result<Response> {
    let (tx, rx) = channel(crate::CHANNEL_SIZE);
    let key: String = cx.param("id").map_err(param_err)?.to_string();
    let id = key.clone().into_bytes();
    request(cx, UrMsg::Delete(id.clone(), reply(tx)), rx).await
}
