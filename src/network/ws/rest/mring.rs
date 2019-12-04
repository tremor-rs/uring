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
use futures::channel::mpsc::channel;
use tide::{Request, Response};

#[derive(Deserialize, Serialize)]
pub struct MRingSize {
    size: u64,
}

pub(crate) async fn get_size(cx: Request<Node>) -> Result<Response> {
    let (tx, rx) = channel(crate::CHANNEL_SIZE);
    request(cx, UrMsg::MRingGetSize(reply(tx)), rx).await
}

pub(crate) async fn set_size(mut cx: Request<Node>) -> Result<Response> {
    let (tx, rx) = channel(crate::CHANNEL_SIZE);
    let body: MRingSize = cx.body_json().await.client_err()?;
    request(cx, UrMsg::MRingSetSize(body.size, reply(tx)), rx).await
}

pub(crate) async fn get_nodes(cx: Request<Node>) -> Result<Response> {
    let (tx, rx) = channel(crate::CHANNEL_SIZE);
    request(cx, UrMsg::MRingGetNodes(reply(tx)), rx).await
}

#[derive(Deserialize, Serialize)]
pub struct MRingNode {
    node: String,
}

pub(crate) async fn add_node(mut cx: Request<Node>) -> Result<Response> {
    let (tx, rx) = channel(crate::CHANNEL_SIZE);
    let body: MRingNode = cx.body_json().await.client_err()?;
    request(cx, UrMsg::MRingAddNode(body.node.clone(), reply(tx)), rx).await
}
