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

use super::server;
use super::*;
use crate::{NodeId, KV};
use actix_web::{http::StatusCode, web, Error as ActixError, HttpRequest, HttpResponse};
use actix_web_actors::ws;
use crossbeam_channel::bounded;
use serde::{Deserialize, Serialize};

/// do websocket handshake and start `UrSocket` actor
pub(crate) fn uring_index(
    r: HttpRequest,
    stream: web::Payload,
    srv: web::Data<Node>,
) -> Result<HttpResponse, ActixError> {
    let res = ws::start(server::Connection::new(srv.get_ref().clone()), &r, stream);
    res
}

#[derive(Deserialize)]
pub struct NoParams {}

pub(crate) fn status(
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

pub(crate) fn get(
    _r: HttpRequest,
    params: web::Path<GetParams>,
    srv: web::Data<Node>,
) -> Result<HttpResponse, ActixError> {
    let (tx, rx) = bounded(1);
    let key = params.id.clone();
    srv.get_ref()
        .tx
        .send(UrMsg::Get(key.clone().into_bytes(), Reply::Direct(tx)))
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
pub struct PostParams {
    id: String,
}

#[derive(Deserialize)]
pub struct PostBody {
    value: String,
}

/// do websocket handshake and start `UrSocket` actor
pub(crate) fn post(
    _r: HttpRequest,
    params: web::Path<PostParams>,
    body: web::Json<PostBody>,
    srv: web::Data<Node>,
) -> Result<HttpResponse, ActixError> {
    let (tx, rx) = bounded(1);
    srv.get_ref()
        .tx
        .send(UrMsg::Put(
            params.id.clone().into_bytes(),
            body.value.clone().into_bytes(),
            Reply::Direct(tx),
        ))
        .unwrap();
    if rx.recv().unwrap().is_some() {
        Ok(HttpResponse::new(StatusCode::from_u16(201).unwrap()))
    } else {
        Ok(HttpResponse::new(StatusCode::from_u16(500).unwrap()))
    }
}

#[derive(Deserialize)]
pub struct CasBody {
    check: String,
    store: String,
}

pub(crate) fn cas(
    _r: HttpRequest,
    params: web::Path<PostParams>,
    body: web::Json<CasBody>,
    srv: web::Data<Node>,
) -> Result<HttpResponse, ActixError> {
    let (tx, rx) = bounded(1);
    srv.get_ref()
        .tx
        .send(UrMsg::Cas(
            params.id.clone().into_bytes(),
            body.check.clone().into_bytes(),
            body.store.clone().into_bytes(),
            Reply::Direct(tx),
        ))
        .unwrap();
    // FIXME improve status when check fails vs succeeds?
    if rx.recv().unwrap().is_some() {
        Ok(HttpResponse::new(StatusCode::from_u16(201).unwrap()))
    } else {
        Ok(HttpResponse::new(StatusCode::from_u16(500).unwrap()))
    }
}

#[derive(Deserialize)]
pub struct DeleteParams {
    id: String,
}

pub(crate) fn delete(
    _r: HttpRequest,
    params: web::Path<DeleteParams>,
    srv: web::Data<Node>,
) -> Result<HttpResponse, ActixError> {
    let (tx, rx) = bounded(1);
    let key = params.id.clone();
    srv.get_ref()
        .tx
        .send(UrMsg::Delete(key.clone().into_bytes(), Reply::Direct(tx)))
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
pub struct GetNodeParams {
    id: NodeId,
}
pub(crate) fn get_node(
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

pub(crate) fn post_node(
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

#[derive(Deserialize, Serialize)]
pub struct MRingSize {
    size: u64,
}
pub(crate) fn get_mring_size(
    _r: HttpRequest,
    srv: web::Data<Node>,
) -> Result<HttpResponse, ActixError> {
    let (tx, rx) = bounded(1);
    srv.get_ref()
        .tx
        .send(UrMsg::MRingGetSize(Reply::Direct(tx)))
        .unwrap();
    if let Some(size) = rx
        .recv()
        .unwrap()
        .and_then(|data| serde_json::from_slice(&data).ok())
    {
        Ok(HttpResponse::Ok().json(MRingSize { size }))
    } else {
        Ok(HttpResponse::new(StatusCode::from_u16(500).unwrap()))
    }
}

pub(crate) fn set_mring_size(
    _r: HttpRequest,
    body: web::Json<MRingSize>,
    srv: web::Data<Node>,
) -> Result<HttpResponse, ActixError> {
    let (tx, rx) = bounded(1);
    srv.get_ref()
        .tx
        .send(UrMsg::MRingSetSize(body.size, Reply::Direct(tx)))
        .unwrap();
    if let Some(size) = rx
        .recv()
        .unwrap()
        .and_then(|data| serde_json::from_slice(dbg!(&data)).ok())
    {
        Ok(HttpResponse::Ok().json(MRingSize { size }))
    } else {
        Ok(HttpResponse::new(StatusCode::from_u16(500).unwrap()))
    }
}

pub(crate) fn get_mring_nodes(
    _r: HttpRequest,
    srv: web::Data<Node>,
) -> Result<HttpResponse, ActixError> {
    let (tx, rx) = bounded(1);
    srv.get_ref()
        .tx
        .send(UrMsg::MRingGetNodes(Reply::Direct(tx)))
        .unwrap();
    if let Some(data) = rx.recv().unwrap() {
        Ok(HttpResponse::Ok()
            .content_type("application/json")
            .body(data))
    } else {
        Ok(HttpResponse::new(StatusCode::from_u16(500).unwrap()))
    }
}

#[derive(Deserialize, Serialize)]
pub struct MRingNode {
    node: String,
}

pub(crate) fn add_mring_node(
    _r: HttpRequest,
    body: web::Json<MRingNode>,
    srv: web::Data<Node>,
) -> Result<HttpResponse, ActixError> {
    let (tx, rx) = bounded(1);
    srv.get_ref()
        .tx
        .send(UrMsg::MRingAddNode(body.node.clone(), Reply::Direct(tx)))
        .unwrap();
    if let Some(data) = rx.recv().unwrap() {
        Ok(HttpResponse::Ok()
            .content_type("application/json")
            .body(data))
    } else {
        Ok(HttpResponse::new(StatusCode::from_u16(500).unwrap()))
    }
}
