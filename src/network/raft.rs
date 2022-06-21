use crate::{app::ExampleApp, Server};
use std::sync::Arc;
use tide::{Body, Request, Response, StatusCode};

// --- Raft communication

pub fn rest(app: &mut Server) {
    let mut raft = app.at("/raft");
    raft.at("/vote").post(vote);
    raft.at("/append").post(append);
    raft.at("/snapshot").post(snapshot);
}
async fn vote(mut req: Request<Arc<ExampleApp>>) -> tide::Result {
    let body = req.body_json().await?;
    let res = req.state().raft.vote(body).await;
    Ok(Response::builder(StatusCode::Ok)
        .body(Body::from_json(&res)?)
        .build())
}

async fn append(mut req: Request<Arc<ExampleApp>>) -> tide::Result {
    let body = req.body_json().await?;
    let res = req.state().raft.append_entries(body).await;
    Ok(Response::builder(StatusCode::Ok)
        .body(Body::from_json(&res)?)
        .build())
}

async fn snapshot(mut req: Request<Arc<ExampleApp>>) -> tide::Result {
    let body = req.body_json().await?;
    let res = req.state().raft.install_snapshot(body).await;
    Ok(Response::builder(StatusCode::Ok)
        .body(Body::from_json(&res)?)
        .build())
}
