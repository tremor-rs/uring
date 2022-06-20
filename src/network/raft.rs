use actix_web::{
    post,
    web::{self, Data},
    Responder,
};
use openraft::raft::{AppendEntriesRequest, InstallSnapshotRequest, VoteRequest};
use web::Json;

use crate::{app::ExampleApp, ExampleNodeId, ExampleTypeConfig};

// --- Raft communication

#[post("/raft-vote")]
pub async fn vote(
    app: Data<ExampleApp>,
    req: Json<VoteRequest<ExampleNodeId>>,
) -> actix_web::Result<impl Responder> {
    let res = app.raft.vote(req.0).await;
    Ok(Json(res))
}

#[post("/raft-append")]
pub async fn append(
    app: Data<ExampleApp>,
    req: Json<AppendEntriesRequest<ExampleTypeConfig>>,
) -> actix_web::Result<impl Responder> {
    let res = app.raft.append_entries(req.0).await;
    Ok(Json(res))
}

#[post("/raft-snapshot")]
pub async fn snapshot(
    app: Data<ExampleApp>,
    req: Json<InstallSnapshotRequest<ExampleTypeConfig>>,
) -> actix_web::Result<impl Responder> {
    let res = app.raft.install_snapshot(req.0).await;
    Ok(Json(res))
}
