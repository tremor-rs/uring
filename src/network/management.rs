use crate::{app::ExampleApp, ExampleNodeId, ExampleTypeConfig, Server};
use openraft::{error::Infallible, Node, RaftMetrics};
use std::{collections::BTreeMap, sync::Arc};
use tide::{Body, Request, Response, StatusCode};

// --- Cluster management

pub fn rest(app: &mut Server) {
    let mut cluster = app.at("/cluster");
    cluster.at("/add-learner").post(add_learner);
    cluster.at("/change-membership").post(change_membership);
    cluster.at("/init").post(init);
    cluster.at("/metrics").get(metrics);
}

/// Add a node as **Learner**.
///
/// A Learner receives log replication from the leader but does not vote.
/// This should be done before adding a node as a member into the cluster
/// (by calling `change-membership`)
async fn add_learner(mut req: Request<Arc<ExampleApp>>) -> tide::Result {
    let body: (ExampleNodeId, String) = req.body_json().await?;
    let node_id = body.0;
    let node = Node {
        addr: body.1.clone(),
        ..Default::default()
    };
    let res = req
        .state()
        .raft
        .add_learner(node_id, Some(node), true)
        .await;
    Ok(Response::builder(StatusCode::Ok)
        .body(Body::from_json(&res)?)
        .build())
}

/// Changes specified learners to members, or remove members.
async fn change_membership(mut req: Request<Arc<ExampleApp>>) -> tide::Result {
    let body = req.body_json().await?;
    let res = req.state().raft.change_membership(body, true, false).await;
    Ok(Response::builder(StatusCode::Ok)
        .body(Body::from_json(&res)?)
        .build())
}

/// Initialize a single-node cluster.
async fn init(req: Request<Arc<ExampleApp>>) -> tide::Result {
    let mut nodes = BTreeMap::new();
    nodes.insert(
        req.state().id,
        Node {
            addr: req.state().addr.clone(),
            data: Default::default(),
        },
    );
    let res = req.state().raft.initialize(nodes).await;
    Ok(Response::builder(StatusCode::Ok)
        .body(Body::from_json(&res)?)
        .build())
}

/// Get the latest metrics of the cluster
async fn metrics(req: Request<Arc<ExampleApp>>) -> tide::Result {
    let metrics = req.state().raft.metrics().borrow().clone();

    let res: Result<RaftMetrics<ExampleTypeConfig>, Infallible> = Ok(metrics);
    Ok(Response::builder(StatusCode::Ok)
        .body(Body::from_json(&res)?)
        .build())
}
