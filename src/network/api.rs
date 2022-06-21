use crate::{app::ExampleApp, ExampleNodeId, Server};
use openraft::{
    error::{CheckIsLeaderError, Infallible},
    raft::ClientWriteRequest,
    EntryPayload,
};
use std::sync::Arc;
use tide::{Body, Request, Response, StatusCode};

pub fn rest(app: &mut Server) {
    app.at("/write").post(write);
    app.at("/read").post(read);
    app.at("/consistent_read").post(consistent_read);
}
/**
 * Application API
 *
 * This is where you place your application, you can use the example below to create your
 * API. The current implementation:
 *
 *  - `POST - /write` saves a value in a key and sync the nodes.
 *  - `POST - /read` attempt to find a value from a given key.
 */
async fn write(mut req: Request<Arc<ExampleApp>>) -> tide::Result {
    let body = req.body_json().await?;
    let request = ClientWriteRequest::new(EntryPayload::Normal(body));
    let res = req.state().raft.client_write(request).await;
    Ok(Response::builder(StatusCode::Ok)
        .body(Body::from_json(&res)?)
        .build())
}

async fn read(mut req: Request<Arc<ExampleApp>>) -> tide::Result {
    let key: String = req.body_json().await?;
    let state_machine = req.state().store.state_machine.read().await;
    let value = state_machine.data.get(&key).cloned();

    let res: Result<String, Infallible> = Ok(value.unwrap_or_default());
    Ok(Response::builder(StatusCode::Ok)
        .body(Body::from_json(&res)?)
        .build())
}

async fn consistent_read(mut req: Request<Arc<ExampleApp>>) -> tide::Result {
    let ret = req.state().raft.is_leader().await;

    match ret {
        Ok(_) => {
            let key: String = req.body_json().await?;
            let state_machine = req.state().store.state_machine.read().await;

            let value = state_machine.data.get(&key).cloned();

            let res: Result<String, CheckIsLeaderError<ExampleNodeId>> =
                Ok(value.unwrap_or_default());
            Ok(Response::builder(StatusCode::Ok)
                .body(Body::from_json(&res)?)
                .build())
        }
        e => Ok(Response::builder(StatusCode::Ok)
            .body(Body::from_json(&e)?)
            .build()),
    }
}
