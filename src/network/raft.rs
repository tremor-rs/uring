use crate::{app::ExampleApp, ExampleTypeConfig};
use openraft::raft::{
    AppendEntriesRequest, AppendEntriesResponse, InstallSnapshotRequest, InstallSnapshotResponse,
    VoteRequest, VoteResponse,
};
use std::sync::Arc;
use toy_rpc::macros::export_impl;

// --- Raft communication

pub struct Raft {
    app: Arc<ExampleApp>,
}

#[export_impl]
impl Raft {
    pub fn new(app: Arc<ExampleApp>) -> Self {
        Self { app }
    }
    #[export_method]
    pub async fn vote(&self, vote: VoteRequest<u64>) -> Result<VoteResponse<u64>, toy_rpc::Error> {
        self.app
            .raft
            .vote(vote)
            .await
            .map_err(|e| toy_rpc::Error::Internal(Box::new(e)))
    }
    #[export_method]
    pub async fn append(
        &self,
        req: AppendEntriesRequest<ExampleTypeConfig>,
    ) -> Result<AppendEntriesResponse<u64>, toy_rpc::Error> {
        self.app
            .raft
            .append_entries(req)
            .await
            .map_err(|e| toy_rpc::Error::Internal(Box::new(e)))
    }
    #[export_method]
    pub async fn snapshot(
        &self,
        req: InstallSnapshotRequest<ExampleTypeConfig>,
    ) -> Result<InstallSnapshotResponse<u64>, toy_rpc::Error> {
        self.app
            .raft
            .install_snapshot(req)
            .await
            .map_err(|e| toy_rpc::Error::Internal(Box::new(e)))
    }
}
