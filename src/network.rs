use crate::{memstore::ClientRequest, MemRaft};
use anyhow::Result as AResult;
use async_trait::async_trait;
use dashmap::DashMap;
use openraft::{
    raft::{
        AppendEntriesRequest, AppendEntriesResponse, InstallSnapshotRequest,
        InstallSnapshotResponse, VoteRequest, VoteResponse,
    },
    Config, RaftNetwork,
};
use serde::{Deserialize, Serialize};
use std::{fmt::Display, sync::Arc};
use toy_rpc::macros::export_impl;
use toy_rpc::{
    client::{Call, Client},
    pubsub::AckModeNone,
};
use tracing::{debug, error, info};

#[derive(Debug, Deserialize, Serialize)]
pub(crate) struct Error(String);
impl Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}
impl std::error::Error for Error {}
impl Into<toy_rpc::Error> for Error {
    fn into(self) -> toy_rpc::Error {
        toy_rpc::Error::Internal(Box::new(self))
    }
}

pub(crate) struct TremorRaft {
    raft: MemRaft,
}

#[export_impl]
impl TremorRaft {
    pub fn new(raft: MemRaft) -> Self {
        Self { raft }
    }

    #[export_method]
    pub async fn append_entries(
        &self,
        req: AppendEntriesRequest<ClientRequest>,
    ) -> Result<AppendEntriesResponse<u64>, Error> {
        info!("append_entries");
        dbg!(&req);
        let res = self
            .raft
            .append_entries(req)
            .await
            .map_err(|e| Error(format!("{e}")));
        if let Err(e) = &res {
            error!("append error: {e}");
        }
        res
    }

    #[export_method]
    pub async fn install_snapshot(
        &self,
        req: InstallSnapshotRequest<u64>,
    ) -> Result<InstallSnapshotResponse<u64>, Error> {
        info!("install_snapshot");
        self.raft
            .install_snapshot(req)
            .await
            .map_err(|e| Error(format!("{e}")))
    }

    #[export_method]
    pub async fn vote(&self, req: VoteRequest<u64>) -> Result<VoteResponse<u64>, Error> {
        info!("vote");
        self.raft.vote(req).await.map_err(|e| Error(format!("{e}")))
    }
}

/// A type which emulates a network transport and implements the `RaftNetwork` trait.
pub struct RaftRouter {
    clients: dashmap::DashMap<u64, Client<AckModeNone>>,
}
impl RaftRouter {
    async fn init_client(&self, node_id: u64) -> AResult<()> {
        if !self.clients.contains_key(&node_id) {
            let addr = format!("127.0.0.1:{}", 10000 + node_id);
            info!("New client: {addr}");
            let client = Client::dial(addr).await?;
            self.clients.insert(node_id, client);
        }
        Ok(())
    }
}

#[async_trait]
impl RaftNetwork<ClientRequest> for RaftRouter {
    /// Send an AppendEntries RPC to the target Raft node (§5).
    async fn send_append_entries(
        &self,
        target: u64,
        rpc: AppendEntriesRequest<ClientRequest>,
    ) -> AResult<AppendEntriesResponse<u64>> {
        self.init_client(target).await?;
        let call: Call<AppendEntriesResponse> = self
            .clients
            .get(&target)
            .expect("client not found")
            .call("TremorRaft.append_entries", rpc);

        Ok(dbg!(call.await)?)
    }

    /// Send an InstallSnapshot RPC to the target Raft node (§7).
    async fn send_install_snapshot(
        &self,
        target: u64,
        rpc: InstallSnapshotRequest<u64>,
    ) -> AResult<InstallSnapshotResponse<u64>> {
        self.init_client(target).await?;

        let call: Call<Result<InstallSnapshotResponse, Error>> = self
            .clients
            .get(&target)
            .expect("client not found")
            .call("TremorRaft.install_snapshot", rpc);
        Ok(call.await??)
    }

    /// Send a RequestVote RPC to the target Raft node (§5).
    async fn send_vote(&self, target: u64, rpc: VoteRequest<u64>) -> AResult<VoteResponse<u64>> {
        self.init_client(target).await?;
        let call: Call<VoteResponse> = self
            .clients
            .get(&target)
            .expect("client not found")
            .call("TremorRaft.vote", rpc);
        Ok(call.await?)
    }
}

impl RaftRouter {
    pub(crate) async fn new(_config: Arc<Config>) -> RaftRouter {
        RaftRouter {
            clients: DashMap::new(),
        }
    }
}
