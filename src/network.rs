use crate::{ClientRequest, MemRaft};
use anyhow::Result as AResult;
use async_raft::{
    raft::{
        AppendEntriesRequest, AppendEntriesResponse, InstallSnapshotRequest,
        InstallSnapshotResponse, VoteRequest, VoteResponse,
    },
    Config, RaftNetwork,
};
use async_trait::async_trait;
use dashmap::DashMap;
use serde::{Deserialize, Serialize};
use std::{collections::HashMap, fmt::Display, sync::Arc};
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
    ) -> Result<AppendEntriesResponse, Error> {
        info!("append_entries");
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
        req: InstallSnapshotRequest,
    ) -> Result<InstallSnapshotResponse, Error> {
        info!("install_snapshot");
        self.raft
            .install_snapshot(req)
            .await
            .map_err(|e| Error(format!("{e}")))
    }

    #[export_method]
    pub async fn vote(&self, req: VoteRequest) -> Result<VoteResponse, Error> {
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
            let client = Client::dial(addr).await?;
            self.clients.insert(node_id, client);
        }
        Ok(())
    }
}

#[async_trait]
impl RaftNetwork<ClientRequest> for RaftRouter {
    /// Send an AppendEntries RPC to the target Raft node (§5).
    async fn append_entries(
        &self,
        target: u64,
        rpc: AppendEntriesRequest<ClientRequest>,
    ) -> AResult<AppendEntriesResponse> {
        self.init_client(target).await?;
        let call: Call<AppendEntriesResponse> = self
            .clients
            .get(&target)
            .expect("client not found")
            .call("TremorRaft.append_entries", rpc);

        Ok(dbg!(call.await)?)
    }

    /// Send an InstallSnapshot RPC to the target Raft node (§7).
    async fn install_snapshot(
        &self,
        target: u64,
        rpc: InstallSnapshotRequest,
    ) -> AResult<InstallSnapshotResponse> {
        self.init_client(target).await?;

        let call: Call<Result<InstallSnapshotResponse, Error>> = self
            .clients
            .get(&target)
            .expect("client not found")
            .call("TremorRaft.install_snapshot", rpc);
        Ok(call.await??)
    }

    /// Send a RequestVote RPC to the target Raft node (§5).
    async fn vote(&self, target: u64, rpc: VoteRequest) -> AResult<VoteResponse> {
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
