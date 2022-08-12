use std::sync::Arc;

use anyhow::Result;
use async_trait::async_trait;
use tracing::debug;

use async_raft::{Config, NodeId, RaftNetwork, SnapshotPolicy};
use async_raft::Raft;
use async_raft::raft::{AppendEntriesRequest, AppendEntriesResponse, InstallSnapshotRequest, InstallSnapshotResponse, VoteRequest, VoteResponse};
use persiststore::{ClientRequest, ClientResponse, PersistStore};

use crate::common::PEERS;

type Store = PersistStore ;
pub type RaftServer = Raft<ClientRequest, ClientResponse, RaftNetworkRouter, Store>;

pub fn new(node_id: NodeId, storage: Arc<Store>) -> RaftServer {
    let config = Config::build("primary-raft-group".into())
        .heartbeat_interval(10 * 1000)// 10Sec
        .election_timeout_min(10 * 1000)
        .election_timeout_max(15 * 1000)
        .snapshot_policy(SnapshotPolicy::LogsSinceLast(3))
        .validate()
        .expect("failed to build Raft config");
    let config = Arc::new(config);
    let network = Arc::new(RaftNetworkRouter::new());

    // Create a new Raft node, which spawns an async task which
    // runs the Raft core logic. Keep this Raft instance around
    // for calling API methods based on events in your app.
    Raft::new(node_id, config, network, storage)
}

pub struct RaftNetworkRouter {
    client: reqwest::Client,
}

impl RaftNetworkRouter {
    fn new() -> Self {
        RaftNetworkRouter { client: reqwest::Client::new() }
    }

    fn build_url(target: NodeId, path: &str) -> anyhow::Result<String> {
        let node_addr: &str = PEERS.get(target as usize).ok_or_else(|| anyhow::anyhow!("node id {target} not found"))?;
        let url = format!("http://{node_addr}/{path}");
        Ok(url)
    }
}

#[async_trait]
impl RaftNetwork<ClientRequest> for RaftNetworkRouter {
    async fn append_entries(&self, target: NodeId, rpc: AppendEntriesRequest<ClientRequest>) -> Result<AppendEntriesResponse> {
        let url = Self::build_url(target, "append_entries")?;
        if !rpc.entries.is_empty() {
           debug!("going to send entries for replication to {url} with {:?}", rpc);
        }
        let resp = self.client
            .post(url)
            .json(&rpc)
            .send()
            .await?
            .json::<AppendEntriesResponse>()
            .await?;

        Ok(resp)
    }

    async fn install_snapshot(&self, target: NodeId, rpc: InstallSnapshotRequest) -> Result<InstallSnapshotResponse> {
        let url = Self::build_url(target, "install_snapshot")?;
        let resp = self.client
            .post(url)
            .json(&rpc)
            .send()
            .await?
            .json::<InstallSnapshotResponse>()
            .await?;
        Ok(resp)
    }

    async fn vote(&self, target: NodeId, rpc: VoteRequest) -> Result<VoteResponse> {
        let url = Self::build_url(target, "vote")?;
        let resp = self.client
            .post(url)
            .json(&rpc)
            .send()
            .await?
            .json::<VoteResponse>()
            .await?;
        Ok(resp)
    }
}