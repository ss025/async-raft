use rocksdb::DB;
use async_raft::raft::MembershipConfig;
use serde::{Serialize, Deserialize};
use async_raft::NodeId;
use anyhow::Result;

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct PersistStoreSnapshotMeta {
    /// The last index covered by this snapshot.
    pub index: u64,
    /// The term of the last index covered by this snapshot.
    pub term: u64,
    /// The last membership config included in this snapshot.
    pub membership: MembershipConfig,
    /// The data of the state machine at the time of this snapshot.
    pub data: Vec<u8>, // Todo: rocks db meta - may be last applied key
}

pub struct PersistStoreSnapshot {
    _node_id: NodeId,
    inner: DB,
}

impl PersistStoreSnapshot {
    pub fn new(node_id: NodeId) -> Self {
        let path = format!("/Users/sohi/personal-git-v2/raft-learning/async-raft/persist-db-dir/{}/snapshots", node_id);
        let inner = DB::open_default(path).unwrap(); // todo sohan-unwrap
        PersistStoreSnapshot { _node_id: node_id, inner }
    }

    pub(crate) async fn save(&self, meta: &PersistStoreSnapshotMeta) ->Result<()> {
        let value = serde_json::to_vec(meta)?;
        self.inner.put("ss".as_bytes(), &value)?;
        Ok(())
    }

    // todo sohan-unwrap
    pub(crate) async fn get(&self) ->Option<PersistStoreSnapshotMeta> {
        let v = self.inner.get("ss").unwrap();
        let option = v.map(|v| serde_json::from_slice::<PersistStoreSnapshotMeta>(&v)).transpose();
        option.unwrap()
    }
}