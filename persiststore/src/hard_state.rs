use async_raft::NodeId;
use async_raft::storage::HardState;

pub struct PersistedHardState {
    inner: sled::Db, //todo:  rocksdb or simple file
}


impl PersistedHardState {
    pub fn new(node_id: NodeId) -> Self {
        let path = format!("/Users/sohi/personal-git-v2/raft-learning/async-raft/persist-db-dir/{}/hs", node_id);
        let db = sled::open(path).unwrap();
        PersistedHardState { inner: db }
    }

    pub fn save(&self, hs: &HardState) -> anyhow::Result<()>{
        let v = serde_json::to_vec(hs)?;
        self.inner.insert("hs", v)?;
        Ok(())
    }

    // todo: refactoring
    pub(crate) fn get(&self) -> Option<HardState> {
        let result = self.inner.get("hs").unwrap_or_else(|_| None); // todo: handle error
        let v = result.map(|v | {
            let v = serde_json::from_slice::<HardState>(&v);
            v.unwrap() // todo: handle unwrap
        }) ;
        v
    }
}