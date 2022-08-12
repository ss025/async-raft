use std::fs;
use std::path::Path;

use anyhow::Result;
use fs_extra::dir::CopyOptions;
use rocksdb::{DB as RocksDB, DBWithThreadMode, IteratorMode, SingleThreaded};
use rocksdb::checkpoint::Checkpoint;
use sled::{Db as SledDb, IVec};
use tracing::trace;
use tracing::debug;

use async_raft::NodeId;

use crate::ClientRequest;

pub struct PersistStateMachine {
    node: NodeId,
    db: RocksDB,
    // todo sohan simple file or rocksdb
    meta: SledDb,
    // todo sohan simple file or rocksdb
    checkpoint_base_path: String,
}

impl PersistStateMachine {
    pub fn new(node: NodeId) -> Self {
        let meta_path = format!("/Users/sohi/personal-git-v2/raft-learning/async-raft/persist-db-dir/{}/sm/meta", node);
        let db = Self::open_new_data_db(node);
        let meta = sled::open(meta_path).unwrap();

        let checkpoint_base_path = format!("/Users/sohi/personal-git-v2/raft-learning/async-raft/persist-db-dir/{}/sm/checkpoints", node);
        fs::create_dir_all(&checkpoint_base_path).unwrap();

        PersistStateMachine { node, db, meta, checkpoint_base_path }
    }

    fn open_new_data_db(node: NodeId) -> DBWithThreadMode<SingleThreaded> {
        let path = format!("/Users/sohi/personal-git-v2/raft-learning/async-raft/persist-db-dir/{}/sm/db", node);
        let db = RocksDB::open_default(path).unwrap();
        db
    }

    pub fn get_last_applied_log(&self) -> u64 {
        let v = self.meta.get("last_applied_log").unwrap(); //todo sohan - unwrap
        match v {
            None => return 0,
            Some(v) => {
                let v = v.as_ref();
                let array: [u8; 8] = v.try_into().unwrap();
                let number = u64::from_be_bytes(array);
                number
            }
        }
    }

    pub fn update_last_applied_log(&self, index: u64) {
        let b = u64::to_be_bytes(index).to_vec();
        let _ = self.meta.insert("last_applied_log", IVec::from(b)); // todo: sohan handle error
    }

    #[tracing::instrument(level = "trace", skip(self))]
    pub(crate) fn apply_entry(&self, request: &ClientRequest) {
        let v = serde_json::to_vec(request).unwrap();// todo: sohan handle unwrap
        self.db.put(&request.client.as_bytes(), v).unwrap();// todo: sohan handle unwrap
        self.db.flush().unwrap(); // todo: remove this
        trace!("persisted entry to db");
    }

    #[tracing::instrument(level = "trace", skip(self))]
    pub fn get(&self, k: &[u8]) -> Option<Vec<u8>> {
        let result = self.db.get(k).unwrap();// todo: sohan handle unwrap
        result
    }

    pub(crate) fn take_checkpoint(&self) -> Result<String> {
        let now = chrono::Utc::now().timestamp_millis();
        let base_path = format!("/Users/sohi/personal-git-v2/raft-learning/async-raft/persist-db-dir/{}/sm/checkpoints", self.node);
        let path = format!("{}/{}", base_path, now);
        let cp = Checkpoint::new(&self.db)?;
        cp.create_checkpoint(&path)?;
        Ok(path)
    }

    pub fn restore_from_checkpoint_local(&self, path: &str) {
        let options = CopyOptions::new(); //Initialize default values for CopyOptions
        // options.mirror_copy = true; // To mirror copy the whole structure of the source directory

        // copy source/dir1 to target/dir1
        fs_extra::dir::copy(path, &self.checkpoint_base_path, &options).unwrap();


        // todo: optimize this
        for item in self.db.iterator(IteratorMode::Start) {
            let (k, _) = item.unwrap();
            self.db.delete(k).unwrap();
        }
        self.db.compact_range(None::<&[u8]>, None::<&[u8]>);


        // add into state machine
        let path1 = Path::new(path);
        let x = path1.file_stem().unwrap();
        let buf = Path::new(&self.checkpoint_base_path).join(x);
        let read_path = buf.as_path();

        println!("going to open check point dir {:?}", read_path);
        let checkpoint = rocksdb::DB::open_default(read_path).unwrap();
        checkpoint.iterator(IteratorMode::Start).for_each(|item| {
            let (k, v) = item.unwrap();
            self.db.put(k, v).unwrap();
        })
    }
}

#[test]
fn test1() {
    let aa = "/Users/sohi/personal-git-v2/raft-learning/async-raft/persist-db-dir/0/sm/checkpoints";
    let path = "/Users/sohi/personal-git-v2/raft-learning/async-raft/persist-db-dir/0/sm/checkpoints/1660122822240";
    let path1 = Path::new(path);
    let option = path1.file_stem().unwrap();

    let buf = Path::new(aa).join(option);
    let out = buf.as_path();


    let read_path = format!("{}/{:?}", aa, option);
    println!("going to open check point dir {:?}", out);

    println!("op {:?}", option);
    println!("path {:?}", path1);
}