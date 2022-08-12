use std::collections::BTreeMap;
use std::io::Read;

use commitlog::CommitLog;
use rocksdb::{DB, IteratorMode, Options, WriteBatch};
use rocksdb::Direction::Forward;
use tokio::sync::RwLock;
use tracing::trace;

use async_raft::NodeId;
use async_raft::raft::{Entry, EntryPayload, MembershipConfig};

use crate::ClientRequest;

const ERR_INCONSISTENT_LOG: &str = "a query was received which was expecting data to be in place which does not exist in the log";


pub(crate) struct RaftLogEntryMeta {
    pub log_index: u64,
    pub log_term: u64,
}

pub struct PersistRaftLog {
    node_id: NodeId,
    // inner: RwLock<BTreeMap<u64, Entry<ClientRequest>>>, // todo: change with commit log(does not support reverse iteration) or rocks db
    //inner2: CommitLog,
    inner: rocksdb::DB,
}

impl PersistRaftLog {
    pub(crate) async fn finalize_snapshot(&self, index: u64, term: u64, delete_through: Option<u64>, id: String) {
        let membership_config;
        {
            membership_config = self.get_membership_config_till(index).await
        }

        /* let mut guard = self.inner.write().await;
         let entry = Entry::new_snapshot_pointer(index, term, id, membership_config);
         match &delete_through {
             Some(through) => {
                 *guard = guard.split_off(through);
             }
             None => guard.clear(),
         }
         guard.insert(index, entry);*/

        let entry: Entry<ClientRequest> = Entry::new_snapshot_pointer(index, term, id, membership_config);
        match &delete_through {
            Some(through) => {
                self.delete_range(0, *through);
            }
            None => {
                self.delete_logs_from(0, None).await;
            }
        }


        self.inner.put(index.to_be_bytes(), serde_json::to_vec(&entry).unwrap()).unwrap();
    }
}


impl PersistRaftLog {
    pub fn new(node_id: NodeId) -> Self {
        let path = format!("/Users/sohi/personal-git-v2/raft-learning/async-raft/persist-db-dir/{}/raft_log", node_id);
        let mut opts = Options::default();
        opts.create_if_missing(true);
        opts.set_disable_auto_compactions(false);

        let db = DB::open(&opts, path).unwrap();
        PersistRaftLog { inner: db, node_id }
    }


    pub(crate) async fn get_membership_config(&self) -> Option<MembershipConfig> {
        self.inner.iterator(IteratorMode::End).find_map(|entry| {
            let result = Self::build_entry_from_rocksdb_entry(entry);
            match result.payload {
                EntryPayload::ConfigChange(cfg) => Some(cfg.membership.clone()),
                EntryPayload::SnapshotPointer(snap) => Some(snap.membership.clone()),
                _ => None,
            }
        })


        /*  let guard = self.inner.read().await;
          guard.values().rev().find_map(|entry| match &entry.payload {
              EntryPayload::ConfigChange(cfg) => Some(cfg.membership.clone()),
              EntryPayload::SnapshotPointer(snap) => Some(snap.membership.clone()),
              _ => None,
          })*/
    }

    pub(crate) async fn get_membership_config_till(&self, last_applied_log: u64) -> MembershipConfig {
        let v = self.inner.iterator(IteratorMode::End)
            .map(|entry| Self::build_entry_from_rocksdb_entry(entry))
            .skip_while(|entry| entry.index > last_applied_log)
            .find_map(|entry| match &entry.payload {
                EntryPayload::ConfigChange(cfg) => Some(cfg.membership.clone()),
                _ => None, // Todo: sohan :: is this not a bug ?? what  if no cfg change entry founnd but snapshot pointer is found ?? this need to be checked
            })
            .unwrap_or_else(|| MembershipConfig::new_initial(self.node_id));
        v
    }

    #[tracing::instrument(level = "trace", skip(self))]
    pub(crate) async fn get_last_entry(&self) -> Option<RaftLogEntryMeta> {
        self.inner
            .iterator(IteratorMode::End)
            .next()
            .map(|e| Self::build_entry_from_rocksdb_entry(e))
            .map(|e| RaftLogEntryMeta { log_index: e.index, log_term: e.term })
        /*let guard = self.inner.read().await;
        guard.values().rev().next().map(|e| RaftLogEntryMeta { log_index: e.index, log_term: e.term })*/
        //None
    }

    #[tracing::instrument(level = "trace", skip(self))]
    pub(crate) async fn read_range(&self, start: u64, stop: u64) -> Vec<Entry<ClientRequest>> {
        /*    let guard = self.inner.read().await;
            let range = guard.range(start..stop).map(|e| e.1.clone());
            range.collect()*/

        let iter = self.inner.iterator(IteratorMode::From(&start.to_be_bytes(), Forward));
        iter.map_while(|i| {
            let e = Self::build_entry_from_rocksdb_entry(i);
            if e.index >= stop { None } else { Some(e) }
        }).collect()
    }

    /// stop is exclusive
    #[tracing::instrument(level = "trace", skip(self))]
    pub(crate) async fn delete_logs_from(&self, start: u64, stop: Option<u64>) {
        trace!("deleting logs from raft log");
        /* let mut guard = self.inner.write().await;
         if stop.is_none() {
             guard.split_off(&start);
             return;
         }*/


        /* for k in start..stop.unwrap() { // todo: stop exclusive ??
             guard.remove(&k);
         }*/

        if stop.is_some() {
            let stop = stop.unwrap(); //+ 1;
            self.delete_range(start, stop);
            return;
        }


        if let Some(e) = self.get_last_entry().await {
            let stop = e.log_index + 1;
            self.delete_range(start, stop);
        }
    }


    #[tracing::instrument(level = "trace", skip(self))]
    pub(crate) async fn truncate_before(&self, offset: u64) {
        trace!("truncate_before logs from raft log");
        /*   let mut guard = self.inner.write().await;
           *guard = guard.split_off(&offset);*/
        self.delete_range(0, offset);
    }

    #[tracing::instrument(level = "trace", skip(self))]
    pub(crate) async fn append_entry(&self, e: &Entry<ClientRequest>) {
        let k = e.index.to_be_bytes();
        let v = serde_json::to_vec(e).unwrap();
        self.inner.put(k, v).unwrap();
    }

    #[tracing::instrument(level = "trace", skip(self))]
    pub(crate) async fn append_entries(&self, entries: &[Entry<ClientRequest>]) {
        for e in entries {
            trace!("appending entry in raft log {e:?}");
            self.append_entry(e).await;
        }
    }

    #[tracing::instrument(level = "trace", skip(self))]
    pub(crate) async fn get_term(&self, index: u64) -> anyhow::Result<u64> {
        trace!("getting term for index");
        let term = self.inner
            .get(index.to_be_bytes())
            .map(|e| Self::build_entry_from_u8(e.unwrap().as_slice()))
            .map(|entry| entry.term)
            .map_err(|_| anyhow::anyhow!(ERR_INCONSISTENT_LOG))?;
        Ok(term)
    }

    ///including "begin_key" and excluding "end_key"
    fn delete_range(&self, begin_key: u64, end_key: u64) {
        let mut batch = rocksdb::WriteBatch::default();
        batch.delete_range(begin_key.to_be_bytes(), end_key.to_be_bytes());
    }

    fn build_entry_from_rocksdb_entry(entry: Result<(Box<[u8]>, Box<[u8]>), rocksdb::Error>) -> Entry<ClientRequest> {
        let (_index, entry) = entry.unwrap(); // todo: sohan handle unwrap
        Self::build_entry_from_u8(&entry)
    }

    fn build_entry_from_u8(entry: &[u8]) -> Entry<ClientRequest> {
        serde_json::from_slice::<Entry<ClientRequest>>(&entry).unwrap()
    }
}

