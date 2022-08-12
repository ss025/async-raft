#![allow(unused_imports)]
#![allow(dead_code)]

use std::io::Cursor;

use anyhow::Result;
use serde::{Deserialize, Serialize};
use thiserror::Error;
use tokio::sync::RwLock;
use tracing::{debug, trace};

use async_raft::{AppData, AppDataResponse, NodeId, RaftStorage};
use async_raft::async_trait::async_trait;
use async_raft::raft::{Entry, EntryPayload, MembershipConfig};
use async_raft::storage::{CurrentSnapshotData, HardState, InitialState};

use crate::hard_state::PersistedHardState;
use crate::persist_state_machine::PersistStateMachine;
use crate::raft_log::{PersistRaftLog, RaftLogEntryMeta};
use crate::snapshot::{PersistStoreSnapshot, PersistStoreSnapshotMeta};

mod persist_state_machine;
mod raft_log;
mod snapshot;
mod hard_state;

const ERR_INCONSISTENT_LOG: &str = "a query was received which was expecting data to be in place which does not exist in the log";

/// The application data request type which the `MemStore` works with.
///
/// Conceptually, for demo purposes, this represents an update to a client's status info,
/// returning the previously recorded status.
#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct ClientRequest {
    /// The ID of the client which has sent the request.
    pub client: String,
    /// The serial number of this request.
    pub serial: u64,
    /// A string describing the status of the client. For a real application, this should probably
    /// be an enum representing all of the various types of requests / operations which a client
    /// can perform.
    pub status: String,
}

impl AppData for ClientRequest {}

/// The application data response type which the `MemStore` works with.
#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct ClientResponse(Option<String>);

impl AppDataResponse for ClientResponse {}

/// Error used to trigger Raft shutdown from storage.
#[derive(Clone, Debug, Error)]
pub enum ShutdownError {
    #[error("unsafe storage error")]
    UnsafeStorageError,
}


/// persistent store implementation
pub struct PersistStore {
    node_id: NodeId,

    raft_log: PersistRaftLog,
    state_machine: PersistStateMachine,

    hard_state: PersistedHardState,
    /// The current snapshot.
    current_snapshot: RwLock<Option<PersistStoreSnapshot>>, // todo: sohan  do we need locking
}

impl PersistStore {
    pub fn new(node_id: NodeId) -> Self {
        Self {
            node_id,
            raft_log: PersistRaftLog::new(node_id),
            state_machine: PersistStateMachine::new(node_id),
            hard_state: PersistedHardState::new(node_id),
            current_snapshot: RwLock::new(Some(PersistStoreSnapshot::new(node_id))), //todo: sohan  this should be read from disk
        }
    }

    pub fn state_machine(&self) -> &PersistStateMachine {
        &self.state_machine
    }
}

#[async_trait]
impl RaftStorage<ClientRequest, ClientResponse> for PersistStore {
    type Snapshot = Cursor<Vec<u8>>;
    type ShutdownError = ShutdownError;

    #[tracing::instrument(level = "trace", skip(self))]
    async fn get_membership_config(&self) -> Result<MembershipConfig> {
        let cfg = self.raft_log.get_membership_config().await;
        Ok(match cfg {
            Some(cfg) => cfg,
            None => MembershipConfig::new_initial(self.node_id),
        })
    }

    #[tracing::instrument(level = "trace", skip(self))]
    async fn get_initial_state(&self) -> Result<InitialState> {
        let membership = self.get_membership_config().await?;
        let last_applied_log = self.state_machine.get_last_applied_log();

        let hs = self.hard_state.get();
        match hs {
            Some(hs) => {
                let last_entry = self.raft_log.get_last_entry().await.unwrap_or_else(|| RaftLogEntryMeta { log_index: 0, log_term: 0 });
                Ok(InitialState {
                    last_log_index: last_entry.log_index,
                    last_log_term: last_entry.log_term,
                    last_applied_log,
                    hard_state: hs.clone(),
                    membership,
                })
            }
            None => {
                let new = InitialState::new_initial(self.node_id);
                self.hard_state.save(&new.hard_state.clone())?;
                Ok(new)
            }
        }
    }

    #[tracing::instrument(level = "trace", skip(self))]
    async fn save_hard_state(&self, hs: &HardState) -> Result<()> {
        self.hard_state.save(hs)
    }

    /// The start value is inclusive in the search and the stop value is non-inclusive
    #[tracing::instrument(level = "trace", skip(self))]
    async fn get_log_entries(&self, start: u64, stop: u64) -> Result<Vec<Entry<ClientRequest>>> {
        trace!("reading log entries");
        if start > stop {
            tracing::error!("invalid request, start > stop");
            return Ok(vec![]);
        }
        let entries = self.raft_log.read_range(start, stop).await;
        Ok(entries)
    }

    #[tracing::instrument(level = "trace", skip(self))]
    async fn delete_logs_from(&self, start: u64, stop: Option<u64>) -> Result<()> {
        if stop.map(|stop| start > stop).unwrap_or(false) {
            tracing::error!("invalid request, start > stop");
            return Ok(());
        }

        self.raft_log.delete_logs_from(start, stop).await;
        Ok(())
    }

    #[tracing::instrument(level = "trace", skip(self))]
    async fn append_entry_to_log(&self, entry: &Entry<ClientRequest>) -> Result<()> {
        self.raft_log.append_entry(entry).await;
        Ok(())
    }

    #[tracing::instrument(level = "trace", skip(self))]
    async fn replicate_to_log(&self, entries: &[Entry<ClientRequest>]) -> Result<()> {
        self.raft_log.append_entries(entries).await;
        Ok(())
    }

    #[tracing::instrument(level = "trace", skip(self))]
    async fn apply_entry_to_state_machine(&self, index: &u64, data: &ClientRequest) -> Result<ClientResponse> {
        self.state_machine.update_last_applied_log(*index);
        self.state_machine.apply_entry(data);
        debug!("apply entry {data:?} to sm  with index {index}");
        Ok(ClientResponse(Some(format!("update client {data:?}"))))
    }

    #[tracing::instrument(level = "trace", skip(self))]
    async fn replicate_to_state_machine(&self, entries: &[(&u64, &ClientRequest)]) -> Result<()> {
        if let Some((index, _)) = entries.last() {
            self.state_machine.update_last_applied_log(**index);
        }

        for (index, e) in entries {
            debug!("replicate entry {e:?} to sm  with index {index}");
            self.state_machine.apply_entry(*e);
        }

        Ok(())
    }

    #[tracing::instrument(level = "trace", skip(self))]
    async fn do_log_compaction(&self) -> Result<CurrentSnapshotData<Self::Snapshot>> {
        trace!("starting log compaction");
        let last_applied_log = self.state_machine.get_last_applied_log();
        let membership_config = self.raft_log.get_membership_config_till(last_applied_log).await;

        // get last applied entry from raft log
        let last_applied_term = self.raft_log.get_term(last_applied_log).await?;
        trace!("last_applied_log {last_applied_log}, membership_config {membership_config:?}, last_applied_term {last_applied_term} ");
        // take rocksdb checkpoint
        let checkpoint_path = self.state_machine.take_checkpoint()?;

        let snapshot_bytes;

        {
            let current_snapshot = self.current_snapshot.write().await;
            // truncate the log
            self.raft_log.truncate_before(last_applied_log).await;
            // insert snapshot pointer entry in raft log
            let snapshot_pointer = Entry::new_snapshot_pointer(last_applied_log, last_applied_term, checkpoint_path.clone(), membership_config.clone());
            self.raft_log.append_entry(&snapshot_pointer).await;

            // update current snapshot
            let snapshot = PersistStoreSnapshotMeta {
                index: last_applied_log,
                term: last_applied_term,
                membership: membership_config.clone(),
                data: checkpoint_path.as_bytes().to_vec(),
            };

            snapshot_bytes = serde_json::to_vec(&snapshot)?;
            if let Some(ss) = current_snapshot.as_ref() {
                ss.save(&snapshot).await?;
            }
        }

        // return current snapshot data
        tracing::trace!({ snapshot_size = snapshot_bytes.len() }, "log compaction complete");
        Ok(CurrentSnapshotData {
            term: last_applied_term,
            index: last_applied_log,
            membership: membership_config,
            snapshot: Box::new(Cursor::new(snapshot_bytes)),
        })
    }

    #[tracing::instrument(level = "trace", skip(self))]
    async fn create_snapshot(&self) -> Result<(String, Box<Self::Snapshot>)> {
        Ok((String::from(""), Box::new(Cursor::new(Vec::new())))) // Snapshot IDs are insignificant to this storage engine.
    }

    #[tracing::instrument(level = "trace", skip(self))]
    async fn finalize_snapshot_installation(&self, index: u64, term: u64, delete_through: Option<u64>, id: String, snapshot: Box<Self::Snapshot>) -> Result<()> {
        //tracing::trace!({ snapshot_size = snapshot.get_ref().len() }, "decoding snapshot for installation");
        let new_snapshot: PersistStoreSnapshotMeta = serde_json::from_slice(snapshot.get_ref().as_slice())?;

        let path = String::from_utf8_lossy(&new_snapshot.data) ;
        trace!("json snapshot :: index = {}, term = {}, membership = {:?}  data = {}",
            new_snapshot.index, new_snapshot.term, new_snapshot.membership, path);


        // Update log.
        self.raft_log.finalize_snapshot(index, term, delete_through, id).await;

        // Update the state machine.
        {
            // for local
            // copy file to db directory
            self.state_machine.restore_from_checkpoint_local(&path);

            // todo: pending for remote - how to read bytes and applying to state machine
            // 1. read custom encoded bytes over network
            // 2. rsync sst file
            // 3. rocksdb backup and restore
            // 4. sst file insertion
            // 5. without using snapshot
        }

        // Update current snapshot.
        let current_snapshot = self.current_snapshot.write().await;
        current_snapshot.as_ref().map(|v| v.save(&new_snapshot));
        Ok(())
    }

    #[tracing::instrument(level = "trace", skip(self))]
    async fn get_current_snapshot(&self) -> Result<Option<CurrentSnapshotData<Self::Snapshot>>> {
        debug!("get current snapshot from storage ");
        match &*self.current_snapshot.read().await {
            Some(snapshot) => {
                match snapshot.get().await {
                    None => Ok(None),
                    Some(snapshot) => {
                        let reader = serde_json::to_vec(&snapshot)?;
                        Ok(Some(CurrentSnapshotData {
                            index: snapshot.index,
                            term: snapshot.term,
                            membership: snapshot.membership.clone(),
                            snapshot: Box::new(Cursor::new(reader)),
                        }))
                    }
                }
            }
            None => Ok(None),
        }
    }
}


#[cfg(test)]
mod tests {
    #[test]
    fn it_works() {
        let result = 2 + 2;
        assert_eq!(result, 4);
    }
}
