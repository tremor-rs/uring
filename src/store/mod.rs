use crate::{ExampleNodeId, ExampleTypeConfig};
use async_std::sync::RwLock;
use openraft::{
    async_trait::async_trait,
    storage::{LogState, Snapshot},
    AnyError, EffectiveMembership, Entry, EntryPayload, ErrorSubject, ErrorVerb, LogId,
    RaftLogReader, RaftSnapshotBuilder, RaftStorage, SnapshotMeta, StateMachineChanges,
    StorageError, StorageIOError, Vote,
};
use rocksdb::{ColumnFamilyDescriptor, Options, DB};
use serde::{Deserialize, Serialize};
use std::{
    collections::BTreeMap,
    fmt::Debug,
    io::Cursor,
    ops::RangeBounds,
    sync::{Arc, Mutex},
};

/**
 * Here you will set the types of request that will interact with the raft nodes.
 * For example the `Set` will be used to write data (key and value) to the raft database.
 * The `AddNode` will append a new node to the current existing shared list of nodes.
 * You will want to add any request that can write data in all nodes here.
 */
#[derive(Serialize, Deserialize, Debug, Clone)]
pub enum ExampleRequest {
    Set { key: String, value: String },
}

/**
 * Here you will defined what type of answer you expect from reading the data of a node.
 * In this example it will return a optional value from a given key in
 * the `ExampleRequest.Set`.
 *
 * TODO: SHould we explain how to create multiple `AppDataResponse`?
 *
 */
#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct ExampleResponse {
    pub value: Option<String>,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct ExampleSnapshot {
    pub meta: SnapshotMeta<ExampleNodeId>,

    /// The data of the state machine at the time of this snapshot.
    pub data: Vec<u8>,
}

/**
 * Here defines a state machine of the raft, this state represents a copy of the data
 * between each node. Note that we are using `serde` to serialize the `data`, which has
 * a implementation to be serialized. Note that for this test we set both the key and
 * value as String, but you could set any type of value that has the serialization impl.
 */
#[derive(Serialize, Deserialize, Debug, Default, Clone)]
pub struct ExampleStateMachine {
    pub last_applied_log: Option<LogId<ExampleNodeId>>,

    // TODO: it should not be Option.
    pub last_membership: EffectiveMembership<ExampleNodeId>,

    /// Application data.
    pub data: BTreeMap<String, String>,
}

#[derive(Debug)]
pub struct ExampleStore {
    db: rocksdb::DB,

    /// The Raft log.
    log: RwLock<BTreeMap<u64, Entry<ExampleTypeConfig>>>,

    /// The Raft state machine.
    pub state_machine: RwLock<ExampleStateMachine>,

    snapshot_idx: Arc<Mutex<u64>>,

    current_snapshot: RwLock<Option<ExampleSnapshot>>,
}

#[async_trait]
impl RaftLogReader<ExampleTypeConfig> for Arc<ExampleStore> {
    async fn get_log_state(
        &mut self,
    ) -> Result<LogState<ExampleTypeConfig>, StorageError<ExampleNodeId>> {
        let log = self.log.read().await;
        let last = log.iter().rev().next().map(|(_, ent)| ent.log_id);

        let last_purged = self
            .db
            .get_cf(self.db.cf_handle("store").unwrap(), b"last_purged_log_id")
            .unwrap()
            .and_then(|v| serde_json::from_slice(&v).ok());

        let last = match last {
            None => last_purged,
            Some(x) => Some(x),
        };

        Ok(LogState {
            last_purged_log_id: last_purged,
            last_log_id: last,
        })
    }

    async fn try_get_log_entries<RB: RangeBounds<u64> + Clone + Debug + Send + Sync>(
        &mut self,
        range: RB,
    ) -> Result<Vec<Entry<ExampleTypeConfig>>, StorageError<ExampleNodeId>> {
        let log = self.log.read().await;
        let response = log
            .range(range.clone())
            .map(|(_, val)| val.clone())
            .collect::<Vec<_>>();
        Ok(response)
    }
}

#[async_trait]
impl RaftSnapshotBuilder<ExampleTypeConfig, Cursor<Vec<u8>>> for Arc<ExampleStore> {
    #[tracing::instrument(level = "trace", skip(self))]
    async fn build_snapshot(
        &mut self,
    ) -> Result<Snapshot<ExampleNodeId, Cursor<Vec<u8>>>, StorageError<ExampleNodeId>> {
        let data;
        let last_applied_log;
        let last_membership;

        {
            // Serialize the data of the state machine.
            let state_machine = self.state_machine.read().await;
            data = serde_json::to_vec(&*state_machine).map_err(|e| {
                StorageIOError::new(
                    ErrorSubject::StateMachine,
                    ErrorVerb::Read,
                    AnyError::new(&e),
                )
            })?;

            last_applied_log = state_machine.last_applied_log;
            last_membership = state_machine.last_membership.clone();
        }

        let last_applied_log = match last_applied_log {
            None => {
                panic!("can not compact empty state machine");
            }
            Some(x) => x,
        };

        let snapshot_idx = {
            let mut l = self.snapshot_idx.lock().unwrap();
            *l += 1;
            *l
        };

        let snapshot_id = format!(
            "{}-{}-{}",
            last_applied_log.leader_id, last_applied_log.index, snapshot_idx
        );

        let meta = SnapshotMeta {
            last_log_id: last_applied_log,
            last_membership,
            snapshot_id,
        };

        let snapshot = ExampleSnapshot {
            meta: meta.clone(),
            data: data.clone(),
        };

        {
            let mut current_snapshot = self.current_snapshot.write().await;
            *current_snapshot = Some(snapshot);
        }

        Ok(Snapshot {
            meta,
            snapshot: Box::new(Cursor::new(data)),
        })
    }
}

#[async_trait]
impl RaftStorage<ExampleTypeConfig> for Arc<ExampleStore> {
    type SnapshotData = Cursor<Vec<u8>>;
    type LogReader = Self;
    type SnapshotBuilder = Self;

    #[tracing::instrument(level = "trace", skip(self))]
    async fn save_vote(
        &mut self,
        vote: &Vote<ExampleNodeId>,
    ) -> Result<(), StorageError<ExampleNodeId>> {
        self.db
            .put_cf(
                self.db.cf_handle("store").unwrap(),
                b"vote",
                serde_json::to_vec(vote).unwrap(),
            )
            .unwrap();
        Ok(())
    }

    async fn read_vote(
        &mut self,
    ) -> Result<Option<Vote<ExampleNodeId>>, StorageError<ExampleNodeId>> {
        let vote = self
            .db
            .get_cf(self.db.cf_handle("store").unwrap(), b"vote")
            .unwrap()
            .and_then(|v| serde_json::from_slice(&v).ok());
        Ok(vote)
    }

    #[tracing::instrument(level = "trace", skip(self, entries))]
    async fn append_to_log(
        &mut self,
        entries: &[&Entry<ExampleTypeConfig>],
    ) -> Result<(), StorageError<ExampleNodeId>> {
        let mut log = self.log.write().await;
        for entry in entries {
            log.insert(entry.log_id.index, (*entry).clone());
        }
        Ok(())
    }

    #[tracing::instrument(level = "debug", skip(self))]
    async fn delete_conflict_logs_since(
        &mut self,
        log_id: LogId<ExampleNodeId>,
    ) -> Result<(), StorageError<ExampleNodeId>> {
        tracing::debug!("delete_log: [{:?}, +oo)", log_id);

        let mut log = self.log.write().await;
        let keys = log
            .range(log_id.index..)
            .map(|(k, _v)| *k)
            .collect::<Vec<_>>();
        for key in keys {
            log.remove(&key);
        }

        Ok(())
    }

    #[tracing::instrument(level = "debug", skip(self))]
    async fn purge_logs_upto(
        &mut self,
        log_id: LogId<ExampleNodeId>,
    ) -> Result<(), StorageError<ExampleNodeId>> {
        tracing::debug!("delete_log: [{:?}, +oo)", log_id);

        {
            self.db
                .put_cf(
                    self.db.cf_handle("store").unwrap(),
                    b"last_purged_log_id",
                    serde_json::to_vec(&log_id).unwrap(),
                )
                .unwrap();
            // assert!(*ld <= Some(log_id));
        }

        {
            let mut log = self.log.write().await;

            let keys = log
                .range(..=log_id.index)
                .map(|(k, _v)| *k)
                .collect::<Vec<_>>();
            for key in keys {
                log.remove(&key);
            }
        }

        Ok(())
    }

    async fn last_applied_state(
        &mut self,
    ) -> Result<
        (
            Option<LogId<ExampleNodeId>>,
            EffectiveMembership<ExampleNodeId>,
        ),
        StorageError<ExampleNodeId>,
    > {
        let state_machine = self.state_machine.read().await;
        Ok((
            state_machine.last_applied_log,
            state_machine.last_membership.clone(),
        ))
    }

    #[tracing::instrument(level = "trace", skip(self, entries))]
    async fn apply_to_state_machine(
        &mut self,
        entries: &[&Entry<ExampleTypeConfig>],
    ) -> Result<Vec<ExampleResponse>, StorageError<ExampleNodeId>> {
        let mut res = Vec::with_capacity(entries.len());

        let mut sm = self.state_machine.write().await;

        for entry in entries {
            tracing::debug!(%entry.log_id, "replicate to sm");

            sm.last_applied_log = Some(entry.log_id);

            match entry.payload {
                EntryPayload::Blank => res.push(ExampleResponse { value: None }),
                EntryPayload::Normal(ref req) => match req {
                    ExampleRequest::Set { key, value } => {
                        sm.data.insert(key.clone(), value.clone());
                        res.push(ExampleResponse {
                            value: Some(value.clone()),
                        })
                    }
                },
                EntryPayload::Membership(ref mem) => {
                    sm.last_membership = EffectiveMembership::new(Some(entry.log_id), mem.clone());
                    res.push(ExampleResponse { value: None })
                }
            };
        }
        Ok(res)
    }

    #[tracing::instrument(level = "trace", skip(self))]
    async fn begin_receiving_snapshot(
        &mut self,
    ) -> Result<Box<Self::SnapshotData>, StorageError<ExampleNodeId>> {
        Ok(Box::new(Cursor::new(Vec::new())))
    }

    #[tracing::instrument(level = "trace", skip(self, snapshot))]
    async fn install_snapshot(
        &mut self,
        meta: &SnapshotMeta<ExampleNodeId>,
        snapshot: Box<Self::SnapshotData>,
    ) -> Result<StateMachineChanges<ExampleTypeConfig>, StorageError<ExampleNodeId>> {
        tracing::info!(
            { snapshot_size = snapshot.get_ref().len() },
            "decoding snapshot for installation"
        );

        let new_snapshot = ExampleSnapshot {
            meta: meta.clone(),
            data: snapshot.into_inner(),
        };

        // Update the state machine.
        {
            let updated_state_machine: ExampleStateMachine =
                serde_json::from_slice(&new_snapshot.data).map_err(|e| {
                    StorageIOError::new(
                        ErrorSubject::Snapshot(new_snapshot.meta.clone()),
                        ErrorVerb::Read,
                        AnyError::new(&e),
                    )
                })?;
            let mut state_machine = self.state_machine.write().await;
            *state_machine = updated_state_machine;
        }

        // Update current snapshot.
        let mut current_snapshot = self.current_snapshot.write().await;
        *current_snapshot = Some(new_snapshot);
        Ok(StateMachineChanges {
            last_applied: meta.last_log_id,
            is_snapshot: true,
        })
    }

    #[tracing::instrument(level = "trace", skip(self))]
    async fn get_current_snapshot(
        &mut self,
    ) -> Result<Option<Snapshot<ExampleNodeId, Self::SnapshotData>>, StorageError<ExampleNodeId>>
    {
        match &*self.current_snapshot.read().await {
            Some(snapshot) => {
                let data = snapshot.data.clone();
                Ok(Some(Snapshot {
                    meta: snapshot.meta.clone(),
                    snapshot: Box::new(Cursor::new(data)),
                }))
            }
            None => Ok(None),
        }
    }

    async fn get_log_reader(&mut self) -> Self::LogReader {
        self.clone()
    }

    async fn get_snapshot_builder(&mut self) -> Self::SnapshotBuilder {
        self.clone()
    }
}
impl ExampleStore {
    pub(crate) fn new(rcp_addr: &str) -> ExampleStore {
        let mut db_opts = Options::default();
        db_opts.create_missing_column_families(true);
        db_opts.create_if_missing(true);
        let db_path = format!("{rcp_addr}.db");

        let store = ColumnFamilyDescriptor::new("store", Options::default());
        let state_machine = ColumnFamilyDescriptor::new("state_machine", Options::default());
        let data = ColumnFamilyDescriptor::new("data", Options::default());

        let db =
            DB::open_cf_descriptors(&db_opts, db_path, vec![store, state_machine, data]).unwrap();

        let current_snapshot = db
            .get_cf(db.cf_handle("store").unwrap(), b"current_snapshot")
            .unwrap()
            .and_then(|v| serde_json::from_slice(&v).ok());
        let snapshot_idx = db
            .get_cf(db.cf_handle("store").unwrap(), b"snapshot_idx")
            .unwrap()
            .and_then(|v| serde_json::from_slice(&v).ok())
            .unwrap_or_default();

        let last_applied_log = db
            .get_cf(db.cf_handle("state_machine").unwrap(), b"last_applied_log")
            .unwrap()
            .and_then(|v| serde_json::from_slice(&v).ok());

        let last_membership = db
            .get_cf(db.cf_handle("state_machine").unwrap(), b"last_membership")
            .unwrap()
            .and_then(|v| serde_json::from_slice(&v).ok())
            .unwrap_or_default();

        let state_machine = ExampleStateMachine {
            last_applied_log,
            last_membership,
            data: Default::default(),
        };

        ExampleStore {
            db,
            log: Default::default(),
            state_machine: RwLock::new(state_machine),
            snapshot_idx: Arc::new(Mutex::new(snapshot_idx)),
            current_snapshot: RwLock::new(current_snapshot),
        }
    }
}
