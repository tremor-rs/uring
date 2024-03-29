// Copyright 2018-2020, Wayfair GmbH
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

// inspired by https://github.com/LucioFranco/kv/blob/master/src/storage.rs

use crate::NodeId;
use async_trait::async_trait;
use bytes::buf::BufMut;
use protobuf::Message;
use raft::prelude::*;
pub use raft::storage::Storage as ReadStorage;
use raft::{Error as RaftError, Result as RaftResult, StorageError};
use serde_derive::{Deserialize, Serialize};
use std::borrow::Borrow;
use std::io::Write;

#[async_trait]
pub trait Storage: WriteStorage + ReadStorage {
    async fn new_with_conf_state(id: NodeId, state: ConfState) -> Self;
    async fn new(id: NodeId) -> Self;
}

/// The missing storage trait from raft-rs ...
#[async_trait]
pub trait WriteStorage {
    async fn append(&self, entries: &[Entry]) -> RaftResult<()>;
    async fn apply_snapshot(&mut self, snapshot: Snapshot) -> RaftResult<()>;
    async fn set_conf_state(&mut self, cs: ConfState) -> RaftResult<()>;
    async fn set_hard_state(&mut self, commit: u64, term: u64) -> RaftResult<()>;
    async fn get(&self, scope: u16, key: &[u8]) -> Option<Vec<u8>>;
    async fn put(&self, keyscope: u16, key: &[u8], value: &[u8]);
    async fn cas(
        &self,
        keyscope: u16,
        key: &[u8],
        check_value: Option<&[u8]>,
        store_value: &[u8],
    ) -> Option<Option<Vec<u8>>>;
    async fn delete(&self, scope: u16, key: &[u8]) -> Option<Vec<u8>>;
}

use rocksdb::{Direction, IteratorMode, WriteBatch, DB};

const CONF_STATE: &'static [u8; 16] = b"\0\0\0\0\0\0\0ConfState";
const HARD_STATE: &'static [u8; 16] = b"\0\0\0\0\0\0\0HardState";

//#[derive(Default)]
pub struct URRocksStorage {
    backend: DB,
    conf_state: Option<ConfState>,
}

pub struct NullStorage {}

#[async_trait]
impl Storage for NullStorage {
    async fn new_with_conf_state(_id: NodeId, _state: ConfState) -> Self {
        NullStorage {}
    }

    async fn new(_id: NodeId) -> Self {
        NullStorage {}
    }
}

#[async_trait]
impl WriteStorage for NullStorage {
    async fn append(&self, _entries: &[Entry]) -> RaftResult<()> {
        unimplemented!()
    }
    async fn apply_snapshot(&mut self, _snapshot: Snapshot) -> RaftResult<()> {
        unimplemented!()
    }
    async fn set_conf_state(&mut self, _cs: ConfState) -> RaftResult<()> {
        unimplemented!()
    }
    async fn set_hard_state(&mut self, _commit: u64, _term: u64) -> RaftResult<()> {
        unimplemented!()
    }
    async fn get(&self, _scope: u16, _key: &[u8]) -> Option<Vec<u8>> {
        unimplemented!()
    }
    async fn put(&self, _keyscope: u16, _key: &[u8], _value: &[u8]) {
        unimplemented!()
    }
    async fn cas(
        &self,
        _keyscope: u16,
        _key: &[u8],
        _check_value: Option<&[u8]>,
        _store_value: &[u8],
    ) -> Option<Option<Vec<u8>>> {
        unimplemented!()
    }
    async fn delete(&self, _scope: u16, _key: &[u8]) -> Option<Vec<u8>> {
        unimplemented!()
    }
}

pub type NullStorageError = raft::Error;

impl ReadStorage for NullStorage {
    fn initial_state(&self) -> Result<RaftState, NullStorageError> {
        Ok(RaftState::default())
    }
    fn entries(
        &self,
        _low: u64,
        _high: u64,
        _max_size: impl Into<Option<u64>>,
    ) -> Result<Vec<Entry>, NullStorageError> {
        unimplemented!()
    }
    fn term(&self, _idx: u64) -> Result<u64, NullStorageError> {
        Ok(1)
    }
    fn first_index(&self) -> Result<u64, NullStorageError> {
        Ok(1)
    }
    fn last_index(&self) -> Result<u64, NullStorageError> {
        Ok(1)
    }
    fn snapshot(&self, _request_index: u64) -> Result<Snapshot, NullStorageError> {
        unimplemented!()
    }
}

unsafe impl Send for URRocksStorage {}

#[async_trait]
impl Storage for URRocksStorage {
    async fn new_with_conf_state(id: NodeId, state: ConfState) -> Self {
        let mut db = Self::new(id).await;

        db.set_conf_state(state).await.unwrap();
        db.set_hard_state(1, 1).await.unwrap();
        db
    }
    async fn new(id: NodeId) -> Self {
        let backend = DB::open_default(&format!("raft-rocks-{}", id.0)).unwrap();
        URRocksStorage {
            backend,
            conf_state: None,
        }
    }
}

#[derive(Serialize, Deserialize, Clone, Debug)]
struct HandoffKV {
    key: String,
    value: String,
}
impl URRocksStorage {
    fn get_hard_state(&self) -> HardState {
        let mut hs = HardState::new();
        if let Ok(Some(data)) = self.backend.get(&HARD_STATE) {
            hs.merge_from_bytes(&data).unwrap();
        };
        hs
    }
    fn get_conf_state(&self) -> ConfState {
        let mut cs = ConfState::new();
        if let Ok(Some(data)) = self.backend.get(&CONF_STATE) {
            cs.merge_from_bytes(&data).unwrap();
        };
        cs
    }
    fn clear_log(&self) {
        self.clear_log_to(u64::max_value());
    }

    fn clear_log_to(&self, before: u64) {
        let before = make_log_key(before);
        self.backend
            .iterator(IteratorMode::From(&LOW_INDEX, Direction::Forward))
            .take_while(|(k, _)| {
                let k: &[u8] = k.borrow();
                k <= &HIGH_INDEX[..]
            })
            .take_while(|(k, _)| {
                let k: &[u8] = k.borrow();
                k <= &before[..]
            })
            .for_each(|(k, _)| self.backend.delete(&k).unwrap());
    }

    fn clear_data(&self) {
        self.backend
            .iterator(IteratorMode::From(&LOW_DATA, Direction::Forward))
            .take_while(|(k, _)| {
                let k: &[u8] = k.borrow();
                k <= &HIGH_DATA[..]
            })
            .for_each(|(k, _)| self.backend.delete(&k).unwrap());
    }

    pub fn data_snapshot(&self) -> Vec<u8> {
        self.backend
            .iterator(IteratorMode::From(&LOW_DATA, Direction::Forward))
            .take_while(|(k, _)| {
                let k: &[u8] = k.borrow();
                k <= &HIGH_DATA[..]
            })
            .map(|(k, v)| {
                serde_json::to_string(&HandoffKV {
                    key: base64::encode(&k),
                    value: base64::encode(&v),
                })
                .unwrap()
            })
            .collect::<Vec<String>>()
            .join("\n")
            .into_bytes()
    }

    pub fn apply_data_snapshot(&self, data: Vec<u8>) {
        self.clear_data();

        for kv in data.split(|c| *c == b'\n') {
            if let Ok(kv) = serde_json::from_slice::<HandoffKV>(&kv) {
                let k = base64::decode(&kv.key).unwrap();
                let v = base64::decode(&kv.value).unwrap();
                self.backend.put(&k, &v).unwrap();
            }
        }
    }
}

#[async_trait]
impl WriteStorage for URRocksStorage {
    async fn get(&self, scope: u16, key: &[u8]) -> Option<Vec<u8>> {
        let key = make_data_key(scope, key);
        self.backend.get(key).unwrap().map(|v| v.to_vec())
    }
    async fn put(&self, scope: u16, key: &[u8], value: &[u8]) {
        let key = make_data_key(scope, key);
        self.backend.put(key, value).unwrap();
    }
    async fn cas(
        &self,
        scope: u16,
        key: &[u8],
        check_value: Option<&[u8]>,
        store_value: &[u8],
    ) -> Option<Option<Vec<u8>>> {
        let value = self.get(scope, key).await;
        if value.as_ref().map(|v| v.as_slice()) == check_value {
            self.put(scope, key, store_value).await;
            None
        } else {
            Some(value)
        }
    }
    async fn delete(&self, scope: u16, key: &[u8]) -> Option<Vec<u8>> {
        match self.get(scope, key).await {
            None => None,
            Some(v) => {
                let v = v.clone();
                let key = make_data_key(scope, key);
                self.backend.delete(key).unwrap();
                Some(v)
            }
        }
    }
    async fn apply_snapshot(&mut self, mut snapshot: Snapshot) -> RaftResult<()> {
        let mut meta = snapshot.take_metadata();
        self.apply_data_snapshot(snapshot.take_data().to_vec());
        let term = meta.term;
        let index = meta.index;

        let first_index = self.first_index().unwrap();
        // Make sure the snapshot is not prior to our first log
        if first_index > index {
            return Err(RaftError::Store(StorageError::SnapshotOutOfDate));
        }

        self.set_hard_state(index, term).await?;
        self.set_conf_state(meta.take_conf_state()).await?;
        // From Mem node do we only want to clear up to index?
        self.clear_log();
        Ok(())
    }

    async fn append(&self, entries: &[Entry]) -> RaftResult<()> {
        if entries.is_empty() {
            return Ok(());
        }
        let mut batch = WriteBatch::default();
        for entry in entries {
            let key = make_log_key(entry.index);
            let data = entry.write_to_bytes()?;
            batch.put(&key, &data);
        }
        self.backend.write(batch).unwrap();
        self.backend.flush().unwrap();

        Ok(())
    }

    async fn set_conf_state(&mut self, cs: ConfState) -> RaftResult<()> {
        self.conf_state = Some(cs.clone());

        let data = cs.write_to_bytes()?;
        self.backend.put(&CONF_STATE, &data).unwrap();
        self.backend.flush().unwrap();
        Ok(())
    }

    async fn set_hard_state(&mut self, commit: u64, term: u64) -> RaftResult<()> {
        let mut hs = HardState::new();
        hs.commit = commit;
        hs.term = term;
        let data = hs.write_to_bytes()?;
        self.backend.put(&HARD_STATE, &data).unwrap();
        self.clear_log_to(commit);
        self.backend.flush().unwrap();
        Ok(())
    }
}

impl ReadStorage for URRocksStorage {
    fn initial_state(&self) -> RaftResult<RaftState> {
        let hard_state = self.get_hard_state();
        if hard_state == HardState::default() {
            return Ok(RaftState::new(hard_state, ConfState::default()));
        };
        let conf_state = self.get_conf_state();
        Ok(RaftState::new(hard_state, conf_state))
    }

    fn entries(
        &self,
        low: u64,
        high: u64,
        max_size: impl Into<Option<u64>>,
    ) -> RaftResult<Vec<Entry>> {
        use std::cmp::max;
        let first_index = self.first_index().unwrap();
        if low < first_index {
            return Err(RaftError::Store(StorageError::Compacted));
        }
        let last_index = self.last_index().unwrap() + 1;
        if high > last_index {
            panic!("index out of bound (last: {}, high: {})", last_index, high);
        }

        let low_key = make_log_key(low);
        let iter = self
            .backend
            .iterator(IteratorMode::From(&low_key, Direction::Forward))
            .take_while(|(k, _)| {
                let k: &[u8] = k.borrow();
                k <= &HIGH_INDEX[..]
            })
            .map(|(_, v)| {
                let mut e = Entry::new();
                e.merge_from_bytes(&v).unwrap();
                e
            })
            .take_while(|e| e.index < high);

        if let Some(max_size) = max_size.into() {
            //FIXME use max_size as size not count
            Ok(iter.take(max(max_size, 1) as usize).collect())
        } else {
            Ok(iter.collect())
        }
    }

    fn term(&self, idx: u64) -> RaftResult<u64> {
        let first_index = self.first_index().unwrap();

        let hs = self.get_hard_state();

        if idx == hs.commit {
            return Ok(hs.term);
        }

        if idx < first_index {
            return Err(RaftError::Store(StorageError::Compacted));
        }

        let key = make_log_key(idx);
        self.backend
            .get(&key)
            .unwrap()
            .map(|v| {
                let mut e = Entry::new();
                e.merge_from_bytes(&v).unwrap();
                e.term
            })
            .ok_or(RaftError::Store(StorageError::Unavailable))
    }

    fn first_index(&self) -> RaftResult<u64> {
        let first = self
            .backend
            .iterator(IteratorMode::From(&LOW_INDEX, Direction::Forward))
            .take_while(|(k, _)| {
                let k: &[u8] = k.borrow();
                k <= &HIGH_INDEX[..]
            })
            .next()
            .map(|(_, v)| {
                let mut e = Entry::new();
                e.merge_from_bytes(&v).unwrap();
                e.index
            })
            .unwrap_or_else(|| self.get_hard_state().commit + 1);
        Ok(first)
    }

    fn last_index(&self) -> RaftResult<u64> {
        let last = self
            .backend
            .iterator(IteratorMode::From(&HIGH_INDEX, Direction::Reverse))
            .take_while(|(k, _)| {
                let k: &[u8] = k.borrow();
                k >= &LOW_INDEX[..]
            })
            .next()
            .map(|(_k, v)| {
                let mut e = Entry::new();
                e.merge_from_bytes(&v).unwrap();
                e.index
            })
            .unwrap_or_else(|| self.get_hard_state().commit);
        Ok(last)
    }

    fn snapshot(&self, request_index: u64) -> RaftResult<Snapshot> {
        let mut snapshot = Snapshot::default();
        let hs = self.get_hard_state();
        // Use the latest applied_idx to construct the snapshot.
        let applied_idx = hs.commit;
        let term = hs.term;
        snapshot.set_data(self.data_snapshot().into());
        let meta = snapshot.mut_metadata();
        meta.index = applied_idx;
        meta.term = term;

        meta.set_conf_state(self.get_conf_state().clone());
        // https://github.com/tikv/raft-rs/blob/3f5171a9f833679cb40437ca47031eb0e9f4aa3e/src/storage.rs#L494
        if meta.index < request_index {
            meta.index = request_index;
        }
        Ok(snapshot)
    }
}

// const CONF_PREFIX: u8 = 0;
const RAFT_PREFIX: u8 = 1;
const DATA_PREFIX: u8 = 255;
// https://github.com/LucioFranco/kv/blob/417dbb7f969bd311e1e9ed91ab9980a1cae25f56/src/storage.rs#L152
const HIGH_INDEX: [u8; 16] = [
    RAFT_PREFIX,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    255,
    255,
    255,
    255,
    255,
    255,
    255,
    255,
];
const LOW_INDEX: [u8; 16] = [RAFT_PREFIX, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0];

const LOW_DATA: [u8; 8] = [DATA_PREFIX, 0, 0, 0, 0, 0, 0, 0];
const HIGH_DATA: [u8; 8] = [DATA_PREFIX, 0, 0, 0, 255, 255, 255, 255];
fn make_log_key(idx: u64) -> Vec<u8> {
    let mut key: Vec<u8> = vec![0; 16];

    {
        // let mut key = Cursor::new(&mut key[..]);
        key.put_u64_le(RAFT_PREFIX as u64);
        key.put_u64_le(idx);
    }

    key
}

fn make_data_key(prefix: u16, key_s: &[u8]) -> Vec<u8> {
    let mut key = vec![0; 8 + key_s.len()];

    {
        key.put_u32_le(DATA_PREFIX as u32);
        key.put_u32_le(prefix as u32);
        key.write_all(key_s).unwrap();
    }

    key
}
