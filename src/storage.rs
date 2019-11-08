// Copyright 2018-2019, Wayfair GmbH
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

use protobuf::Message;
use raft::eraftpb::{ConfState, HardState};
use raft::eraftpb::{Entry, Snapshot};
use raft::storage::{MemStorage, RaftState, Storage as ReadStorage};
use raft::{Error as RaftError, Result as RaftResult, StorageError};
use std::borrow::Borrow;
/// The missing storage trait from raft-rs ...
pub trait WriteStorage: ReadStorage {
    fn append(&self, entries: &[Entry]) -> RaftResult<()>;
    fn apply_snapshot(&mut self, snapshot: Snapshot) -> RaftResult<()>;
    fn set_conf_state(
        &mut self,
        cs: ConfState,
        pending_membership_change: Option<(ConfState, u64)>,
    ) -> RaftResult<()>;
    fn set_hard_state(&mut self, commit: u64, term: u64) -> RaftResult<()>;
}

/*
pub trait Storable: ?Sized {
    fn encode(self) -> Bytes;
    fn decode(bytes: Bytes) -> Option<Self>;
}
*/
#[derive(Default)]
pub struct URMemStorage {
    backend: MemStorage,
}

#[allow(dead_code)]
impl URMemStorage {
    pub fn new_with_conf_state(_id: u64, state: ConfState) -> Self {
        Self {
            backend: MemStorage::new_with_conf_state(state),
        }
    }
    pub fn new(_id: u64) -> Self {
        Self {
            backend: MemStorage::new(),
        }
    }
}

impl WriteStorage for URMemStorage {
    fn apply_snapshot(&mut self, snapshot: Snapshot) -> RaftResult<()> {
        self.backend.wl().apply_snapshot(snapshot)
    }

    fn append(&self, entries: &[Entry]) -> RaftResult<()> {
        self.backend.wl().append(entries)
    }

    fn set_conf_state(
        &mut self,
        cs: ConfState,
        pending_membership_change: Option<(ConfState, u64)>,
    ) -> RaftResult<()> {
        self.backend.wl().set_conf_state(cs);
        Ok(())
    }

    fn set_hard_state(&mut self, commit: u64, term: u64) -> RaftResult<()> {
        let mut s = self.backend.wl();
        s.mut_hard_state().commit = commit;
        s.mut_hard_state().term = term;
        Ok(())
    }
}

impl ReadStorage for URMemStorage {
    fn first_index(&self) -> RaftResult<u64> {
        self.backend.first_index()
    }

    fn last_index(&self) -> RaftResult<u64> {
        self.backend.last_index()
    }

    fn term(&self, idx: u64) -> RaftResult<u64> {
        self.backend.term(idx)
    }

    fn initial_state(&self) -> RaftResult<RaftState> {
        self.backend.initial_state()
    }
    fn entries(
        &self,
        low: u64,
        high: u64,
        max_size: impl Into<Option<u64>>,
    ) -> RaftResult<Vec<Entry>> {
        self.backend.entries(low, high, max_size)
    }
    fn snapshot(&self, request_index: u64) -> RaftResult<Snapshot> {
        self.backend.snapshot(request_index)
    }
}

use rocksdb::{Direction, IteratorMode, WriteBatch, DB};

const CONF_STATE: &'static [u8; 16] = b"\0\0\0\0\0\0\0ConfState";
const HARD_STATE: &'static [u8; 16] = b"\0\0\0\0\0\0\0HardState";

//#[derive(Default)]
pub struct URRocksStorage {
    backend: DB,
    pending_conf_state: Option<ConfState>,
    pending_conf_state_start_index: Option<u64>,
    conf_state: Option<ConfState>,
}

impl URRocksStorage {
    pub fn new_with_conf_state(id: u64, state: ConfState) -> Self {
        let mut db = Self::new(id);

        db.set_conf_state(state, None).unwrap();
        db.set_hard_state(1, 1).unwrap();
        db
    }
    pub fn new(id: u64) -> Self {
        let backend = DB::open_default(&format!("raft-rocks-{}", id)).unwrap();
        Self {
            backend,
            pending_conf_state: None,
            pending_conf_state_start_index: None,
            conf_state: None,
        }
    }

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
        self.backend
            .iterator(IteratorMode::From(&LOW_INDEX, Direction::Forward))
            .filter_map(|(k, _)| {
                let k1: &[u8] = k.borrow();
                if k1 != &HARD_STATE[..] && k1 != &CONF_STATE[..] {
                    Some(k)
                } else {
                    None
                }
            })
            .take_while(|k| {
                let k: &[u8] = k.borrow();
                k != &HIGH_INDEX[..]
            })
            .for_each(|k| self.backend.delete(&k).unwrap());
    }
}

impl WriteStorage for URRocksStorage {
    fn apply_snapshot(&mut self, mut snapshot: Snapshot) -> RaftResult<()> {
        let mut meta = snapshot.take_metadata();
        let term = meta.term;
        let index = meta.index;

        let first_index = self.first_index().unwrap();
        // Make sure the snapshot is not prior to our first log
        if first_index > index {
            return Err(RaftError::Store(StorageError::SnapshotOutOfDate));
        }

        self.set_hard_state(index, term)?;
        self.set_conf_state(meta.take_conf_state(), None)?;
        // From Mem node do we only want to clear up to index?
        self.clear_log();
        Ok(())
    }

    fn append(&self, entries: &[Entry]) -> RaftResult<()> {
        if entries.is_empty() {
            return Ok(());
        }
        //dbg!(&entries);
        let mut batch = WriteBatch::default();
        for entry in entries {
            let key = make_log_key(entry.index);
            let data = entry.write_to_bytes()?;
            batch.put(&key, &data).unwrap();
        }
        self.backend.write(batch).unwrap();
        self.backend.flush().unwrap();

        Ok(())
    }

    fn set_conf_state(
        &mut self,
        cs: ConfState,
        pending_membership_change: Option<(ConfState, u64)>,
    ) -> RaftResult<()> {
        self.conf_state = Some(cs.clone());

        let data = cs.write_to_bytes()?;
        self.backend.put(&CONF_STATE, &data).unwrap();
        self.backend.flush().unwrap();
        if let Some((cs, idx)) = pending_membership_change {
            self.pending_conf_state = Some(cs);
            self.pending_conf_state_start_index = Some(idx);
        }
        Ok(())
    }

    fn set_hard_state(&mut self, commit: u64, term: u64) -> RaftResult<()> {
        let mut hs = HardState::new();
        hs.commit = commit;
        hs.term = term;
        let data = hs.write_to_bytes()?;
        self.backend.put(&HARD_STATE, &data).unwrap();
        self.backend.flush().unwrap();
        Ok(())
    }
}

impl ReadStorage for URRocksStorage {
    fn initial_state(&self) -> RaftResult<RaftState> {
        let mut initial_state = RaftState::default();
        if let Some(ref cs) = self.conf_state {
            initial_state.conf_state = cs.clone()
        }
        // initial_state.conf_state = self.get_conf_state();

        //let mut cs = self.get_conf_state();
        //initial_state.conf_state.set_nodes(cs.take_nodes());
        //initial_state.conf_state.set_learners(cs.take_learners());

        initial_state.hard_state = self.get_hard_state();
        Ok(initial_state)
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
            .filter(|(k, _)| {
                let k: &[u8] = k.borrow();
                k != &HARD_STATE[..] && k != &CONF_STATE[..]
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
            .filter(|(k, _)| {
                let k: &[u8] = k.borrow();
                k != &HARD_STATE[..] && k != &CONF_STATE[..]
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
            .filter(|(k, _)| {
                let k: &[u8] = k.borrow();
                k != &HARD_STATE[..] && k != &CONF_STATE[..]
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

const CONF_PREFIX: u8 = 0;
const RAFT_PREFIX: u8 = 1;
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
fn make_log_key(idx: u64) -> [u8; 16] {
    use bytes::BufMut;
    use std::io::Cursor;
    let mut key = [0; 16];

    {
        let mut key = Cursor::new(&mut key[..]);
        key.put_u64_le(1);
        key.put_u64_le(idx);
    }

    key
}
