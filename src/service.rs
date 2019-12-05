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

pub mod kv;
pub mod mring;
pub mod status;
pub mod version;
use crate::{pubsub, storage};

use async_std::sync::Mutex;
use async_trait::async_trait;
use raft::RawNode;
use std::{fmt, io};

#[derive(Debug)]
pub enum Error {
    Io(io::Error),
    Generic(String),
    Kv(String),
    UnknownEvent,
}
impl std::error::Error for Error {}
impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{:?}", self)
    }
}

#[async_trait]
pub trait Service<Storage>: Send + Sync
where
    Storage: storage::Storage + Send + Sync,
{
    async fn execute(
        &mut self,
        node: &Mutex<RawNode<Storage>>,
        pubsub: &mut pubsub::Channel,
        event: Vec<u8>,
    ) -> Result<(u16, Vec<u8>), Error>;
    fn is_local(&self, event: &[u8]) -> Result<bool, Error>;
}
