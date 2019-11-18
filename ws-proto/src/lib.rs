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
// use crate::{NodeId, KV};

use serde::{Deserialize, Serialize};
use uring_common::*;

#[derive(Serialize, Deserialize, Debug, Clone, Copy)]
pub enum Protocol {
    URing,
    KV,
    MRing,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub enum ProtocolSelect {
    Select {
        rid: RequestId,
        protocol: Protocol,
    },
    Selected {
        rid: RequestId,
        protocol: Protocol,
    },
    As {
        protocol: Protocol,
        cmd: serde_json::Value,
    },
    Subscribe {
        channel: String,
    },
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub enum KVRequest {
    Get {
        rid: RequestId,
        key: String,
    },
    Put {
        rid: RequestId,
        key: String,
        store: String,
    },
    Delete {
        rid: RequestId,
        key: String,
    },
    Cas {
        rid: RequestId,
        key: String,
        check: String,
        store: String,
    },
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub enum MRRequest {
    SetSize { rid: RequestId, size: u64 },
    GetSize { rid: RequestId },
    GetNodes { rid: RequestId },
    AddNode { rid: RequestId, node: String },
    RemoveNode { rid: RequestId, node: String },
}

#[derive(Deserialize, Serialize)]
pub struct Reply {
    pub rid: RequestId,
    pub data: Option<serde_json::Value>,
}
