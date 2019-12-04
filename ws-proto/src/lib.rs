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

use serde_derive::{Deserialize, Serialize};
use uring_common::*;

#[derive(Serialize, Deserialize, Debug, Clone, Copy)]
pub enum Protocol {
    URing,
    KV,
    MRing,
    Version,
    Status,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub enum ProtocolSelect {
    Status {
        rid: RequestId,
    },
    Version {
        rid: RequestId,
    },
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
        check: Option<String>,
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

#[derive(Serialize, Deserialize, Debug, Clone)]
pub enum VRequest {
    Get { rid: RequestId },
}
#[derive(Serialize, Deserialize, Debug, Clone)]
pub enum SRequest {
    Get { rid: RequestId },
}

#[derive(Deserialize, Serialize, Debug)]
pub struct Reply {
    pub rid: RequestId,
    pub data: Option<serde_json::Value>,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub enum PSMRing {
    SetSize {
        size: u64,
        strategy: String,
    },
    NodeAdded {
        node: String,
        strategy: String,
        next: MRingNodes,
        relocations: Relocations,
    },
    NodeRemoved {
        node: String,
        strategy: String,
        next: MRingNodes,
        relocations: Relocations,
    },
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub enum PSURing {
    StateChange {
        node: NodeId,
        prev_state: String,
        next_state: String,
    },
    ProposalReceived {
        node: NodeId,
        from: NodeId,
        pid: ProposalId,
        sid: ServiceId,
        eid: EventId,
    },
    AddNode {
        node: NodeId,
        new_node: NodeId,
    },
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub enum SubscriberMsg {
    Msg {
        channel: String,
        msg: serde_json::Value,
    },
}
