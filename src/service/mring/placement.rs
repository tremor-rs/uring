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

pub mod continuous;
use uring_common::{MRingNodes, Relocations};

pub trait Placement {
    fn add_node(count: u64, current: MRingNodes, new: String) -> (MRingNodes, Relocations);
    fn remove_node(count: u64, current: MRingNodes, old: String) -> (MRingNodes, Relocations);
    fn new(count: u64, new: String) -> MRingNodes;
    fn name() -> String;
}
