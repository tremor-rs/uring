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

pub mod continuous;
use super::{Node, Nodes};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

#[derive(Default, Serialize, Deserialize, Debug, Clone)]
pub struct Relocation {
    destinations: HashMap<String, Vec<u64>>,
}
pub type Relocations = HashMap<String, Relocation>;

pub trait Placement {
    fn add_node(count: u64, current: Vec<Node>, new: String) -> (Nodes, Relocations);
    fn remove_node(count: u64, current: Vec<Node>, old: String) -> (Vec<Node>, Relocations);
    fn new(count: u64, new: String) -> Vec<Node>;
    fn name() -> String;
}
