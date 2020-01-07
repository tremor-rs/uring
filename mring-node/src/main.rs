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

#![recursion_limit = "2048"]

mod handoff;
mod uring;
mod vnode;

use async_std::task;
use futures::channel::mpsc::channel;
use slog::Drain;
use std::env;

const CHANNEL_SIZE: usize = 64usize;

#[macro_use]
extern crate slog;

fn main() {
    let decorator = slog_term::TermDecorator::new().build();
    let drain = slog_term::FullFormat::new(decorator).build().fuse();
    let drain = slog_async::Async::new(drain).build().fuse();
    let logger = slog::Logger::root(drain, o!());

    let (tasks_tx, tasks_rx) = channel(crate::CHANNEL_SIZE);

    let local = env::args()
        .nth(1)
        .unwrap_or_else(|| panic!("this program requires at least two arguments"));

    // Specify the server address to which the client will be connecting.
    let remote = env::args()
        .nth(2)
        .unwrap_or_else(|| panic!("this program requires at least two argument"));

    task::spawn(vnode::run(
        logger.clone(),
        local.clone(),
        tasks_rx,
        tasks_tx.clone(),
    ));

    task::spawn(handoff::listener(
        logger.clone(),
        local.to_string(),
        tasks_tx.clone(),
    ));

    task::block_on(uring::run(logger, local, remote, tasks_tx))
}
