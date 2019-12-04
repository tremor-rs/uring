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

//! Generalized Distributed Rate Limiting
//!
//! The most important value in this is probably `q` the performance
//! indicator, so I will loose a few words about it here. While at
//! first counterintuitive it makes a lot of sense once explained.
//! q does not, as one might expect measure the load of on a limiter.
//! Instead it uses the quality on it. To make this more digestable
//! lets look at an example.
//!
//! If our limiter is used to shed messages, `q` could be the
//! percentage of messages dropped. The limiter will then try to
//! equalize the percentage of messages dropped everywhere.
//!
//! An alternative would be picking `q` as the messages delivered
//! in that case our limiter would try to equilize the delivered
//! messages.
//!
//! It is also possible to use the number of dropped messages that way
//! we would try to equilize on the number of drops.
//!
//! Bottom line, the limiter itself doesn't care how `q` is computed
//! as long as it is possible for `q` to converge to a stable value.

pub struct State {
    /// Numuber of limiters
    n: u64,
    /// The capacity assigned
    c: f64,
    /// The performance metric
    q: f64,
    /// Factor by which the adjustment is made
    eta: f64,
    /*
     refresh_time :: integer(),     % Interval for refresh
     neighbours = [] :: [pid()],    % Neighbours to sync with,
     demander :: undefined | pid(), % The demander
     non_update_ticks = 0 :: non_neg_integer() %% How often we ticked w/o updating
    */
}

impl Default for State {
    fn default() -> Self {
        Self {
            eta: 0.1,
            n: 1,
            c: 0.0,
            q: 1.0,
        }
    }
}

impl State {
    fn exchange_q_stage1() {}
}

#[cfg(test)]
mod tests {
    #[test]
    fn it_works() {
        assert_eq!(2 + 2, 4);
    }
}
