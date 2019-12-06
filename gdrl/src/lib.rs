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

//! This is a generic rate limiting node implemented based on the
//! work of Rade Stanojevic and Robert Shorten init:
//!
//! Generalized Distributed Rate Limiting
//! (http://www.hamilton.ie/person/rade/GeneralizedDRL_IWQoS2009.pdf)
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
#![recursion_limit = "256"]
//use async_trait::async_trait;
use futures::channel::mpsc::{channel, Receiver, Sender};
use futures::{select, SinkExt, StreamExt};

#[derive(Debug)]
pub enum ConnectionError {
    Oops(u64),
}

pub struct Neighbour {
    connection: Sender<(f64, Sender<f64>)>,
}

impl Neighbour {
    pub fn new(connection: Sender<(f64, Sender<f64>)>) -> Self {
        Self { connection }
    }
    async fn exchange_q(&mut self, qi: f64, tx: Sender<f64>) -> Result<(), ConnectionError> {
        self.connection
            .send((qi, tx))
            .await
            .map_err(|_| ConnectionError::Oops(1))?;
        Ok(())
    }
}

pub struct Limiter {
    /// The gain parameter
    eta: f64,
    /// The capacity of this limiter
    ci: f64,
    /// The last capaicty this limiter had
    last_ci: f64,
    /// The number of stable ticks since the last Ci change
    stable_ticks: u64,
    /// The performance indicator for this limiter
    qi: f64,
    /// Neighboring nodes we communicate with.
    neighbours: Vec<Neighbour>,
    /// The local connection endpoint
    connection: Receiver<(f64, Sender<f64>)>,
    rx: Receiver<f64>,
    tx: Sender<f64>,
    /*
     refresh_time :: integer(),     % Interval for refresh
     neighbours = [] :: [pid()],    % Neighbours to sync with,
     demander :: undefined | pid(), % The demander
     non_update_ticks = 0 :: non_neg_integer() %% How often we ticked w/o updating
    */
}

impl Limiter {
    pub fn new(connection: Receiver<(f64, Sender<f64>)>, eta: f64, ci: f64) -> Self {
        let (tx, rx) = channel(64);
        Self {
            connection,
            eta,
            ci,
            qi: 0.0,
            last_ci: 0.0,
            stable_ticks: 0,
            neighbours: Vec::new(),
            rx,
            tx,
        }
    }

    pub fn stable_ticks(&self) -> u64 {
        self.stable_ticks
    }

    pub fn q(&self) -> f64 {
        self.qi
    }

    pub fn c(&self) -> f64 {
        self.ci
    }

    pub fn add_neighbour(&mut self, neighbour: Neighbour) {
        self.neighbours.push(neighbour);
    }
    /// Performs a tick and sets a new q(ality) value.
    ///
    /// * `qi` stands for the current quality of the usaing
    ///   service where 0 is perfect quality any number above
    ///   that means a certan degration in service.
    ///
    /// * The return value is the new service rate for this caller
    ///   after this tick - not it can take multiple ticks to
    ///   see an effect of changing q values as communications
    ///   have to be made.
    ///
    pub async fn tick(&mut self, qi: f64) -> Result<f64, ConnectionError> {
        self.qi = qi;

        for j in &mut self.neighbours {
            j.exchange_q(self.qi, self.tx.clone()).await?
        }

        loop {
            select! {
                msg = self.rx.next() => {
                    match msg {
                        None => return Err(ConnectionError::Oops(2)),
                        Some(delta) => {
                            // We substract here as we equalivalize for the counter side.
                            self.ci = self.ci - delta;
                            if self.ci == self.last_ci {
                                self.stable_ticks += 1;
                            } else {
                                self.last_ci = self.ci;
                                self.stable_ticks = 0;
                            }
                        }
                    }
                }
                msg = self.connection.next() => {
                    match msg {
                        None => (),
                        Some((qj, mut tx)) => {
                            let delta = self.eta * (self.qi - qj);
                            tx.send(delta).await.map_err(|_| ConnectionError::Oops(4))?;
                            self.ci = self.ci + delta;
                            if self.ci == self.last_ci {
                                self.stable_ticks += 1;
                            } else {
                                self.last_ci = self.ci;
                                self.stable_ticks = 0;
                            }
                        },
                    }
                },
                default => break,
                complete => break,
            };
        }

        //dbg!("END");
        Ok(self.ci)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use async_std::task;
    use std::time::Duration;

    fn print_state(_n: u8, _c: f64, _q: f64, _s: u64) {
        println!("=============L{}(c: {} / q: {} / s: {})=============", _n, _c, _q, _s);
    }
    #[test]
    fn reaches_quiescence_3x1() {
        // Degree of our graph
        let c = 1000.0;
        let nodes = 3;
        let connections = 1;
        let eta = 1.0/(2.0 * ((nodes * connections) as f64));
        let (tx1, rx1) = channel(64);
        let mut l1 = Limiter::new(rx1, eta, c);
        let (tx2, rx2) = channel(64);
        let mut l2 = Limiter::new(rx2, eta, 0.0);
        let (tx3, rx3) = channel(64);
        let mut l3 = Limiter::new(rx3, eta, 0.0);


        // add l2 as a nighbour node to l1
        l1.add_neighbour(Neighbour::new(tx2));
        l2.add_neighbour(Neighbour::new(tx3));
        l3.add_neighbour(Neighbour::new(tx1));

        println!("=============XX=============");
        task::block_on(async {
            let tick_duration = Duration::from_millis(10);
            let (mut tx1, mut rx1) = channel(1);
            let (mut tx2, mut rx2) = channel(1);
            let (mut tx3, mut rx3) = channel(1);
            task::spawn(async move {
                let n = 1u8;
                let mut l = l1;
                while l.tick(c - l.c()).await.is_ok() && l.stable_ticks() < 10 {
                    task::sleep(tick_duration).await;
                    print_state(n, l.c(), l.q(), l.stable_ticks());
                }
                tx1.send(true).await
            });
            task::spawn(async move {
                let n = 2u8;
                let mut l = l2;
                while l.tick(c - l.c()).await.is_ok() && l.stable_ticks() < 10 {
                    task::sleep(tick_duration).await;
                    print_state(n, l.c(), l.q(), l.stable_ticks());
                }
                tx2.send(true).await
            });

            task::spawn(async move {
                let n = 3u8;
                let mut l = l3;
                while l.tick(c - l.c()).await.is_ok() && l.stable_ticks() < 10 {
                    task::sleep(tick_duration).await;
                    print_state(n, l.c(), l.q(), l.stable_ticks());
                }
                tx3.send(true).await
            });

            rx1.next().await;            
            rx2.next().await;            
            rx3.next().await;                        
        });
    }

    #[test]
    fn reaches_quiescence_5x2() {
        // Degree of our graph
        let c = 1000.0;
        let nodes = 5;
        let connections = 2;
        let eta = 1.0/(2.0 * ((nodes * connections) as f64));
        let (tx1, rx1) = channel(64);
        let mut l1 = Limiter::new(rx1, eta, c);
        let (tx2, rx2) = channel(64);
        let mut l2 = Limiter::new(rx2, eta, 0.0);
        let (tx3, rx3) = channel(64);
        let mut l3 = Limiter::new(rx3, eta, 0.0);
        let (tx4, rx4) = channel(64);
        let mut l4 = Limiter::new(rx4, eta, 0.0);
        let (tx5, rx5) = channel(64);
        let mut l5 = Limiter::new(rx5, eta, 0.0);

        l1.add_neighbour(Neighbour::new(tx2.clone()));
        l1.add_neighbour(Neighbour::new(tx3.clone()));

        l2.add_neighbour(Neighbour::new(tx3.clone()));
        l2.add_neighbour(Neighbour::new(tx4.clone()));

        l3.add_neighbour(Neighbour::new(tx4.clone()));
        l3.add_neighbour(Neighbour::new(tx5.clone()));

        l4.add_neighbour(Neighbour::new(tx5.clone()));
        l4.add_neighbour(Neighbour::new(tx1.clone()));

        l5.add_neighbour(Neighbour::new(tx1.clone()));
        l5.add_neighbour(Neighbour::new(tx2.clone()));

        println!("=============XX=============");
        task::block_on(async {
            let tick_duration = Duration::from_millis(10);
            let (mut tx1, mut rx1) = channel(1);
            let (mut tx2, mut rx2) = channel(1);
            let (mut tx3, mut rx3) = channel(1);
            let (mut tx4, mut rx4) = channel(1);
            let (mut tx5, mut rx5) = channel(1);
            let min_stable_ticks = 20;
            task::spawn(async move {
                let n = 1u8;
                let mut l = l1;
                while l.tick(c - l.c()).await.is_ok() && l.stable_ticks() < min_stable_ticks {
                    task::sleep(tick_duration).await;
                    print_state(n, l.c(), l.q(), l.stable_ticks());
                }
                tx1.send(true).await
            });
            task::spawn(async move {
                let n = 2u8;
                let mut l = l2;
                while l.tick(c - l.c()).await.is_ok() && l.stable_ticks() < min_stable_ticks {
                    task::sleep(tick_duration).await;
                    print_state(n, l.c(), l.q(), l.stable_ticks());
                }
                tx2.send(true).await
            });

            task::spawn(async move {
                let n = 3u8;
                let mut l = l3;
                while l.tick(c - l.c()).await.is_ok() && l.stable_ticks() < min_stable_ticks {
                    task::sleep(tick_duration).await;
                    print_state(n, l.c(), l.q(), l.stable_ticks());
                }
                tx3.send(true).await
            });

            task::spawn(async move {
                let n = 4u8;
                let mut l = l4;
                while l.tick(c - l.c()).await.is_ok() && l.stable_ticks() < min_stable_ticks {
                    task::sleep(tick_duration).await;
                    print_state(n, l.c(), l.q(), l.stable_ticks());
                }
                tx4.send(true).await
            });

            task::spawn(async move {
                let n = 5u8;
                let mut l = l5;
                while l.tick(c - l.c()).await.is_ok() && l.stable_ticks() < min_stable_ticks {
                    task::sleep(tick_duration).await;
                    print_state(n, l.c(), l.q(), l.stable_ticks());
                }
                tx5.send(true).await
            });

            rx1.next().await;            
            rx2.next().await;            
            rx3.next().await;                        
            rx4.next().await;                        
            rx5.next().await;                        
        });
    }
}
