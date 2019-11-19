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

use super::Placement;
use uring_common::{MRingNode, MRingNodes, Relocation, Relocations};

pub struct Strategy {}

impl Placement for Strategy {
    fn name() -> String {
        "continuous".into()
    }
    fn new(count: u64, new: String) -> MRingNodes {
        vec![MRingNode {
            id: new,
            vnodes: (0..count).collect(),
        }]
    }
    fn add_node(count: u64, mut current: MRingNodes, new: String) -> (MRingNodes, Relocations) {
        let mut rs = Relocations::new();
        // skip nodes we know
        for n in &current {
            if n.id == new {
                return (current, rs);
            }
        }
        let new_node = MRingNode {
            id: new,
            vnodes: Vec::new(),
        };
        current.push(new_node);
        let vnodes_per_node = count as usize / current.len();
        let mut extra = count as usize % current.len();
        for i in 0..current.len() - 1 {
            let src_id = current[i].id.clone();
            let dst_id = current[i + 1].id.clone();
            let mut moves = Vec::new();
            let max_vnodes = if extra > 0 {
                extra -= 1;
                vnodes_per_node + 1
            } else {
                vnodes_per_node
            };
            while current[i].vnodes.len() > max_vnodes {
                let id = current[i].vnodes.pop().unwrap();
                moves.push(id);
                current[i + 1].vnodes.push(id);
            }
            current[i + 1].vnodes.sort();
            let mut r = Relocation::default();
            moves.sort();
            r.destinations.insert(dst_id, moves);

            rs.insert(src_id, r);
        }
        (current, rs)
    }

    fn remove_node(count: u64, mut current: MRingNodes, old: String) -> (MRingNodes, Relocations) {
        let mut rs = Relocations::new();
        let mut i = 0;
        let vnodes_per_node = count as usize / (current.len() - 1);
        let mut extra = count as usize % (current.len() - 1);
        while i < current.len() - 1 && current[i].id != old {
            let mut moves = Vec::new();
            let src_id = current[i + 1].id.clone();
            let dst_id = current[i].id.clone();
            current[i + 1].vnodes.reverse();
            let max = if extra > 0 {
                extra -= 1;
                vnodes_per_node + 1
            } else {
                vnodes_per_node
            };
            while current[i].vnodes.len() < max {
                let id = current[i + 1].vnodes.pop().unwrap();
                moves.push(id);
                current[i].vnodes.push(id);
            }
            current[i + 1].vnodes.reverse();
            let mut r = Relocation::default();
            moves.sort();
            r.destinations.insert(dst_id, moves);
            rs.insert(src_id, r);
            i += 1;
        }

        if i < current.len() && current[i].id == old {
            let mut deleted = current.remove(i);
            // If this was the last node we have already moved the vnodes
            // out
            if i == current.len() {
                return (current, rs);
            }
            let r = rs.entry(deleted.id).or_default();
            let ds = r.destinations.entry(current[i].id.clone()).or_default();
            ds.append(&mut deleted.vnodes.clone());
            ds.sort();

            current[i].vnodes.append(&mut deleted.vnodes);
            current[i].vnodes.sort();
        }

        for i in i..current.len() - 1 {
            let src_id = current[i].id.clone();
            let dst_id = current[i + 1].id.clone();
            let mut moves = Vec::new();
            let max = if extra > 0 {
                extra -= 1;
                vnodes_per_node + 1
            } else {
                vnodes_per_node
            };
            while current[i].vnodes.len() > max {
                let id = current[i].vnodes.pop().unwrap();
                moves.push(id);
                current[i + 1].vnodes.push(id);
            }
            current[i + 1].vnodes.sort();
            if !moves.is_empty() {
                let mut r = Relocation::default();
                moves.sort();
                r.destinations.insert(dst_id, moves);

                rs.insert(src_id, r);
            }
        }
        (current, rs)
    }
}

#[cfg(test)]
mod test {
    use super::*;
    macro_rules! assert_move {
        ($r:expr, $src:expr, $($dst:expr, $exp:expr),*) => {
            let mut src = $r.remove($src).unwrap();
            $(
                let dst = src.destinations.remove($dst).unwrap();
                assert_eq!(dst, $exp);
            )*
            assert!(src.destinations.is_empty());
        };
        ($r:expr, $src:expr, $dst:expr, $exp:expr) => {
            let mut src = $r.remove($src).unwrap();
            let dst = src.destinations.remove($dst).unwrap();
            assert_eq!(dst, $exp);
            assert!(src.destinations.is_empty());
        };
    }
    #[test]
    fn continuous_add() {
        let p = Strategy::new(8, "n1".into());
        assert_eq!(
            p,
            vec![MRingNode {
                id: "n1".into(),
                vnodes: vec![0, 1, 2, 3, 4, 5, 6, 7]
            }]
        );
        let (p, mut r) = Strategy::add_node(8, p, "n2".into());
        assert_eq!(
            p,
            vec![
                MRingNode {
                    id: "n1".into(),
                    vnodes: vec![0, 1, 2, 3]
                },
                MRingNode {
                    id: "n2".into(),
                    vnodes: vec![4, 5, 6, 7]
                }
            ]
        );
        assert_move!(r, "n1", "n2", &[4, 5, 6, 7]);
        assert!(r.is_empty());

        let (p, mut r) = Strategy::add_node(8, p, "n3".into());
        assert_eq!(
            p,
            vec![
                MRingNode {
                    id: "n1".into(),
                    vnodes: vec![0, 1, 2]
                },
                MRingNode {
                    id: "n2".into(),
                    vnodes: vec![3, 4, 5]
                },
                MRingNode {
                    id: "n3".into(),
                    vnodes: vec![6, 7]
                }
            ]
        );
        assert_move!(r, "n1", "n2", &[3]);
        assert_move!(r, "n2", "n3", &[6, 7]);
        assert!(r.is_empty());

        let (p, mut r) = Strategy::add_node(8, p, "n4".into());
        assert_eq!(
            p,
            vec![
                MRingNode {
                    id: "n1".into(),
                    vnodes: vec![0, 1]
                },
                MRingNode {
                    id: "n2".into(),
                    vnodes: vec![2, 3]
                },
                MRingNode {
                    id: "n3".into(),
                    vnodes: vec![4, 5]
                },
                MRingNode {
                    id: "n4".into(),
                    vnodes: vec![6, 7]
                }
            ]
        );
        assert_move!(r, "n1", "n2", &[2]);
        assert_move!(r, "n2", "n3", &[4, 5]);
        assert_move!(r, "n3", "n4", &[6, 7]);
        assert!(r.is_empty());
    }

    #[test]
    fn continuous_remove_end() {
        let p = vec![
            MRingNode {
                id: "n1".into(),
                vnodes: vec![0, 1],
            },
            MRingNode {
                id: "n2".into(),
                vnodes: vec![2, 3],
            },
            MRingNode {
                id: "n3".into(),
                vnodes: vec![4, 5],
            },
            MRingNode {
                id: "n4".into(),
                vnodes: vec![6, 7],
            },
        ];

        let (p, mut r) = Strategy::remove_node(8, p, "n4".into());
        assert_eq!(
            p,
            vec![
                MRingNode {
                    id: "n1".into(),
                    vnodes: vec![0, 1, 2],
                },
                MRingNode {
                    id: "n2".into(),
                    vnodes: vec![3, 4, 5],
                },
                MRingNode {
                    id: "n3".into(),
                    vnodes: vec![6, 7],
                },
            ]
        );

        assert_move!(r, "n2", "n1", &[2]);
        assert_move!(r, "n3", "n2", &[4, 5]);
        assert_move!(r, "n4", "n3", &[6, 7]);
        assert!(r.is_empty());

        let (p, mut r) = Strategy::remove_node(8, p, "n3".into());
        assert_eq!(
            p,
            vec![
                MRingNode {
                    id: "n1".into(),
                    vnodes: vec![0, 1, 2, 3]
                },
                MRingNode {
                    id: "n2".into(),
                    vnodes: vec![4, 5, 6, 7]
                }
            ]
        );
        assert_move!(r, "n2", "n1", &[3]);
        assert_move!(r, "n3", "n2", &[6, 7]);
        assert!(r.is_empty());

        let (p, mut r) = Strategy::remove_node(8, p, "n2".into());
        assert_eq!(
            p,
            vec![MRingNode {
                id: "n1".into(),
                vnodes: vec![0, 1, 2, 3, 4, 5, 6, 7]
            },]
        );
        assert_move!(r, "n2", "n1", &[4, 5, 6, 7]);
        assert!(r.is_empty());
    }

    #[test]
    fn continuous_remove_start() {
        let p = vec![
            MRingNode {
                id: "n1".into(),
                vnodes: vec![0, 1],
            },
            MRingNode {
                id: "n2".into(),
                vnodes: vec![2, 3],
            },
            MRingNode {
                id: "n3".into(),
                vnodes: vec![4, 5],
            },
            MRingNode {
                id: "n4".into(),
                vnodes: vec![6, 7],
            },
        ];

        let (p, mut r) = Strategy::remove_node(8, p, "n1".into());
        assert_eq!(
            p,
            vec![
                MRingNode {
                    id: "n2".into(),
                    vnodes: vec![0, 1, 2],
                },
                MRingNode {
                    id: "n3".into(),
                    vnodes: vec![3, 4, 5],
                },
                MRingNode {
                    id: "n4".into(),
                    vnodes: vec![6, 7],
                },
            ]
        );

        assert_move!(r, "n1", "n2", &[0, 1]);
        assert_move!(r, "n2", "n3", &[3]);
        assert!(dbg!(r).is_empty());

        let (p, mut r) = Strategy::remove_node(8, p, "n2".into());
        assert_eq!(
            p,
            vec![
                MRingNode {
                    id: "n3".into(),
                    vnodes: vec![0, 1, 2, 3]
                },
                MRingNode {
                    id: "n4".into(),
                    vnodes: vec![4, 5, 6, 7]
                }
            ]
        );
        assert_move!(r, "n2", "n3", &[0, 1, 2]);
        assert_move!(r, "n3", "n4", &[4, 5]);
        assert!(r.is_empty());

        let (p, mut r) = Strategy::remove_node(8, p, "n3".into());
        assert_eq!(
            p,
            vec![MRingNode {
                id: "n4".into(),
                vnodes: vec![0, 1, 2, 3, 4, 5, 6, 7]
            },]
        );
        assert_move!(r, "n3", "n4", &[0, 1, 2, 3]);
        assert!(r.is_empty());
    }

    #[test]
    fn continuous_remove_mid() {
        let p = vec![
            MRingNode {
                id: "n1".into(),
                vnodes: vec![0, 1],
            },
            MRingNode {
                id: "n2".into(),
                vnodes: vec![2, 3],
            },
            MRingNode {
                id: "n3".into(),
                vnodes: vec![4, 5],
            },
            MRingNode {
                id: "n4".into(),
                vnodes: vec![6, 7],
            },
        ];

        let (p, mut r) = Strategy::remove_node(8, p, "n2".into());
        assert_eq!(
            p,
            vec![
                MRingNode {
                    id: "n1".into(),
                    vnodes: vec![0, 1, 2],
                },
                MRingNode {
                    id: "n3".into(),
                    vnodes: vec![3, 4, 5],
                },
                MRingNode {
                    id: "n4".into(),
                    vnodes: vec![6, 7],
                },
            ]
        );
        assert_move!(r, "n2", "n1", [2], "n3", [3]);
        assert!(r.is_empty());

        let (p, mut r) = Strategy::remove_node(8, p, "n3".into());
        assert_eq!(
            p,
            vec![
                MRingNode {
                    id: "n1".into(),
                    vnodes: vec![0, 1, 2, 3]
                },
                MRingNode {
                    id: "n4".into(),
                    vnodes: vec![4, 5, 6, 7]
                }
            ]
        );
        assert_move!(r, "n3", "n1", [3], "n4", [4, 5]);
        assert!(r.is_empty());
    }
}
