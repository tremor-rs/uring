**uring**

RAFT spike based on the raft-rs framework that powers tikv/tidb
to evaluate adopting raft-rs.


```bash
# start first node and clear current cache
rm -rf raft-rocks-* 
cargo run -- -e 127.0.0.1:8081 -p 127.0.0.1:8082 -p 127.0.0.1:8083 -i1 -b
# join second node
cargo run -- -e 127.0.0.1:8082 -p 127.0.0.1:8081 -p 127.0.0.1:8083 -i2
# join third node
cargo run -- -e 127.0.0.1:8083 -p 127.0.0.1:8081 -p 127.0.0.1:8082 -i3
# activate nodes in raft group (triggers AddNode message)
curl -X POST 127.0.0.1:8081/node/2
curl -X POST 127.0.0.1:8081/node/3

# kill node 1
# restart node 1
cargo run -- -e 127.0.0.1:8081 -i 1 -p 127.0.0.1:8082
# kill new leader
# restart new leader
# kill  new leader
...
```

### Alternate startup procedure

```bash
$ PYTHONPATH=. ./bin/3u.py # spin up a nascent 3-node test cluster
```

For dependencies ( if needed )

```bash
$ pip3 install -r ./requirements.txt
```
