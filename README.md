# uring

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

## Alternate startup procedure

```bash
PYTHONPATH=. ./bin/3u.py # spin up a nascent 3-node test cluster
```

For dependencies ( if needed )

```bash
pip3 install -r ./requirements.txt
```

To output formatted pretty-printed json logs:

```bash
$ PYTHONPATH=. ./bin/3u.py 2>&1 | jq -R 'fromjson?'
```

## Acceptance tests

```bash
go get -u github.com/landoop/coyote
PYTHONPATH=. bin/3u.py
coyote -c contrib/3u-test.yml
```

To view test report:

```bash
python -m SimpleHTTPServer
open http://localhost:8000/coyote.html
```

## vnode

```bash
curl -H 'Content-Type: applicaiton/json' -X GET http://localhost:8081/mring
curl -H 'Content-Type: applicaiton/json' -X POST -d '{"size":64}' http://localhost:8081/mring
curl -H 'Content-Type: applicaiton/json' -X POST -d '{"node":"n1"}' http://localhost:8081/mring/node
curl -H 'Content-Type: applicaiton/json' -X GET http://localhost:8081/mring/node
curl -H 'Content-Type: applicaiton/json' -X POST -d '{"node":"n2"}' http://localhost:8081/mring/node
```

## ws - kv

```bash
websocat ws://localhost:8081/uring

{"Select": {"rid": 1, "protocol": "KV"}}

{"Get": {"rid": 2, "key": "snot"}}

{"Put": {"rid": 3, "key": "snot", "store": "badger"}}

{"Get": {"rid": 2, "key": "snot"}}

{"Put": {"rid": 3, "key": "snot", "store": "badger2"}}

{"Delete": {"rid": 2, "key": "snot"}}

{"Get": {"rid": 2, "key": "snot"}}

```

```bash
{"Subscribe": {"channel": "kv"}}
{"As": {"protocol": "KV", "cmd": {"Get": {"rid": 2, "key": "snot"}}}}

{"As": {"protocol": "KV", "cmd": {"Put": {"rid": 3, "key": "snot", "store": "badger"}}}}
```

## ws - mring

```bash
websocat ws://localhost:8081/uring

{"Select": {"rid": 1, "protocol": "MRing"}}

{"GetSize": {"rid": 2}}

{"SetSize": {"rid": 3, "size": 32}}

{"GetSize": {"rid": 2}}

{"AddNode": {"rid": 3, "node": "n1"}}
{"AddNode": {"rid": 3, "node": "n2"}}
{"AddNode": {"rid": 3, "node": "n3"}}
```

## ws - pubsub

```bash
websocat ws://localhost:8081/uring

{"Subscribe": {"channel": "kv"}}
{"Subscribe": {"channel": "mring"}}
```

## python client example

```python
#!/usr/bin/env python3
import contrib
import time

rc = contrib.RaftClient()
rc.set_host('127.0.0.1')
rc.set_port(8081)
rc.ws_start()

def report(subject,json):
    print("{} Event: {}".format(subject, json))

time.sleep(1)
rc.subscribe('kv', lambda json: report('KV', json))
rc.subscribe('mring', lambda json: report('MRing', json))

while True:
    time.sleep(1)
```
