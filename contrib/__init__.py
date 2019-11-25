# Copyright 2018-2019, Wayfair GmbH
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import sys;
import time;
import os;
import signal;
import subprocess;
import requests;
import simplejson;
import websocket;
import threading;

#
# Simple ( simplistic ) library for driving test scripts
#

def rotate(arr):
     return arr[1:] + [arr[0]]

class Cluster():
    """Encapsulates a uring cluster"""

    def __init__(self):
        self.servers = []
        self.clients = []
        self.config = None

    def configure(self, config_file):
        self.config = simplejson.load(open(config_file, 'r'))
        ports = [];
        for node in self.config:
            ports = ports + [node['port']]

        for node in self.config:
            port = node['port']
            id = node['id']

            us = UringServer()
            us.set_node_id(id)
            us.set_node_ports(ports)
            if id == '1':
                us.set_bootstrap(True)
            us.reset()
            us.start()
            self.servers.append(us)

            time.sleep(1)

            rc = RaftClient()
            rc.set_name(f'u{id}')
            rc.set_node_id(id)
            rc.set_port(port)
            rc.set_host("localhost")
            self.clients.append(rc)

            time.sleep(1)

            rc.ws_start()

            ports = rotate(ports)

            print('{}: {}'.format(id, port))

    def adjoin(self):
        time.sleep(10)
        for node in self.config:
            id = node['id']
            if id != '1':
                requests.post("http://127.0.0.1:8081/node/{}".format(id))

class UringServer():
    """Handles interactions with uring (raft) instance."""

    def __init__(self):
        """Constructs a new uring (raft) instance."""
        self.id = None
        self.node_ports = None
        self.bootstrap = False

    def set_node_id(self, id):
        self.id = id

    def set_node_ports(self, node_ports):
        self.node_ports = node_ports

    def set_bootstrap(self, bootstrap):
        self.bootstrap = bootstrap

    def reset(self):
        subprocess.call("rm -rf raft-rocks-{}".format(self.id).split())

    def start(self):
        endpoint = self.node_ports[0]
        peers = self.node_ports[1:]
        pwd = os.path.join(os.path.dirname(__file__))
        cmd = os.path.join(pwd, "../target/debug/uring -e 127.0.0.1:{} ".format(endpoint))
        for peer in peers:
          cmd += "-p 127.0.0.1:{} ".format(peer)
        if self.bootstrap:
            cmd += " -b"
        cmd += " -i{}".format(self.id)

#        print(cmd)

        self.cmd = subprocess.Popen(cmd, cwd = './', shell=True)
        print("Started process with id: {}".format(self.cmd.pid))

    def pid(self):
        return self.cmd.pid

    def die(self):
        self.cmd.kill()

def synchronized(method):
    def f(*args):
        self = args[0]
        self.mutex.acquire();
        try:
            return apply(method, args)
        finally:
            self.mutex.release();
        return f

def synchronize(kclazz, names=None):
    if type(names)==type(''):
        names = names.split()
    for (name, method_handle) in klazz.__dict.items():
        if callable(name) and name != '__init__' and (names == None or name in names):
            klazz.__dict__[name] = synchronized(method_handle)

class Synchronizer:
    def __init__(self):
        self.mutex = threading.RLock()

class ChannelObserver(Synchronizer):
    def __init__(self):
        self.obs = {}
        Synchronizer.__init__(self)
        return self

    def watch(self, channel, handler):
        if channel in self.obs:
            self.obs[channel].append(handler)
        else:
            self.obs[channel] = [handler]
    


class RaftClient(ChannelObserver):
    """Handles client interactions to raft node."""

    def __init__(self):
        """Constructs a new raft client."""
        self.name = None
        self.node_id = None
        self.host = None
        self.port = None
        self.handlers = {}
        self.callbacks = {}
        self.ws = None
        self.wsc_thread = None
        self.hub = ChannelObserver.__init__(self)
        self.rid = 0

    def on_message(self, message):
        as_json = simplejson.loads(message)
        if 'channel' in as_json['Msg']:
            for handlers in self.hub.obs[as_json['Msg']['channel']]:
                handlers(as_json)
        return message

    def on_error(self, error):
        print(error)
        return error

    def on_close(self):
        print("### closed ###")

    def ws_start(self):
        self.ws = websocket.WebSocketApp(
            "ws://{}:{}/uring".format(self.host, self.port),
            on_open=self.on_open,
            on_message=self.on_message,
            on_error=self.on_error,
            on_close=self.on_close,
        )
        self.wsc_thread = threading.Thread(name='wsc', target=self.ws.run_forever)
        self.wsc_thread.setDaemon(True)
        self.wsc_thread.start()

    def select(self, protocol):        
        self.ws.send(simplejson.dumps({
            "Select": {"rid": self.rid, "protocol": protocol }
        }))
        self.rid = self.rid + 1
    
    def execute_as(self, protocol, json):
        self.ws.send(simplejson.dumps(
            {
                "As": {
                    "protocol": protocol,
                    "cmd": json
                }
            }
        ))

    def subscribe(self, channel, handler=None):
        if not handler is None:
            self.hub.watch(channel, handler)

        self.ws.send(simplejson.dumps({
            "Subscribe": {
                "channel": channel
            }
        }))

    def kv_get(self, key):
        self.ws.send(simplejson.dumps({
            "Get": {
                "rid": self.rid,
                "key": key
            }
        }))
        self.rid = self.rid + 1

    def kv_put(self, key, store):
        self.ws.send(simplejson.dumps({
            "Put": {
                "rid": self.rid,
                "key": key,
                "store": store
            }
        }))
        self.rid = self.rid + 1
    
    def kv_cas(self, key, check, store):
        self.ws.send(simplejson.dumps({
            "Cas": {
                "rid": self.rid,
                "key": key,
                "check": check,
                "store": store
            }
        }))
        self.rid = self.rid + 1

    def kv_delete(self, key):
        self.ws.send(simplejson.dumps({
            "Delete": {
                "rid": self.rid,
                "key": key
            }
        }))
        self.rid = self.rid + 1

    def mring_get_size(self):
        self.ws.send(simplejson.dumps({
            "GetSize": {
                "rid": self.rid,
            }
        }))
        self.rid = self.rid + 1

    def mring_set_size(self, size):
        self.ws.send(simplejson.dumps({
            "SetSize": {
                "rid": self.rid,
                "size": size,
            }
        }))
        self.rid = self.rid + 1

    def mring_add_node(self, name):
        self.ws.send(simplejson.dumps({
            "AddNode": {
                "rid": self.rid,
                "node": name,
            }
        }))
        self.rid = self.rid + 1

    def on_open(self):
        # FIXME TODO call virtual ws.open channel ...
        True

    def set_name(self, name):
        self.name = name

    def set_node_id(self, id):
        self.node_id = id

    def set_host(self, host):
        self.host = host

    def set_port(self, port):
        self.port = port

    def on(self, msg_type, handler):
        if msg_type in self.handlers:
            raise RuntimeError('Handler for message type ' + msg_type + ' already registered')
        self.handlers[msg_type] = handler;

    def status(self):
        url = "http://{}:{}/status"
        headers = { 'Content-type': 'application/json' }
        response = requests.get(url.format(self.host, self.port), headers=headers);
        return response

    def register(self):
        url = "http://{}:{}/node/{}"
        response = requests.post(url.format(self.host, self.port, self.node_id));
        return response

    def get(self,k):
        url = "http://{}:{}/data/{}"
        response = requests.get(url.format(self.host, self.port, k));
        return response

    def put(self,k,v):
        url = "http://{}:{}/data/{}"
        headers = { 'Content-type': 'application/json' }
        response = requests.post(url.format(self.host, self.port, k), v, headers=headers);
        return response

    def cas(self,k,v):
        url = "http://{}:{}/data/{}/cas"
        headers = { 'Content-type': 'application/json' }
        response = requests.post(url.format(self.host, self.port, k), v, headers=headers);
        return response

    def delete(self,k):
        url = "http://{}:{}/data/{}"
        response = requests.delete(url.format(self.host, self.port, k));
        return response
