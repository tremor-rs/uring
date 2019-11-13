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

            rc = RaftClient()
            rc.set_node_id(id)
            rc.set_port(port)
            rc.set_host("localhost")
            self.clients.append(rc)

            us = UringServer()
            us.set_node_id(id)
            us.set_node_ports(ports)
            if id == '1':
                us.set_bootstrap(True)
            us.reset()
            us.start()
            self.servers.append(us)


            ports = rotate(ports)

            print('{}: {}'.format(id, port))

    def adjoin(self):
        time.sleep(10)
        for node in self.config:
            id = node['id']
            if id != '1':
                requests.post("http://localhost:8081/node/{}".format(id))

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

        self.cmd = subprocess.Popen(cmd.split(), cwd = './')
        print("Started process with id: {}".format(self.cmd.pid))

    def die(self):
        self.cmd.kill()

class RaftClient():
    """Handles client interactions to raft node."""

    def __init__(self):
        """Constructs a new raft client."""
        self.node_id = None
        self.host = None
        self.port = None
        self.handlers = {}
        self.callbacks = {}

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


