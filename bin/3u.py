#!/usr/bin/env python3

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

import sys
import contrib
import simplejson

config_file = 'contrib/3u.json'

#
# Simplistic script to spin up a 3 node raft cluster on localhost
# - No registration/join events
# - Resets storage as a side-effect
# - Press any key to shutdown server
#

class Cluster():

    def __init__(self):
        self.servers = []
        self.clients = []

    def configure(self, config_file):
        config = simplejson.load(open(config_file, 'r'))
        ports = [];
        for node in config:
            ports = ports + [node['port']]

        print(ports)

        for node in config:
            port = node['port']
            id = node['id']

            rc = contrib.RaftClient()
            rc.set_node_id(id)
            rc.set_port(port)
            rc.set_host("localhost")
            self.clients.append(rc)

            us = contrib.UringServer()
            us.set_node_id(id)
            us.set_node_ports(ports)
            if id == '1':
                us.set_bootstrap(True)
            us.reset()
            us.start()
            self.servers.append(us)


            ports = contrib.rotate(ports)
 
            print('{}: {}'.format(id, port))

cluster = Cluster()
cluster.configure(config_file)
print('{}'.format(cluster.clients))

for line in sys.stdin.readline():
    print(line)
    for server in cluster.servers:
        print("Killing: {}".format(server.id))
        server.die()

