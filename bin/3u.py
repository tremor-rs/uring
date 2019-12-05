#!/usr/bin/env python3

# Copyright 2018-2020, Wayfair GmbH
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
import requests

config_file = 'contrib/3u.json'

#
# Simplistic script to spin up a 3 node raft cluster on localhost
# - Resets storage as a side-effect
# - Press any key to shutdown server
#

cluster = contrib.Cluster()
cluster.configure(config_file)
cluster.adjoin()

for line in sys.stdin.readline():
    for server in cluster.servers:
        print("Killing: {}".format(server.id))
        server.die()

