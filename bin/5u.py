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
import requests
import websockets
import threading
import asyncio
import subprocess

config_file = 'contrib/5u.json'

#
# Simplistic script to spin up a 5 node raft cluster on localhost
# - Resets storage as a side-effect
# - Press any key to shutdown server
#

cluster = contrib.Cluster()
cluster.configure(config_file)
cluster.adjoin()

async def wss_loop(wss, path):
    while True:
        args = await wss.recv()
        cmd = args.split()
        if cmd[0] == 'pause':
            for server in cluster.servers:
                if server.id == cmd[1]:
                    print(f'Server {server.id} pause')
                    subprocess.run(["kill", "-STOP", f"{server.pid()}"])
        if cmd[0] == 'resume':
            for server in cluster.servers:
                if server.id == cmd[1]:
                    print(f'Server {server.id} resume')
                    subprocess.run(["kill", "-CONT", f"{server.pid()}"])
            await wss.send(args)
        if cmd[0] == 'quit':
            await wss.send('bye')
            await wss.close()
            break
    await wss.close()

def wss():
    wss = websockets.serve(wss_loop, "0.0.0.0", 8000)
    loop = asyncio.get_event_loop()
    loop.run_until_complete(wss)
    loop.run_forever()

t = threading.Thread(target=wss())
t.start()

for line in sys.stdin.readline():
    for server in cluster.servers:
        print("Killing: {}".format(server.id))
        server.die()

