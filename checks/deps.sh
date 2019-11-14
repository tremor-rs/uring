#  Copyright 2018-2019, Wayfair GmbH
#
#  Licensed under the Apache License, Version 2.0 (the "License");
#  you may not use this file except in compliance with the License.
#  You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#  limitations under the License.

#!/bin/sh

if [ -d "$1" ]
then
    path="$1"
else
    path="."
fi

cnt=0

for d in $(remarshal -i $path/Cargo.toml -of json | jq -r '.dependencies | keys []')
do
    dep=$(echo $d | sed -e 's/-/_/g')
    if ! rg "use $dep(::|;| )" $path -trust > /dev/null
    then
        if ! rg "extern crate $dep;" $path -trust > /dev/null
        then
            if ! rg "[^a-z]$dep::" $path -trust > /dev/null
            then
                cnt=$((cnt + 1))
                echo "Not used: $d";
            fi

        fi
    fi
done

exit $cnt
