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

#!/usr/bin/env sh

count=0

for file in $(find . -name '*.rs' | grep -v '/target')
do
    if ! grep 'Copyright 2018-2019, Wayfair GmbH' "$file" > /dev/null
    then
        echo "##[error] Copyright missing in $file"
        count=$((count + 1))
    fi
done

exit $count
