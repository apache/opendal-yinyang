#!/usr/bin/env bash
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements. See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership. The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License. You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied. See the License for the
# specific language governing permissions and limitations
# under the License.

# Waits up to MAX_SECONDS for a run to finish, then prints its status.
#
# Usage: wait.sh LABEL [MAX_SECONDS]
set -euo pipefail
label=${1:?label required}
max=${2:-1500}
runs="$HOME/.local/state/yinyang-nier/runs"
deadline=$(( $(date +%s) + max ))
while [[ ! -f "$runs/$label.exit" ]] && (( $(date +%s) < deadline )); do
    if ! tmux has-session -t "=yy-$label" 2>/dev/null; then
        sleep 2
        break
    fi
    sleep 10
done
exec "$(dirname "${BASH_SOURCE[0]}")/status.sh" "$label" 30
