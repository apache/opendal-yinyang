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

# Starts run.sh in a detached tmux session named yy-LABEL.
#
# Usage: start.sh LABEL BRANCH SHA COMMAND [ARG...]
set -euo pipefail
label=${1:?label required}
[[ "$label" =~ ^[a-z0-9][a-z0-9-]*$ ]] || { echo "invalid label: $label" >&2; exit 2; }
devbox=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
session="yy-$label"
if tmux has-session -t "=$session" 2>/dev/null; then
    echo "session already exists: $session" >&2
    exit 2
fi
if [[ -e "$HOME/.local/state/yinyang-nier/runs/$label.log" ]]; then
    echo "label already used: $label" >&2
    exit 2
fi
command=$(printf '%q ' "$devbox/run.sh" "$@")
tmux new-session -d -s "$session" "$command"
echo "started $session"
