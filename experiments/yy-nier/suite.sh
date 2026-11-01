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

# Runs CI-equivalent validation steps in the current checkout.
#
# Usage: suite.sh quick | full | STEP...
# Every step runs even after an earlier failure; each prints its own exit.
set -uo pipefail

case "${1:-quick}" in
    quick) steps=(lint check test) ;;
    full) steps=(lockfile lint check envelope-fault test licenses msrv s3 managed-sync linux-fuse linux-direct linux-operations) ;;
    *) steps=("$@") ;;
esac

# GitHub's Check, Test and MSRV jobs run without storage settings; only the
# acceptance jobs export them. Mirror that split so offline behavior is tested.
offline() {
    env -u YINYANG_S3_BUCKET -u YINYANG_S3_ENDPOINT -u YINYANG_S3_REGION \
        -u YINYANG_S3_ACCESS_KEY_ID -u YINYANG_S3_SECRET_ACCESS_KEY \
        -u YINYANG_S3_ROOT -u YINYANG_STORAGE_PROFILE \
        -u YINYANG_DIRECT_VERSIONED_BUCKET -u YINYANG_SERVICE_TOKEN "$@"
}

failed=()
for step in "${steps[@]}"; do
    start=$(date +%s)
    echo "=== STEP $step begin $(date -Is)"
    case "$step" in
        lockfile) offline cargo update -w --locked ;;
        lint) offline cargo x lint ;;
        check) offline cargo x check ;;
        envelope-fault) offline python3 tests/linux_operating_envelope_fault_test.py ;;
        test) offline cargo x test ;;
        licenses) offline cargo x licenses ;;
        msrv) offline cargo +1.91.0 check --workspace --all-targets --locked ;;
        s3) cargo test --workspace --all-targets s3_ -- --ignored --nocapture \
                --skip isolated_s3_cli_smoke --skip real_amazon_s3_cli_smoke --skip direct_amazon_s3_ ;;
        managed-sync) cargo x test-managed-sync ;;
        linux-fuse) cargo x test-linux-fuse ;;
        linux-direct) cargo x test-linux-direct ;;
        linux-operations) cargo x test-linux-operations ;;
        *) echo "unknown step: $step"; false ;;
    esac
    rc=$?
    echo "=== STEP $step exit=$rc seconds=$(( $(date +%s) - start ))"
    if [[ $rc -ne 0 ]]; then
        failed+=("$step")
    fi
done
echo "=== SUITE steps=${steps[*]} failed=${failed[*]:-none}"
test "${#failed[@]}" -eq 0
