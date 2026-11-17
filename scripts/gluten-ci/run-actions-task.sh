#!/usr/bin/env bash

# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

set -euo pipefail

work="$GITHUB_WORKSPACE/bolt-ci-work"
helpers="$GITHUB_WORKSPACE/bolt-ci-helpers/scripts/gluten-ci"
printf 'artifact_task=%s\n' "${CI_TASK//:/-}" >> "$GITHUB_OUTPUT"
mkdir -p "$work/upload/reports"
log="$work/upload/reports/task.log"

for pair in "source:$GLUTEN_SHA" "bolt:$BOLT_SHA" "driver:$DRIVER_SHA"; do
  checkout="$work/${pair%%:*}"
  test "$(git -C "$checkout" rev-parse --verify HEAD)" = "${pair#*:}"
done

# The old CI image's code user has passwordless sudo. Use an unprivileged
# identity with no supplementary groups or privilege escalation.
command -v setpriv > /dev/null
worker=(setpriv --reuid=65534 --regid=65534 --clear-groups --no-new-privs)
if "${worker[@]}" test -w /var/run/docker.sock; then
  echo "The task user must not have write access to the runner's Docker socket" >&2
  exit 1
fi
chmod go-w "$work" "$work/driver"
chown -hR 65534:65534 "$work/source" "$work/bolt"
install -d -m 0700 -o 65534 -g 65534 "$work/home" "$work/reports" "$work/cache"

artifacts="$work/native-input"
options=()
native_env=()
if [[ "$CI_TASK" == native ]]; then
  artifacts="$work/native-output"
  install -d -m 0700 -o 65534 -g 65534 "$artifacts"
  native_env=(
    "BOLT_CI_CONAN_REMOTE=http://conanserver:9300"
    "CCACHE_DIR=$work/cache/ccache"
    "CCACHE_SECONDARY_STORAGE=file:///data/ccache-data|layout=subdirs|read-only"
  )
  if "${worker[@]}" test -r /opt/bolt-arrow-cache.py && [[ -f /opt/bolt-arrow-cache.py ]]; then
    native_env+=("BOLT_CI_ARROW_CACHE_SCRIPT=/opt/bolt-arrow-cache.py")
  fi
  if [[ "$CI_MODE" == compile ]]; then
    options+=(--compile-only)
  elif [[ "$CI_MODE" == native ]]; then
    options+=(--native-only)
  fi
fi

status=0
"${worker[@]}" env -i "HOME=$work/home" "PATH=$PATH" "JAVA_HOME=$JAVA_HOME" LC_ALL=C.UTF-8 "GLUTEN_MAVEN_REPO=$work/cache/maven" "CONAN_HOME=$work/cache/conan" "GLUTEN_CACHE_ROOT=$work/cache/spark" "PIP_CACHE_DIR=$work/cache/pip" "GLUTEN_CI_HELPER_SHA=$HELPER_SHA" "GLUTEN_CI_DRIVER_SHA=$DRIVER_SHA" "GLUTEN_CI_CONFIG_SHA256=$GLUTEN_CI_CONFIG_SHA256" GIT_CONFIG_COUNT=1 GIT_CONFIG_KEY_0=safe.directory "GIT_CONFIG_VALUE_0=$work/driver" "${native_env[@]}" python3 "$helpers/run.py" --task "$CI_TASK" --source "$work/source" --bolt "$work/bolt" --driver "$work/driver" --reports "$work/reports" --artifacts "$artifacts" "${options[@]}" > "$log" 2>&1 || status=$?

staging_status=0
python3 "$helpers/actions.py" stage "$work" "$CI_TASK" "$status" || staging_status=$?
printf 'Gluten Bolt task %s finished with exit status %s. See the evidence artifact.\n' "$CI_TASK" "$status"
if ((status != 0)); then
  exit "$status"
fi
exit "$staging_status"
