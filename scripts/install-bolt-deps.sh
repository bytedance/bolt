#!/usr/bin/env bash
# Copyright (c) ByteDance Ltd. and/or its affiliates.
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
#
set -euo pipefail

CUR_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" > /dev/null && pwd)"
cd "${CUR_DIR}"

CONAN_CENTER_COMMIT_ID="bad5c95b810e859c1c31553b92584246fe436d69"
CCI_HOME="${CONAN_HOME:-~/.conan2}/conan-center-index"

if ! command -v conan &> /dev/null; then
  echo "❌ Error: 'conan' command not found."
  exit 1
fi

# Does a shallow checkout of conan-center-index at the given commit id in $1
checkout_conan_center_index() {
  local target_commit="$1"
  local need_clone=true

  if [ -d "${CCI_HOME}/.git" ]; then
    echo "ℹ️  Found existing conan-center-index cache..."
    pushd "${CCI_HOME}" > /dev/null

    local current_commit
    current_commit=$(git rev-parse HEAD 2> /dev/null || echo "")

    if [ "${current_commit}" == "${target_commit}" ]; then
      echo "✅ Cache hit: conan-center-index is already at ${target_commit}. Skipping download."
      git reset --hard HEAD > /dev/null 2>&1
      git clean -fd > /dev/null 2>&1
      need_clone=false
    else
      echo "🔄 Cache outdated. Updating to ${target_commit}..."
      if git fetch --depth 1 origin "${target_commit}" > /dev/null 2>&1; then
        git checkout FETCH_HEAD > /dev/null 2>&1
        need_clone=false
      fi
    fi
    popd > /dev/null
  fi

  if $need_clone; then
    echo "⬇️  Cloning conan-center-index (Commit: ${target_commit})..."
    rm -rf "${CCI_HOME}"
    mkdir -p "${CCI_HOME}"
    pushd "${CCI_HOME}" > /dev/null
    git init -q
    git remote add origin https://github.com/conan-io/conan-center-index.git
    git fetch --depth 1 origin "${target_commit}"
    git checkout -q FETCH_HEAD
    popd > /dev/null
  fi
}

update_conan_remote() {
  local remote_name="$1"
  local remote_url="$2"
  local remote_type="${3:-}"

  echo "⚙️  Configuring remote '${remote_name}'..."
  conan remote remove "${remote_name}" > /dev/null 2>&1 || true

  if [ -n "$remote_type" ]; then
    conan remote add -t "$remote_type" "${remote_name}" "${remote_url}" > /dev/null
  else
    conan remote add "${remote_name}" "${remote_url}" > /dev/null
  fi
}

update_conan_remote "bolt-local" "${CUR_DIR}/conan" "local-recipes-index"

checkout_conan_center_index "${CONAN_CENTER_COMMIT_ID}"

echo "🩹 Applying patches..."
for patch_file in "${CUR_DIR}/conan/patches"/*.patch; do
  if [ ! -f "$patch_file" ]; then
    continue
  fi
  patch_name=$(basename "$patch_file")

  if output=$(patch -p1 -f -N -d "${CCI_HOME}" -i "$patch_file" 2>&1); then
    echo "✅ Applied: $patch_name"
  else
    if echo "$output" | grep -q "Reversed (or previously applied) patch detected"; then
      echo "⚠️  Skipped: $patch_name (Already applied)"
    else
      echo "❌ Failed to apply $patch_name"
      echo "--- Patch Output ---"
      echo "$output"
      echo "--------------------"
      exit 1
    fi
  fi
done

update_conan_remote "bolt-cci-local" "${CCI_HOME}" "local-recipes-index"

update_conan_remote "conancenter" "https://center2.conan.io"

echo "🎉 All done! Conan remotes configured."
