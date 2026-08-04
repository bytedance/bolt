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

# Download the conan recipes for Bolt dependencies from the https://github.com/bytedance/conan-center-index.git.
# And configure it as a local conan remote for building Bolt dependencies.
#
# For each Bolt release, a corresponding tag is created in bytedance/conan-center-index. This script downloads the Conan recipes from the tag matching the current Bolt version and configures them as a local Conan remote.
# If the --branch argument is provided, it must be a valid branch or tag in the conan-center-index repository.

readonly SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" > /dev/null && pwd)"
readonly BOLT_ROOT="$(git -C "${SCRIPT_DIR}" rev-parse --show-toplevel)"
readonly CCI_REPOSITORY="https://github.com/bytedance/conan-center-index.git"
readonly CCI_HOME="${CONAN_HOME:-${HOME}/.conan2}/conan-center-index"

usage() {
  echo "Usage: $0 [--branch <branch-or-tag>]"
}

die() {
  echo "❌ Error: $*" >&2
  exit 1
}

parse_args() {
  CCI_REF=""

  while [[ $# -gt 0 ]]; do
    case "$1" in
      --branch)
        [[ $# -ge 2 && -n "$2" ]] || die "--branch requires a value"
        CCI_REF="$2"
        shift 2
        ;;
      -h | --help)
        usage
        exit 0
        ;;
      *)
        usage >&2
        die "Unknown argument: $1"
        ;;
    esac
  done
}

resolve_cci_ref() {
  if [[ -n "${CCI_REF}" ]]; then
    return
  fi

  # Bolt release tags have matching tags in bytedance/conan-center-index.
  CCI_REF="$(git -C "${BOLT_ROOT}" describe --exact-match --tags HEAD 2> /dev/null || true)"
  CCI_REF="${CCI_REF:-main}"
}

download_conan_recipes() {
  echo "ℹ️  Cloning conan-center-index at ${CCI_REF} from ${CCI_REPOSITORY}..."
  rm -rf "${CCI_HOME}"
  git clone --quiet --depth 1 --branch "${CCI_REF}" "${CCI_REPOSITORY}" "${CCI_HOME}"
}

update_conan_remote() {
  local remote_name="$1"
  local remote_url="$2"
  local remote_index="$3"
  local remote_type="${4:-}"

  echo "⚙️  Configuring remote '${remote_name}'..."
  conan remote remove "${remote_name}" > /dev/null 2>&1 || true

  if [[ -n "${remote_type}" ]]; then
    conan remote add --index "${remote_index}" --type "${remote_type}" "${remote_name}" "${remote_url}" > /dev/null
  else
    conan remote add --index "${remote_index}" "${remote_name}" "${remote_url}" > /dev/null
  fi
}

main() {
  parse_args "$@"
  command -v conan > /dev/null 2>&1 || die "'conan' command not found"
  resolve_cci_ref
  download_conan_recipes
  update_conan_remote "bolt-cci-local" "${CCI_HOME}" 0 "local-recipes-index"

  echo "🎉 All done! Conan remotes configured."
}
