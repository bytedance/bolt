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

# Configure Bolt's Conan recipe index. Release tags use matching recipe tags;
# other checkouts use main unless --branch selects a branch or tag explicitly.

readonly SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" > /dev/null && pwd)"
readonly BOLT_ROOT="$(git -C "${SCRIPT_DIR}" rev-parse --show-toplevel)"
readonly DEFAULT_CCI_REPOSITORY="https://github.com/bytedance/conan-center-index.git"
readonly CCI_HOME="${CONAN_HOME:-${HOME}/.conan2}/conan-center-index"

usage() {
  echo "Usage: $0 [--branch <branch-or-tag>] [--repository <repository-url>]"
}

die() {
  echo "❌ Error: $*" >&2
  exit 1
}

parse_args() {
  CCI_REF=""
  CCI_REPOSITORY="${DEFAULT_CCI_REPOSITORY}"

  while [[ $# -gt 0 ]]; do
    case "$1" in
      --branch)
        [[ $# -ge 2 && -n "$2" ]] || die "--branch requires a value"
        CCI_REF="$2"
        shift 2
        ;;
      --repository)
        [[ $# -ge 2 && -n "$2" ]] || die "--repository requires a value"
        CCI_REPOSITORY="$2"
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
  local ref="$1"
  if [[ -z "$ref" ]]; then
    ref=$(git -C "${BOLT_ROOT}" describe --exact-match --tags HEAD 2> /dev/null || true)
  fi
  echo "${ref:-main}"
}

download_conan_recipes() {
  local cci_repository="$1"
  local cci_ref="$2"

  echo "ℹ️  Cloning conan-center-index at ${cci_ref} from ${cci_repository}..."
  rm -rf "${CCI_HOME}"
  git clone --quiet --depth 1 --branch "$cci_ref" "${cci_repository}" "${CCI_HOME}"
}

configure_conan_remote() {
  local remote_name="bolt-cci-local"
  echo "⚙️  Configuring remote '${remote_name}'..."
  conan remote remove "$remote_name" > /dev/null 2>&1 || true
  conan remote add --index 0 --type local-recipes-index \
    "$remote_name" "$CCI_HOME" > /dev/null
}

main() {
  parse_args "$@"
  command -v conan > /dev/null 2>&1 || die "'conan' command not found"
  local cci_ref
  cci_ref=$(resolve_cci_ref "$CCI_REF")
  download_conan_recipes "$CCI_REPOSITORY" "$cci_ref"
  configure_conan_remote

  echo "🎉 All done! Conan remotes configured."
}

if [[ "${BASH_SOURCE[0]}" == "$0" ]]; then
  main "$@"
fi
