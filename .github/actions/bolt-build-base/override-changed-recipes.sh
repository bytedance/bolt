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
# Detect Conan recipes modified compared to a base branch and add them
# to an override remote at index 0. This ensures modified recipes
# rebuild from source while everything else reuses cached binaries
# from the CI remote.
#
# Two sources of recipe changes:
#   1. scripts/conan/patches/*.patch — CCI recipe patches
#   2. scripts/conan/recipes/<name>/  — bolt-local recipes
#
# Usage: ./scripts/override-changed-recipes.sh [base_ref]
#   base_ref: git ref to diff against (default: main)
set -euo pipefail

BASE_REF="${1:-main}"
CCI_HOME="${CONAN_HOME:-$HOME/.conan2}/conan-center-index"
OVERRIDE_DIR="${CONAN_HOME:-$HOME/.conan2}/patched-recipes"

changed_files=$(git diff --name-only "origin/${BASE_REF}" -- scripts/conan/ 2> /dev/null || true)
if [ -z "$changed_files" ]; then
  echo "ℹ️  No Conan recipe changes detected."
  exit 0
fi

rm -rf "${OVERRIDE_DIR}"
mkdir -p "${OVERRIDE_DIR}/recipes"

override_recipes=""

# 1. CCI patches: extract recipe names from patch diff content
for patch_file in $(echo "$changed_files" | grep '^scripts/conan/patches/.*\.patch$' || true); do
  if [ ! -f "$patch_file" ]; then
    continue
  fi
  for recipe in $(sed -n 's|.*recipes/\([^/]*\)/.*|\1|p' "$patch_file" | sort -u); do
    if [ -d "${CCI_HOME}/recipes/${recipe}" ] && [ ! -d "${OVERRIDE_DIR}/recipes/${recipe}" ]; then
      cp -r "${CCI_HOME}/recipes/${recipe}" "${OVERRIDE_DIR}/recipes/${recipe}"
      override_recipes="${override_recipes} ${recipe}"
    fi
  done
done

# 2. bolt-local recipes: extract recipe name from path
for recipe in $(echo "$changed_files" | sed -n 's|^scripts/conan/recipes/\([^/]*\)/.*|\1|p' | sort -u); do
  if [ -d "scripts/conan/recipes/${recipe}" ] && [ ! -d "${OVERRIDE_DIR}/recipes/${recipe}" ]; then
    cp -r "scripts/conan/recipes/${recipe}" "${OVERRIDE_DIR}/recipes/${recipe}"
    override_recipes="${override_recipes} ${recipe}"
  fi
done

if [ -z "$override_recipes" ]; then
  echo "ℹ️  No recipe overrides needed."
  exit 0
fi

conan remote add --index=0 -t "local-recipes-index" "bolt-patched" "${OVERRIDE_DIR}"
echo "✅ Override recipes:${override_recipes}"
