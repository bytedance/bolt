#!/usr/bin/env bash
# Copyright (c) Facebook, Inc. and its affiliates.
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
# --------------------------------------------------------------------------
# Copyright (c) ByteDance Ltd. and/or its affiliates.
# SPDX-License-Identifier: Apache-2.0
#
# This file has been modified by ByteDance Ltd. and/or its affiliates on
# 2025-11-11.
#
# Original file was released under the Apache License 2.0,
# with the full license text available at:
#     http://www.apache.org/licenses/LICENSE-2.0
#
# This modified file is released under the same license.
# --------------------------------------------------------------------------

CUR_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" > /dev/null && pwd)"

set -euo pipefail

OS_TYPE=$(uname -s)
ARCH_TYPE=$(uname -m)
CPP_STANDARD="gnu17"

function download_file() {
  local url="$1"
  local dest="$2"

  if command -v wget > /dev/null 2>&1; then
    wget --progress=dot:mega "$url" -O "$dest"
  elif command -v curl > /dev/null 2>&1; then
    echo "wget not found, using curl..."
    curl -L "$url" -o "$dest"
  else
    echo "❌ Error: Neither 'wget' nor 'curl' found."
    return 1
  fi
}

function check_basic_tools() {
  local all_installed=true
  if command -v wget > /dev/null 2>&1; then
    echo "✅ Already installed wget"
  elif command -v curl > /dev/null 2>&1; then
    echo "wget not found, using curl..."
  else
    echo "❌ Error: Neither 'wget' nor 'curl' found."
    all_installed=false
  fi

  local tools=("make" "cmake" "ninja")

  for tool in "${tools[@]}"; do
    if command -v "$tool" > /dev/null 2>&1; then
      printf "✅ Already installed %s\n" "$tool"
    else
      printf "❌ Not installed %s\n" "$tool"
      all_installed=false
    fi
  done

  if ! $all_installed; then
    if [[ "$OS_TYPE" == "Darwin" ]]; then
      echo "👉 Please install missing tools using brew install wget make cmake ninja"
    else
      echo "👉 Please install missing tools using your package manager (apt/yum)."
    fi
    return 1
  else
    echo "All required tools have been installed!"
    return 0
  fi
}

function check_cpp_standard() {
  local COMPILER_CMD="g++"
  if [[ "$OS_TYPE" == "Darwin" ]] && command -v g++ > /dev/null; then
    COMPILER_CMD="g++"
  elif [ -n "${CXX:-}" ]; then
    COMPILER_CMD="$CXX"
  fi

  if ! command -v "$COMPILER_CMD" > /dev/null 2>&1; then
    echo "❌ Compiler '$COMPILER_CMD' not found."
    return 1
  fi
  local TEST_FILE=$(mktemp /tmp/test_compiler.XXXXXX)
  echo "int main(){ return 0; }" > "$TEST_FILE"

  if "$COMPILER_CMD" -std=$CPP_STANDARD -fsyntax-only "$TEST_FILE" > /dev/null 2>&1; then
    echo "✅ Compiler $COMPILER_CMD supports $CPP_STANDARD."
    rm -f "$TEST_FILE"
    return 0
  else
    echo "❌ Compiler ('$COMPILER_CMD') too old, does not support $CPP_STANDARD."
    $COMPILER_CMD --version
    rm -f "$TEST_FILE"
    if [[ "$OS_TYPE" == "Darwin" ]]; then
      echo "👉 Try: brew install gcc@12 or higher."
    else
      echo "👉 Please install gcc-12 or higher."
    fi
    return 1
  fi
}

function check_compiler() {
  if [[ "$OS_TYPE" == "Darwin" ]]; then
    if /usr/bin/gcc --version 2>&1 | grep -q "Apple clang"; then
      echo "✅ Detected Apple Clang."
      return 0
    fi
  fi
  if command -v gcc &> /dev/null; then
    gcc_version=$(gcc -dumpversion | cut -f1 -d.)

    if [ "$gcc_version" -ge 10 ]; then
      echo "✅ GCC version: $gcc_version"
      return 0
    fi
  else
    echo "❌ GCC is not installed"
  fi
  if [[ "$OS_TYPE" == "Darwin" ]]; then
    echo "👉 On macOS, install Xcode Command Line Tools or 'brew install gcc'."
  else
    echo "Install compiler by running(with root user): bash ${CUR_DIR}/install-gcc.sh"
  fi
  return 1
}

function runtime_conf_path() {
  local SHELL_CONFIG="$HOME/.bashrc"
  if [[ "$OS_TYPE" == "Darwin" && "$SHELL" == */zsh ]]; then
    SHELL_CONFIG="$HOME/.zshrc"
  elif [[ "$OS_TYPE" == "Darwin" ]]; then
    SHELL_CONFIG="$HOME/.bash_profile"
  fi
  echo "$SHELL_CONFIG"
}

function install_python_dep() {
  local SHELL_CONFIG=$(runtime_conf_path)
  local CONDA_INSTALL_DIR="$HOME/miniconda3"
  if [ -d $CONDA_INSTALL_DIR ]; then
    echo "Conda has been installed"
  else
    echo "Installing conda"
    MINICONDA_URL_BASE="https://repo.anaconda.com/miniconda"
    MINICONDA_VERSION="py310_23.1.0-1"
    if [[ "$OS_TYPE" == "Darwin" ]]; then
      if [[ "$ARCH_TYPE" == "arm64" ]]; then
        MINICONDA_URL="${MINICONDA_URL_BASE}/Miniconda3-${MINICONDA_VERSION}-MacOSX-arm64.sh"
      else
        MINICONDA_URL="${MINICONDA_URL_BASE}/Miniconda3-${MINICONDA_VERSION}-MacOSX-x86_64.sh"
      fi
    else
      MINICONDA_URL="${MINICONDA_URL_BASE}/Miniconda3-${MINICONDA_VERSION}-Linux-$(arch).sh"
    fi

    download_file "${MINICONDA_URL}" /tmp/miniconda.sh
    chmod +x /tmp/miniconda.sh && /tmp/miniconda.sh -b -u -p $CONDA_INSTALL_DIR && rm -f /tmp/miniconda.sh
    echo "export PATH=$CONDA_INSTALL_DIR/bin:\$PATH" >> "$SHELL_CONFIG"
    export PATH="$CONDA_INSTALL_DIR/bin:$PATH"
  fi
  # shellcheck source=/dev/null
  $CONDA_INSTALL_DIR/bin/pip install --upgrade pip || true
  $CONDA_INSTALL_DIR/bin/pip install -r "${CUR_DIR}/../requirements.txt"
}

function check_conan() {
  if [ -z "${CONAN_HOME:-}" ]; then
    export CONAN_HOME=~/.conan2
  fi
  SETTING_KEY="compiler.cppstd"
  PROFILE_FILE="${CONAN_HOME}/profiles/default"
  if [ ! -f "${CONAN_HOME}/profiles/default" ]; then
    conan profile detect
  fi

  echo "Configuring conan profile to use $CPP_STANDARD standard by default"
  if grep -q "^${SETTING_KEY}=${CPP_STANDARD}$" "${PROFILE_FILE}"; then
    echo "✅ ${SETTING_KEY} is already set to ${CPP_STANDARD}. Skipping."
  elif grep -q "^${SETTING_KEY}=" "${PROFILE_FILE}"; then
    echo "🔄 Updating ${SETTING_KEY} to ${CPP_STANDARD}..."
  else
    echo "➕ Adding ${SETTING_KEY}=${CPP_STANDARD} to ${PROFILE_FILE}..."
    echo "${SETTING_KEY}=${CPP_STANDARD}" >> "${PROFILE_FILE}"
  fi
}

function install_git_hooks() {
  # ensure pre-commit has been installed
  if ! command -v "pre-commit" > /dev/null 2>&1; then
    echo "❌ Expect pre-commit has been installed"
    return 1
  fi
  # install hooks
  pushd "${CUR_DIR}/../" > /dev/null
  if [ -f ".pre-commit-config.yaml" ]; then
    pre-commit install
    echo "✅ Installing all git hooks successfully"
    popd > /dev/null
    return 0
  fi
  # error handling
  echo "❌ Expect .pre-commit-config.yaml exists in path ${CUR_DIR}/../"
  popd > /dev/null
  return 1
}

check_basic_tools
check_compiler
check_cpp_standard
install_python_dep
check_conan
install_git_hooks

install_bolt_deps_script="${CUR_DIR}/install-bolt-deps.sh"
if [ -f "${install_bolt_deps_script}" ]; then
  bash "${install_bolt_deps_script}" "$@"
else
  echo "⚠️  Warning: ${install_bolt_deps_script} not found, skipping."
fi

echo "💡💡💡 Please execute 'source $(runtime_conf_path)' to activate your env! 💡💡💡"
