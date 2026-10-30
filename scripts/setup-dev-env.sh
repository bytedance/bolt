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

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" > /dev/null && pwd)"

set -euo pipefail

OS_TYPE=$(uname -s)
ARCH_TYPE=$(uname -m)
CLANG_COMPILER=""
CLANG_LINKER=""
CONDA_INSTALL_DIR="$HOME/miniconda3"

run_as_root() {
  if [[ $EUID -eq 0 ]]; then
    "$@"
  else
    sudo "$@"
  fi
}

download_file() {
  local url="$1"
  local dest="$2"

  if command -v wget > /dev/null 2>&1; then
    wget --progress=dot:mega "$url" -O "$dest"
  elif command -v curl > /dev/null 2>&1; then
    echo "wget not found, using curl..."
    curl -fL "$url" -o "$dest"
  else
    echo "❌ Error: Neither 'wget' nor 'curl' found."
    return 1
  fi
}

check_basic_tools() {
  local missing_tools=()
  local tool
  local tools=("cmake" "make" "git" "wget" "curl")

  for tool in "${tools[@]}"; do
    if command -v "$tool" > /dev/null 2>&1; then
      printf "✅ Already installed %s\n" "$tool"
    else
      missing_tools+=("$tool")
    fi
  done

  if ((${#missing_tools[@]} == 0)); then
    return 0
  fi

  printf 'Installing missing tools: %s\n' "${missing_tools[*]}"
  if [[ "$OS_TYPE" == "Darwin" ]]; then
    if ! command -v brew > /dev/null 2>&1; then
      echo "❌ Install Homebrew (https://brew.sh) before running setup."
      return 1
    fi
    brew install "${missing_tools[@]}"
  elif command -v apt-get > /dev/null 2>&1; then
    run_as_root apt-get update
    run_as_root apt-get install -y "${missing_tools[@]}"
  elif command -v dnf > /dev/null 2>&1; then
    run_as_root dnf install -y "${missing_tools[@]}"
  elif command -v yum > /dev/null 2>&1; then
    run_as_root yum install -y "${missing_tools[@]}"
  else
    echo "❌ No supported package manager found. Install ${missing_tools[*]} manually."
    return 1
  fi

  for tool in "${missing_tools[@]}"; do
    if ! command -v "$tool" > /dev/null 2>&1; then
      printf '❌ Required tool still missing after installation: %s\n' "$tool"
      return 1
    fi
  done
}

find_installed_clang() {
  CLANG_COMPILER=""
  local search_dirs=()
  IFS=: read -r -a search_dirs <<< "$PATH"
  search_dirs+=(/usr/lib/llvm-*/bin /opt/homebrew/opt/llvm/bin /usr/local/opt/llvm/bin)
  local dir candidate version compiler_bin installed_version
  local newest_major=21
  for dir in "${search_dirs[@]}"; do
    for candidate in "${dir:-.}/clang" "${dir:-.}"/clang-[0-9]*; do
      [[ -x "$candidate" ]] || continue
      version=$("$candidate" -dumpversion 2> /dev/null) || continue
      [[ "$version" =~ ^[0-9]+(\.[0-9]+)*$ ]] || continue
      ((${version%%.*} > newest_major)) || continue
      # Use the compiler's installation directory, not an alternatives symlink.
      compiler_bin=$("$candidate" --version | sed -n 's/^InstalledDir: //p')
      installed_version=$("$compiler_bin/clang" -dumpversion 2> /dev/null) || installed_version=""
      newest_major=${version%%.*}
      if [[ "$installed_version" == "$version" ]]; then
        CLANG_COMPILER="$compiler_bin/clang"
      else
        # A versioned executable can coexist with an older unversioned clang.
        CLANG_COMPILER="$(cd "$(dirname "$candidate")" && pwd)/$(basename "$candidate")"
      fi
    done
  done
  [[ -n "$CLANG_COMPILER" ]] || return 1

  echo "✅ Found Clang $newest_major: $CLANG_COMPILER; skipping LLVM installation."
}

find_clang_linker() {
  local version
  version=$("$CLANG_COMPILER" -dumpversion)
  local linker_name=ld.lld
  [[ "$OS_TYPE" != "Darwin" ]] || linker_name=ld64.lld
  CLANG_LINKER="$(dirname "$CLANG_COMPILER")/$linker_name"
  if [[ ! -x "$CLANG_LINKER" ]]; then
    CLANG_LINKER=$(command -v "$linker_name-${version%%.*}" || command -v "$linker_name" || true)
    if [[ -z "$CLANG_LINKER" && "$OS_TYPE" == "Darwin" ]] && command -v brew > /dev/null 2>&1; then
      CLANG_LINKER="$(brew --prefix lld)/bin/$linker_name"
    fi
  fi
}

install_llvm_macos() {
  if ! command -v brew > /dev/null 2>&1; then
    echo "❌ Install Homebrew (https://brew.sh) before running setup."
    return 1
  fi
  brew update
  brew install llvm lld
  brew upgrade llvm lld
  CLANG_COMPILER="$(brew --prefix llvm)/bin/clang"
  CLANG_LINKER="$(brew --prefix lld)/bin/ld64.lld"
  export PATH="$(brew --prefix llvm)/bin:$(brew --prefix lld)/bin:$PATH"
}

install_llvm_debian() {
  run_as_root apt-get update
  run_as_root apt-get install -y lsb-release wget software-properties-common gnupg

  local llvm_script
  llvm_script=$(mktemp /tmp/bolt-llvm.XXXXXX)
  download_file "https://apt.llvm.org/llvm.sh" "$llvm_script"
  # Follow the official stable release, including future major versions.
  local llvm_version
  llvm_version=$(sed -n 's/^CURRENT_LLVM_STABLE=\([0-9][0-9]*\)$/\1/p' "$llvm_script")
  if [[ ! "$llvm_version" =~ ^[0-9]+$ ]] || ((llvm_version < 22)); then
    rm -f "$llvm_script"
    echo "❌ Cannot determine a supported stable LLVM version (Clang 22+ required)."
    return 1
  fi
  run_as_root bash "$llvm_script" "$llvm_version" all
  rm -f "$llvm_script"
  # The official installer's "all" set does not include LLVM BOLT.
  run_as_root apt-get install -y "bolt-${llvm_version}"
  CLANG_COMPILER="/usr/lib/llvm-${llvm_version}/bin/clang"
  CLANG_LINKER="/usr/bin/ld.lld-${llvm_version}"
  export PATH="/usr/lib/llvm-${llvm_version}/bin:$PATH"
}

check_llvm_toolchain() {
  local version
  version=$("$CLANG_COMPILER" -dumpversion)
  if ((${version%%.*} < 22)); then
    echo "❌ Clang 22 or newer is required; found ${version}."
    return 1
  fi
  if [[ ! -x "$CLANG_LINKER" ]]; then
    echo "❌ Clang is installed, but LLD is missing. Install the matching LLD package."
    return 1
  fi
  check_libcxx
}

install_clang() {
  if find_installed_clang; then
    find_clang_linker
  elif [[ "$OS_TYPE" == "Darwin" ]]; then
    install_llvm_macos
  elif command -v apt-get > /dev/null 2>&1; then
    install_llvm_debian
  else
    echo "❌ Automatic Clang installation supports Debian/Ubuntu and macOS."
    echo "Install the latest stable Clang (22+) and LLD, then follow the manual setup in CONTRIBUTING.md."
    return 1
  fi
  check_llvm_toolchain
}

check_libcxx() {
  local probe_dir
  probe_dir=$(mktemp -d /tmp/bolt-libcxx.XXXXXX)
  # Compile and link so missing libc++ headers and runtime libraries both fail.
  if ! printf '#include <string>\nint main() { return std::string("bolt").size() != 4; }\n' \
    | "$CLANG_COMPILER" --driver-mode=g++ -x c++ -std=c++20 -stdlib=libc++ \
      "-fuse-ld=$CLANG_LINKER" - -o "$probe_dir/probe"; then
    rm -rf "$probe_dir"
    echo "❌ Cannot compile and link with libc++. Install libc++ and libc++abi development packages matching Clang."
    return 1
  fi
  rm -rf "$probe_dir"
}

runtime_conf_path() {
  local shell_config="$HOME/.bashrc"
  if [[ "$OS_TYPE" == "Darwin" && "$SHELL" == */zsh ]]; then
    shell_config="$HOME/.zshrc"
  elif [[ "$OS_TYPE" == "Darwin" ]]; then
    shell_config="$HOME/.bash_profile"
  fi
  echo "$shell_config"
}

persist_llvm_path() {
  local llvm_bin="$1"
  local linker_bin="$2"
  local shell_config
  shell_config=$(runtime_conf_path)
  local path_line
  printf -v path_line 'export PATH=%q:%q:$PATH' "$llvm_bin" "$linker_bin"
  touch "$shell_config"
  if ! grep -Fxq "$path_line" "$shell_config"; then
    printf '\n%s\n' "$path_line" >> "$shell_config"
  fi
}

register_llvm_alternatives() {
  local llvm_bin="$1"
  local linker_bin="$2"
  local version
  version=$("$CLANG_COMPILER" -dumpversion)
  local lld_driver="$linker_bin/lld-${version%%.*}"
  [[ -x "$lld_driver" ]] || lld_driver="$linker_bin/lld"
  local compiler_name
  compiler_name=$(basename "$CLANG_COMPILER")
  local tool tool_path
  for tool in clang clang++ lld ld.lld; do
    case "$tool" in
      clang) tool_path="$CLANG_COMPILER" ;;
      clang++) tool_path="$llvm_bin/${compiler_name/clang/clang++}" ;;
      lld) tool_path="$lld_driver" ;;
      ld.lld) tool_path="$CLANG_LINKER" ;;
    esac
    if [[ ! -x "$tool_path" ]]; then
      echo "❌ Required LLVM tool is missing: $tool_path"
      return 1
    fi
    run_as_root update-alternatives --install "/usr/bin/$tool" "$tool" \
      "$tool_path" "${version%%.*}"
    run_as_root update-alternatives --set "$tool" "$tool_path"
  done
}

set_default_llvm() {
  local llvm_bin linker_bin
  llvm_bin=$(dirname "$CLANG_COMPILER")
  linker_bin=$(dirname "$CLANG_LINKER")
  if [[ "$OS_TYPE" == "Darwin" ]]; then
    persist_llvm_path "$llvm_bin" "$linker_bin"
  else
    register_llvm_alternatives "$llvm_bin" "$linker_bin"
  fi
  export PATH="$llvm_bin:$linker_bin:$PATH"
  hash -r
}

miniconda_url() {
  local version="py310_23.1.0-1"
  local platform="Linux-$ARCH_TYPE"
  if [[ "$OS_TYPE" == "Darwin" ]]; then
    platform="MacOSX-x86_64"
    [[ "$ARCH_TYPE" != "arm64" ]] || platform="MacOSX-arm64"
  fi
  echo "https://repo.anaconda.com/miniconda/Miniconda3-${version}-${platform}.sh"
}

install_miniconda() {
  if [[ -d "$CONDA_INSTALL_DIR" ]]; then
    echo "Conda has been installed"
    return 0
  fi

  echo "Installing conda"
  local installer_dir installer shell_config
  installer_dir=$(mktemp -d /tmp/bolt-miniconda.XXXXXX)
  # The installer requires a .sh filename even when invoked with bash.
  installer="$installer_dir/miniconda.sh"
  download_file "$(miniconda_url)" "$installer"
  bash "$installer" -b -u -p "$CONDA_INSTALL_DIR"
  rm -f "$installer"
  rmdir "$installer_dir"
  shell_config=$(runtime_conf_path)
  printf 'export PATH=%q:$PATH\n' "$CONDA_INSTALL_DIR/bin" >> "$shell_config"
}

install_python_dependencies() {
  # Use the same environment for installation and all subsequent setup steps.
  export PATH="$CONDA_INSTALL_DIR/bin:$PATH"
  "$CONDA_INSTALL_DIR/bin/pip" install --upgrade pip || true
  local requirements_file
  requirements_file=$(mktemp /tmp/bolt-python-requirements.XXXXXX)
  "$CONDA_INSTALL_DIR/bin/python" - "${SCRIPT_DIR}/../requirements.txt" "$requirements_file" << 'PY'
import importlib.metadata
import json
import pathlib
import sys

installed_requirement = None
try:
    distribution = importlib.metadata.distribution("pypaimon")
    direct_url = json.loads(distribution.read_text("direct_url.json") or "{}")
    vcs = direct_url.get("vcs_info", {})
    if vcs.get("vcs") == "git" and vcs.get("requested_revision"):
        installed_requirement = (
            f"pypaimon @ git+{direct_url.get('url')}@{vcs['requested_revision']}"
            f"#subdirectory={direct_url.get('subdirectory')}"
        )
except (importlib.metadata.PackageNotFoundError, ValueError):
    pass

requirements = []
for line in pathlib.Path(sys.argv[1]).read_text().splitlines(keepends=True):
    if line.strip() == installed_requirement:
        print(f"Already installed {line.strip()}; skipping installation.")
    else:
        requirements.append(line)
pathlib.Path(sys.argv[2]).write_text("".join(requirements))
PY
  "$CONDA_INSTALL_DIR/bin/pip" install -r "$requirements_file"
  rm -f "$requirements_file"
  # Existing minimum-version installations may not recognize newer Clang releases.
  "$CONDA_INSTALL_DIR/bin/pip" install --upgrade 'conan>=2.33.0'
}

configure_conan_profile() {
  # as a conan extension, add (libc, version) as conan settings.
  PATH="$(dirname "$CLANG_COMPILER"):$PATH" python "${SCRIPT_DIR}/configure-conan-profile.py" \
    --compiler="$CLANG_COMPILER" --libcxx=libc++ --linker="$CLANG_LINKER"
}

install_git_hooks() {
  if ! command -v pre-commit > /dev/null 2>&1; then
    echo "❌ Expect pre-commit has been installed"
    return 1
  fi
  if [[ ! -f "${SCRIPT_DIR}/../.pre-commit-config.yaml" ]]; then
    echo "❌ Expect .pre-commit-config.yaml exists in path ${SCRIPT_DIR}/../"
    return 1
  fi
  (cd "${SCRIPT_DIR}/../" && pre-commit install)
  echo "✅ Installing all git hooks successfully"
}

install_bolt_dependencies() {
  local script="${SCRIPT_DIR}/install-bolt-deps.sh"
  if [[ -f "$script" ]]; then
    bash "$script" "$@"
  else
    echo "⚠️  Warning: $script not found, skipping."
  fi
}

main() {
  check_basic_tools
  install_clang
  set_default_llvm
  install_miniconda
  install_python_dependencies
  configure_conan_profile
  echo "All required tools have been installed!"
  install_bolt_dependencies "$@"
  install_git_hooks
  echo "💡💡💡 Please execute 'source $(runtime_conf_path)' to activate your env! 💡💡💡"
}

if [[ "${BASH_SOURCE[0]}" == "$0" ]]; then
  main "$@"
fi
