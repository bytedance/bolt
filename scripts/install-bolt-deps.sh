
#!/usr/bin/env bash
# Copyright (c) 2025 ByteDance Ltd. and/or its affiliates.
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

CUR_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null && pwd )"

cd ${CUR_DIR}/

# Some dependencies are not available on conan-center,
# we need to export the recipes to local cache first.
function export_conan_recipes_to_local_cache() {
    cd ${CUR_DIR}/conan

    declare -A bolt_deps
    bolt_deps["folly"]="2022.10.31.00"
    bolt_deps["arrow"]="15.0.1-oss"
    bolt_deps["sonic-cpp"]="1.0.2"
    bolt_deps["ryu"]="2.0.1"
    bolt_deps["roaring"]="4.3.1"
    bolt_deps["utf8proc"]="2.11.0"
    bolt_deps["date"]="3.0.4-bolt"
    bolt_deps["llvm-core"]="13.0.0"

    for dep in "${!bolt_deps[@]}"; do
        ver=${bolt_deps[$dep]}
        conan export recipes/${dep}/all/conanfile.py --name ${dep} --version ${ver}
    done

    for dep in "${!bolt_deps[@]}"; do
        ver=${bolt_deps[$dep]}
        conan cache path ${dep}/${ver}@
        if [ $? -eq 0 ]; then
            echo "✅ The conan recipe of ${dep}/${ver} has been exported to local cache."
        else
            echo "❌ Failed to export ${dep}/${ver} to local cache."
            exit 1
        fi
    done
    cd -
}

export_conan_recipes_to_local_cache
