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

if ! conan remote list | grep -q 'bolt-local'; then
    conan remote add -t local-recipes-index bolt-local ${CUR_DIR}/conan
fi 

# Clone conan-center-index and apply Bolt's patch
cci_home=${CONAN_HOME-~/.conan2}/conan-center-index
conan_center_commit_id="bad5c95"
\rm -rf ${cci_home} && git clone https://github.com/conan-io/conan-center-index.git ${cci_home}
cd  ${cci_home} && git checkout ${conan_center_commit_id}
for patch_file in "${CUR_DIR}/conan/patches"/*.patch; do
    if [ ! -f "$patch_file" ]; then
        continue
    fi
    patch_name=$(basename "$patch_file")
    if patch -p1 -t -d "$cci_home" -i "$patch_file" >/dev/null 2>&1; then
        echo "✅ $patch_name has been applied to conan-center-index@${conan_center_commit_id} successfully"
    else
        echo "❌ Failed to apply $patch_name" >&2
    fi
done

if ! conan remote list | grep -q 'bolt-cci-local'; then
    conan remote add -t local-recipes-index bolt-cci-local ${cci_home}
fi 

# Move conancenter to the end of the list
if conan remote list | grep -q 'conancenter'; then
    conan remote remove conancenter
    conan remote add conancenter https://center2.conan.io
fi
