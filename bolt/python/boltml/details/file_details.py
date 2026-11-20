# Copyright (c) ByteDance Ltd. and/or its affiliates
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

from collections.abc import Iterable
from itertools import chain
from typing import Optional

from pybolt import FileSystem


class FileUtils:
    @staticmethod
    def listFiles(uri: str, fs: Optional[FileSystem] = None) -> Iterable[str]:
        """
        Use pybolt filesystem interface to recursively list files from a
        path/uri.
        """
        if fs is None:
            fs = FileSystem.get(uri)
        if fs.isDirectory(uri):
            return chain.from_iterable(
                FileUtils.listFiles(entry, fs) for entry in fs.list(uri)
            )
        else:
            return [uri]
