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

from .base import Expression


class FieldExpression(Expression):
    """
    Expression referencing a field in a dataframe.
    """

    def __init__(self, name: str, dataframe: "DataFrame"):  # noqa: F821
        self.__name = name
        self.__rowType = dataframe.dtype
        # Find and store field index during initialization for faster access
        try:
            self.__fieldIndex = self.__rowType.names().index(self.__name)
        except ValueError:
            raise ValueError(f"Field '{self.__name}' not found in DataFrame schema.")
        super().__init__(name, dataframe.dtype.findChild(name), dataframe)

    @property
    def name(self) -> str:
        return self.__name

    @property
    def fieldIndex(self) -> int:
        return self.__fieldIndex
