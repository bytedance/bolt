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

from typing import Any, Optional

from pybolt import BoltType

from ..details import _inferPyboltType_
from .base import Expression


def isLiteral(value: Any) -> bool:
    return any((isinstance(value, t) for t in (bool, int, float, str, bytes)))


class LiteralExpression(Expression):
    def __init__(self, value: Any, dtype: Optional[BoltType] = None):
        if not isLiteral(value):
            raise TypeError(f"Invalid literal type: {type(value)}")
        self.__value: Any = value
        self.__dtype = dtype if dtype is not None else _inferPyboltType_(value)
        super().__init__(self.expr(), self.__dtype, None)

    @property
    def dtype(self) -> BoltType:
        return self.__dtype

    def expr(self) -> str:
        if isinstance(self.__value, str):
            return f"'{self.__value}'"
        if isinstance(self.__value, bool):
            return str(self.__value).upper()
        return str(self.__value)

    @property
    def value(self) -> Any:
        return self.__value

    def asLiteral(self) -> Optional["LiteralExpression"]:
        return self
