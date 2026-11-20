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
from .project import ProjectExpression


class NamedExpression(ProjectExpression):
    """
    Expression that is meant to be assigned to a column with target name.
    """

    def __init__(self, name: str, expr: Expression):
        super().__init__(expr)
        self.__name = name

    @property
    def name(self) -> str:
        return self.__name

    def expr(self) -> str:
        return f"{self.inner().expr()} AS {self.__name}"
