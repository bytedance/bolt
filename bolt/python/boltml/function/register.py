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

import inspect
from dataclasses import dataclass
from typing import Callable, Union

from pybolt import BoltType


@dataclass
class UserFunction:
    name: str
    fn: Callable
    outputType: BoltType


class UserFunctionRegister:
    def __init__(self):
        self.__functions: dict[str, UserFunction] = {}

    def __iter__(self):
        return iter(self.__functions.values())

    @staticmethod
    def __fnName(fn) -> str:
        from .stateful import StatefulFunction

        # Check if it's a class first
        if inspect.isclass(fn):
            return fn.__name__
        # Check if it's an instance of StatefulFunction
        elif isinstance(fn, StatefulFunction):
            return fn.functionName()
        # Check if it's a function
        elif inspect.isfunction(fn):
            return fn.__name__
        # Default to class name of the instance
        else:
            return fn.__class__.__name__

    def __getitem__(self, fn: Union[Callable, "Aggregator"]):  # noqa: F821
        return self.__functions[UserFunctionRegister.__fnName(fn)]

    def __setitem__(
        self,
        fn: Union[Callable, "Aggregator"],  # noqa: F821
        outputType: BoltType,
    ):
        name = UserFunctionRegister.__fnName(fn)
        if name in self.__functions:
            raise RuntimeError(f"Function {name} has already been registered.")

        self.__functions[name] = UserFunction(name=name, fn=fn, outputType=outputType)

    def __contains__(self, fn: Union[Callable, "Aggregator"]):  # noqa: F821
        return UserFunctionRegister.__fnName(fn) in self.__functions
