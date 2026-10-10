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
from typing import Optional, Sequence, Union

from pybolt import BoltType

from ..executor.base import Executor
from ..executor.local import LocalExecutor
from .register import UserFunctionRegister

functionRegister = UserFunctionRegister()

ExecutorsArgument = Optional[Union[Sequence[Executor], Executor]]


def normalizeExecutors(executors: ExecutorsArgument = None) -> list[Executor]:
    if executors is None:
        return [LocalExecutor()]
    if isinstance(executors, Executor):
        return [executors]
    return list(executors)


def registerVectorFunction(
    fn, name, outputType, executors: ExecutorsArgument, mapBatch: bool
):
    signature = inspect.signature(fn)
    defaultArgs = [
        v.default
        for _, v in signature.parameters.items()
        if v.default is not inspect.Parameter.empty
    ]
    for executor in normalizeExecutors(executors):
        executor.registerVectorFunction(
            fn,
            name,
            outputType,
            len(signature.parameters),
            defaultArgs,
            mapBatch,
        )
    functionRegister[fn] = outputType
    return fn


def vectorFunction(
    outputType: BoltType,
    executors: ExecutorsArgument = None,
):
    def decorator(fn):
        return registerVectorFunction(
            fn,
            fn.__name__,
            outputType,
            normalizeExecutors(executors),
            False,
        )

    return decorator
