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

from functools import wraps
from typing import Callable

from pybolt import BoltType

from .vector import ExecutorsArgument, normalizeExecutors, registerVectorFunction


def registerScalarFunction(
    fn: Callable, name, outputType: BoltType, executors: ExecutorsArgument
):
    @wraps(fn)
    def rowWrapper(*args, **kwargs):
        # Imports deferred into the closure body so cloudpickle does
        # not capture the bound names from this module's globals when
        # the wrapper traverses Ray's task argument serialiser. The
        # pybolt C extension exposes its free functions as PyCapsules
        # with ``__qualname__`` ``"PyCapsule.<name>"``; cloudpickle's
        # by-qualname lookup fails and falls back to inline pickling,
        # which dies with ``cannot pickle 'PyCapsule' object``. Once
        # any scalar UDF is registered on a ``RayExecutor``, every
        # subsequent ``StageTaskSpec.udfRegistry`` ships the wrapper
        # to remote workers and the entire driver-side Ray submit
        # fails. Deferred imports leave only the picklable ``pybolt``
        # module reference in the closure's captured globals;
        # ``fromList`` and ``_isVectorType_`` resolve on the worker
        # at call time, after pybolt has been initialised by
        # ``reregisterUdfsOnPybolt``.
        from pybolt import fromList

        from ..details import _isVectorType_

        output = []
        if len(args) == 0:
            raise ValueError(f"Scalar function {name} was not provided any argument.")
        if not _isVectorType_(args[0]):
            raise TypeError(
                f"Scalar function {name} first argument {args[0]} is not a vector."
            )

        nrows = len(args[0])
        for i in range(nrows):
            args_ = tuple(
                arg[i] if (_isVectorType_(arg) and len(arg) == nrows) else arg
                for arg in args
            )
            # Most of the argument are expected to be bolt column vector.
            # For those, the user expect to get the vector element at the
            # row index `i`.
            # However, for other scalar values provided as UDF arguments,
            # the user expects to get the value.
            # For instance, in the expression: "my_udf(c0, 4)", the
            # user expects to get c0[i] and 4 as arguments  for each row `i`.
            kwargs_ = {
                k: v[i] if (_isVectorType_(v) and len(v[i]) == nrows) else v
                for k, v in kwargs.items()
            }
            output.append(fn(*args_, **kwargs_))
        return fromList(output)

    registerVectorFunction(rowWrapper, name, outputType, executors, False)
    return rowWrapper


def scalarFunction(
    outputType: BoltType,
    executors: ExecutorsArgument = None,
):
    def decorator(func):
        return registerScalarFunction(
            func, func.__name__, outputType, normalizeExecutors(executors)
        )

    return decorator
