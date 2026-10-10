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

import functools

import pyarrow
import pybolt

from ..details import _isVectorType_


def arrowAdapter(dtype=pyarrow.StructArray):
    """
    User function decorator to convert bolt input to arrow inputs and
    arrow outputs to bolt outputs.

    When using this decorator, you can implement your function as if
    inputs were pyarrow input instead of pybolt input.

    Adapter conversions work as follow:
    * Vector functions (including `Aggregator.aggregate()`):
      - Input: tuple[pybolt.BaseVector] -> tuple[pyarrow.StructArray]
      - Output: pyarrow.StructArray -> pybolt.BaseVector
    * Dataframe functions:
      - Input: DataFrame -> pybolt.RowVector -> pyarrow.StructArray
      - Output: pyarrow.StructArray -> pybolt.RowVector -> DataFrame

    The `dtype` argument can modify the input and output type of the decorated
    function. The argument is only used for dataframe functions.
    The accepted values are as follow:
    * `pyarrow.StructArray` will use the conversions described above.
    * `pyarrow.RecordBatch`:
      - Input: DataFrame -> pybolt.RowVector -> pyarrow.StructArray ->
               pyarrow.RecordBatch
      - Output: pyarrow.RecordBatch -> pyarrow.StructArray ->
               pybolt.RowVector -> DataFrame
    """

    # We need to check whether `dtype` is the user function or an arrow type.
    # This may vary whether the user uses `arrowAdapter` with or without
    # argument.
    _argIsArrowType_ = dtype.__name__ in dir(pyarrow)

    def decorator(fn):
        validTypes = [pyarrow.StructArray, pyarrow.RecordBatch]
        if _argIsArrowType_ and dtype not in validTypes:
            raise ValueError(f"Unsupported arrowDecorator type: '{dtype}'")

        # The adapter for vector functions
        def vectorAdapter(*args, **kwargs):
            if len(kwargs) != 0:
                raise ValueError(
                    "User vector functions does not support keywordarguments."
                )
            if len(args) == 0:
                raise ValueError(
                    "User vector functions does not support emptyparameter list."
                )

            def __maybeBoltToArrow(v):
                if _isVectorType_(v):
                    structArr = pybolt.exportToArrow(v)
                    if hasattr(dtype, "from_struct_array"):
                        return dtype.from_struct_array(structArr)
                    return structArr
                else:
                    return v

            args = [__maybeBoltToArrow(v) for v in args]
            result = fn(*args, **kwargs)
            if hasattr(result, "to_struct_array"):
                result = result.to_struct_array()
            return pybolt.importFromArrow(result)

        # The adapter for dataframe functions
        def dataframeAdapter(*args, **kwargs):
            from ..dataframe import DataFrame

            if len(args) == 0 or not isinstance(args[0], DataFrame):
                raise ValueError(
                    "DataFrame to arrow adapter requires first"
                    "non-keyword argument to be a RowVector."
                )
            arrowVector = pybolt.exportToArrow(args[0]._data_)
            if dtype is pyarrow.RecordBatch:
                arrowVector = pyarrow.RecordBatch.from_struct_array(arrowVector)

            args = (arrowVector, *args[1:])
            arrowResult = fn(*args, **kwargs)

            if dtype is pyarrow.RecordBatch:
                arrowResult = arrowResult.to_struct_array()
            pyboltResult = pybolt.importFromArrow(arrowResult)

            return DataFrame(pyboltResult)

        # The generic adapter that will call either the vector function
        # adapter or the dataframe adapter.
        @functools.wraps(fn)
        def switchAdapter(*args, **kwargs):
            from ..dataframe import DataFrame

            if len(args) > 0 and isinstance(args[0], DataFrame):
                return dataframeAdapter(*args, **kwargs)
            else:
                return vectorAdapter(*args, **kwargs)

        return switchAdapter

    # When the arrowAdapter decorator is used without arguments,
    # the user function will be provided as argument to it. In that
    # case, `dtype` will contain the user function.
    # Therefore, we return the adapter function wrapping the
    # user function.
    if not _argIsArrowType_:
        return decorator(dtype)
    # When the arrowAdapter decorator is used with arguments,
    # the user provided `type` argument will be provided instead.
    # In that case we need to return the actual decorator.
    return decorator
