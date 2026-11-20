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
from typing import Any, Optional, Sequence

import numpy as np
import pybolt

from .vector import vectorFunction


_TENSOR_META = "_tensor_adapter_meta"
_SUPPORTED_USER_DTYPES = (np.ndarray,)


def _validate_shape(shape: Sequence[int], context: str) -> list:
    if not shape:
        raise ValueError(f"{context}: shape must be non-empty")
    shape = [int(d) for d in shape]
    for d in shape:
        if d <= 0:
            raise ValueError(f"{context}: shape dimensions must be positive, got {d}")
    return shape


def tensorFunction(element_type: pybolt.BoltType, shape: Sequence[int]):
    shape = _validate_shape(shape, "tensorFunction")

    inner = vectorFunction(pybolt.ArrayType(element_type))

    def wrap(fn):
        # If the inner is a tensorAdapter awaiting metadata, plug it in.
        meta = getattr(fn, _TENSOR_META, None)
        if meta is not None:
            meta["element_type"] = element_type
            meta["shape"] = shape

        wrapped = inner(fn)
        wrapped.tensor_element_type = element_type
        wrapped.tensor_shape = shape
        return wrapped

    return wrap


def _is_supported_user_dtype(dt: Any) -> bool:
    return any(dt is t for t in _SUPPORTED_USER_DTYPES)


def tensorAdapter(
    inputDtype: Optional[type] = None,
    outputDtype: Optional[type] = None,
    shape: Optional[Sequence[int]] = None,
):
    if inputDtype is None and outputDtype is None:
        raise ValueError(
            "tensorAdapter: at least one of inputDtype / outputDtype must be set"
        )
    if inputDtype is not None and not _is_supported_user_dtype(inputDtype):
        raise ValueError(
            f"tensorAdapter: inputDtype={inputDtype!r} not supported; "
            f"expected one of {_SUPPORTED_USER_DTYPES}"
        )
    if outputDtype is not None and not _is_supported_user_dtype(outputDtype):
        raise ValueError(
            f"tensorAdapter: outputDtype={outputDtype!r} not supported; "
            f"expected one of {_SUPPORTED_USER_DTYPES}"
        )

    explicit_shape = (
        _validate_shape(shape, "tensorAdapter") if shape is not None else None
    )

    def decorator(fn):
        # Shared metadata dict — filled by an outer `@tensorFunction` at
        # decoration time, OR populated up front from the explicit
        # `shape` arg if there's no outer tensorFunction.
        meta: dict = {}
        if explicit_shape is not None:
            meta["shape"] = explicit_shape

        def _resolve(name: str, required: bool) -> Optional[Any]:
            v = meta.get(name)
            if v is None and required:
                raise RuntimeError(
                    f"tensorAdapter: {name!r} is not set. Either pair this "
                    "adapter with an outer `@tensorFunction(element_type, "
                    "shape)`, or pass `shape=[...]` explicitly."
                )
            return v

        @functools.wraps(fn)
        def adapter(*args, **kwargs):
            if inputDtype is np.ndarray:
                shape_ = _resolve("shape", required=True)
                args = tuple(
                    a.to_numpy(shape=shape_)
                    if hasattr(a, "to_numpy") and callable(a.to_numpy)
                    else a
                    for a in args
                )
            result = fn(*args, **kwargs)
            if outputDtype is np.ndarray:
                if not isinstance(result, np.ndarray):
                    raise TypeError(
                        f"tensorAdapter: declared outputDtype=numpy.ndarray "
                        f"but user function returned {type(result).__name__}"
                    )
                element_type = _resolve("element_type", required=True)
                shape_ = _resolve("shape", required=True)
                result = pybolt.fromNumpy(result, element_type, shape_)
            return result

        # Expose the meta dict so an outer `@tensorFunction` can fill it.
        setattr(adapter, _TENSOR_META, meta)
        return adapter

    return decorator
