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

from typing import Union

from pybolt import SortOrder


def _orderByArg_(
    columns: Union[str, list[str], list[tuple[str, SortOrder]]],
) -> list[tuple[str, SortOrder]]:
    cols: list[tuple[str, SortOrder]] = []
    if type(columns) is str:
        cols = [(columns, SortOrder.ASC_NULLS_LAST)]
    elif type(columns) is list:
        for c in columns:
            if type(c) is str:
                cols.append((c, SortOrder.ASC_NULLS_LAST))
            elif (
                type(c) is tuple
                and len(c) == 2
                and type(c[0]) is str
                and type(c[1]) is SortOrder
            ):
                cols.append(c)
            else:
                raise ValueError(f"Invalid orderBy column item: {c}")
    else:
        raise TypeError(f"Invalid argument type for sorting columns: {type(columns)}")
    return cols
