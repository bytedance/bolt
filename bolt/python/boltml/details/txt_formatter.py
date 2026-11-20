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

from bisect import bisect_right
from itertools import accumulate
from typing import Optional
import os
import sys


class TxtFormatter:
    def __init__(self, length: int, ellipsis: str = "...", justify: str = "left"):
        self.length = length
        self.ellipsis = ellipsis
        self.rhsSize = (self.length - len(self.ellipsis)) // 2
        self.lhsSize = self.length - len(self.ellipsis) - self.rhsSize
        self.truncate = (
            lambda s: s[: self.lhsSize]
            + self.ellipsis
            + (s[-self.rhsSize :] if self.rhsSize > 0 else "")
        )
        if justify == "left":
            self.justify = lambda s: s.ljust(self.length)
        elif justify == "center":
            self.justify = lambda s: s.center(self.length)
        else:
            self.justify = lambda s: s.rjust(self.length)

    @staticmethod
    def typeSize(t: type) -> int:
        if t is int:
            return len(str(-1 * sys.maxsize))
        elif t is float:
            return 16
        elif t is str:
            return 20
        else:
            return 20

    def formatString(self, string: str) -> str:
        if len(string) > self.length:
            return self.truncate(string)
        elif len(string) < self.length:
            return self.justify(string)
        else:
            return string

    def formatList(
        self,
        values: list,
        colSizes: list[int],
        sep: str = " | ",
        ellipsis: str = "...",
        justify: str = "right",
        ldelim="| ",
        rdelim=" |",
    ) -> str:
        sizes = list(accumulate((s + len(sep) for s in colSizes)))
        i = bisect_right(sizes, self.length)
        values = values[:i]
        colSizes = colSizes[:i]
        if i < len(sizes):
            values.append(self.ellipsis)
            colSizes.append(len(self.ellipsis))
        listStr = sep.join(
            [
                TxtFormatter(s - len(sep), ellipsis, justify).formatString(str(v))
                for v, s in zip(values, colSizes)
            ]
        )
        return ldelim + listStr + rdelim

    def rowSep(
        self,
        colSizes: list[int],
        char: str = "-",
        sep: str = "-+-",
        ellipsis: str = "...",
        ldelim="+-",
        rdelim="-+",
    ) -> str:
        return self.formatList(
            ["".rjust(s - len(sep), char) for s in colSizes],
            colSizes,
            sep,
            ellipsis,
            ldelim=ldelim,
            rdelim=rdelim,
        )

    @staticmethod
    def formatDataframe(
        df,
        maxRows: Optional[int] = None,
        truncateColumns: bool = False,
        colSep: str = " | ",
    ) -> list[str]:
        try:
            termsize = os.get_terminal_size()
            linesize = termsize.columns if truncateColumns else sys.maxsize
        except Exception:
            linesize = sys.maxsize
        formatter = TxtFormatter(linesize, justify="left")

        if maxRows is None:
            maxRows = len(df)
        maxRows = min(maxRows, len(df))

        colSizes = [len(n) + len(colSep) for n in df.names]
        if maxRows > 0:
            for j in range(0, len(colSizes)):
                nameSize = colSizes[j]
                typeSize = TxtFormatter.typeSize(type(df[0, j]))
                maxSize = max((len(str(df[i, j])) for i in range(0, maxRows)))
                colSizes[j] = max(nameSize, min(typeSize, maxSize) + len(colSep))

        rows = []

        # Header
        rows.append(formatter.rowSep(colSizes))
        rows.append(
            formatter.formatList(df.names, colSizes, sep=colSep, justify="center")
        )
        rows.append(rows[0])

        # Values
        for i in range(0, maxRows):
            values = [df[i, j] for j in df.names]
            rows.append(
                formatter.formatList(values, colSizes, sep=colSep, justify="right")
            )

        # Footer
        if maxRows < len(df):
            rows.append(f"... {len(df) - maxRows} remaining rows")
        else:
            rows.append(rows[0])
        return rows
