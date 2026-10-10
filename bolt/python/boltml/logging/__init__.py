# Copyright (c) ByteDance Ltd. and/or its affiliates.
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

from __future__ import annotations

import logging

_debugLogger = logging.getLogger("boltml")
_traceLogger = logging.getLogger("boltml.trace")


def boltmlDebugEnabled() -> bool:
    return _debugLogger.isEnabledFor(logging.DEBUG)


def boltmlDebugLog(component: str, message: str) -> None:
    if _debugLogger.isEnabledFor(logging.DEBUG):
        _debugLogger.debug("[%s] %s", component, message)


def boltmlTraceLog(component: str, message: str) -> None:
    if _traceLogger.isEnabledFor(logging.DEBUG):
        _traceLogger.debug("[%s] %s", component, message)
