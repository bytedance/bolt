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

from pybolt import BaseVector, fromList, VarbinaryType

from .vector import ExecutorsArgument, functionRegister, normalizeExecutors


def downloadImage(
    url,
    timeout_s: float = 5.0,
    user_agent: str = "pyvelox-image/1.0",
):
    import urllib.error
    import urllib.request

    if url is None:
        return None
    if isinstance(url, (bytes, bytearray)):
        url = url.decode("utf-8", errors="ignore")
    req = urllib.request.Request(
        url,
        headers={"User-Agent": user_agent, "Accept": "image/*,*/*;q=0.8"},
        method="GET",
    )
    try:
        with urllib.request.urlopen(req, timeout=float(timeout_s)) as resp:
            content_type = (resp.headers.get("Content-Type") or "").lower()
            data = resp.read()
        if not data or (content_type and not content_type.startswith("image/")):
            return None
        return data
    except (urllib.error.URLError, urllib.error.HTTPError, ValueError):
        return None


def downloadImages(
    urls: BaseVector,
    timeout_s: float = 5.0,
    user_agent: str = "pyvelox-image/1.0",
) -> BaseVector:
    out = []
    for i in range(len(urls)):
        out.append(downloadImage(urls[i], timeout_s=timeout_s, user_agent=user_agent))
    return fromList(out, VarbinaryType())


def register(executors: ExecutorsArgument = None):
    """
    Register the production image functions with the provided executors.

    Module import registers them with the default local executor for backwards
    compatibility. Tests or applications using custom executor instances should
    call this function with those executors before using ``downloadImages``.
    """
    import inspect

    outputType = VarbinaryType()
    signature = inspect.signature(downloadImages)
    defaultArgs = [
        v.default
        for _, v in signature.parameters.items()
        if v.default is not inspect.Parameter.empty
    ]
    for executor in normalizeExecutors(executors):
        executor.registerVectorFunction(
            downloadImages,
            downloadImages.__name__,
            outputType,
            len(signature.parameters),
            defaultArgs,
            False,
        )
    if downloadImages not in functionRegister:
        functionRegister[downloadImages] = outputType
    return downloadImages


register()
