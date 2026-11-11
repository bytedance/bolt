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

FROM docker:28-cli AS docker_cli
FROM python:3.11-slim-bookworm

RUN apt-get update && apt-get install -y --no-install-recommends git ca-certificates \
    && rm -rf /var/lib/apt/lists/*
COPY --from=docker_cli /usr/local/bin/docker /usr/bin/docker
WORKDIR /opt/controller
COPY bolt_ci ./bolt_ci
ENTRYPOINT ["/usr/local/bin/python3", "-m", "bolt_ci"]
