<!--
Copyright (c) ByteDance Ltd. and/or its affiliates.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
-->

# Arrow source cache

Seeds checksum-verified Arrow 15.0.1 and Substrait 0.27.0 archives for
`arrow/15.0.1-oss`. Existing valid blobs in `conan-source-cache/s` are reused.
No CI image rebuild, GitHub token or repository variable is needed.

## Start and verify

Run as root in the ECS shell, using this PR's checkout:

```bash
cd /root/projects/bolt/.github/runners
export HOST_DATA_DIR=/data
export BOLT_ARROW_CACHE_DIR="$HOST_DATA_DIR/conan-source-cache"
# Use runners to replace an existing bolt-source-cache service in that project.
# For a separate deployment, use bolt-arrow-cache instead.
export ARROW_CACHE_PROJECT=runners
python3 arrow-cache.py up
python3 arrow-cache.py verify --image localhost:5000/bolt-ci:20260921
```

`up` downloads missing/corrupt blobs atomically, checks both SHA-256 hashes,
installs the helper and Compose files under `BOLT_ARROW_CACHE_DIR`, and starts
only `bolt-source-cache`. It does not recreate runners, Conan or the registry.
A conflicting port fails startup; it never stops an unrelated service.
`verify` checks both HTTP downloads and runs Conan in the selected CI image
as UID 65534 with **network disabled**. Both archives must be read from the
local download cache. This verifies the cache independently of a CI build.

The HTTP service defaults to `127.0.0.1:9301`; set `ARROW_CACHE_PORT` to change
it. Use `ARROW_CACHE_BIND` only when explicitly exposing the mirror on another
interface. Containers on the same Compose network can use
`http://bolt-source-cache:8080/s/`. This hostname is not automatically available
inside a runner's nested Docker daemon. GitHub jobs use the mounts below.
Only the two archive hashes and `/healthz` are served; there are no write or
listing endpoints. `python3 arrow-cache.py stop` preserves all cached files.
For a combined stack, include the installed `compose.arrow-cache.yml` with the
existing Compose files. `prepare` installs the files without starting Docker.

## Enable existing runners

When runners are idle, load the **same environment and all Compose files** used
to create them. Append this override to that existing invocation:

```bash
docker compose -p runners \
  -f docker-compose.yml \
  -f "$BOLT_ARROW_CACHE_DIR/compose.arrow-cache-runners.yml" \
  up -d --no-deps runner-medium runner-small
```

The example assumes only the base file; keep any existing runner overrides
before the cache override and retain it in future startup commands. Never omit
repository, label or registration settings from the original deployment.
The override adds only these read-only mounts to both runner sizes:

- `conan-source-cache/s` → `/opt/bolt-arrow-sources`
- `conan-source-cache/arrow-cache.py` → `/opt/bolt-arrow-cache.py`

The existing Gluten workflow forwards them into each job and calls
`configure-local` after setting its private `CONAN_HOME`. The helper verifies
and copies both blobs into that home's writable `source-backup/s` directory,
so Conan's locks, metadata and dirty markers never modify the shared seed.
It replaces source-mirror URLs with `origin` fallback and preserves Conan
package remotes and unrelated configuration. No ECS IP/hostname is added to
public workflow configuration or the job's cache-helper output.

For external Gluten CI, the existing `ARROW_CACHE_SCRIPT` and
`ARROW_SOURCE_DIR` environment settings point to these same installed files.
Re-run its prepare step before starting new workers after an upgrade.

## Local tests

```bash
python3 -m venv /tmp/bolt-arrow-cache-test
source /tmp/bolt-arrow-cache-test/bin/activate
python3 -m pip install conan==2.32.0
python3 -m unittest discover -s .github/runners/tests -p 'test_arrow_cache.py' -v
```

Tests need loopback sockets. The real Docker/Conan acceptance check is the
`verify` command above. Updating Arrow's recipe version requires reviewing
both pinned source hashes in `arrow-cache.py`.
