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

Seeds checksum-verified source archives from `arrow-cache-sources.json` (Arrow
15.0.1 and Substrait 0.27.0 by default). Existing valid blobs in
`conan-source-cache/s` are reused.
No GitHub token or repository variable is needed.

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

Use `up --manifest /path/to/sources.json` for another source set. Each entry in
`{"sources": [...]}` has a `name`, `sha256` and `urls` list. The manifest is
installed both beside the helper and at `s/manifest.json`; deployed commands
and CI jobs do not need the original checkout or manifest path.

`up` downloads missing/corrupt blobs atomically, checks their SHA-256 hashes,
installs the helper and Compose files under `BOLT_ARROW_CACHE_DIR`, and starts
only `bolt-source-cache`. It does not recreate runners, Conan or the registry.
A conflicting port fails startup; it never stops an unrelated service.
`verify` checks both HTTP downloads and runs Conan in the selected CI image
as UID 65534 with **network disabled**. All configured archives must be read from the
local download cache. This verifies the cache independently of a CI build.

The HTTP service defaults to `127.0.0.1:9301`; set `ARROW_CACHE_PORT` to change
it. Use `ARROW_CACHE_BIND` only when explicitly exposing the mirror on another
interface. Containers on the same Compose network can use
`http://bolt-source-cache:8080/s/`. This hostname is not automatically available
inside a runner's nested Docker daemon. GitHub jobs use the image layer below.
Only the configured archive hashes and `/healthz` are served; there are no write or
listing endpoints. `python3 arrow-cache.py stop` preserves all cached files.
For a combined stack, include the installed `compose.arrow-cache.yml` with the
existing Compose files. `prepare` installs the files without starting Docker.

## Enable automatic CI use

Build a cache layer on the **current** CI image; the original Dockerfile and
compiler layers are unchanged. Use a distinct output tag to test before publishing:

```bash
python3 arrow-cache.py image \
  --image localhost:5000/bolt-ci:20260921 \
  --output-image localhost:5000/bolt-ci:20260921-cache
```

Optionally add `--home-seed /path/to/trusted-prewarmed-home`. Only `.m2/repository`,
`.cache/pip` and `.cache/spark` are included; settings, credentials and job outputs
are excluded. Spark archives belong under `.cache/spark/shims/` in the layout
produced by Gluten's `install-spark-resources.sh`. Use trusted prewarming output,
not dependencies modified by a PR. Missing packages still download normally.

The layer uses `BASH_ENV` to populate each job's private HOME/CONAN_HOME silently.
It supports the non-login Bash shells used by Actions and changed HOME after
privilege dropping. Existing job files are preserved; tasks never write back to
the seed. Missing or invalid optional seeds do not block shell startup. A base
image already defining `BASH_ENV` is rejected rather than replacing its setup.
Source hashes are verified before import. Internal download URLs are not copied
into the image. The workflow needs no source-cache mounts or initialization step.

After testing the image, publish it to the registry used by the runners. Keep the
original image digest for rollback. Updating the existing CI tag requires no
workflow change; using another tag requires changing the workflow image reference.
For later refreshes, pass the original unmodified base digest to `--image`, not
the tag that now points to the cache layer.
Each ECS registry must receive the image. Do this between jobs; no runner
re-registration is needed. Cache snapshots are refreshed by rebuilding this layer,
not by saving untrusted job output. This trades automatic write-back for isolation.

The standalone HTTP service remains optional and independent of the CI image.
`verify` checks the service and Conan cache format; it does not validate deployment
of the new image. Check the actual image with an ordinary `bash --noprofile --norc`
command and a fresh writable HOME before rollout.

## Local tests

```bash
python3 -m venv /tmp/bolt-arrow-cache-test
source /tmp/bolt-arrow-cache-test/bin/activate
python3 -m pip install conan==2.32.0
python3 -m unittest discover -s .github/runners/tests -p 'test_arrow_cache.py' -v
```

Tests need loopback sockets. The real Docker/Conan acceptance check is the
`verify` command above. Updating Arrow's recipe version requires reviewing
the URLs and hashes in `arrow-cache-sources.json`.
