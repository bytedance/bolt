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

# Local Conan source cache

Each ECS prewarms its own archives. No HTTP service, cross-ECS connection, Conan
server change or source-cache image layer is required. CI imports verified files
into its private Conan cache; writable metadata and locks are never shared.

## Prepare on each ECS

Use this PR's checkout and the current CI image available on the host:

```bash
cd /root/projects/bolt/.github/runners
export HOST_DATA_DIR=/data
export BOLT_CI_IMAGE=localhost:5000/bolt-ci:20260921
python3 arrow-cache.py prepare
```

This runs the existing `install-bolt-deps.sh` and `conan source` for Arrow
15.0.1-oss, including Substrait as required by the recipe. `--cci-ref` selects
the recipe-index branch/tag used by CI; `--version` selects the Arrow version.
Conan owns the download URLs and checksums. Existing archives under
`$HOST_DATA_DIR/conan-source-cache/s` are reused. To migrate the previous HTTP
layout, add `--seed-dir "$HOST_DATA_DIR/conan-server-data/source-backup/s"`.

Only verified hash-named archives are published under
`$HOST_DATA_DIR/conan-source-cache/archives`. The script also installs
`compose.local-sources.yml` next to that directory. Refresh with `prepare`;
no image rebuild or runner restart is needed for later archive updates.

## Attach to the runners

Wait until the selected runners are idle. Load the same environment and registration
credentials used for their normal startup. Keep **all original Compose files in
order**, including repository/name/label overrides, then append the installed overlay:

```bash
docker compose --project-name runners \
  -f /path/to/existing/docker-compose.yml \
  -f /path/to/existing/runner-overrides.yml \
  -f "$HOST_DATA_DIR/conan-source-cache/compose.local-sources.yml" \
  up -d --no-deps --no-build --pull never runner-medium runner-small
```

Omit the override only if the original stack has none. This recreates only the
runner containers to add their mounts; the normal startup may register them again.
It does not build images or update Conan/registry services. Preserve the overlay
in future startup commands. Alternatively, `arrow-cache.py up` combines preparation
and this update; pass each original file with `--compose-file`, in the same order.

The overlay sets `RUNNER_TOOL_CACHE=/opt/bolt-runner-tools` and mounts the published
archives into its `bolt-source-cache` child read-only. GitHub Runner automatically
mounts its tool directory into job containers, including with Docker-in-Docker.
The CI adapter imports from the job's translated `RUNNER_TOOL_CACHE`, checks each
SHA-256, and discards shared metadata. Workflow YAML needs no cache paths, hostnames,
IP addresses or repository variables. Missing or corrupt optional seeds fall back
to normal dependency downloads. Other tool-cache entries remain writable.

If the previous HTTP deployment was installed, its containers/images can be
retired separately after switching jobs to the local cache. This command does
not delete their data or recreate those services.

## Optional Maven, Spark and pip seeds

The separate `image` command still supports trusted HOME dependency seeds:

```bash
python3 arrow-cache.py image \
  --image localhost:5000/bolt-ci:20260921 \
  --output-image localhost:5000/bolt-ci:20260921-home-cache \
  --home-seed /path/to/trusted-prewarmed-home
```

Only `.m2/repository`, `.cache/pip` and `.cache/spark` are copied. Spark archives
use the `.cache/spark/shims/` layout from Gluten's `install-spark-resources.sh`.
Credentials and job outputs are excluded. `BASH_ENV` imports these into private
job HOME directories without overwriting existing files or writing back to seeds.
Missing seeds do not block startup. The original base must not define `BASH_ENV`;
use its unmodified digest when refreshing this optional layer. Publish it to the
runner registry separately. This command does not contain or configure Arrow
source archives and is not required for the deployment above.
