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

# Conan source backups

The existing Conan server also serves read-only source archives on port 9301.
Archives live in `${HOST_DATA_DIR}/conan-server-data/source-backup/s`, alongside
binary storage but outside the package layout. CI already mounts this data into
its `conanserver` service; no additional workflow service, host address or source
cache image layer is needed.

## Deploy on each ECS

Use this PR's checkout and the current CI image available on the host:

```bash
cd /root/projects/bolt/.github/runners
export HOST_DATA_DIR=/data
export BOLT_CI_IMAGE=localhost:5000/bolt-ci:20260921
python3 arrow-cache.py up --project runners \
  --seed-dir "$HOST_DATA_DIR/conan-source-cache/s"
```

`up` uses `install-bolt-deps.sh` and `conan source` to prepare Arrow 15.0.1-oss,
including the Substrait archive requested by that recipe. `--cci-ref` selects
the recipe-index branch/tag used by CI; `--version` selects the Arrow version.
Conan owns the URLs and checksums. Valid archives from `--seed-dir` are reused;
omit it for a new deployment. `prepare` only warms the data, without deployment.

The command builds and pushes the existing `conan-server:latest` image through
`docker-compose.yml`, then updates only `conan_server`. The registry must already
be running, and the stack's `variables.env` must exist. It does not restart runners
or change their registration. Run between jobs that use the host Conan server.
Each ECS needs the source data; each registry used by runners needs the new server
image. New Actions jobs pull that image automatically.

CI configures Conan's native source backup support through the generic job service
URL `http://conanserver:9301/s/`. No ECS hostname, IP or source credentials are passed
to the job or printed in its logs. If the endpoint is absent during setup, existing
Conan settings are preserved. Once enabled, a service outage is a Conan error;
a missing archive falls back to its original URL. Nginx permits only hash-named
archive reads and synthetic empty Conan metadata; private metadata, directory
listings and upload endpoints are unavailable.
The port need not be published on the ECS host.

The old standalone `bolt-source-cache` container can be stopped after migration;
its data is retained. Arrow and Substrait no longer need to be baked into the CI
image. Refresh sources with `prepare`; an archive refresh does not rebuild images.

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
source backups and is not required for the deployment above.
