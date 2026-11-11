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

# External Gluten CI

Bolt PRs and merge queue candidates compile the Gluten Bolt backend on Bolt's
GitHub Actions runners. The external `bolt-gluten-ci` Compose project compiles
the backend for relevant PRs in `WATCH_REPOSITORY` using a pinned Bolt commit.
Both PR paths build the native libraries and verify consumption of the selected
Bolt package. The external controller reports compilation in one PR comment and
keeps raw logs and reports private. Publishing a required check on Apache Gluten
needs upstream permission and branch protection configuration.

Bolt's nightly workflow runs the full 39-task Gluten x86 matrix on Bolt `main`
with a pinned Gluten revision. It runs native tests, 12 packaging tasks and 26
Spark tasks, including JVM, connector and local TPC checks. The schedule is
02:00 Asia/Shanghai (18:00 UTC). GitHub may delay scheduled runs. Manual dispatch
runs from the `main` branch.

Bolt owns the callable workflow, task scripts and matrix in `scripts/gluten-ci`.
Apache Gluten supplies only backend source and necessary native/Spark test fixes;
it does not need to host CI workflows or deployment scripts. Spark 3.4 is outside
this CI scope. Helpers come from the same Bolt revision as the caller or bundle.

Jobs use Linux x86_64 runners labelled `medium` and the existing CI image and
registry. Native jobs mount `/data/ccache-data`, `/opt/bolt-arrow-sources` and
`/opt/bolt-arrow-cache.py` read-only. The Conan service mounts
`/data/conan-server-data` read-write for revision locks; its HTTP uploads remain
disabled. Matrix edits must update the reviewed digest in `bolt_ci/config.py`.

## Install

Use root on Debian 12 x86_64 with Python 3.11+, Git, rootful Docker/Compose,
iptables, iproute2 and systemd. Enable IPv4 forwarding and bridge netfilter.
Use Docker's iptables backend for the network guard.
Reserve CPU, memory and disk alongside existing Bolt jobs.

Verify the published checksum before extracting to a persistent, versioned path.

```bash
(cd /path/to/download && sha256sum -c bolt-gluten-ci.tar.gz.sha256) || exit 1
install -d -m 0755 /data/gluten-external-ci/deploy/release
tar --no-same-owner -xzf /path/to/download/bolt-gluten-ci.tar.gz \
  -C /data/gluten-external-ci/deploy/release
cd /data/gluten-external-ci/deploy/release/bolt-gluten-ci/deploy
python3 -B bundle.py verify ..
install -m 0600 env.example /etc/bolt-gluten-ci.env
install -d -m 0700 /etc/bolt-gluten-ci-secrets
```

In the environment file, set the full engine commit in `BOLT_REF`, a validation
PR in `ONLY_PR`, and existing image/cache paths. `BOLT_CHECKOUT_SEED` can point to
a trusted local checkout containing that commit. Keep two distinct PATs in the
configured root-owned, mode-0600 files: one reads GitHub APIs, the other comments
on the target PR. Never put token values in the environment file or image.

For the existing host-published Conan service, use this command to find its
full container ID.

```bash
docker ps --no-trunc --filter label=com.docker.compose.project=runners \
  --filter label=com.docker.compose.service=conan_server --format '{{.ID}}'
```

Use the configured `host.docker.internal:9300` endpoint or an explicit IPv4/port.
Re-prepare after replacing the Conan container. To disable this remote, clear
both `CONAN_REMOTE` and `CONAN_CACHE_CONTAINER`. Workers mount Arrow sources
read-only and copy other trusted cache seeds into task-private writable
directories.

## Validate and run

```bash
source /etc/bolt-gluten-ci.env
./ci.sh prepare
./ci.sh enqueue --pr "$ONLY_PR" --dry-run
./ci.sh up
./ci.sh enqueue --pr "$ONLY_PR"
./ci.sh status
./ci.sh logs
```

Prepare pins the worker image digest, verifies the bundled driver/helpers,
downloads checksum-verified JDKs, prepares tools, and probes worker isolation.
It preserves the original CI image and registry/Conan/Arrow services. Dry-run
reads the PR without executing its code or posting comments. Existing PRs form
a baseline on first startup. Explicit enqueue starts the validation run.

The ECS operator validates real compilation, comments, caches and restart
recovery before expanding scope. Stop, clear `ONLY_PR`, source the file, then
prepare/up to watch all eligible PRs. PR authors and configured maintainers can
request a rerun with `Run Gluten Bolt CI`.

## Operations

- Keep all runners serving Bolt. Retire only verified, idle Gluten-specific
  runners by their exact container IDs.
  Disable superseded workflows only in the old Gluten fork.
- `./ci.sh stop` freezes scheduling, interrupts this instance's workers and stops
  its controls. It preserves caches, state, firewall and guard timer. Never use
  `docker compose down -v` to clean up either deployment.
- For an upgrade, the ECS operator stops the old release and backs up SQLite
  before unpacking the verified bundle in a new directory. Keep the existing
  environment and credentials, then run `prepare` and `up` from the new release.
  Jobs and reports live under `${HOST_DATA_DIR}/gluten-external-ci/jobs`. SQLite
  state is under `control/state`. Legacy requests without target-branch identity
  are retired and revalidated. Upgrades
  supersede incompatible active attempts and reevaluate their current PRs with
  the compile-only graph. Untouched baseline PRs are not backfilled. Schema
  rollback requires the prior backup.
- The `bolt-gluten-ci-network.timer` checks isolation about every 20 seconds.
  Missing/stale proof blocks new jobs and interrupts observed workers. Inspect
  `journalctl -u bolt-gluten-ci-network.service` for details. After repair, run
  `stop`, `prepare` and `up`.
- Worker containers receive no PAT, control database or Docker socket. Keep
  raw logs private. After changing credentials, image, cache endpoint or release,
  stop and prepare again. Preserve the old release for rollback.

## Build a bundle

Bundle the reviewed Bolt helpers together with the controller, using the same
Bolt commit. The Apache source prerequisite is listed in
[Gluten prerequisites](../../scripts/gluten-ci-upstream.md). Version 2 bundles no
longer consume a Gluten helper checkout; rebuild instead of using the old preview.

```bash
python3 -B bundle.py create \
  --bolt-repo /path/to/bolt --bolt-revision "$BOLT_DEPLOY_SHA" \
  --driver-repo /path/to/pinned-bolt-driver \
  --output /path/to/bolt-gluten-ci.tar.gz
(cd /path/to && sha256sum bolt-gluten-ci.tar.gz > bolt-gluten-ci.tar.gz.sha256)
```

The manifest records revisions, hashes and executable bits. Helpers are included from Bolt. ECS runtime acceptance is still required.
