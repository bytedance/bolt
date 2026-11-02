# Gluten CI and runner caches

The independent `Gluten Bolt Backend` workflow compiles the tested Bolt commit
with Apache Gluten `main` for pull requests and merge groups. It resolves Gluten
once and records the exact SHA; every downstream job consumes that same source
and the native libraries from that run. It does not apply Gluten patches.

The check is informational during rollout. Do not add it to required checks or
to the existing merge-queue gate yet. Compilation failures remain visible as
failures; they are not converted into successful build results.

PRs compile native code and the Spark 3.5 / Scala 2.12 JVM profile.
Compilation uses Bolt `make release_spark` / `make export_release` and Gluten
`make release-with-tests` / `make jar_spark35`. The JVM job builds the complete
bundle, including backend test sources; `-DskipTests` skips test execution only.
Conan metadata commands check that Gluten consumes the exact exported Bolt package.
Spark SQL tests (`spark-ut`) run only on the daily schedule (02:17 Asia/Shanghai)
or a manual run with `run_tests=true`. The daily schedule takes effect after
the workflow is merged into the default branch. Scheduled tests cover only
Spark 3.5 / Scala 2.12. Backend/Delta, the three ordinary Spark groups, Extended SQL and Slow
Hive run in separate jobs. Spark 3.5 also runs TPC-H/TPC-DS comparisons. There is
no Bolt-maintained testcase blacklist. Reports distinguish real failures, empty
test selections and missing native libraries.

Scheduled tests retain direct Maven/CTest commands where existing make
targets do not provide the required selection or reports. In particular,
`make test_spark35` does not enable `spark-ut` and ignores test failures, while
`make cpp-test-release` rebuilds native code instead of using transferred tests.
Scheduled JVM shards install sibling modules before running selected suites;
TPC checks also install the complete bundle for `tools/gluten-it`.

## Time budgets

The native compilation job has a 30-minute limit and each JVM compilation job
has a 25-minute limit, including setup and artifact transfer. The warm-build
target remains approximately 20 minutes per compilation job. A shell deadline
also terminates compiler subprocesses
when that job's budget expires. Scheduled tests start as soon as native libraries
are available, without waiting for JVM compilation; each has a 35-minute limit.
Native CTest runs in a separate 10-minute job using the original compiled test
executables and fixtures. The intended longest execution path is native build
(target below 25 minutes) plus a Spark test shard (35 minutes). The native-plus-test
portion can reach 65 minutes, plus up to three minutes of source preparation and
runner queue time, so a cold run can exceed the one-hour target;
GitHub schedules and runner queues do not provide a start-time SLA.

PR native and JVM compilation run sequentially and need one available runner
slot at a time. The scheduled matrix has seven Spark 3.5 jobs plus native CTest;
running these alongside the JVM compilation job requires nine available slots.
Fewer slots require measuring shorter shards or accepting additional
queue time. Set `BOLT_GLUTEN_CI_RUNNER` to a JSON label array and
`BOLT_GLUTEN_CI_THREADS` to the native build parallelism appropriate to each host.
JVM compilation uses four Maven reactor threads by default; override
`BOLT_GLUTEN_CI_MAVEN_THREADS` if required by runner memory. Test execution retains
its separate sequential module selection.

## Runner preparation

Keep compiler, C++ standard library, JDK and image identical across native and
JVM jobs. Prefer an image digest in `BOLT_GLUTEN_CI_IMAGE`. The default is the
existing `bolt-registry:5000/bolt-ci:20260921` image; the Conan service defaults to
`bolt-registry:5000/conan-server:latest` and can be overridden with
`BOLT_GLUTEN_CI_CONAN_IMAGE`. Bake `jq`, `libxml2-utils`, `unzip`, `perl` (shasum),
JDK 17, Python test dependencies and the Maven version required by Gluten into
the image to avoid repeated installation. The workflow installs missing report
tools for compatibility with existing images.

Create these directories on **every selected runner host**, outside the checkout:

```bash
sudo mkdir -p /data/ccache-data /data/gluten-ci-cache /data/gluten-ci-seed
# Run inside the configured CI image with /data/ccache-data mounted:
CCACHE_DIR=/data/ccache-data ccache --max-size=100G
```

For the repository's Docker-in-Docker deployment, these paths must also be
mounted into the **outer runner containers**. `runner-base-compose.yml` now maps
`${HOST_DATA_DIR}/gluten-ci-cache` and `${HOST_DATA_DIR}/gluten-ci-seed` to the
`/data/...` paths consumed by the job containers. The seed is read-only in runners.
Creating directories only on the physical host does not expose them to an
already-running Docker-in-Docker daemon.

On each host, use the existing deployment's `HOST_DATA_DIR` and Compose project.
Drain its runner jobs before stopping or recreating containers. Preserve caches
from existing containers before recreation, since the earlier unmounted paths
live in each outer container's writable layer:

```bash
# Run from .github/runners with the existing deployment environment.
: "${HOST_DATA_DIR:?Set the existing runner deployment data directory}"
sudo mkdir -p "${HOST_DATA_DIR}/gluten-ci-cache" "${HOST_DATA_DIR}/gluten-ci-seed"
docker compose stop runner-medium runner-small
docker compose ps --all --quiet runner-medium runner-small
```

For each stopped container that has an existing cache, select its ID from the
list above and copy the cache. Verify that every required copy succeeds and that
the destination contains the expected `conan`, `maven` and `artifacts` data.
Retain the stopped containers until that verification is complete:

```bash
gluten_runner_container='<stopped-container-id>'
sudo docker cp "${gluten_runner_container}:/data/gluten-ci-cache/." \
  "${HOST_DATA_DIR}/gluten-ci-cache/"
sudo ls -la "${HOST_DATA_DIR}/gluten-ci-cache/"
```

Only after the existing caches have been copied and verified, recreate the
runners with the updated mounts. A new deployment without existing caches can
proceed directly to this step:

```bash
docker compose up -d --no-deps --force-recreate runner-medium runner-small
```

Runner/profile subdirectories are distinct and can be merged into the shared
host cache. Stop the relevant containers before copying their Conan databases.
For administrator seed-export containers, bind the physical
`${HOST_DATA_DIR}/gluten-ci-cache` and `${HOST_DATA_DIR}/gluten-ci-seed` to the
`/data/...` paths below, with write access for publishing. Existing runners use
the old mounts until they are recreated. On direct-host Docker deployments,
the physical `/data/...` directories are already the daemon-side paths.

The native job also persists this ccache size limit, so other jobs using the
same directory do not fall back to a smaller default and evict its entries.
The container runs as root. If the runner deployment remaps container UIDs, give
its mapped UID write access to the first two directories and read access to the
seed. Preserve the existing `/data/conan-server-data` volume as well.

| Runner/Docker-daemon path | Reused content | Behavior |
| --- | --- | --- |
| `/data/ccache-data` | Bolt and Gluten C/C++ compiler results | 100 GB limit; same compiler-check and dependency paths as existing Bolt CI; both compiler launchers enabled |
| `/data/gluten-ci-cache/maven/<runner>/<profile>` | Maven dependencies, compiled Zinc bridges (`.zinc/`) and plugins | Separate runner/profile directories prevent concurrent reactors sharing mutable snapshots |
| `/data/gluten-ci-cache/maven-tools/<runner>` | Maven distributions downloaded by `build/mvn` | Reused across checkouts |
| `/data/gluten-ci-cache/pip/<runner>` | Downloaded Python wheels | Survives job containers |
| `/data/gluten-ci-cache/spark/<key>` | Verified Spark binary and SQL resources | Key includes installer contents, exact Spark version, Scala version and resource profile; copied into each test checkout |
| `/data/gluten-ci-cache/conan/<runner>/<profile>` | Conan recipes and prebuilt dependencies | Exclusive per runner/profile; preserves package paths across sequential jobs |
| `/data/gluten-ci-cache/artifacts` | Verified native artifact ZIPs | Artifact ID + SHA-256 key; locked and atomically published per host; entries older than three days removed |
| `/data/gluten-ci-cache/conan-downloads` | Conan package downloads | Avoids downloading the same archives repeatedly |
| `/data/gluten-ci-seed/conan.tgz` | Prebuilt Conan dependencies and recipes | Mounted read-only; restored when the seed changes |
| `/data/conan-server-data` | Existing Conan service binaries | Retain current Bolt CI prewarming |

Do not mount one writable Conan package database into concurrent jobs. The
workflow links each container job's `/github/home/.conan2` to an exclusive
runner/profile database. Previous `bolt/ci-*` candidate packages are removed before each native build.
Sequential jobs retain dependency package paths, which
would otherwise change on rebuild and prevent compiler-cache reuse. Multiple
runner processes on one host must have distinct runner names. To reset a cache,
stop that runner and remove its `/data/gluten-ci-cache/conan/<runner>` directory.
Local helper runs use a task directory under `RUNNER_TEMP` instead of clearing the
user's default cache. It never uploads the
candidate Bolt package to a remote or reuses an earlier candidate as the tested
binary. Only native shared libraries and their runtime dependencies are sent to
JVM jobs; large LLVM/static-engine archives are not copied to every test job.

To create the dependency seed, use an administrator-controlled container with
the **same image, profile and options as the native job**. Run the native build
once with a longer temporary budget. From that completed Bolt checkout:

```bash
(
  set -euo pipefail
  # gluten-install.json is produced by the native job, including the exact graph.
  conan list --graph .ci/gluten-bolt/gluten-install.json \
    --graph-binaries='*' --format=json > /tmp/gluten-packages.json
  # Exclude candidate engine packages; each PR builds and exports its own engine.
  jq 'with_entries(.value |= with_entries(select(.key | startswith("bolt/") | not)))' \
    /tmp/gluten-packages.json > /tmp/gluten-dependencies.json
  conan cache save --list /tmp/gluten-dependencies.json --no-source \
    --file /tmp/gluten-conan-seed.tgz
  gluten_seed_new="$(sudo mktemp /data/gluten-ci-seed/conan.tgz.XXXXXX)"
  sudo install -m 0644 /tmp/gluten-conan-seed.tgz "${gluten_seed_new}"
  sudo mv -f "${gluten_seed_new}" /data/gluten-ci-seed/conan.tgz
)
```

If a native job has already warmed the persistent cache, an administrator can
export its dependencies directly, including when the job later hit its time limit.
Pause that runner while exporting, then run these commands inside the configured
CI image with both host directories mounted read-write at the same paths:

```bash
(
  set -euo pipefail
  gluten_seed_runner='bolt-ci-amd64-runners-runner-medium-1' # choose the warmed runner
  gluten_seed_slot="$(printf '%s' "${gluten_seed_runner}" | sha256sum | cut -c1-16)"
  export CONAN_HOME="/data/gluten-ci-cache/conan/${gluten_seed_slot}/native"
  conan list '*:*#latest' --format=json > /tmp/gluten-cached-packages.json
  jq 'with_entries(.value |= with_entries(select(.key | startswith("bolt/") | not)))' \
    /tmp/gluten-cached-packages.json > /tmp/gluten-cached-dependencies.json
  conan cache save --list /tmp/gluten-cached-dependencies.json --no-source \
    --file /tmp/gluten-conan-seed.tgz
  gluten_seed_new="$(mktemp /data/gluten-ci-seed/conan.tgz.XXXXXX)"
  install -m 0644 /tmp/gluten-conan-seed.tgz "${gluten_seed_new}"
  mv -f "${gluten_seed_new}" /data/gluten-ci-seed/conan.tgz
)
```

Copy that seed to `/data/gluten-ci-seed/conan.tgz` on **every selected host**
using the same image/toolchain. Stage each copy in that host's seed directory,
then atomically rename it to `conan.tgz`, so readers see a complete archive.
Resume the runner after exporting. The seed must
contain completed dependency packages; the workflow always builds its own Bolt
candidate. Preserve the persistent cache directories when restarting runners.

Publish a seed only from a completed dependency build. Update it after compiler,
ABI, dependency recipe/options or image changes; keep a seed per image when
running different toolchains. If the image provides
`configure-bolt-conan-cache`, the workflow also uses that source-cache helper,
as in Gluten PR #2. Otherwise dependency sources can be rebuilt on cache misses.

Native archive downloads are cached once per host and artifact digest. Each
consumer still queries GitHub for the current run's artifact identity and verifies
the complete ZIP digest and integrity before extraction. Incomplete or corrupt
entries are downloaded again; new candidates use distinct keys. The first copy
on a host still requires network access. Transfers retry up to three times, with
a five-minute cap per attempt and a one-minute low-throughput timeout, within
the compilation job's remaining budget. This prevents one stalled transfer from
consuming the entire job before retrying.

The Scala Maven plugin's `secondaryCacheDir` points inside that persistent
Maven directory, so compiled Zinc bridges survive the job's temporary home.

Warm Maven and Spark caches on each runner/profile by running the compilation
and scheduled paths once. Mere `dependency:go-offline` does not build the Scala
compiler bridge or prime native ccache. Inspect the uploaded `ccache-stats.txt`,
generated compiler commands, dependency graph and JVM logs. Never cache a prior
check result or the entire Gluten `target/` tree as proof that a new candidate
compiled.

## Reproduction and make-only verification

Use clean worktrees and a prepared Conan toolchain. The native commands and all
options are visible in `.github/workflows/gluten-bolt.yml`; the helper can also
be sourced locally. Set `GLUTEN_DIR` when Gluten is outside the Bolt checkout.
`GLUTEN_COMPILE_DEADLINE=$(( $(date +%s) + 1800 ))` enables the native compile budget.

The primary compilation entry points are:

```bash
# In the exact Bolt checkout under test.
make release_spark BUILD_VERSION=ci-<bolt-sha> \
  CONAN_OVERRIDE='-o bolt/*:enable_testutil=True -o bolt/*:enable_s3=False'
make export_release BUILD_VERSION=ci-<bolt-sha>

# In Apache Gluten main, using that locally exported Bolt version.
make release-with-tests BOLT_BUILD_VERSION=ci-<bolt-sha> \
  GLUTEN_BUILD_VERSION=main ENABLE_HDFS=True ENABLE_S3=False
make jar_spark35
```

Do not run `make bolt-recipe` for a candidate build: it deletes `ep/bolt`, clones
a remote branch and exports that recipe. `test_spark35` currently ignores Maven
test failures and does not enable `spark-ut`; it is not the scheduled test
entry point. `jar_spark35` moves the compatibility `gluten-package-*.jar` name into
`output/`; the current main still produces that compatibility JAR, and this
make target passed the local verification below.

## Validation and remaining limits

Public Bolt `edd7850234f16bd13d1433a77f68bf21acf8262e` and Apache Gluten
`8a8300e2b036864180452b3d058f5824b9032d7e` compiled without Gluten patches.
The make-only sequence above passed, including the Spark 3.5 bundle. A transferred
native test run reported 149 passed, seven skipped and zero failures.

[Actions run 41409853501](https://github.com/bytedance/bolt/actions/runs/41409853501)
built native code in 23 minutes 25 seconds and the Spark 3.5 JVM job in about
nine minutes. Its full compilation chain took about 38 minutes including queues;
that earlier multi-version matrix also exposed upstream test-source compilation
failures in profiles no longer selected by this workflow.

Compiler-cache reuse depends on matching compiler settings, dependency paths
and locale with the standard Bolt jobs. Changing dependency recipe revisions
can invalidate cached objects in both workflows. Record the Conan recipe SHA
and compare the before/after ccache counters; shared-host counters include other
concurrent jobs and are not a per-job hit rate.

Full scheduled Spark/TPC execution and the one-hour runtime target remain
unverified. Run the scheduled path once with `run_tests=true` after deploying
the runner cache mounts, and measure it on the available runner pool.
