<!--
Copyright (c) ByteDance Ltd. and/or its affiliates.
SPDX-License-Identifier: Apache-2.0
-->

# Python build and deployment

Start with the [Python quick start](../README.md) for installation and API
examples. Commands below run from the repository root unless noted.

## Build configuration

`make python-wheel` needs Python with pip, a C++ toolchain, development headers,
Git, Bash, and network access. It installs build tools in isolation, prepares
Conan dependencies, and builds with scikit-build-core. Native setup details are
in [CONTRIBUTING.md](../../../CONTRIBUTING.md).

| Make setting | Purpose |
| --- | --- |
| `PYTHON_EXECUTABLE` | Python interpreter whose ABI the wheel targets; must have pip. |
| `BUILD_TYPE` | Bolt build type; default `Release`. |
| `PROFILE` | Conan host profile; default `default`. |
| `DEPENDENCY_BUILD_TYPE` | Dependency build type; default `Release`. |
| `NUM_THREADS` / `NUM_LINK_JOB` | Compile/link concurrency; defaults follow Make. |

For direct pip/uv builds, use `-Ccmake.build-type=Debug`, `BOLT_CONAN_PROFILE`,
and `BOLT_DEPENDENCY_BUILD_TYPE` for the corresponding settings. Parallelism
uses the same defaults; `CMAKE_BUILD_PARALLEL_LEVEL` overrides `NUM_THREADS`.
Lower concurrency on memory-limited hosts.

Advanced builds can set `BOLT_CONAN_GENERATORS` to reuse a configured Conan
generators directory. Its compiler, architecture and build type must match,
and its preset must enable Python/test utilities with `BOLT_TEST_LINKAGE=static`.

## Development wheels

```bash
make python-wheel
```

The command prints a uniquely named wheel under `dist/`. It requires no uv and
does not install Bolt into the calling environment. Native build output is
reused, although pip's isolated environment can trigger CMake reconfiguration.

From a uv application next to a Bolt checkout named `bolt`, select the
application's interpreter (with pip installed), build, and add the wheel:

```bash
BOLT_DEV_WHEEL="$(make --no-print-directory -s -C ../bolt python-wheel \
  PYTHON_EXECUTABLE="$(uv run python -c 'import sys; print(sys.executable)')")"
uv add "$BOLT_DEV_WHEEL"
```

Keep that wheel available for future syncs. After rebuilding, install the new
filename; for an existing environment, use `uv pip install --reinstall /path/to/new.whl`.

## uv wheel resolution

A local wheel supports one Python ABI. For a CPython 3.11 wheel, set the
application's `requires-python` to `>=3.11,<3.12`, or restrict its
[uv resolution environments](https://docs.astral.sh/uv/concepts/resolution/#limited-resolution-environments).
`uv python pin` alone does not narrow the lock's Python range. Source dependencies
build for the selected interpreter and do not need this single-wheel constraint.

## Compatibility

The validated target is CPython 3.11 on Linux x86-64. Workers must match the
wheel's Python ABI, architecture, and OS libraries, and provide system `tzdata`.
The current native recipe also requires AVX2, FMA, F16C, LZCNT, and BMI2.

## Hive Metastore bindings

Builds download pinned [Apache IDLs](../boltml/third_party/hive/README.md), verify
SHA-256 hashes, and generate Python bindings with Conan's Thrift compiler.
Valid cached IDLs are reused; the first build needs `raw.githubusercontent.com`.
Installing a wheel needs neither the IDLs nor the compiler.

Editable builds generate into `boltml/_generated/` for IDE navigation. Select
the application's interpreter and add `bolt/python` to the IDE's source paths.
Jedi 0.19.2 cannot resolve the tested editable loader; use Pyright/Pylance.
After changing the IDLs or generation logic, run
`uv sync --reinstall-package bolt-engine` in the consuming project.

## Source distributions

```bash
uv build ./bolt/python --sdist --out-dir ./dist
```

The archive includes native build sources under `native/`, without requiring
a neighboring checkout. Extract it and run `uv build --wheel .` in its root.
Building that wheel still requires the native toolchain and network access.

## Testing

The `python-check` job in [build-test.yml](../../../.github/workflows/build-test.yml)
builds one wheel, installs it, and runs Python unit tests plus integration tests
for installed files, Hive binding serialization, and local Ray execution. It
excludes Hive/Paimon service tests and does not validate multi-machine deployment,
sdist builds, or editable installs. To run the integration tests locally:

```bash
uv pip install --python /path/to/venv/bin/python \
  --group bolt/python/pyproject.toml:test /path/to/the-built-wheel.whl
/path/to/venv/bin/python -I -B -m unittest discover \
  -s "$PWD/bolt/python/tests/integration" -p 'test_*.py' -v
```

Ray and image HTTP fixtures need local socket access. Four known regressions
remain expected failures: string-constant broadcasting, `Count()` with NULL
rows, initial NumPy tensor conversion, and writer statistics.

## Ray wheel distribution

Use a cluster with the Jobs API enabled and matching Python/Ray versions.
Install its `ray[default]` version in the submitting environment. Put your
application in `app/main.py` and initialize Ray with `ray.init(address="auto")`.
Submit the application and the wheel produced by `make python-wheel`:

```python
from ray.job_submission import JobSubmissionClient

client = JobSubmissionClient("http://127.0.0.1:8265")  # Your cluster's Jobs API.
job = client.submit_job(
    entrypoint="python main.py",
    runtime_env={
        "working_dir": "./app",
        "py_modules": ["/absolute/path/to/the-built-wheel.whl"],
    },
)
print(job)
```

Ray uploads the application and wheel for the driver and workers. Workers must
meet the [compatibility requirements](#compatibility), be able to install wheel
dependencies, and have access to input data, credentials, and UDF dependencies.
For BoltML remote stages, use `REMOTE_LEAF` as in the [Ray example](../README.md#run-on-ray).
Use the [Job SDK](https://docs.ray.io/en/latest/cluster/running-applications/job-submission/sdk.html)
to retrieve status and logs.

## Release guidance

A local Linux wheel is not automatically portable. Before publishing, build for
the intended ABI baseline using [manylinux](https://github.com/pypa/manylinux),
repair with [auditwheel](https://github.com/pypa/auditwheel), and test on the target
CPU/OS. Version `0.0.1` is development metadata; no PyPI release or validated
release workflow is provided here.
