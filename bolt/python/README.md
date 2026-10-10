<!--
Copyright (c) ByteDance Ltd. and/or its affiliates.
SPDX-License-Identifier: Apache-2.0
-->

# BoltML: data processing with Bolt in Python

Use Bolt's C++ execution engine from Python to read data, run queries, and
prepare ML features. The **`bolt-engine`** distribution includes two APIs:

- **`boltml`**: read Parquet and ORC files, filter, join, and aggregate data, and
  apply Python functions through a DataFrame API. Work with Arrow and NumPy,
  process image bytes, or configure execution on Ray workers.
- **`pybolt`**: work directly with native types and vectors, construct query
  plans, and execute them with Bolt from Python.

Start with `boltml` for data processing. Use `pybolt` when you need to construct
query plans directly; BoltML also uses its types for schemas and Python
function return values.

## What can BoltML do?

| Task | API and examples |
| --- | --- |
| Filter rows, derive columns, sort, join, and group data | `DataFrame`, `filter`, `transform`, `orderBy`, `join`, `groupBy().aggregate()`; [examples](tests/integration/test_boltml.py). |
| Apply Python logic | Scalar, vector, and batch UDFs, plus custom aggregations; [examples](tests/integration/test_boltml.py). |
| Exchange data with Python libraries | Arrow input/output and NumPy adapters, including fixed-shape tensor UDFs; [examples](tests/integration/test_boltml.py). |
| Read and write local Parquet or ORC files | `LocalTableReader` and `LocalTableWriter`, including partitioned and bucketed writes; [examples](tests/integration/test_local_io.py). |
| Process images | Image bytes in binary columns, with Pillow in Python UDFs; [example](tests/integration/test_boltml.py). |
| Execute stages on Ray workers | Explicitly configured `RayExecutor` remote execution; [example](#run-on-ray). |
| Access Hive or Paimon tables | Catalog readers/writers require external services and configuration; see [Hive](boltml/test/test_hive.py) and [Paimon](boltml/test/test_paimon.py) examples. |

The quick start below runs locally, with no Ray cluster or catalog service.
See [test coverage and known limitations](packaging/BUILDING.md#testing) before
adopting more advanced features; Hive and Paimon service integration is outside
the current CI checks.

## Quick start

The validated platform is **CPython 3.11 on Linux x86-64**. See the
[CPU and system requirements](packaging/BUILDING.md#compatibility).

### 1. Install

With [uv](https://docs.astral.sh/uv/getting-started/installation/) installed and
the Bolt checkout at `./bolt`, create an application project next to it and
install BoltML from source:

```bash
uv init --python '>=3.11,<3.12' boltml-demo
cd boltml-demo
uv add ../bolt/bolt/python
```

For an existing uv project, run the `uv add` command from that project's
directory and adjust the checkout path as needed.
If migrating from the old distribution, run `uv remove bytedbolt` before
`uv add ../bolt/bolt/python`: both distributions provide the same `boltml` and
`pybolt` packages.

This builds and installs Bolt automatically, including Pillow, Ray, and the
Paimon client. The first build needs a supported
Linux C++ toolchain, development headers, Git, Bash, and network access, and can
take substantial time and memory. The backend prepares Conan dependencies;
see [workstation setup](../../CONTRIBUTING.md) and
[build configuration](packaging/BUILDING.md#build-configuration) for prerequisites
and options.

### 2. Run your first query

Save this as `quickstart.py`:

```python
import pyarrow as pa
from boltml import DataFrame
from boltml.function.aggregation import Sum

orders = DataFrame({
    "customer": ["alice", "bob", "alice"],
    "amount": [10, 20, 30],
})

# Keep orders worth at least 20, then apply a discount of 5 per order.
orders.filter(orders["amount"] >= 20)
orders.transform(net_amount=lambda df: df["amount"] - 5)
totals = orders.groupBy("customer").aggregate(total=Sum("net_amount"))

print(totals.orderBy("customer").toArrow(pa.Table).to_pylist())
```

Run it in your application's environment:

```bash
uv run python quickstart.py
```

Expected output:

```text
[{'customer': 'alice', 'total': 25}, {'customer': 'bob', 'total': 15}]
```

Transformations build a query plan; `toArrow(pa.Table)` executes it and returns
a PyArrow table. Methods such as `filter`, `transform`, and `orderBy` modify the
current DataFrame. Use `frame.copy()` before branching a pipeline, and build
column expressions from the frame being transformed. Calling `toArrow()` with
no argument returns an Arrow `StructArray` instead of a table.

### 3. Add a Python function

For custom logic, declare the return type with `scalarFunction` and apply the
function to a column. Save this as `udf_example.py` and run it with
`uv run python udf_example.py`:

```python
import pybolt
from boltml import DataFrame
from boltml.function import scalarFunction


@scalarFunction(pybolt.BigintType())
def addOne(value):
    return None if value is None else value + 1


def main():
    frame = DataFrame({"value": [1, None, 3]})
    frame.transform(incremented=lambda df: df["value"].map(addOne))
    print(list(frame["incremented"]))  # [2, None, 4]


if __name__ == "__main__":
    main()
```

This function handles nulls explicitly. Materializing a column as a list also
executes the plan. Keep UDF DataFrames inside a function as shown: retaining
them as module globals can currently cause a native memory-pool error when
Python exits.

For Arrow vector functions, batch functions, tensors, and
image processing, see the [runnable integration examples](tests/integration/test_boltml.py).
Pillow is installed by default for image decoding, resizing, and encoding in
Python UDFs. Images are stored as bytes in binary columns.

## Run on Ray

Ray is included in the installation above. Save the following as
`ray_example.py`. It starts a local Ray cluster and runs a DataFrame
transformation in a worker process:

```python
import ray
from boltml import DataFrame
from boltml.distributed.ray import RayExecutionConfig, RayExecutionMode
from boltml.executor.ray import RayExecutor


def main():
    ray.init(address="local", num_cpus=2, include_dashboard=False)
    try:
        executor = RayExecutor(
            config=RayExecutionConfig(mode=RayExecutionMode.REMOTE_LEAF)
        )
        frame = DataFrame({"x": [1, 2, 3]}, executor=executor)
        frame.transform(result=lambda df: df["x"] + 1)
        print(list(frame["result"]))

        stages = executor.lastExecutionSummary.stages
        assert stages and all(
            stage.remote and stage.fallbackReason is None for stage in stages
        )
        print("Remote tasks:", sum(stage.remoteTaskCount for stage in stages))
    finally:
        ray.shutdown()


if __name__ == "__main__":
    main()
```

For this local example, let Ray workers reuse the installed application
environment. Disable Ray's automatic uv project upload, which does not include
the Bolt checkout outside the application directory:

```bash
RAY_ENABLE_UV_RUN_RUNTIME_ENV=0 uv run python ray_example.py
```

In addition to Ray's startup logs, the output is:

```text
[2, 3, 4]
Remote tasks: 1
```

`REMOTE_LEAF` enables remote execution; the default `RayExecutor()` runs stages
locally. The assertion checks that this query ran remotely without local
fallback. This in-memory input creates one task even though two CPUs are
available. See the [Ray integration examples](tests/integration/test_boltml_ray.py)
for parallel file scans and Python UDFs on workers.

When running on an existing cluster node or in a submitted Ray job, replace
the initialization call with `ray.init(address="auto")` and make Bolt and your
input data available to the workers. Follow the
[wheel distribution guide](packaging/BUILDING.md#ray-wheel-distribution) to
package your local build and submit a Ray job.

## Develop with uv

If you are modifying BoltML itself, install the checkout in editable mode:

```bash
uv add --editable ../bolt/bolt/python
```

Your application owns its `pyproject.toml`, `uv.lock`, and environment. Python
edits take effect on the next run. After C++ or CMake edits, run
`uv sync --reinstall-package bolt-engine` and restart Python. Keep the checkout
available. See [generated Hive bindings and IDE setup](packaging/BUILDING.md#hive-metastore-bindings)
if your editor cannot resolve those imports after the first build.

## Build a wheel

From the Bolt checkout:

```bash
make python-wheel
```

This build command needs Python with pip; it does not require uv.
It prints the wheel's absolute path under `dist/`. Install that path
with `uv add` in your application, or distribute the wheel to matching hosts.
Each build has a unique filename and reuses native build output. For selecting
your application's interpreter and updating a uv dependency, see
[development wheels](packaging/BUILDING.md#development-wheels).

## Distribute to Ray workers

Use a built wheel with the standard
[Ray Job submission example](packaging/BUILDING.md#ray-wheel-distribution).
`runtime_env.py_modules` uploads it for the job's driver and workers. An editable
installation on your workstation does not install Bolt on the cluster.

For BoltML-managed remote stages, explicitly select `RayExecutionMode.REMOTE_LEAF`
when configuring `RayExecutor`; its default mode executes stages locally. The
deployment guide covers this configuration and worker data/dependency access.

See [native pybolt examples](tests/integration/test_pybolt.py) for lower-level
vectors and plans, [BUILDING.md](packaging/BUILDING.md) for build and deployment
details, including [tests and known limitations](packaging/BUILDING.md#testing).
