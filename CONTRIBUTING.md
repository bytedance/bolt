# Contributing to Bolt

Welcome to the Bolt community! Bolt is a C++ acceleration library focused on high performance, designed to provide a consistent physical execution layer for various frameworks and data formats. We welcome and appreciate all forms of contributions from the community, whether it's fixing bugs, improving documentation, adding tests, optimizing performance, or implementing new features.

## List of Contents
- [How You Can Contribute](#how-you-can-contribute)
- [Development Environment Setup](#development-environment-setup)
- [Fork and PR Workflow](#fork-and-pr-workflow)
- [Building and Testing](#building-and-testing)
  - [Common Targets](#common-targets)
  - [Gluten Bolt backend CI](#gluten-bolt-backend-ci)
  - [Working with IDE](#working-with-ide)
- [Code Style and Static Analysis](#code-style-and-static-analysis)
  - [Formatting (clang-format)](#formatting-clang-format)
  - [Static Analysis (clang-tidy)](#static-analysis-clang-tidy)
  - [Code of Conduct](#code-of-conduct)
- [Unit Testing](#unit-testing)
- [Copyright and Licensing](#copyright-and-licensing)
- [Getting Help](#getting-help)


## How You Can Contribute

- **Report Issues**: Describe bugs or suggest enhancements to GitHub Issues. Please provide detailed reproduction steps and environment information. For example:
	- **Reporting Bugs**: If you find a crash, wrong result, or build failure, please file a [Bug Report](https://github.com/bytedance/bolt/issues/new?template=01_bug_report.yml).

	- **Performance Tuning**: Bolt is all about speed. If you identify a bottleneck, file a [Performance Issue](https://github.com/bytedance/bolt/issues/new?template=06_performance_issue.yml).

- **Contribute Code**: Fix bugs, implement small features, or drive architectural improvements.

- **Contribute Tests**: Add unit tests, benchmarks, or regression cases for bug fixes and new features.

- **Review Code**: Participate in pull request discussions and offer constructive feedback (requires relevant context and experience).

- **Improve Documentation**: Correct errors, clarify usage, or add new developer guides.

- **Help Community Members**: Answer questions, share best practices, and support other users in [Discussions](https://github.com/bytedance/bolt/discussions).


If you're a first-time contributor, we recommend starting with issues labeled "good-first-issue" or "help-wanted", see [issue list](https://github.com/bytedance/bolt/issues?q=is%3Aissue%20state%3Aopen%20label%3A%22good%20first%20issue%22%20label%3A%22help%20wanted%22)

## Development Environment Setup

Bolt uses **Conan 2** for dependency management and a Makefile to drive its build and test processes. You can set up the environment automatically using our helper script, or configure it manually if you prefer granular control over your toolchain.

### Prerequisites

- **OS**: Linux (Ubuntu 20.04+, CentOS 7+) or macOS (Experimental).

- **Compiler**: GCC 10+ or Clang 16+ (Must support C++17).
- **Build Tools**: CMake 3.25+, Ninja.
- **Dependency Manager**: Conan 2.0+.

### Setup Options

#### Option 1: Automatic Setup (Recommended)

Run the helper script to bootstrap your development environment:

```Bash
scripts/setup-dev-env.sh
```
**⚠️ Important: What this script does** This script is "opinionated" and performs the following actions. Please review them to ensure they fit your workflow:

1. **Python Environment (Miniconda)**:
   - Checks for `~/miniconda3`.
   - **If missing**: Automatically downloads and installs Miniconda locally.
   - **Shell Modification**: Appends the Miniconda path to your shell configuration file (`~/.bashrc` or `~/.zshrc`).
   - Installs Python dependencies (including `conan`) into this environment.
2. **Conan Configuration**:
   - Sets the C++ standard to `gnu17` in the default Conan profile.
   - **Clones & Patches Recipes**: Calls `install-bolt-deps.sh` to download a specific version of `conan-center-index` to `~/.conan2/conan-center-index` and applies Bolt-specific patches (required for `folly`, `arrow`, etc.).
   - Configures local Conan remotes to prioritize these patched recipes.
3. **Git Hooks**: Installs `pre-commit` hooks for automatic code formatting.

#### Option 2: Manual Setup (For Advanced Users)

If you prefer to manage your own Python environment (e.g., via system packages, pyenv, or poetry) or use a specific compiler, you can skip the setup script. However, you **must** still configure the Conan dependencies manually.

1. **Install Python Dependencies**:

   ```Bash
   pip install -r requirements.txt
   pre-commit install
   ```

2. **Configure Conan Dependencies (Crucial)**: Bolt relies on patched versions of several libraries. You must run the dependency installation script to set up the local recipe index and apply necessary patches:

   ```Bash
   # This sets up the local conan-center-index with required patches
   # and configures the 'bolt-local' and 'bolt-cci-local' remotes.
   scripts/install-bolt-deps.sh
   ```

### Platform Specific Notes

- **First-Time Build**: The first build will compile dependencies from source (e.g., Folly, Arrow), which can take a significant amount of time. Please be patient.

- **macOS Users**: Ensure Xcode Command Line Tools are installed:

  ```bash
  xcode-select --install
  ```

## Fork and PR Workflow

We follow the standard GitHub fork-and-PR workflow. For details, please refer to [workflow](./doc/workflow.md):

1. **Fork** the `bytedance/bolt` repository to your personal GitHub account.

2. **Clone** your fork locally and create a new topic branch from your target branch (usually `main`).

3. **Develop and commit** your changes on the topic branch. Each commit should represent a logical unit of work.

4. **Push** the topic branch to your remote fork.

5. **Open a** **pull request** to the upstream repository with a clear title and description.


## Building and Testing

Bolt's build and test workflows are centralized in the root Makefile. You can run `make help` to see all common targets and their descriptions.

### Common Targets

```Bash
# Standard builds (for presto)
make debug        # Build a debug version
make release      # Build an optimized release version

# Build with tests and run them
make release_with_test       # Build a release version with tests enabled
make unittest                # Run unit tests in the debug build directory
make unittest_release        # Run unit tests in the release build directory

# Code coverage (requires a debug build with coverage flags)
make unittest_coverage

# Example build for Spark/Gluten (see README)
make release_spark
```

**Notes**:

- Build artifacts are placed in `_build/<BuildType>` (e.g., `_build/Release`).

- Unit tests are driven by CTest (using GoogleTest). `make unittest[_release]` invokes `ctest` in the corresponding build directory.

- You can set variables like `BUILD_VERSION` and `FILE_SYSTEM` as needed (see the Makefile for details). For example:


```Bash
make release_spark BUILD_VERSION=main
```


### Gluten Bolt backend CI

The [Gluten Bolt Backend workflow](.github/workflows/gluten-bolt.yml) is called
by Build and Test on pull requests and merge groups, and also supports manual
dispatch. It tests the candidate Bolt code against `apache/gluten:main`.
Gluten builds the candidate Bolt engine and test utility library with
`release_spark` and `enable_testutil=True`; Bolt's own UT executables are not
built on this path. The existing `release_spark_with_test` job still builds and
runs Bolt's UTs independently, so Gluten does not wait for it. Both paths enable
HDFS and use Bolt's existing compiler cache. Gluten shares its native build and
newly built dependencies with all downstream test jobs through short-lived
Actions artifacts, without publishing to a Conan remote. The `gluten-bolt-check`
job in Build and Test requires every Gluten job to succeed.
Native CTest runs with the shared build. JVM compilation runs once per
Spark/Scala/storage configuration. Each job verifies the native library hashes,
then tests its own JVM build without transferring it between jobs.
Native Iceberg connector tests are skipped because Bolt does not implement that
connector. The inherited Java fixtures are still compiled; Iceberg helper and
ordinary backend Java UTs run separately from Scala tests because their ORC
dependencies conflict. Scala tests use the regular Spark 3.4 profile for
Extended SQL/Slow Hive and retain the Iceberg fallback test that checks ordinary
Parquet scans remain offloaded when native Iceberg reads are disabled.
Reports and logs are retained separately for each test group and uploaded per profile.
The regular x86 test scope follows Gluten's `velox_backend_x86.yml`:

| Spark / Scala | Storage profiles | Spark SQL groups |
| --- | --- | --- |
| 3.4 / 2.12 | Iceberg (helper Java UTs), Delta, Hudi, Paimon | Three regular groups, Extended SQL, Slow Hive |
| 3.5 / 2.12 | Iceberg, Delta, Hudi, Paimon | Three regular groups, Extended SQL, Slow Hive |
| 3.5 / 2.13 | Iceberg, Delta | Three regular groups |
| 4.0 / 2.13 | Delta, Paimon (Paimon excluded from Slow Hive) | Three regular groups, Extended SQL, Slow Hive |
| 4.1 / 2.13 | Delta (regular groups only) | Three regular groups, Extended SQL, Slow Hive |

These 23 SQL groups use Velox's suite/case selection and SQL-query golden resources via
`GLUTEN_BOLT_USE_VELOX_TEST_SETTINGS=true`. The workflow has ten jobs: one resolves
Apache main, one builds the shared native backend, and eight compile and test the
Spark/Scala/storage profiles above. Each profile reuses its JVM build in the same
job and runs its SQL groups sequentially. There are no JVM artifact transfers or
SQL suite shards; fewer jobs trade some parallelism for less setup and network
traffic. Backend JVM tests run for all five Spark/Scala combinations. Isolated
Delta suites run for Spark 3.5, 4.0 and 4.1;
Spark 3.4's Delta 2.4 coverage is in the backend JVM suites. TPC-H/TPC-DS
comparison runs on Spark 3.5. Backend tests are excluded from the SQL reactor runs
to avoid duplication. Additional OS/JDK/ARM, ANSI and fault-injection matrices
are outside this workflow's scope. Test selection parity does not imply that
Bolt passes every selected test; temporary exact exclusions remain explicit.
Temporary source compatibility fixes live in
`.github/gluten-ci-patches/` and are applied explicitly to the resolved Apache
main. Already-applied patches are accepted; incompatible patches fail the job.

The default image and runner match Bolt's existing CI. Missing or empty
repository Actions variables use these defaults:

| Optional variable | Default |
| --- | --- |
| `BOLT_GLUTEN_CI_IMAGE` | `bolt-registry:5000/bolt-ci:20260921` |
| `BOLT_GLUTEN_CI_CONAN_IMAGE` | `bolt-registry:5000/conan-server:latest` |
| `BOLT_GLUTEN_CI_RUNNER` | `["self-hosted", "medium"]` |

Image overrides must be available to every selected runner. The runner must
reach the Bolt registry and Conan service. The workflow
reuses `bolt-build-base` for Conan setup and `install-bolt-deps.sh`, installs a
full JDK 17 with `setup-java`, and provisions the pinned Python test runtime.
Maven builds and tests use the same local repository, defaulting to
`$HOME/.m2/repository`. Set `GLUTEN_MAVEN_REPO` to reuse a different local cache.
Only the native build is transferred between jobs using GitHub Actions artifacts;
Bolt and Gluten Conan packages are not uploaded to a Conan remote. Downloads are
retried and checked against GitHub's SHA256 digest before extraction.
Spark binaries and SQL test resources come from Gluten's existing
`.github/workflows/util/install-spark-resources.sh`; its SHA512 verification
requires `shasum` in the CI image.

Local execution requires Linux x86_64, a full JDK 17 with AWT, Python 3.10+,
Conan 2.32.0, Maven 3.9+, CMake 3.31, Ninja, mold, curl, jq,
xmllint (libxml2-utils), unzip, and a C++ compiler. Provide a Spark 3.5.5 binary
distribution with Python ZIPs and the matching source `sql/`
test resources, plus PySpark 3.5.5, pandas 2.2.3 and PyArrow 20.0.0. Set
`BOLT_GLUTEN_SPARK_HOME` to the binary directory; its `sql/` can be a symlink
to the source tree. The container default is `/opt/shims/spark35/spark_home`.

Use an isolated checkout and Conan home, with the same linker settings as the
workflow. From the Bolt repository root:

```bash
git clone --branch main https://github.com/apache/gluten.git gluten
export JAVA_HOME=/path/to/jdk17
export PATH="$JAVA_HOME/bin:$PATH"
export BOLT_GLUTEN_SPARK_HOME=/path/to/spark-3.5.5
export CONAN_HOME="$PWD/.ci/conan"
./scripts/install-bolt-deps.sh
python3 scripts/configure-conan-profile.py \
  --compiler clang --libcxx libstdc++11 --linker mold --lto none
export CI_BUILD=spark35
export GLUTEN_BOLT_USE_VELOX_TEST_SETTINGS=true
source .github/scripts/gluten_ci.sh
ci_apply_patches
ci_init
ci_spark_environment
# Run one backend suite against the native backend built from this checkout.
CI_SUITES=org.apache.gluten.execution.BoltLiteralSuite \
  ci_maven_test jvm backends-bolt "${MVN[@]}" test \
    "-DargLine=-Xmx4g -Dspark.test.home=${SPARK_HOME}"
```

Build and test commands are in the `build` and `test` jobs' `run` blocks in
`.github/workflows/gluten-bolt.yml`; execute them in order for a complete local
run. The build block configures Release and exports the
candidate Bolt package into the selected Conan home. The example above runs
one suite after that build. Set `CI_NUM_THREADS` for local build parallelism.
Reports and failure logs go to `.ci/gluten-bolt/`. Test JVMs use a 4 GiB heap so
heap-dependent Spark tests behave consistently across local and CI hosts.
The shell helper handles runtime activation, logging and fresh report capture.
Maven test stages also require valid fresh XML reports with executed tests;
missing, malformed, empty or entirely skipped reports cannot pass a stage.
Tests use CTest, Maven and Gluten's `gluten-it.sh` entry points directly; their
exit status determines whether a stage succeeds. The workflow checks that
Gluten consumes the exported candidate Bolt package and native libraries.

`.github/gluten-ci-blacklist.txt` lists temporary exact testcase exclusions.
`ci_init` exports its path as `GLUTEN_TEST_BLACKLIST`; a Gluten test-layer patch
registers matching cases as ignored before executing their bodies. Unlisted
failures, compilation errors, suite aborts and crashes remain fatal to Maven.
Entries use `[Spark/Scala]suite#exact testcase`, for example the version prefix
`[3.5/2.12]`; exclusions apply only to that Spark minor and Scala binary version.
The blacklist covers exact observed failures in supported tests. Unsupported
native Iceberg connector tests are excluded in `backends-bolt` rather than
enumerated in this file. Unset
`GLUTEN_TEST_BLACKLIST` after `ci_init` to retest excluded cases. Remove entries
only after verifying the corresponding fixes.

### Working with IDE

Vscode guide in [English](./doc/vscode-config-en.md) or [Chinese](./doc/vscode-config.md).

## Code Style and Static Analysis

To maintain consistent code quality and style, please run the following checks before submitting your changes.

- **C++ Standard**: We use **C++17**.

- **Formatting**: We use `clang-format-14`.

- **Naming**: Follow the [Style Guide](./doc/coding-style.md).


### Formatting (clang-format)

Bolt uses [pre-commit](https://pre-commit.com/) to manage all git hooks, so clang-format checks/formatting can also be done within `pre-commit`.

If you have executed `scripts/setup-dev-env.sh`, then `pre-commit` and all the pre-defined git hooks are already installed. When you commit code using `git commit ...`, clang-format will be executed automatically, you only need to `git add ...` the formatted file again.

By the way, you can execute `pre-commit install` to install all git hooks manually.

You also can use below command to format manually, but please ensure clang-format version is equal to `14.0.6`. Different version clang-format may has different default behavior.

```Bash
# Run the format check (same as in CI)
make clang-format-check

# format on a single file
clang-format -i -style=file path/to/your/file.cpp

# format all files in batch
find bolt -name "*.h" -o -name "*.cpp" | xargs clang-format -i -style=file
```

### Static Analysis (clang-tidy)

The repository provides a `.clang-tidy` configuration and a helper script at `scripts/run-clang-tidy.py`.

#### Prerequisites
It is best to run this after a successful build, as it relies on the compilation database (`compile_commands.json`). By default, the script looks for this database in `_build/Release` or the current directory.

```Bash
# First, create a release build to generate the compile database (compile_commands.json)
make release

```

#### Basic Usage
You can run the script on specific files, directories, or based on git changes.

```Bash
# 1. Run on specific files
python3 scripts/run-clang-tidy.py src/path/to/file.cpp src/other/file.h

# 2. Run recursively on a directory (New feature)
# This will scan all .cpp/.h/.cc etc. files in 'src/'
python3 scripts/run-clang-tidy.py -d src/

# 3. Parallel execution (Recommended for speed)
# Use '-j' to specify number of threads (defaults to half of CPU cores)
python3 scripts/run-clang-tidy.py -d src/ -j 12
```

#### Advanced Filtering & Configuration
The script includes default filters to skip tests and benchmarks. You can customize this behavior.

```Bash
# Exclude specific files using Regex
# (Default excludes: tests/, benchmarks/, *Test.cpp, etc.)
python3 scripts/run-clang-tidy.py -d src/ --exclude "legacy/|.*_generated.cpp"

# Specify a custom build path if not in _build/Release
python3 scripts/run-clang-tidy.py -d src/ -p build/debug/

# Use a specific clang-tidy binary or config file
python3 scripts/run-clang-tidy.py -d src/ \
    --clang-tidy-binary /usr/bin/clang-tidy-14 \
    --config-file .clang-tidy-strict
```

#### Focused Checks (Git Awareness)
To save time, you can check only the files or lines changed in a specific commit.

```Bash
# Check only the lines changed in the last commit (Incremental check)
python3 scripts/run-clang-tidy.py --commit HEAD~1 src/

# Check files changed relative to the main branch
python3 scripts/run-clang-tidy.py --commit origin/main src/
```

#### Auto-Fixing

```Bash
# Automatically apply fixes (Use with caution and review changes)
python3 scripts/run-clang-tidy.py --fix src/
```

#### CI/CD & Automation Features

**GitHub Actions Auto-Annotations** The script contains built-in logic to detect GitHub Actions environments. When running in a CI pipeline, it translates clang-tidy errors into GitHub Annotations, allowing them to show up directly in Pull Request diff views.

**Standard Input (Stdin)** You can pipe file paths directly into the script using the `-` argument:

```Bash
find src/ -name "Legacy*.cpp" | python3 scripts/run-clang-tidy.py -
```

**Environment Variables**

- `BUILD_`BUILD_PATH`: Overrides the default search path for `compile_commands.json` (alternative to `-p`).
- `GITHUB_ACTIONS`: Triggers the CI-friendly output format (automatically set by GitHub).

#### Script Options Reference

| Option                                    | Description                                                  |
| :---------------------------------------- | :----------------------------------------------------------- |
| `FILES` or `-`                            | List of specific files. Use `-` to read file list from stdin. |
| `-d`, `--directory`--directory`           | Recursively scan a directory for `.cpp`, `.h`, `.cc`, `.cxx`, etc. |
| `-j`, `--jobs`                            | Number of parallel threads (Batch size is fixed at 5 files per job). |
| `--commit`                                | **Incremental Mode**: Analyze only modified lines relative to a git ref. |
| `--fix`--fix`                             | Apply suggested fixes to the files automatically.            |
| `--exclude`                               | Regex to ignore files. Default includes tests, benchmarks, and test files. |
| `-p`                                      | Path to build dir. Falls back to `BUILD_PATH` env var, then `_build/Release`, then `.`. |
| `--clang-tidy-binary`--clang-tidy-binary` | Custom path to executable (e.g. `/usr/bin/clang-tidy-15`).   |
| `--config-file`                           | Path to custom config. If omitted, uses standard `.clang-tidy` lookup (`--`--format-style=file`). |

The script uses different check sets for test and main code. Please ensure your changes introduce no new warnings before committing.

### Code of Conduct

The Bolt project and all its contributors are governed by a [Code of Conduct.](https://www.apache.org/foundation/policies/conduct.html) By participating, you are expected to uphold this code.

## Unit Testing

- **Framework**: We use GoogleTest (see `bolt/bolt/expression/tests/*` for example).

- **Coverage**: Bug fixes and new features must be accompanied by minimal, reproducible unit tests with sufficient assertions. Avoid flaky tests or those with external dependencies.

- **Location**: Place test files in the `tests` directory of the corresponding module, following existing naming and structure conventions.

### Test Target Conventions

- GoogleTest unit test targets must link `bolt_gtest_main`. It provides the
  standard test entry point and the public `bolt_testutils` dependency.
- Do not define `main()` in individual GoogleTest source files or compile a
  second copy of the test entry point.
- Consume test utilities through `bolt_gtest_main` or `bolt_testutils`. Do not
  collect or link the object libraries that implement `bolt_testutils`
  directly.
- Select shared or static test linkage through `BOLT_TEST_LINKAGE`. Do not
  hard-code a concrete engine or testutils implementation target in an
  individual test.

- **Execution**:


```Bash
make unittest           # Build in debug and run ctest in _build/Debug
make unittest_release   # Build in release and run ctest in _build/Release
make unittest_coverage  # generate a coverage report
make unittest_release_spark # Tests in Spark

# run a specific test
cd bolt/_build/Release/bolt/functions/sparksql/tests
./bolt_functions_spark_test --gtest_filter=SparkCastExprTest.stringToTimestamp
```

### Sanitizers (Crucial for C++)

Before submitting a complex PR, it is highly recommended to run tests with AddressSanitizer (ASAN) and UndefinedBehaviorSanitizer (UBSAN) to catch memory leaks and undefined behaviors.

```Bash
ENABLE_ASAN=True make unittest_release
```

## Copyright and Licensing

- **License**: Apache 2.0 (see `LICENSE` and `NOTICE.txt`).

- **Headers**: All new source files must include the Apache 2.0 license header. You can copy it from the `license.header` template file and paste it (as a comment) at the top of all source code files.


## Getting Help

- **Questions and** **Feedback**: Open an issue on GitHub with your environment details, reproduction steps, and logs.

- **Usage and Development Discussions**: Refer to the `README.md` and this guide. If we open a discussion forum or chat channel, we will announce it in the README.

	<br>


Thank you for contributing to Bolt!
