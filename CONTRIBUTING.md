# Contributing to Bolt

Welcome to the Bolt community! Bolt is a C++ acceleration library focused on high performance, designed to provide a consistent physical execution layer for various frameworks and data formats. We welcome and appreciate all forms of contributions from the community, whether it's fixing bugs, improving documentation, adding tests, optimizing performance, or implementing new features.

## List of Contents
- [How You Can Contribute](#how-you-can-contribute)
- [Development Environment Setup](#development-environment-setup)
- [Fork and PR Workflow](#fork-and-pr-workflow)
- [Building and Testing](#building-and-testing)
  - [Common Targets](#common-targets)
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

The repository provides a `.clang-tidy` configuration and a helper script at `scripts/run-clang-tidy.py`. It's best to run this after a successful build, as it relies on the compilation database in `-p=build/release/`.

```Bash
# First, create a release build to generate the compile database
make release

# Run clang-tidy on specific files (reports issues without fixing)
python3 scripts/run-clang-tidy.py bolt/path/to/*.cpp bolt/other/path/*.h

# Check only the lines changed in the last commit (more focused)
python3 scripts/run-clang-tidy.py --commit HEAD~1 bolt/path/to/*.cpp

# Automatically apply fixes (use with caution and review changes)
python3 scripts/run-clang-tidy.py --fix bolt/path/to/*.cpp
```

The script uses different check sets for test and main code. Please ensure your changes introduce no new warnings before committing.

### Code of Conduct

The Bolt project and all its contributors are governed by a [Code of Conduct.](https://www.apache.org/foundation/policies/conduct.html) By participating, you are expected to uphold this code.

## Unit Testing

- **Framework**: We use GoogleTest (see `bolt/bolt/expression/tests/*` for example).

- **Coverage**: Bug fixes and new features must be accompanied by minimal, reproducible unit tests with sufficient assertions. Avoid flaky tests or those with external dependencies.

- **Location**: Place test files in the `tests` directory of the corresponding module, following existing naming and structure conventions.

- **Execution**:


```Bash
make unittest           # Build in debug and run ctest in _build/Debug
make unittest_release   # Build in release and run ctest in _build/Release
make unittest_coverage  # generate a coverage report
make unittest_release_spark # Tests in Spark

# run a specific test
cd bolt/_build/Release/bolt/functions/sparksql/tests
./velox_functions_spark_test --gtest_filter=SparkCastExprTest.stringToTimestamp
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
