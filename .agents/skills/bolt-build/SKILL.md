---
name: bolt-build
description: Use when working on the Bolt repository and you need to configure or compile code correctly and efficiently. This skill defines the canonical Bolt build workflow, including when and how to run `make`, `cmake --build ...`, and when build reconfiguration is necessary due to added files and test or build target changes.
---

# Bolt Build

Use this skill for any Bolt task that requires configuring or compiling the repository for binaries
tests, or benchmarks

## Workflow

- **Configure** the repository using instructions from the "Configure" section ONLY if necessary
- **Compile** the project using instructions from "Compile" section
  - Wait for all units to finish. If compilation exits early, check the error
  - If killed (e.g. due to OOM), restart the build, but lower the number of jobs
  by adding the `--parallel` flag and specify a lower number than the machine's available cores.
  If the build is repeatedly killed to due OOMs, decrease it after each failure.


## Core rule

- Use `cmake --build --preset conan-${BUILD_TYPE} --target ${TARGET}` for all routine compilations

## Configure

Configuration sets up the `_build` directory with the correct files to allow
cmake and ninja to compile all files in the project.

## When to reconfigure

Run `make <TARGET> BOLT_CONAN_CONFIGURE_ONLY=1` again only if:
- The required build type is different from the current one
- The desired target does not exist in the current configuration
- You need to include or exclude tests and the current configuration does not match
Configure only if any of these conditions are met
- debug info is not available in the existing binary and the goal is to debug
with GDB, or the user requests debug build.

Otherwise, just use the existing configured build.

## Configuration Command

To configure a build, run:

```bash
make <TARGET> BOLT_CONAN_CONFIGURE_ONLY=1
```

This chooses the active configuration, including:
- Build type: `debug`, `release`, or `relwithdebinfo`
- Whether tests are included for that target/configuration
- Available targets are listed in the `Makefile` in the root of the project.
  - If building tests, usually need `*_with_test`
  - If building benchmarks, usually need `benchmarks-*`

## Compile

If the build is already configured for the needed target and build type, compile with:

```bash
cmake --build --preset conan-${BUILD_TYPE} --target ${TARGET}
```

Valid preset suffixes:
- `debug`
- `release`
- `relwithdebinfo`

## Detecting the current build type

Check `_build/.build_type` to determine the currently configured build type:

```bash
cat _build/.build_type
```

This returns one of: `debug`, `release`, or `relwithdebinfo`.

Use this to decide whether you can reuse the existing preset or need to reconfigure for a different build type.

## Working rules

- Prefer the narrowest target that validates the change.
- Reuse the current configured preset whenever possible.

## Finding the narrowest target

To identify the smallest target that contains an edited compilation unit:

1. Locate the edited `*.cpp` file's directory.
2. Find `CMakeLists.txt` in that directory (or the nearest parent directory).
3. Find the target with the edited `*.cpp` file in its source list.

Example:
- Edited file: `bolt/core/execution/ExecutionPlan.cpp`
- Check: `bolt/core/execution/CMakeLists.txt` (or `bolt/core/CMakeLists.txt`)
- Look for: `add_library(bolt_core ... ExecutionPlan.cpp ...)` or `add_executable(bolt_core_tests ...)`
- Build: `cmake --build --preset conan-release --target bolt_core`

This ensures you compile only what's necessary to validate your change.
