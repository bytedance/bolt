---
name: bolt-build
description: Use when working on the Bolt repository and you need to configure or compile code correctly and efficiently. This skill defines the canonical Bolt build workflow, including when to run `make <TARGET> BOLT_CONAN_CONFIGURE_ONLY=1`, when to switch to `cmake --build --preset conan-<build type> --target <TARGET>`, and when reconfiguration is required because the build type or test inclusion has changed.
---

# Bolt Build

Use this skill for any Bolt task that requires configuring or compiling the repository.

## Core rule

Do not use `make` for routine incremental compilation once the build is configured.

## Configure

To configure a build, run:

```bash
make <TARGET> BOLT_CONAN_CONFIGURE_ONLY=1
```

This chooses the active configuration, including:
- Build type: `debug`, `release`, or `relwithdebinfo`
- Whether tests are included for that target/configuration

## Build

If the build is already configured for the needed target and build type, compile with:

```bash
cmake --build --preset conan-<build type> --target <TARGET>
```

Valid preset suffixes:
- `debug`
- `release`
- `relwithdebinfo`

## When to reconfigure

Run `make <TARGET> BOLT_CONAN_CONFIGURE_ONLY=1` again only if:
- The required build type is different from the current one
- The desired target does not exist in the current configuration
- You need to include or exclude tests and the current configuration does not match

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
- If a compile fails, fix the smallest coherent issue and re-run the same narrow build first.
- In status updates and final summaries, state whether you reused an existing configured preset or had to reconfigure.

## Finding the narrowest target

To identify the smallest target that contains an edited compilation unit:

1. Locate the edited `*.cpp` file's directory.
2. Find the `CMakeLists.txt` in that directory (or the nearest parent directory).
3. Search for the target that lists the edited `*.cpp` file in its source list.
4. Build that target.

Example:
- Edited file: `bolt/core/execution/ExecutionPlan.cpp`
- Check: `bolt/core/execution/CMakeLists.txt` (or `bolt/core/CMakeLists.txt`)
- Look for: `add_library(bolt_core ... ExecutionPlan.cpp ...)` or `add_executable(bolt_core_tests ...)`
- Build: `cmake --build --preset conan-release --target bolt_core`

This ensures you compile only what's necessary to validate your change.

