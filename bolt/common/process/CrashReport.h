/*
 * Copyright (c) ByteDance Ltd. and/or its affiliates
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#pragma once
namespace bytedance::bolt::process {
enum class StackTraceEngine {
  kLibUnwind,
  kBacktrace,
  kLibBacktrace,
};
/**
 * Install a crash report signal handler that prints a stack trace
 * when the program terminates due to a fatal signal (e.g. SIGSEGV).
 * All stack trace engines use the same frame format. Frame addresses include a
 * cached module path and an `elf=` address when available. The latter can be
 * passed directly to an offline symbolizer with the matching unstripped binary
 * or debug file. When entries remain unresolved, the report includes a fallback
 * shell filter that reads the complete report, resolves only `in ??` entries,
 * and writes the complete report back with all other lines unchanged. Module
 * mappings are snapshotted during installation; frames from modules loaded
 * later remain raw rather than consulting the dynamic loader after a crash.
 *
 * @param runInJvm   Whether to check if the current process is running inside a
 *                     JVM.
 *                   - The JVM overrides and specially manages signal handlers.
 *                   - If true, this will verify that libjsig.so has been
 *                     preloaded (via LD_PRELOAD). If it has not, the handler
 *                     will not be installed to avoid interfering with JVM
 *                     behavior.
 * @param demangle   Whether the offline report filter demangles C++ symbols.
 *                   No demangling runs in the signal handler.
 * @param engine     The stack trace engine to use.
 *                   All engines collect raw program counters only.
 *                   - kLibUnwind: Uses libunwind register/step operations.
 *                   - kBacktrace: Uses execinfo backtrace address collection.
 *                   - kLibBacktrace: Uses backtrace_simple without loading
 *                     symbol or source information.
 */
void installCrashReportHandler(
    bool runInJvm = false,
    bool demangle = false,
    StackTraceEngine engine = StackTraceEngine::kLibBacktrace);
/**
 * A set of async-signal-safe helper functions for writing output directly
 * to standard output or standard error. These avoid non-signal-safe
 * library functions (such as printf or std::string) and can therefore
 * be safely used inside signal handlers.
 */

/** Write a string of the given length to standard error. */
void writeToStderr(const char* msg, unsigned long len);
/** Write a string of the given length to standard output. */
void writeToStdout(const char* msg, unsigned long len);
/** Write a null-terminated string to standard error. */
void writeToStderr(const char* msg);
/** Write a null-terminated string to standard output. */
void writeToStdout(const char* msg);
/**
 * Write a long integer value to standard output.
 * @param value  The numeric value to print.
 * @param base   Number base (default = 10), supported bases are 10 and 16.
 * @param width  Minimum field width (padded with spaces if necessary,
 *               -1 means unspecified).
 */
void writeToStdout(long value, int base = 10, int width = -1);
/**
 * Write a long integer value to standard error.
 * @param value  The numeric value to print.
 * @param base   Number base (default = 10), supported bases are 10 and 16.
 * @param width  Minimum field width (padded with spaces if necessary,
 *               -1 means unspecified).
 */
void writeToStderr(long value, int base = 10, int width = -1);
} // namespace bytedance::bolt::process
