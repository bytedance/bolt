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

#include "bolt/common/process/CrashReport.h"

#include <gtest/gtest.h>
#include <signal.h>
#include <sys/resource.h>
#include <unistd.h>

#include <cstring>

namespace bytedance::bolt::process {
namespace {

void disableCoreDumps() {
  struct rlimit limit {};
  limit.rlim_cur = 0;
  limit.rlim_max = 0;
  (void)setrlimit(RLIMIT_CORE, &limit);
}

void returningAbortHandler(int) {
  constexpr char kMessage[] = "PREVIOUS_ABORT_HANDLER\n";
  (void)write(STDERR_FILENO, kMessage, sizeof(kMessage) - 1);
}

void installReturningAbortHandler() {
  struct sigaction action {};
  action.sa_handler = returningAbortHandler;
  sigemptyset(&action.sa_mask);
  ASSERT_EQ(sigaction(SIGABRT, &action, nullptr), 0);
}

void ignoreAbortSignal() {
  struct sigaction action {};
  action.sa_handler = SIG_IGN;
  sigemptyset(&action.sa_mask);
  ASSERT_EQ(sigaction(SIGABRT, &action, nullptr), 0);
}

TEST(CrashReportTest, ReportsAndTerminatesWithOriginalSignal) {
  EXPECT_EXIT(
      {
        disableCoreDumps();
        installCrashReportHandler();
        raise(SIGABRT);
        _exit(99);
      },
      testing::KilledBySignal(SIGABRT),
      "Stack trace \\(best-effort, backend=libbacktrace\\):"
      "(.|\n)*#   0 0x(.|\n)*elf=0x(.|\n)*in [?][?]"
      "(.|\n)*Offline symbolization fallback for unresolved entries:"
      "(.|\n)*symbolize_report"
      "(.|\n)*--no-demangle --no-inlines");
}

TEST(CrashReportTest, FormatsLibunwindBackendConsistently) {
  EXPECT_EXIT(
      {
        disableCoreDumps();
        installCrashReportHandler(false, false, StackTraceEngine::kLibUnwind);
        raise(SIGABRT);
        _exit(99);
      },
      testing::KilledBySignal(SIGABRT),
      "Stack trace \\(best-effort, backend=libunwind\\):"
      "(.|\n)*#   0 0x(.|\n)*elf=0x(.|\n)*in [?][?]");
}

TEST(CrashReportTest, FormatsBacktraceBackendConsistently) {
  EXPECT_EXIT(
      {
        disableCoreDumps();
        installCrashReportHandler(false, false, StackTraceEngine::kBacktrace);
        raise(SIGABRT);
        _exit(99);
      },
      testing::KilledBySignal(SIGABRT),
      "Stack trace \\(best-effort, backend=backtrace\\):"
      "(.|\n)*#   0 0x(.|\n)*elf=0x(.|\n)*in [?][?]");
}

TEST(CrashReportTest, ReportsSegmentationSignalAsRawAddresses) {
  EXPECT_EXIT(
      {
        disableCoreDumps();
        installCrashReportHandler(false, false, StackTraceEngine::kLibUnwind);
        raise(SIGSEGV);
        _exit(99);
      },
      testing::KilledBySignal(SIGSEGV),
      "Signal: 11 SIGSEGV(.|\n)*Instruction pointer: 0x"
      "(.|\n)*in [?][?](.|\n)*--- END OF CRASH REPORT ---");
}

TEST(CrashReportTest, DefersRequestedDemanglingToOfflineSymbolizer) {
  EXPECT_EXIT(
      {
        disableCoreDumps();
        installCrashReportHandler(false, true, StackTraceEngine::kLibBacktrace);
        raise(SIGABRT);
        _exit(99);
      },
      testing::KilledBySignal(SIGABRT),
      "Demangling was requested and will be performed only by the offline "
      "symbolizer(.|\n)*in [?][?](.|\n)*--demangle --no-inlines");
}

TEST(CrashReportTest, ChainsPreviousHandlerAndStillTerminates) {
  EXPECT_EXIT(
      {
        disableCoreDumps();
        installReturningAbortHandler();
        installCrashReportHandler();
        raise(SIGABRT);
        _exit(99);
      },
      testing::KilledBySignal(SIGABRT),
      "PREVIOUS_ABORT_HANDLER");
}

TEST(CrashReportTest, FatalSignalCannotRemainIgnored) {
  EXPECT_EXIT(
      {
        disableCoreDumps();
        ignoreAbortSignal();
        installCrashReportHandler();
        raise(SIGABRT);
        _exit(99);
      },
      testing::KilledBySignal(SIGABRT),
      "BOLT CRASH REPORT");
}

TEST(CrashReportTest, InstallsAlternateStackForCallingThread) {
  EXPECT_EXIT(
      {
        installCrashReportHandler();
        stack_t alternateStack{};
        if (sigaltstack(nullptr, &alternateStack) != 0 ||
            (alternateStack.ss_flags & SS_DISABLE) != 0) {
          _exit(1);
        }
        _exit(0);
      },
      testing::ExitedWithCode(0),
      "");
}

TEST(CrashReportTest, CanRetryAfterInvalidStackTraceEngine) {
  EXPECT_EXIT(
      {
        disableCoreDumps();
        installCrashReportHandler(
            false, false, static_cast<StackTraceEngine>(999));
        installCrashReportHandler();
        raise(SIGABRT);
        _exit(99);
      },
      testing::KilledBySignal(SIGABRT),
      "Unknown stack trace engine(.|\n)*BOLT CRASH REPORT");
}

} // namespace
} // namespace bytedance::bolt::process

int main(int argc, char** argv) {
  if (argc == 3 && std::strcmp(argv[1], "--crash-report-helper") == 0) {
    bytedance::bolt::process::StackTraceEngine engine;
    if (std::strcmp(argv[2], "libunwind") == 0) {
      engine = bytedance::bolt::process::StackTraceEngine::kLibUnwind;
    } else if (std::strcmp(argv[2], "backtrace") == 0) {
      engine = bytedance::bolt::process::StackTraceEngine::kBacktrace;
    } else if (std::strcmp(argv[2], "libbacktrace") == 0) {
      engine = bytedance::bolt::process::StackTraceEngine::kLibBacktrace;
    } else {
      return 2;
    }

    struct rlimit limit {};
    limit.rlim_cur = 0;
    limit.rlim_max = 0;
    (void)setrlimit(RLIMIT_CORE, &limit);
    bytedance::bolt::process::installCrashReportHandler(false, false, engine);
    raise(SIGABRT);
    _exit(99);
  }

  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
