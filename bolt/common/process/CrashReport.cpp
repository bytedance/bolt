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

#include <backtrace.h>
#include <cxxabi.h>
#include <dlfcn.h>
#include <execinfo.h>
#if defined(__linux__) || defined(__FreeBSD__)
#include <libunwind.h>
#endif
#include <string.h>
#include <unistd.h>

#include <algorithm>
#include <csignal>
#include <cstdint>
#include <cstdlib>
#include <mutex>
#include <vector>

static struct backtrace_state* G_BACKTRACE_STATE = NULL;
namespace bytedance::bolt::process {

struct FatalSignals {
  int32_t signum;
  const char* name;
  const char* description;
  struct sigaction oldAction;
};

FatalSignals kFatalSignals[NSIG] = {};

[[maybe_unused]] static bool SIGNAL_INITIALIZED = []() -> bool {
  kFatalSignals[SIGSEGV] = {
      SIGSEGV, "SIGSEGV", "Invalid access to storage", {}};
  kFatalSignals[SIGILL] = {SIGILL, "SIGILL", "Illegal instruction", {}};
  kFatalSignals[SIGFPE] = {
      SIGFPE, "SIGFPE", "Erroneous arithmetic operation", {}};
  kFatalSignals[SIGABRT] = {SIGABRT, "SIGABRT", "Abnormal termination", {}};
  kFatalSignals[SIGBUS] = {SIGBUS, "SIGBUS", "Bus error", {}};
  return true;
}();

namespace {

#define SAFE_STRLEN_MAX 40960

size_t safeStrlen(const char* s) {
  if (s == NULL)
    return 0;

  size_t len = 0;
  while (len < SAFE_STRLEN_MAX && s[len] != '\0') {
    ++len;
  }
  return len;
}

inline void writeTo(int32_t fd, const char* msg, uint64_t len) {
#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wunused-result"
  write(fd, msg, len);
#pragma GCC diagnostic pop
}
void printInteger(int32_t fd, int64_t value, int32_t base, int32_t width) {
  constexpr int32_t BUFFER_SIZE = 64;
  char buf[BUFFER_SIZE];

  char* ptr = &buf[BUFFER_SIZE];

  const char* hexDigits = "0123456789abcdef";

  bool isNegative = false;
  uint64_t num;

  if (value == 0) {
    *--ptr = '0';
  } else {
    if (base == 10 && value < 0) {
      isNegative = true;
      num = -static_cast<uint64_t>(value);
    } else {
      num = static_cast<uint64_t>(value);
    }

    while (num > 0) {
      if (base == 16) {
        *--ptr = hexDigits[num % 16];
        num /= 16;
      } else { // base 10
        *--ptr = '0' + (num % 10);
        num /= 10;
      }
    }

    if (isNegative) {
      *--ptr = '-';
    }
  }

  uint64_t len = &buf[BUFFER_SIZE] - ptr;
  if (base == 16) {
    writeTo(fd, "0x", 2);
  }

  writeTo(fd, ptr, len);

  int32_t paddingNeeded = width - static_cast<int32_t>(len);
  if (paddingNeeded > 0) {
    constexpr int32_t SPACE_BUF_SIZE = 16;
    char spaces[SPACE_BUF_SIZE];
    for (int32_t i = 0; i < SPACE_BUF_SIZE; ++i) {
      spaces[i] = ' ';
    }

    while (paddingNeeded > 0) {
      int32_t toWrite =
          (paddingNeeded > SPACE_BUF_SIZE) ? SPACE_BUF_SIZE : paddingNeeded;
      writeTo(fd, spaces, toWrite);
      paddingNeeded -= toWrite;
    }
  }
}

void setupAlternateStack() {
  size_t stackSize = std::max<size_t>(MINSIGSTKSZ, 65536);
  static std::vector<char> alternateStack(stackSize);

  stack_t ss = {};
  ss.ss_sp = alternateStack.data();
  ss.ss_size = alternateStack.size();
  ss.ss_flags = 0;

  if (sigaltstack(&ss, nullptr) != 0) {
    writeToStderr("[CrashReport] Warning: Failed to set up alternate stack.\n");
  }
}

template <bool demangle>
int32_t fullCallback(
    void* data,
    uintptr_t pc,
    const char* filename,
    int32_t lineno,
    const char* function) {
  auto* frameCounter = static_cast<int32_t*>(data);
  writeToStderr("#");
  writeToStderr(*frameCounter, 10, 4);
  writeToStderr(pc, 16);
  if (function) {
    writeToStderr(" in ");
    if constexpr (demangle) {
      static char demangleBuf[4096];
      int32_t status = 0;
      auto len = sizeof(demangleBuf);
      const char* demangled =
          abi::__cxa_demangle(function, demangleBuf, &len, &status);
      if (status == 0) {
        writeToStderr(demangled);
      } else {
        writeToStderr(function);
      }
    } else {
      writeToStderr(function);
    }
  }
  if (filename) {
    writeToStderr(" at ");
    writeToStderr(filename);
    writeToStderr(":");
    writeToStderr(lineno, 10);
  }
  writeToStderr("\n");

  (*frameCounter)++;
  return 0;
}

void errorCallback(void* data, const char* msg, int32_t errnum) {
  (void)data; // unused
  writeToStderr("libbacktrace error: ");
  writeToStderr(msg);
  writeToStderr(" (");
  writeToStderr(errnum, 10);
  writeToStderr(")\n");
}

template <bool demangle>
[[maybe_unused]] void printStacktraceLibBacktrace() {
  int32_t frameCounter = 0;
  backtrace_full(
      G_BACKTRACE_STATE,
      1,
      fullCallback<demangle>,
      errorCallback,
      &frameCounter);
}

[[maybe_unused]] void printStacktraceBacktrace() {
  void* buffer[100];
  int nptrs = backtrace(buffer, 100);
  if (nptrs > 0) {
    backtrace_symbols_fd(buffer, nptrs, STDERR_FILENO);
  } else {
    writeToStderr("backtrace() failed to get any frames.\n");
  }
}

template <bool demangle>
[[maybe_unused]] void printStacktraceLibunwind() {
#if defined(__linux__) || defined(__FreeBSD__)
  unw_context_t context;
  unw_cursor_t cursor;
  unw_word_t ip;
  unw_word_t offset;
  if (unw_getcontext(&context) < 0) {
    writeToStderr("libunwind failed to get any frames.\n");
    return;
  }
  if (unw_init_local(&cursor, &context) < 0) {
    writeToStderr("libunwind failed to initialize unw_cursor.\n");
    return;
  }
  int frameNum = 0;
  while (unw_step(&cursor) > 0) {
    writeToStderr("#");
    writeToStderr(frameNum, 10, 4);

    frameNum++;
    if (unw_get_reg(&cursor, UNW_REG_IP, &ip) < 0) {
      writeToStderr(" at [failed to get IP]\n");
      continue;
    }
    writeToStderr(ip, 16);
    // unw_get_proc_name will call malloc, which is not signal safe.
    char procNameBuf[512];
    if (unw_get_proc_name(&cursor, procNameBuf, sizeof(procNameBuf), &offset) ==
        0) {
      writeToStderr(" ");
      if constexpr (demangle) {
        static char demangleBuf[4096];
        int32_t status = 0;
        uint64_t len = sizeof(demangleBuf);
        const char* demangled =
            abi::__cxa_demangle(procNameBuf, demangleBuf, &len, &status);
        if (status == 0) {
          writeToStderr(demangled);
        } else {
          writeToStderr(procNameBuf);
          writeToStderr("+");
          writeToStderr(offset, 16);
        }
      } else {
        writeToStderr(procNameBuf);
        writeToStderr("+");
        writeToStderr(offset, 16);
      }
    }
    writeToStderr("\n");
  }
#else
  writeToStderr("libunwind not supported on this system.\n");
#endif
}

template <bool demangle, StackTraceEngine engine>
void crashReportHandler(int32_t sig, siginfo_t* info, void* ucontext) {
  FatalSignals& sigInfo = kFatalSignals[sig];

  writeToStderr("\n--- BOLT CRASH REPORT ---\n");
  writeToStderr("Signal: ");
  writeToStderr(sig, 10);
  if (sigInfo.name) {
    writeToStderr(" ");
    writeToStderr(sigInfo.name);
    writeToStderr(": ");
    writeToStderr(sigInfo.description);
  }
  writeToStderr("\n");
  struct timespec ts;
  if (clock_gettime(CLOCK_REALTIME, &ts) == 0) {
    writeToStderr("Timestamp: ");
    writeToStderr(ts.tv_sec, 10);
    writeToStderr(".");
    writeToStderr(ts.tv_nsec, 10);
    writeToStderr("\n");
  }
  writeToStderr("Fault address: ");
  writeToStderr(reinterpret_cast<uint64_t>(info->si_addr), 16);
  writeToStderr("\n");
#if defined(__x86_64__)
  ucontext_t* uc = static_cast<ucontext_t*>(ucontext);
  uint64_t rip = uc->uc_mcontext.gregs[REG_RIP];
  writeToStderr("Instruction pointer: ");
  writeToStderr(rip, 16);
  writeToStderr("\n");
#endif
  writeToStderr("Stack trace:\n");
  if constexpr (engine == StackTraceEngine::kLibBacktrace) {
    printStacktraceLibBacktrace<demangle>();
  } else if constexpr (engine == StackTraceEngine::kBacktrace) {
    printStacktraceBacktrace();
  } else if constexpr (engine == StackTraceEngine::kLibUnwind) {
    printStacktraceLibunwind<demangle>();
  } else {
    writeToStderr("Unknown stack trace engine.\n");
  }

  writeToStderr("--- END OF CRASH REPORT ---\n");
  if (sigInfo.name) {
    // restore old action
    sigaction(sigInfo.signum, &sigInfo.oldAction, NULL);
    // chain call old handler, check if it's a function pointer
    if (sigInfo.oldAction.sa_handler != SIG_DFL &&
        sigInfo.oldAction.sa_handler != SIG_IGN) {
      if (sigInfo.oldAction.sa_flags & SA_SIGINFO) {
        // if old handler use sa_sigaction, call it
        if (sigInfo.oldAction.sa_sigaction) {
          sigInfo.oldAction.sa_sigaction(sig, info, ucontext);
          return;
        }
      } else {
        // if old handler use sa_handler, call it
        if (sigInfo.oldAction.sa_handler) {
          sigInfo.oldAction.sa_handler(sig);
          return;
        }
      }
    }
    // if old handler is SIG_DFL or SIG_IGN, raise signal to default handler
    raise(sig);
    return;
  }
}

class BacktraceInitializer {
 public:
  BacktraceInitializer() {
    writeToStderr("Initializing backtrace handler (C++ static init)...\n");

    G_BACKTRACE_STATE = backtrace_create_state(NULL, 1, errorCallback, NULL);
    if (!G_BACKTRACE_STATE) {
      writeToStderr(
          "FATAL: Failed to create backtrace state in shared library.\n");
      return;
    }
  }
};

static BacktraceInitializer G_BACKTRACE_INITIALIZER;

} // namespace

void writeToStderr(const char* msg, uint64_t len) {
  writeTo(STDERR_FILENO, msg, len);
}

void writeToStderr(const char* msg) {
  writeToStderr(msg, safeStrlen(msg));
}

void writeToStdout(const char* msg, uint64_t len) {
  writeTo(STDOUT_FILENO, msg, len);
}

void writeToStdout(const char* msg) {
  writeToStdout(msg, safeStrlen(msg));
}
void writeToStdout(long value, int32_t base, int32_t width) {
  printInteger(STDOUT_FILENO, value, base, width);
}
void writeToStderr(long value, int32_t base, int32_t width) {
  printInteger(STDERR_FILENO, value, base, width);
}
constexpr const char* kJsigLibSymbol = "JVM_get_signal_action";
void installCrashReportHandler(
    bool runInJvm,
    bool demangle,
    StackTraceEngine engine) {
  static std::once_flag onceFlag;
  std::call_once(onceFlag, [runInJvm, demangle, engine]() {
    if (runInJvm) {
      // check if libjsig.so is loaded
      bool jsigLoaded = false;
      void* handle = dlopen(NULL, RTLD_NOW);
      if (handle) {
        void* symbol = dlsym(handle, kJsigLibSymbol);
        jsigLoaded = (symbol != nullptr);
        dlclose(handle);
      }
      if (jsigLoaded) {
        writeToStderr("[CrashReporter] libjsig.so is loaded.\n");
      } else {
        writeToStderr(
            "[CrashReporter] Since runInJvm is true, assuming current process "
            "is running in JVM. However, libjsig.so is not preloaded. "
            "So crash report handler will not be installed.\n");
        return;
      }
    }
    writeToStderr("[CrashReporter] Initializing crash report handler.\n");

    if (demangle) {
      writeToStderr(
          "[CrashReporter] !!! IMPORTANT !!! Demangling is not async-signal safe."
          " This should not be used in production.\n");
    }

    setupAlternateStack();
    struct sigaction sa {};
    memset(&sa, 0, sizeof(sa));
    // on entry, we do not want any signal masked.
    sigfillset(&sa.sa_mask);
    // By default signal handlers are run on the signaled thread's stack.
    // In case of stack overflow running the SIGSEGV signal handler on
    // the same stack leads to another SIGSEGV and crashes the program.
    // Use SA_ONSTACK, so alternate stack is used (only if configured via
    // sigaltstack).
    // Golang also requires SA_ONSTACK. See:
    // https://golang.org/pkg/os/signal/#hdr-Go_programs_that_use_cgo_or_SWIG
    sa.sa_flags |= SA_SIGINFO | SA_ONSTACK;
    if (demangle) {
      switch (engine) {
        case StackTraceEngine::kLibBacktrace:
          sa.sa_sigaction =
              &crashReportHandler<true, StackTraceEngine::kLibBacktrace>;
          break;
        case StackTraceEngine::kBacktrace:
          sa.sa_sigaction =
              &crashReportHandler<true, StackTraceEngine::kBacktrace>;
          break;
        case StackTraceEngine::kLibUnwind:
          sa.sa_sigaction =
              &crashReportHandler<true, StackTraceEngine::kLibUnwind>;
          break;
        default:
          writeToStderr("Unknown stack trace engine.\n");
          break;
      }
    } else {
      switch (engine) {
        case StackTraceEngine::kLibBacktrace:
          sa.sa_sigaction =
              &crashReportHandler<false, StackTraceEngine::kLibBacktrace>;
          break;
        case StackTraceEngine::kBacktrace:
          sa.sa_sigaction =
              &crashReportHandler<false, StackTraceEngine::kBacktrace>;
          break;
        case StackTraceEngine::kLibUnwind:
          sa.sa_sigaction =
              &crashReportHandler<false, StackTraceEngine::kLibUnwind>;
          break;
        default:
          writeToStderr("Unknown stack trace engine.\n");
          break;
      }
    }

    for (int i = 0; i < NSIG; ++i) {
      if (kFatalSignals[i].name) {
        sigaction(kFatalSignals[i].signum, &sa, &kFatalSignals[i].oldAction);
      }
    }
  });
}

} // namespace bytedance::bolt::process
