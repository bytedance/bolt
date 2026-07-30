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
#include <dlfcn.h>
#include <execinfo.h>
#include <link.h>
#if defined(__linux__) || defined(__FreeBSD__)
#include <libunwind.h>
#endif
#include <signal.h>
#include <sys/mman.h>
#include <time.h>
#include <ucontext.h>
#include <unistd.h>

#include <atomic>
#include <cerrno>
#include <cstddef>
#include <cstdint>
#include <mutex>

namespace bytedance::bolt::process {
namespace {

constexpr size_t kSafeStrlenMax = 40960;
constexpr size_t kAlternateStackSize = 64 * 1024;
constexpr size_t kMaxStackFrames = 100;
constexpr size_t kMaxLoadedObjects = 256;
constexpr size_t kMaxObjectPathSize = 1024;
constexpr const char* kJsigLibSymbol = "JVM_get_signal_action";

struct FatalSignalState {
  int32_t signum;
  const char* name;
  const char* description;
  struct sigaction oldAction;
};

struct LoadedObject {
  uintptr_t begin;
  uintptr_t end;
  uintptr_t loadBias;
  char path[kMaxObjectPathSize];
};

struct OfflineAddress {
  const char* objectPath;
  uint64_t elfAddress;
};

struct OfflineSymbolizationFallback {
  size_t unresolvedWithModule;
  size_t unresolvedWithoutModule;
};

FatalSignalState kFatalSignals[] = {
    {SIGSEGV, "SIGSEGV", "Invalid access to storage", {}},
    {SIGILL, "SIGILL", "Illegal instruction", {}},
    {SIGFPE, "SIGFPE", "Erroneous arithmetic operation", {}},
    {SIGABRT, "SIGABRT", "Abnormal termination", {}},
    {SIGBUS, "SIGBUS", "Bus error", {}},
};

constexpr size_t kFatalSignalCount =
    sizeof(kFatalSignals) / sizeof(kFatalSignals[0]);

std::mutex kInstallMutex;
bool kHandlerInstalled{false};
std::atomic_flag kHandlingFatalSignal = ATOMIC_FLAG_INIT;
volatile sig_atomic_t kLibunwindAvailable{0};
volatile sig_atomic_t kSelectedEngine{
    static_cast<sig_atomic_t>(StackTraceEngine::kLibBacktrace)};
volatile sig_atomic_t kOfflineDemangle{0};
struct backtrace_state* kBacktraceState{nullptr};
LoadedObject kLoadedObjects[kMaxLoadedObjects]{};
volatile sig_atomic_t kLoadedObjectCount{0};
char kExecutablePath[kMaxObjectPathSize]{};

size_t safeStrlen(const char* s) {
  if (s == nullptr) {
    return 0;
  }

  size_t len = 0;
  while (len < kSafeStrlenMax && s[len] != '\0') {
    ++len;
  }
  return len;
}

void copyString(char* destination, size_t capacity, const char* source) {
  if (capacity == 0) {
    return;
  }
  size_t index = 0;
  if (source != nullptr) {
    while (index + 1 < capacity && source[index] != '\0') {
      destination[index] = source[index];
      ++index;
    }
  }
  destination[index] = '\0';
}

void writeTo(int32_t fd, const char* msg, size_t len) {
  const int32_t savedErrno = errno;
  while (len > 0) {
    const ssize_t written = write(fd, msg, len);
    if (written > 0) {
      msg += written;
      len -= static_cast<size_t>(written);
      continue;
    }
    if (written < 0 && errno == EINTR) {
      continue;
    }
    break;
  }
  errno = savedErrno;
}

void printUnsignedInteger(
    int32_t fd,
    uint64_t value,
    int32_t base,
    int32_t width,
    bool prefixHex) {
  constexpr int32_t kBufferSize = 64;
  char buffer[kBufferSize];
  char* cursor = &buffer[kBufferSize];
  const char* hexDigits = "0123456789abcdef";

  if (base != 10 && base != 16) {
    base = 10;
  }

  do {
    const uint64_t digit = value % static_cast<uint64_t>(base);
    *--cursor = base == 16 ? hexDigits[digit] : '0' + digit;
    value /= static_cast<uint64_t>(base);
  } while (value != 0);

  const size_t digits = static_cast<size_t>(&buffer[kBufferSize] - cursor);
  const size_t prefixLength = base == 16 && prefixHex ? 2 : 0;
  int32_t paddingNeeded = width - static_cast<int32_t>(digits + prefixLength);
  constexpr char kSpaces[] = "                ";
  while (paddingNeeded > 0) {
    const size_t chunk =
        paddingNeeded > 16 ? 16 : static_cast<size_t>(paddingNeeded);
    writeTo(fd, kSpaces, chunk);
    paddingNeeded -= static_cast<int32_t>(chunk);
  }

  if (prefixLength != 0) {
    writeTo(fd, "0x", prefixLength);
  }
  writeTo(fd, cursor, digits);
}

void printSignedInteger(
    int32_t fd,
    int64_t value,
    int32_t base,
    int32_t width) {
  if (base == 10 && value < 0) {
    writeTo(fd, "-", 1);
    const uint64_t magnitude = -static_cast<uint64_t>(value);
    printUnsignedInteger(fd, magnitude, base, width - 1, false);
    return;
  }
  printUnsignedInteger(
      fd, static_cast<uint64_t>(value), base, width, base == 16);
}

void writeUnsignedToStderr(uint64_t value, int32_t base, int32_t width = -1) {
  printUnsignedInteger(STDERR_FILENO, value, base, width, base == 16);
}

FatalSignalState* findFatalSignal(int32_t signum) {
  for (size_t i = 0; i < kFatalSignalCount; ++i) {
    if (kFatalSignals[i].signum == signum) {
      return &kFatalSignals[i];
    }
  }
  return nullptr;
}

bool isValidEngine(StackTraceEngine engine) {
  switch (engine) {
    case StackTraceEngine::kLibUnwind:
    case StackTraceEngine::kBacktrace:
    case StackTraceEngine::kLibBacktrace:
      return true;
  }
  return false;
}

const char* engineName(StackTraceEngine engine) {
  switch (engine) {
    case StackTraceEngine::kLibUnwind:
      return "libunwind";
    case StackTraceEngine::kBacktrace:
      return "backtrace";
    case StackTraceEngine::kLibBacktrace:
      return "libbacktrace";
  }
  return "unknown";
}

int32_t cacheLoadedObject(
    struct dl_phdr_info* info,
    size_t /* size */,
    void* /* data */) {
  const size_t objectIndex = static_cast<size_t>(kLoadedObjectCount);
  if (objectIndex >= kMaxLoadedObjects) {
    return 1;
  }

  uintptr_t begin = UINTPTR_MAX;
  uintptr_t end = 0;
  for (ElfW(Half) index = 0; index < info->dlpi_phnum; ++index) {
    const auto& header = info->dlpi_phdr[index];
    if (header.p_type != PT_LOAD || header.p_memsz == 0) {
      continue;
    }
    const uintptr_t segmentBegin =
        static_cast<uintptr_t>(info->dlpi_addr) + header.p_vaddr;
    const uintptr_t segmentEnd = segmentBegin + header.p_memsz;
    if (segmentBegin < begin) {
      begin = segmentBegin;
    }
    if (segmentEnd > end) {
      end = segmentEnd;
    }
  }
  if (begin == UINTPTR_MAX || end <= begin) {
    return 0;
  }

  auto& object = kLoadedObjects[objectIndex];
  object.begin = begin;
  object.end = end;
  object.loadBias = static_cast<uintptr_t>(info->dlpi_addr);
  const char* path = info->dlpi_name;
  if (path == nullptr || path[0] == '\0') {
    path = kExecutablePath[0] == '\0' ? "<main-executable>" : kExecutablePath;
  }
  copyString(object.path, sizeof(object.path), path);

  // Publish the entry only after all of its fields have been initialized.
  kLoadedObjectCount = static_cast<sig_atomic_t>(objectIndex + 1);
  return 0;
}

void cacheLoadedObjects() {
#if defined(__linux__)
  const ssize_t pathLength =
      readlink("/proc/self/exe", kExecutablePath, sizeof(kExecutablePath) - 1);
#elif defined(__FreeBSD__)
  const ssize_t pathLength = readlink(
      "/proc/curproc/file", kExecutablePath, sizeof(kExecutablePath) - 1);
#else
  const ssize_t pathLength = -1;
#endif
  if (pathLength > 0) {
    kExecutablePath[pathLength] = '\0';
  } else {
    kExecutablePath[0] = '\0';
  }

  kLoadedObjectCount = 0;
  (void)dl_iterate_phdr(cacheLoadedObject, nullptr);
  if (static_cast<size_t>(kLoadedObjectCount) == kMaxLoadedObjects) {
    writeToStderr(
        "[CrashReporter] Warning: Loaded-object cache is full; addresses in "
        "additional modules may require a core dump to symbolize.\n");
  }
}

const LoadedObject* findLoadedObject(uintptr_t address) {
  const size_t objectCount = static_cast<size_t>(kLoadedObjectCount);
  for (size_t index = 0; index < objectCount; ++index) {
    const auto& object = kLoadedObjects[index];
    if (address >= object.begin && address < object.end) {
      return &object;
    }
  }
  return nullptr;
}

bool resolveOfflineAddress(uintptr_t address, OfflineAddress& result) {
  // Deliberately use only the immutable installation-time snapshot here.
  // Consulting the dynamic loader from a fatal-signal handler can deadlock if
  // the interrupted thread held its lock. Modules loaded later remain raw.
  const LoadedObject* object = findLoadedObject(address);
  if (object != nullptr) {
    result.objectPath = object->path;
    result.elfAddress = static_cast<uint64_t>(address - object->loadBias);
    return true;
  }
  return false;
}

void writeOfflineAddress(const OfflineAddress& address) {
  writeToStderr(" [");
  writeToStderr(address.objectPath);
  writeToStderr(" elf=");
  writeUnsignedToStderr(address.elfAddress, 16);
  writeToStderr("]");
}

bool setupAlternateStack() {
  stack_t currentStack{};
  if (sigaltstack(nullptr, &currentStack) != 0) {
    writeToStderr(
        "[CrashReporter] Warning: Failed to inspect alternate stack, errno=");
    writeToStderr(errno);
    writeToStderr("\n");
    return false;
  }

  // Respect an alternate stack already installed by the host (for example,
  // HotSpot). Replacing it would break the host's signal handling.
  if ((currentStack.ss_flags & SS_DISABLE) == 0) {
    return true;
  }

  long pageSizeResult = sysconf(_SC_PAGESIZE);
  if (pageSizeResult <= 0) {
    pageSizeResult = 4096;
  }
  const size_t pageSize = static_cast<size_t>(pageSizeResult);
  const size_t minimumStackSize = static_cast<size_t>(MINSIGSTKSZ);
  size_t usableSize = minimumStackSize > kAlternateStackSize
      ? minimumStackSize
      : kAlternateStackSize;
  usableSize = ((usableSize + pageSize - 1) / pageSize) * pageSize;
  const size_t mappingSize = usableSize + 2 * pageSize;

  void* mapping =
      mmap(nullptr, mappingSize, PROT_NONE, MAP_PRIVATE | MAP_ANONYMOUS, -1, 0);
  if (mapping == MAP_FAILED) {
    writeToStderr(
        "[CrashReporter] Warning: Failed to allocate alternate stack, errno=");
    writeToStderr(errno);
    writeToStderr("\n");
    return false;
  }

  auto* stackMemory = static_cast<char*>(mapping) + pageSize;
  if (mprotect(stackMemory, usableSize, PROT_READ | PROT_WRITE) != 0) {
    const int32_t error = errno;
    munmap(mapping, mappingSize);
    writeToStderr(
        "[CrashReporter] Warning: Failed to protect alternate stack, errno=");
    writeToStderr(error);
    writeToStderr("\n");
    return false;
  }

  stack_t alternateStack{};
  alternateStack.ss_sp = stackMemory;
  alternateStack.ss_size = usableSize;
  alternateStack.ss_flags = 0;
  if (sigaltstack(&alternateStack, nullptr) != 0) {
    const int32_t error = errno;
    munmap(mapping, mappingSize);
    writeToStderr(
        "[CrashReporter] Warning: Failed to install alternate stack, errno=");
    writeToStderr(error);
    writeToStderr("\n");
    return false;
  }

  // The crash handler is process-wide and cannot be uninstalled through the
  // public API. Leave the mapping in place until process exit.
  return true;
}

bool warmUpLibunwind() {
#if defined(__linux__) || defined(__FreeBSD__)
  unw_context_t context;
  unw_cursor_t cursor;
  if (unw_getcontext(&context) < 0 || unw_init_local(&cursor, &context) < 0) {
    return false;
  }
  unw_word_t ip = 0;
  (void)unw_get_reg(&cursor, UNW_REG_IP, &ip);
  (void)unw_step(&cursor);
  // Warm up the signal-frame entry point as well, so its first invocation
  // cannot trigger lazy binding from inside the crash handler.
  (void)unw_init_local2(&cursor, &context, UNW_INIT_SIGNAL_FRAME);
  return true;
#else
  return false;
#endif
}

bool warmUpExecinfo() {
  void* frames[4];
  return backtrace(frames, 4) > 0;
}

// Fatal-signal handling deliberately stops at raw program counters. Do not add
// dladdr, function-name lookup, source lookup, or demangling here: those paths
// can allocate or acquire locks already held by the interrupted thread.
void writeStackFrame(
    size_t frame,
    uintptr_t address,
    OfflineSymbolizationFallback& fallback) {
  writeToStderr("#");
  writeUnsignedToStderr(frame, 10, 4);
  writeToStderr(" ");
  writeUnsignedToStderr(static_cast<uint64_t>(address), 16);

  OfflineAddress offlineAddress{};
  const bool hasOfflineAddress = resolveOfflineAddress(address, offlineAddress);
  if (hasOfflineAddress) {
    writeOfflineAddress(offlineAddress);
  }
  writeToStderr(" in ??");
  writeToStderr("\n");

  if (hasOfflineAddress) {
    ++fallback.unresolvedWithModule;
  } else {
    ++fallback.unresolvedWithoutModule;
  }
}

struct LibBacktraceContext {
  size_t frame;
  OfflineSymbolizationFallback* fallback;
};

int32_t libBacktraceCallback(void* data, uintptr_t pc) {
  auto* context = static_cast<LibBacktraceContext*>(data);
  if (context->frame >= kMaxStackFrames) {
    return 1;
  }
  writeStackFrame(context->frame, pc, *context->fallback);
  ++context->frame;
  return 0;
}

void libBacktraceErrorCallback(
    void* /* data */,
    const char* message,
    int32_t errorNumber) {
  writeToStderr("libbacktrace error: ");
  writeToStderr(message);
  writeToStderr(" (");
  writeToStderr(errorNumber);
  writeToStderr(")\n");
}

int32_t warmUpLibbacktraceCallback(void* data, uintptr_t /* pc */) {
  *static_cast<bool*>(data) = true;
  return 1;
}

bool warmUpLibbacktrace() {
  if (kBacktraceState == nullptr) {
    return false;
  }
  bool frameSeen = false;
  (void)backtrace_simple(
      kBacktraceState,
      0,
      warmUpLibbacktraceCallback,
      libBacktraceErrorCallback,
      &frameSeen);
  return frameSeen;
}

void printLibBacktrace(OfflineSymbolizationFallback& fallback) {
  if (kBacktraceState == nullptr) {
    writeToStderr("  libbacktrace state is unavailable\n");
    return;
  }

  LibBacktraceContext context{0, &fallback};
  backtrace_simple(
      kBacktraceState,
      1,
      libBacktraceCallback,
      libBacktraceErrorCallback,
      &context);
}

void printExecinfoBacktrace(OfflineSymbolizationFallback& fallback) {
  void* frames[kMaxStackFrames];
  const int32_t frameCount =
      backtrace(frames, static_cast<int32_t>(kMaxStackFrames));
  if (frameCount <= 0) {
    writeToStderr("  backtrace() returned no frames\n");
    return;
  }
  for (int32_t frame = 0; frame < frameCount; ++frame) {
    const auto address = reinterpret_cast<uintptr_t>(frames[frame]);
    writeStackFrame(static_cast<size_t>(frame), address, fallback);
  }
}

uintptr_t getInstructionPointer(void* ucontext) {
  if (ucontext == nullptr) {
    return 0;
  }

#if defined(__x86_64__) && defined(REG_RIP)
  const auto* context = static_cast<const ucontext_t*>(ucontext);
  return static_cast<uintptr_t>(context->uc_mcontext.gregs[REG_RIP]);
#elif defined(__aarch64__)
  const auto* context = static_cast<const ucontext_t*>(ucontext);
  return static_cast<uintptr_t>(context->uc_mcontext.pc);
#else
  return 0;
#endif
}

void printLibunwindStacktrace(
    void* ucontext,
    OfflineSymbolizationFallback& fallback) {
#if defined(__linux__) || defined(__FreeBSD__)
  if (kLibunwindAvailable == 0) {
    writeToStderr("  libunwind is unavailable\n");
    return;
  }

  unw_cursor_t cursor;
  unw_context_t currentContext;
  int32_t initResult = -1;
  if (ucontext != nullptr) {
    initResult = unw_init_local2(
        &cursor, static_cast<unw_context_t*>(ucontext), UNW_INIT_SIGNAL_FRAME);
  }
  if (initResult < 0) {
    if (unw_getcontext(&currentContext) < 0) {
      writeToStderr("  failed to capture unwind context\n");
      return;
    }
    initResult = unw_init_local(&cursor, &currentContext);
  }
  if (initResult < 0) {
    writeToStderr("  failed to initialize unwind cursor\n");
    return;
  }

  size_t frame = 0;
  while (frame < kMaxStackFrames) {
    unw_word_t ip = 0;
    if (unw_get_reg(&cursor, UNW_REG_IP, &ip) < 0) {
      break;
    }
    if (ip != 0) {
      writeStackFrame(frame, static_cast<uintptr_t>(ip), fallback);
      ++frame;
    }

    const int32_t stepResult = unw_step(&cursor);
    if (stepResult <= 0) {
      break;
    }
  }
  if (frame == 0) {
    writeToStderr("  no unwind frames available\n");
  }
#else
  (void)ucontext;
  writeToStderr("  raw unwinding is unsupported on this platform\n");
#endif
}

void printSelectedStacktrace(
    void* ucontext,
    OfflineSymbolizationFallback& fallback) {
  const auto engine = static_cast<StackTraceEngine>(kSelectedEngine);
  writeToStderr("Stack trace (best-effort, backend=");
  writeToStderr(engineName(engine));
  writeToStderr("):\n");

  switch (engine) {
    case StackTraceEngine::kLibUnwind:
      printLibunwindStacktrace(ucontext, fallback);
      return;
    case StackTraceEngine::kBacktrace:
      printExecinfoBacktrace(fallback);
      return;
    case StackTraceEngine::kLibBacktrace:
      printLibBacktrace(fallback);
      return;
  }
  writeToStderr("Stack trace engine is unavailable\n");
}

void writeOfflineSymbolizationFallback(
    const OfflineSymbolizationFallback& fallback) {
  if (fallback.unresolvedWithModule == 0) {
    if (fallback.unresolvedWithoutModule != 0) {
      writeToStderr(
          "\nOffline symbolization fallback unavailable: no module-relative "
          "addresses were captured. Preserve the core dump and matching "
          "binaries.\n");
    }
    return;
  }

  writeToStderr(
      "\nOffline symbolization fallback for unresolved entries:\n"
      "# Copy the script below to bolt-symbolize-report.sh, then pass the "
      "complete crash report to it.\n"
      "#!/bin/sh\n"
      "# --- BEGIN BOLT OFFLINE SYMBOLIZATION SCRIPT ---\n"
      "# Requires llvm-symbolizer and the exact binaries/debug files from the "
      "crashing build.\n"
      "# Set BOLT_SYMBOL_ROOT to prefix runtime module paths when symbolizing "
      "on another filesystem.\n"
      "# Usage: sh bolt-symbolize-report.sh [CRASH_REPORT|-] > "
      "symbolized-report.txt\n"
      "# With no argument (or '-'), the report is read from standard input.\n"
      "symbolizer=${LLVM_SYMBOLIZER:-llvm-symbolizer}\n"
      "symbol_root=${BOLT_SYMBOL_ROOT:-}\n"
      "if command -v \"$symbolizer\" >/dev/null 2>&1; then\n"
      "  symbolizer_available=1\n"
      "else\n"
      "  symbolizer_available=0\n"
      "  printf '%s\\n' \"warning: llvm-symbolizer is unavailable; report "
      "will be passed through unchanged\" >&2\n"
      "fi\n"
      "symbolize_report() {\n"
      "  while IFS= read -r line || [ -n \"$line\" ]; do\n"
      "    case \"$line\" in\n"
      "      *\" elf=0x\"*\" in ??\"*)\n"
      "        metadata=${line#* \\[}\n"
      "        object=${metadata% elf=*}\n"
      "        address=${metadata##* elf=}\n"
      "        address=${address%%]*}\n"
      "        hex=${address#0x}\n"
      "        if [ \"$metadata\" = \"$line\" ] || [ -z \"$object\" ] || "
      "[ -z \"$hex\" ] || [ \"$hex\" = \"$address\" ]; then\n"
      "          printf '%s\\n' \"$line\"\n"
      "          continue\n"
      "        fi\n"
      "        case \"$hex\" in\n"
      "          *[!0123456789abcdefABCDEF]*)\n"
      "            printf '%s\\n' \"$line\"\n"
      "            continue\n"
      "            ;;\n"
      "        esac\n"
      "        if [ \"$symbolizer_available\" -eq 0 ]; then\n"
      "          printf '%s\\n' \"$line\"\n"
      "          continue\n"
      "        fi\n"
      "        if [ -n \"$symbol_root\" ]; then\n"
      "          object=\"${symbol_root}${object}\"\n"
      "        fi\n"
      "        resolved=$(\"$symbolizer\" ");
  writeToStderr(kOfflineDemangle != 0 ? "--demangle " : "--no-demangle ");
  writeToStderr(
      "--no-inlines "
      "--pretty-print --output-style=GNU --obj=\"$object\" \"$address\" "
      "2>/dev/null | sed -n '1p')\n"
      "        case \"$resolved\" in\n"
      "          \"\"|\"?? at \"*)\n"
      "            printf '%s\\n' \"$line\"\n"
      "            ;;\n"
      "          *)\n"
      "            prefix=${line%%\" in ??\"*}\n"
      "            printf '%s in %s\\n' \"$prefix\" \"$resolved\"\n"
      "            ;;\n"
      "        esac\n"
      "        ;;\n"
      "      *)\n"
      "        printf '%s\\n' \"$line\"\n"
      "        ;;\n"
      "    esac\n"
      "  done\n"
      "}\n"
      "if [ \"$#\" -gt 1 ]; then\n"
      "  printf 'usage: %s [CRASH_REPORT|-]\\n' \"$0\" >&2\n"
      "  exit 2\n"
      "elif [ \"$#\" -eq 1 ] && [ \"$1\" != \"-\" ]; then\n"
      "  symbolize_report < \"$1\"\n"
      "else\n"
      "  symbolize_report\n"
      "fi\n");

  if (fallback.unresolvedWithoutModule != 0) {
    writeToStderr(
        "# Entries without an elf= address remain unchanged; use the core dump "
        "for those entries.\n");
  }
  writeToStderr("# --- END BOLT OFFLINE SYMBOLIZATION SCRIPT ---\n");
}

[[noreturn]] void terminateWithDefaultAction(int32_t signum) {
  struct sigaction defaultAction{};
  sigemptyset(&defaultAction.sa_mask);
  defaultAction.sa_handler = SIG_DFL;
  defaultAction.sa_flags = 0;
  (void)sigaction(signum, &defaultAction, nullptr);

  sigset_t signalSet;
  sigemptyset(&signalSet);
  sigaddset(&signalSet, signum);
  (void)sigprocmask(SIG_UNBLOCK, &signalSet, nullptr);

  (void)raise(signum);
  _exit(128 + signum);
}

void invokePreviousHandler(
    FatalSignalState& signalState,
    int32_t signum,
    siginfo_t* info,
    void* ucontext) {
  const struct sigaction previousAction = signalState.oldAction;
  if (previousAction.sa_handler == SIG_DFL ||
      previousAction.sa_handler == SIG_IGN) {
    return;
  }

  // Restore before invoking the previous handler so any signal it raises is
  // not sent back to this handler.
  (void)sigaction(signum, &previousAction, nullptr);
  (void)sigprocmask(SIG_BLOCK, &previousAction.sa_mask, nullptr);

  if ((previousAction.sa_flags & SA_SIGINFO) != 0) {
    if (previousAction.sa_sigaction != nullptr) {
      previousAction.sa_sigaction(signum, info, ucontext);
    }
  } else if (previousAction.sa_handler != nullptr) {
    previousAction.sa_handler(signum);
  }
}

void crashReportHandler(int32_t signum, siginfo_t* info, void* ucontext) {
  if (kHandlingFatalSignal.test_and_set(std::memory_order_relaxed)) {
    _exit(128 + signum);
  }

  FatalSignalState* signalState = findFatalSignal(signum);
  writeToStderr("\n--- BOLT CRASH REPORT ---\n");
  writeToStderr("Signal: ");
  writeToStderr(signum);
  if (signalState != nullptr) {
    writeToStderr(" ");
    writeToStderr(signalState->name);
    writeToStderr(": ");
    writeToStderr(signalState->description);
  }
  writeToStderr("\n");

  struct timespec timestamp{};
  if (clock_gettime(CLOCK_REALTIME, &timestamp) == 0) {
    writeToStderr("Timestamp: ");
    writeToStderr(timestamp.tv_sec);
    writeToStderr(".");
    writeToStderr(timestamp.tv_nsec);
    writeToStderr("\n");
  }

  writeToStderr("Fault address: ");
  writeUnsignedToStderr(
      info == nullptr
          ? 0
          : static_cast<uint64_t>(reinterpret_cast<uintptr_t>(info->si_addr)),
      16);
  writeToStderr("\n");

  OfflineSymbolizationFallback fallback{};
  const uintptr_t instructionPointer = getInstructionPointer(ucontext);
  if (instructionPointer != 0) {
    writeToStderr("Instruction pointer: ");
    writeUnsignedToStderr(static_cast<uint64_t>(instructionPointer), 16);
    OfflineAddress offlineAddress{};
    const bool hasOfflineAddress =
        resolveOfflineAddress(instructionPointer, offlineAddress);
    if (hasOfflineAddress) {
      writeOfflineAddress(offlineAddress);
      ++fallback.unresolvedWithModule;
    } else {
      ++fallback.unresolvedWithoutModule;
    }
    writeToStderr(" in ??");
    writeToStderr("\n");
  }

  printSelectedStacktrace(ucontext, fallback);
  writeOfflineSymbolizationFallback(fallback);
  writeToStderr("--- END OF CRASH REPORT ---\n");

  if (signalState != nullptr) {
    invokePreviousHandler(*signalState, signum, info, ucontext);
  }

  // A fatal crash handler must not return to the faulting instruction. If a
  // previous custom handler returns, terminate with the original signal so
  // wait status and core-dump behavior remain meaningful.
  terminateWithDefaultAction(signum);
}

bool isLibjsigLoaded() {
  return dlsym(RTLD_DEFAULT, kJsigLibSymbol) != nullptr;
}

} // namespace

void writeToStderr(const char* msg, unsigned long len) {
  writeTo(STDERR_FILENO, msg, static_cast<size_t>(len));
}

void writeToStderr(const char* msg) {
  writeToStderr(msg, static_cast<unsigned long>(safeStrlen(msg)));
}

void writeToStdout(const char* msg, unsigned long len) {
  writeTo(STDOUT_FILENO, msg, static_cast<size_t>(len));
}

void writeToStdout(const char* msg) {
  writeToStdout(msg, static_cast<unsigned long>(safeStrlen(msg)));
}

void writeToStdout(long value, int32_t base, int32_t width) {
  printSignedInteger(STDOUT_FILENO, value, base, width);
}

void writeToStderr(long value, int32_t base, int32_t width) {
  printSignedInteger(STDERR_FILENO, value, base, width);
}

void installCrashReportHandler(
    bool runInJvm,
    bool demangle,
    StackTraceEngine engine) {
  std::lock_guard<std::mutex> lock(kInstallMutex);
  if (kHandlerInstalled) {
    return;
  }

  if (!isValidEngine(engine)) {
    writeToStderr(
        "[CrashReporter] Unknown stack trace engine; not installed.\n");
    return;
  }

  if (runInJvm && !isLibjsigLoaded()) {
    writeToStderr(
        "[CrashReporter] Since runInJvm is true, assuming current process "
        "is running in JVM. However, libjsig.so is not preloaded. "
        "The crash report handler was not installed.\n");
    return;
  }

  writeToStderr("[CrashReporter] Initializing crash report handler.\n");
  writeToStderr("[CrashReporter] Requested stack engine: ");
  writeToStderr(engineName(engine));
  writeToStderr("\n");
  writeToStderr(
      "[CrashReporter] In-process symbolization is disabled; raw stack "
      "addresses will be resolved by the offline fallback.\n");
  if (demangle) {
    writeToStderr(
        "[CrashReporter] Demangling was requested and will be performed only "
        "by the offline symbolizer.\n");
  }

  kLibunwindAvailable = warmUpLibunwind() ? 1 : 0;
  if (kLibunwindAvailable == 0) {
    writeToStderr(
        "[CrashReporter] Warning: libunwind is unavailable; "
        "the fault instruction pointer will still be reported.\n");
  }
  if (engine == StackTraceEngine::kLibBacktrace) {
    kBacktraceState =
        backtrace_create_state(nullptr, 1, libBacktraceErrorCallback, nullptr);
    if (kBacktraceState == nullptr) {
      writeToStderr(
          "[CrashReporter] Warning: Failed to initialize libbacktrace; "
          "falling back to best-effort libunwind.\n");
      engine = StackTraceEngine::kLibUnwind;
    } else if (!warmUpLibbacktrace()) {
      writeToStderr(
          "[CrashReporter] Warning: Failed to warm up raw libbacktrace "
          "collection; crash-time unwinding remains best-effort.\n");
    }
  } else if (engine == StackTraceEngine::kBacktrace && !warmUpExecinfo()) {
    writeToStderr(
        "[CrashReporter] Warning: Failed to warm up execinfo backtrace; "
        "crash-time unwinding remains best-effort.\n");
  }
  kSelectedEngine = static_cast<sig_atomic_t>(engine);
  kOfflineDemangle = demangle ? 1 : 0;
  cacheLoadedObjects();

  // sigaltstack is per-thread. This protects the installing thread and also
  // preserves a host-provided alternate stack, but cannot configure threads
  // created later through this process-wide API.
  (void)setupAlternateStack();

  struct sigaction action{};
  action.sa_sigaction = &crashReportHandler;
  action.sa_flags = SA_SIGINFO | SA_ONSTACK;
  sigemptyset(&action.sa_mask);
  for (size_t i = 0; i < kFatalSignalCount; ++i) {
    sigaddset(&action.sa_mask, kFatalSignals[i].signum);
  }

  size_t installedCount = 0;
  for (; installedCount < kFatalSignalCount; ++installedCount) {
    auto& signalState = kFatalSignals[installedCount];
    if (sigaction(signalState.signum, &action, &signalState.oldAction) != 0) {
      const int32_t error = errno;
      writeToStderr("[CrashReporter] Failed to install handler for signal ");
      writeToStderr(signalState.signum);
      writeToStderr(", errno=");
      writeToStderr(error);
      writeToStderr(". Rolling back.\n");
      break;
    }
  }

  if (installedCount != kFatalSignalCount) {
    while (installedCount > 0) {
      --installedCount;
      auto& signalState = kFatalSignals[installedCount];
      (void)sigaction(signalState.signum, &signalState.oldAction, nullptr);
    }
    return;
  }

  kHandlerInstalled = true;
}

} // namespace bytedance::bolt::process
