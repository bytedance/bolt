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

#include "bolt/common/process/ExceptionTrace.h"
#include "bolt/common/process/ExceptionTraceContext.h"
#include "bolt/common/process/StackTrace.h"

#include <cxxabi.h>
#include <dirent.h>
#include <fmt/format.h>
#include <folly/String.h>
#include <glog/logging.h>
#include <sys/syscall.h>
#include <iostream>
#include <mutex>
#include <string>
namespace bytedance::bolt::process {

// exception_stack_level controls when to print exception's stack
// -1, enable print all exceptions' stack
// 0, disable print exceptions' stack
// 1, print exceptions' stack whose prefix is in the white list(default)
// 2, print exceptions' stack whose prefix is not in the black list
// other value means the default value

ExceptionTraceContext& ExceptionTraceContext::get_instance() {
  static ExceptionTraceContext context;
  return context;
}
// as exception' name is not large, so we can think there are no exceptions.
std::string ExceptionTraceContext::get_exception_name(const void* info) {
  auto* exception_info = (std::type_info*)info;
  int demangle_status;
  char* demangled_exception_name;
  std::string exception_name = "unknown";
  if (exception_info != nullptr) {
    // Demangle the name of the exception using the GNU C++ ABI:
    demangled_exception_name = abi::__cxa_demangle(
        exception_info->name(), nullptr, nullptr, &demangle_status);
    if (demangled_exception_name != nullptr) {
      exception_name = std::string(demangled_exception_name);
      // Free the memory from __cxa_demangle():
      free(demangled_exception_name);
    } else {
      // NOTE: if the demangle fails, we do nothing, so the
      // non-demangled name will be printed. That's ok.
      exception_name = std::string(exception_info->name());
    }
  }
  return exception_name;
}

bool ExceptionTraceContext::prefix_in_black_list(
    const std::string& exception) const {
  for (auto const& str : black_list_) {
    if (exception.rfind(str, 0) == 0) {
      return true;
    }
  }
  return false;
}

bool ExceptionTraceContext::prefix_in_white_list(
    const std::string& exception) const {
  for (auto const& str : white_list_) {
    if (exception.rfind(str, 0) == 0) {
      return true;
    }
  }
  return false;
}

int ExceptionTraceContext::get_level() const {
  return level_;
}

void ExceptionTraceContext::set_level(int level) {
  std::lock_guard<std::mutex> guard(mutex_);
  level_ = level;
}

// prefix with the namespace of expressions, delimited by comma.
void ExceptionTraceContext::set_whitelist(const std::string& white_list) {
  std::lock_guard<std::mutex> guard(mutex_);
  white_list_.clear();
  folly::split(',', white_list, white_list_);
}

ExceptionTraceContext::ExceptionTraceContext() {
  level_ = 1;
  // other values mean the default value.
  if (level_ < -1 || level_ > 2) {
    level_ = 1;
  }
  white_list_ = {"std::"};
  black_list_ = {};
}

#if ENABLE_EXCEPTION_TRACE

__attribute__((constructor)) static void ensureExceptionWrapperLinked() {}
// wrap libc's _cxa_throw that must not throw exceptions again, otherwise
// causing crash.
#ifdef __clang__
void __wrap___cxa_throw(
    void* thrown_exception,
    std::type_info* info,
    void (*dest)(void*)) {
#elif defined(__GNUC__)
void __wrap___cxa_throw(
    void* thrown_exception,
    void* info,
    void (*dest)(void*)) {
#endif
  auto print_level = ExceptionTraceContext::get_instance().get_level();
  if (print_level != 0) {
    // /todo avoid recursively throwing std::bad_alloc exception when check
    // memory limit in memory tracker.
    std::string exception_name =
        ExceptionTraceContext::get_exception_name((void*)info);
    if ((print_level == 1 &&
         ExceptionTraceContext::get_instance().prefix_in_white_list(
             exception_name)) ||
        print_level == -1 ||
        (print_level == 2 &&
         !ExceptionTraceContext::get_instance().prefix_in_black_list(
             exception_name))) {
      auto stack = fmt::format(
          "thread throws exception: {}, trace:\n {} \n",
          exception_name.c_str(),
          StackTrace().toString(true).c_str());
      std::cerr << stack << std::endl;
    }
  }
  // call the real __cxa_throw():

#if defined(ADDRESS_SANITIZER)
  __interceptor___cxa_throw(thrown_exception, info, dest);
#else
  __real___cxa_throw(thrown_exception, info, dest);
#endif
}
#endif
} // namespace bytedance::bolt::process
