/*
 * Copyright (c) ByteDance Ltd. and/or its affiliates.
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

#include "bolt/dwio/lance/NativeLanceScanWindow.h"

#include "bolt/common/base/Exceptions.h"

namespace bytedance::bolt::lance::reader {

NativeLanceScanWindow::NativeLanceScanWindow(
    uint64_t generation,
    uint64_t rowStart,
    uint64_t requestedRows)
    : generation_(generation),
      rowStart_(rowStart),
      requestedRows_(requestedRows) {
  BOLT_CHECK_GT(generation_, 0);
  BOLT_CHECK_GT(requestedRows_, 0);
}

void NativeLanceScanWindow::beginDecode(bool hasFilters) {
  BOLT_CHECK(state_ == NativeLanceWindowState::kCreated);
  state_ = hasFilters ? NativeLanceWindowState::kDecodingFilters
                      : NativeLanceWindowState::kDecodingValues;
}

void NativeLanceScanWindow::filtersReady() {
  BOLT_CHECK(state_ == NativeLanceWindowState::kDecodingFilters);
  state_ = NativeLanceWindowState::kDecodingValues;
}

void NativeLanceScanWindow::beginAssembly() {
  BOLT_CHECK(state_ == NativeLanceWindowState::kDecodingValues);
  state_ = NativeLanceWindowState::kAssembling;
}

void NativeLanceScanWindow::publish(uint64_t scannedRows, VectorPtr result) {
  BOLT_CHECK(state_ == NativeLanceWindowState::kAssembling);
  BOLT_CHECK_GT(scannedRows, 0);
  BOLT_CHECK_LE(scannedRows, requestedRows_);
  BOLT_CHECK_NOT_NULL(result);
  scannedRows_ = scannedRows;
  result_ = std::move(result);
  state_ = NativeLanceWindowState::kOutputReady;
}

uint64_t NativeLanceScanWindow::drain(VectorPtr& result) {
  BOLT_CHECK(state_ == NativeLanceWindowState::kOutputReady);
  BOLT_CHECK_NOT_NULL(result_);
  result = std::move(result_);
  state_ = NativeLanceWindowState::kDrained;
  return scannedRows_;
}

void NativeLanceScanWindow::fail(std::exception_ptr error) {
  if (state_ == NativeLanceWindowState::kCancelled ||
      state_ == NativeLanceWindowState::kDrained) {
    return;
  }
  failure_ = std::move(error);
  result_.reset();
  state_ = NativeLanceWindowState::kFailed;
}

void NativeLanceScanWindow::cancel() {
  if (state_ == NativeLanceWindowState::kDrained ||
      state_ == NativeLanceWindowState::kFailed ||
      state_ == NativeLanceWindowState::kCancelled) {
    return;
  }
  result_.reset();
  state_ = NativeLanceWindowState::kCancelled;
}

} // namespace bytedance::bolt::lance::reader
