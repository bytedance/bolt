/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 * --------------------------------------------------------------------------
 * Copyright (c) ByteDance Ltd. and/or its affiliates.
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file has been modified by ByteDance Ltd. and/or its affiliates on
 * 2025-11-11.
 *
 * Original file was released under the Apache License 2.0,
 * with the full license text available at:
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * This modified file is released under the same license.
 * --------------------------------------------------------------------------
 */

#pragma once

#include <atomic>
#include <cstdint>
#include <memory>
#include <random>
#include <thread>
#include <utility>
#include <vector>

#include <bolt/common/base/Exceptions.h>
#include <qpl/qpl.h>
namespace bytedance::bolt::shuffle::sparksql {
namespace qpl {

/// QplJobHWPool is resource pool to provide the job objects, which is
/// used for storing context information during.
/// Memory for QPL job will be allocated when the QPLJobHWPool instance is
/// created
///
//  QPL job can offload RLE-decoding/Filter/(De)compression works to hardware
//  accelerator.
class QplJobHWPool {
 public:
  static QplJobHWPool& GetInstance();

  /// Acquire QPL job
  ///
  /// @param jobId QPL job id, used when release QPL job
  /// \return Pointer to the QPL job. If acquire job failed, return nullptr.
  qpl_job* AcquireJob(uint32_t& jobId);

  /// \brief Release QPL job by the jobId.
  void ReleaseJob(uint32_t jobId);

  /// \brief Return if the QPL job is allocated successfully.
  static const bool& IsJobPoolReady() {
    return jobPoolReady;
  }

 private:
  QplJobHWPool();
  ~QplJobHWPool();
  static void InitJobPool();
  bool tryLockJob(uint32_t index);
  void unLockJob(uint32_t index);

  static inline void CheckJobIndex(uint32_t index) {
    if (index >= MAX_JOB_NUMBER) {
      BOLT_FAIL("Index exceeds MAX_JOB_NUMBER 512: " + std::to_string(index));
    }
  }

  /// Max jobs in QPL_JOB_POOL
  static constexpr auto MAX_JOB_NUMBER = 64;
  /// Entire buffer for storing all job objects
  static std::unique_ptr<uint8_t[]> hwJobsBuffer;
  /// Job pool for storing all job object pointers
  static std::array<qpl_job*, MAX_JOB_NUMBER> jobPool;
  /// Locks for accessing each job object pointers
  static std::array<std::atomic_bool, MAX_JOB_NUMBER> jobLocks;

  static bool jobPoolReady;
  std::mt19937 randomEngine;
  std::uniform_int_distribution<int> distribution;
};

} //  namespace qpl
} // namespace bytedance::bolt::shuffle::sparksql
