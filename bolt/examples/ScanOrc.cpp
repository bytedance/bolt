/*
 * Copyright (c) Facebook, Inc. and its affiliates.
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

#include <folly/init/Init.h>
#include <algorithm>

#include "bolt/common/file/FileSystems.h"
#include "bolt/common/memory/Memory.h"
#include "bolt/common/testutil/TempDirectoryPath.h"
#include "bolt/dwio/dwrf/reader/DwrfReader.h"
#include "bolt/vector/BaseVector.h"
using namespace bytedance::bolt;
using namespace bytedance::bolt::dwio::common;
using namespace bytedance::bolt::dwrf;

// A temporary program that reads from ORC file and prints its content
// Used to compare the ORC data read by DWRFReader against apache-orc repo.
// Usage: bolt_example_scan_orc {orc_file_path}
int main(int argc, char** argv) {
  // todo: use folly::Init init after upgrade folly lib
  folly::init(&argc, &argv);

  if (argc < 2) {
    return 1;
  }

  // To be able to read local files, we need to register the local file
  // filesystem. We also need to register the dwrf reader factory:
  filesystems::registerLocalFileSystem();
  dwrf::registerDwrfReaderFactory();
  bytedance::bolt::memory::MemoryManager::initialize(
      bytedance::bolt::memory::MemoryManager::Options{});
  auto pool = bytedance::bolt::memory::memoryManager()->addLeafPool();

  std::string filePath{argv[1]};
  dwio::common::ReaderOptions readerOpts{pool.get()};
  // To make DwrfReader reads ORC file, setFileFormat to FileFormat::ORC
  readerOpts.setFileFormat(FileFormat::ORC);
  auto reader = DwrfReader::create(
      std::make_unique<BufferedInput>(
          std::make_shared<LocalReadFile>(filePath),
          readerOpts.getMemoryPool()),
      readerOpts);

  VectorPtr batch;
  RowReaderOptions rowReaderOptions;
  auto rowReader = reader->createRowReader(rowReaderOptions);
  while (rowReader->next(500, batch)) {
    auto rowVector = batch->as<RowVector>();
    for (vector_size_t i = 0; i < rowVector->size(); ++i) {
      std::cout << rowVector->toString(i) << std::endl;
    }
  }

  return 0;
}
