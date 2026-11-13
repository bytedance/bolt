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

#include <mach-o/loader.h>

#include <cstddef>
#include <cstdint>
#include <cstring>

namespace bytedance::bolt::process::detail {

struct MachOSegment {
  uintptr_t begin;
  uintptr_t end;

  bool contains(uintptr_t address) const {
    return address >= begin && address < end;
  }
};

// dyld supplies native-endian headers. Swapped and unknown magic are rejected.
inline size_t machOHeaderSize(uint32_t magic) {
  switch (magic) {
    case MH_MAGIC:
      return sizeof(mach_header);
    case MH_MAGIC_64:
      return sizeof(mach_header_64);
    default:
      return 0;
  }
}

// Called only during installation, never from a signal handler. On failure,
// count stays zero and the caller must not publish any partially written
// ranges. imageSize bounds the header and load commands, not the mapped segment
// data.
inline bool collectMachOExecutableSegments(
    const void* image,
    size_t imageSize,
    intptr_t slide,
    MachOSegment* segments,
    size_t capacity,
    size_t& count) {
  count = 0;
  if (image == nullptr || imageSize < sizeof(mach_header)) {
    return false;
  }
  mach_header header;
  std::memcpy(&header, image, sizeof(header));
  const size_t headerSize = machOHeaderSize(header.magic);
  if (headerSize == 0 || headerSize > imageSize ||
      header.sizeofcmds > imageSize - headerSize) {
    return false;
  }
  const auto* cursor = static_cast<const char*>(image) + headerSize;
  size_t remaining = header.sizeofcmds;
  size_t segmentCount = 0;
  for (uint32_t index = 0; index < header.ncmds; ++index) {
    if (remaining < sizeof(load_command)) {
      return false;
    }
    load_command command;
    std::memcpy(&command, cursor, sizeof(command));
    const size_t alignment = header.magic == MH_MAGIC_64 ? 8 : 4;
    if (command.cmdsize < sizeof(command) || command.cmdsize > remaining ||
        command.cmdsize % alignment != 0) {
      return false;
    }
    uint64_t address = 0;
    uint64_t size = 0;
    vm_prot_t protection = 0;
    if (command.cmd == LC_SEGMENT_64) {
      if (header.magic != MH_MAGIC_64 ||
          command.cmdsize < sizeof(segment_command_64)) {
        return false;
      }
      segment_command_64 segment;
      std::memcpy(&segment, cursor, sizeof(segment));
      if (segment.nsects >
          (command.cmdsize - sizeof(segment)) / sizeof(section_64)) {
        return false;
      }
      address = segment.vmaddr;
      size = segment.vmsize;
      protection = segment.initprot;
    } else if (command.cmd == LC_SEGMENT) {
      if (header.magic != MH_MAGIC ||
          command.cmdsize < sizeof(segment_command)) {
        return false;
      }
      segment_command segment;
      std::memcpy(&segment, cursor, sizeof(segment));
      if (segment.nsects >
          (command.cmdsize - sizeof(segment)) / sizeof(section)) {
        return false;
      }
      address = segment.vmaddr;
      size = segment.vmsize;
      protection = segment.initprot;
    }
    // Consumers resolve PCs only. Excluding non-executable segments also avoids
    // __PAGEZERO and shared-cache data ranges that span other images' code.
    if ((protection & VM_PROT_EXECUTE) != 0 && size != 0) {
      if (address > UINTPTR_MAX) {
        return false;
      }
      uintptr_t begin = static_cast<uintptr_t>(address);
      if (slide >= 0) {
        if (static_cast<uintptr_t>(slide) > UINTPTR_MAX - begin) {
          return false;
        }
        begin += static_cast<uintptr_t>(slide);
      } else {
        const uintptr_t magnitude =
            uintptr_t{0} - static_cast<uintptr_t>(slide);
        if (begin < magnitude) {
          return false;
        }
        begin -= magnitude;
      }
      if (size > UINTPTR_MAX - begin || segmentCount == capacity) {
        return false;
      }
      segments[segmentCount++] = {begin, begin + static_cast<uintptr_t>(size)};
    }
    cursor += command.cmdsize;
    remaining -= command.cmdsize;
  }
  if (remaining != 0) {
    return false;
  }
  count = segmentCount;
  return true;
}

} // namespace bytedance::bolt::process::detail
