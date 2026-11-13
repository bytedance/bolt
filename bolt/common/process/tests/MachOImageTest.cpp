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
#include "bolt/common/process/MachOImage-inl.h"

#include <dlfcn.h>
#include <gtest/gtest.h>
#include <mach-o/dyld.h>
#include <signal.h>
#include <unistd.h>

#include <array>
#include <cstdlib>
#include <vector>

namespace bytedance::bolt::process {
namespace {

struct Image64 {
  mach_header_64 header{};
  std::array<segment_command_64, 4> commands{};

  Image64() {
    header.magic = MH_MAGIC_64;
    header.ncmds = commands.size();
    header.sizeofcmds = sizeof(commands);
    // A nonzero reserved word catches traversal starting at header + 28.
    header.reserved = 0xdeadbeef;
    for (size_t i = 0; i < commands.size(); ++i) {
      auto& segment = commands[i];
      segment.cmd = LC_SEGMENT_64;
      segment.cmdsize = sizeof(segment);
      segment.vmaddr = 0x1000 + i * 0x2000;
      segment.vmsize = 0x1000;
      segment.initprot = VM_PROT_READ | VM_PROT_EXECUTE;
    }
  }
};

class MachOImageTest : public testing::Test {
 protected:
  bool collect(intptr_t slide = 0, size_t capacity = 4) {
    return detail::collectMachOExecutableSegments(
        &image_, sizeof(image_), slide, ranges_.data(), capacity, count_);
  }

  Image64 image_;
  std::array<detail::MachOSegment, 4> ranges_{};
  size_t count_{99};
};

TEST_F(MachOImageTest, Uses64BitHeaderAndPreservesSegmentHoles) {
  ASSERT_TRUE(collect(0x10000));
  ASSERT_EQ(count_, 4);
  for (size_t i = 0; i < count_; ++i) {
    EXPECT_EQ(ranges_[i].begin, 0x11000 + i * 0x2000);
    EXPECT_EQ(ranges_[i].end, 0x12000 + i * 0x2000);
    EXPECT_TRUE(ranges_[i].contains(ranges_[i].begin));
    EXPECT_TRUE(ranges_[i].contains(ranges_[i].end - 1));
    EXPECT_FALSE(ranges_[i].contains(ranges_[i].end));
    EXPECT_FALSE(ranges_[i].contains(ranges_[i].begin - 1));
    for (size_t j = 0; j < count_; ++j) {
      EXPECT_FALSE(ranges_[j].contains(ranges_[i].end + 0x800));
    }
    if (i > 0) {
      EXPECT_LT(ranges_[i - 1].end, ranges_[i].begin);
    }
  }
}

TEST_F(MachOImageTest, ExcludesPageZeroDataAndEmptySegments) {
  auto& pageZero = image_.commands[0];
  std::memcpy(pageZero.segname, "__PAGEZERO", 10);
  pageZero.vmaddr = 0;
  pageZero.vmsize = 0x100000000;
  pageZero.initprot = VM_PROT_NONE;
  image_.commands[1].initprot = VM_PROT_READ | VM_PROT_WRITE;
  image_.commands[2].vmsize = 0;
  ASSERT_TRUE(collect());
  ASSERT_EQ(count_, 1);
  EXPECT_EQ(ranges_[0].begin, 0x7000);
  EXPECT_EQ(ranges_[0].end, 0x8000);
}

TEST_F(MachOImageTest, RejectsInvalidHeadersAndTruncatedCommandRegion) {
  for (uint32_t magic : {0u, MH_CIGAM, MH_CIGAM_64}) {
    image_.header.magic = magic;
    EXPECT_FALSE(collect());
    EXPECT_EQ(count_, 0);
  }
  image_.header.magic = MH_MAGIC_64;
  for (size_t size : {size_t{0}, sizeof(mach_header), sizeof(image_) - 1}) {
    EXPECT_FALSE(detail::collectMachOExecutableSegments(
        &image_, size, 0, ranges_.data(), ranges_.size(), count_));
    EXPECT_EQ(count_, 0);
  }
  EXPECT_FALSE(detail::collectMachOExecutableSegments(
      nullptr, sizeof(image_), 0, ranges_.data(), ranges_.size(), count_));
}

TEST_F(MachOImageTest, RejectsBadCommandSizesWithoutPublishingPartialRanges) {
  for (uint32_t size : {0u, 4u, 8u, 71u, 73u, UINT32_MAX}) {
    image_.commands[1].cmdsize = size;
    EXPECT_FALSE(collect()) << size;
    EXPECT_EQ(count_, 0);
  }
}

TEST_F(MachOImageTest, ValidatesCommandCountAndSectionBounds) {
  image_.header.ncmds = 5;
  EXPECT_FALSE(collect());
  image_.header.ncmds = 3;
  EXPECT_FALSE(collect());
  image_.header.ncmds = 4;
  image_.commands[0].nsects = 1;
  EXPECT_FALSE(collect());
  EXPECT_EQ(count_, 0);
}

TEST_F(MachOImageTest, SkipsUnknownCommandsAndRejectsWrongSegmentWidth) {
  image_.commands[0].cmd = LC_UUID;
  ASSERT_TRUE(collect());
  EXPECT_EQ(count_, 3);
  image_.commands[0].cmd = LC_SEGMENT;
  EXPECT_FALSE(collect());
  EXPECT_EQ(count_, 0);
}

TEST_F(MachOImageTest, BoundsCapacityAndAddressArithmetic) {
  EXPECT_FALSE(collect(0, 3));
  EXPECT_EQ(count_, 0);
  EXPECT_FALSE(collect(0, 0));
  ASSERT_TRUE(collect(-0x1000));
  EXPECT_EQ(ranges_[0].begin, 0);
  EXPECT_FALSE(collect(-0x1001));
  EXPECT_FALSE(collect(INTPTR_MIN));
  image_.commands[0].vmaddr = UINTPTR_MAX - 0x1000;
  EXPECT_FALSE(collect(0x2000));
  image_.commands[0].vmsize = 0x2000;
  EXPECT_FALSE(collect());
  EXPECT_EQ(count_, 0);
}

TEST_F(MachOImageTest, SupportsNative32BitHeader) {
  struct {
    mach_header header{};
    segment_command command{};
  } image;
  image.header.magic = MH_MAGIC;
  image.header.ncmds = 1;
  image.header.sizeofcmds = sizeof(image.command);
  image.command.cmd = LC_SEGMENT;
  image.command.cmdsize = sizeof(image.command);
  image.command.vmaddr = 0x1000;
  image.command.vmsize = 0x1000;
  image.command.initprot = VM_PROT_EXECUTE;
  ASSERT_TRUE(detail::collectMachOExecutableSegments(
      &image, sizeof(image), 0x2000, ranges_.data(), ranges_.size(), count_));
  ASSERT_EQ(count_, 1);
  EXPECT_EQ(ranges_[0].begin, 0x3000);
  EXPECT_EQ(ranges_[0].end, 0x4000);
}

TEST(MachOImageIntegrationTest, SharedCacheMallocMatchesItsActualImage) {
  // dladdr is deliberately used only by this test, never by the signal handler.
  const auto pc = reinterpret_cast<uintptr_t>(&malloc);
  Dl_info expected{};
  ASSERT_NE(dladdr(reinterpret_cast<const void*>(pc), &expected), 0);
  size_t matches = 0;
  for (uint32_t i = 0; i < _dyld_image_count(); ++i) {
    const auto* header = _dyld_get_image_header(i);
    ASSERT_NE(header, nullptr);
    std::vector<detail::MachOSegment> ranges(header->ncmds);
    size_t count = 0;
    ASSERT_TRUE(detail::collectMachOExecutableSegments(
        header,
        detail::machOHeaderSize(header->magic) + header->sizeofcmds,
        _dyld_get_image_vmaddr_slide(i),
        ranges.data(),
        ranges.size(),
        count))
        << _dyld_get_image_name(i);
    for (size_t j = 0; j < count; ++j) {
      if (ranges[j].contains(pc)) {
        ++matches;
        EXPECT_EQ(static_cast<const void*>(header), expected.dli_fbase);
      }
    }
  }
  EXPECT_EQ(matches, 1);
}

TEST(MachOImageIntegrationTest, InstallsHandlerWithRealDyldImages) {
  EXPECT_EXIT(
      {
        installCrashReportHandler(false, false, StackTraceEngine::kBacktrace);
        struct sigaction action {};
        if (sigaction(SIGABRT, nullptr, &action) != 0 ||
            (action.sa_flags & SA_SIGINFO) == 0) {
          _exit(1);
        }
        _exit(0);
      },
      testing::ExitedWithCode(0),
      "Initializing crash report handler");
}

} // namespace
} // namespace bytedance::bolt::process
