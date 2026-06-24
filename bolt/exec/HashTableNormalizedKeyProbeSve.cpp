/*
 * Copyright (c) Facebook, Inc. and its affiliates.
 * Licensed under the Apache License, Version 2.0
 * --------------------------------------------------------------------------
 * Copyright (c) ByteDance Ltd. and/or its affiliates.
 * SPDX-License-Identifier: Apache-2.0
 */

#include <arm_sve.h>

#include "bolt/exec/HashTable.h"

namespace bytedance::bolt::exec {
namespace {

inline __attribute__((always_inline)) svbool_t
get_uniq_mask2(svbool_t pg, const svuint64_t val) {
  svuint64_t s1 = svext_u64(val, val, 1);
  svbool_t mask2 = svcmpeq(svwhilelt_b64(0, 3), val, s1);

  svuint64_t s2 = svext_u64(val, val, 2);
  svbool_t mask3 = svcmpeq(svwhilelt_b64(0, 2), val, s2);
  svbool_t mask12 = svorr_b_z(pg, mask2, mask3);

  svuint64_t s3 = svext_u64(val, val, 3);
  svbool_t mask4 = svcmpeq(svwhilelt_b64(0, 1), val, s3);

  svbool_t mask = svorr_b_z(pg, mask4, mask12);
  mask = svnot_b_z(pg, mask);

  return mask;
}

} // namespace

template <bool ignoreNullKeys>
void HashTable<ignoreNullKeys>::groupNormalizedKeyProbeSVE(HashLookup& lookup) {
  constexpr int32_t kVectorWidth = 4;
  constexpr int32_t kTagShiftBits = 38;
  constexpr uint64_t kTagMask = 0x80;
  constexpr int32_t kProbeStep = 32;
  constexpr int32_t kBitsPerByte = 8;
  constexpr int32_t kInvalidIndex = INT_MAX;
  constexpr int32_t kPrefetchDistance = 8;

  int32_t numProbes = lookup.rows.size();
  const int32_t* rows = lookup.rows.data();
  auto hashes = lookup.hashes.data();
  auto groups = lookup.hits.data();
  auto normalizedKeys = lookup.normalizedKeys.data();

  bool all = false;
  if (lookup.rows.size() - 1 == lookup.rows[numProbes - 1]) {
    all = true;
  }

  // Core vector build loop for this group
  // svuint64_t keyVec = svdup_n_u64(0);
  svuint64_t valVec = svdup_n_u64(0);
  // svuint64_t tabKey = svdup_n_u64(0);

  svuint64_t currIndex = svdup_n_u64(0);
  // svuint64_t indexVec = svindex_u64(0, 1);
  svint64_t rowId = svdup_n_s64(0);
  svbool_t indexMask = svptrue_b64();
  svbool_t emptyMask = svptrue_b64();
  // svuint8_t zeroMask = svdup_n_u8(0);


  int32_t i = 0;
  svbool_t predicateMask = svptrue_b64();
  while (i + kVectorWidth < numProbes) {
    if (i + kVectorWidth + kPrefetchDistance < numProbes) {
      for (int32_t p = 0; p < kVectorWidth; ++p) {
        int32_t prefetchRow = i + kVectorWidth + kPrefetchDistance + p;
        uint64_t prefetchIdx = hashes[prefetchRow] & (capacity_ - 1);
        // Prefetch tag/value/key for upcoming slots
        svprfb(svptrue_b8(), getTagPtr() + prefetchIdx, SV_PLDL1STRM);
        svprfd(svptrue_b64(), getValuePtr() + prefetchIdx, SV_PLDL1STRM);
        svprfd(svptrue_b64(), getKeyPtr() + prefetchIdx, SV_PLDL1STRM);
      }
    }

    // load normalized keys and curr_index
    if (all) {
      currIndex = svld1(predicateMask, hashes + i);
    } else {
      rowId = svld1sw_s64(predicateMask, rows + i);
      svint64_t rowOffset = svlsl_n_s64_z(predicateMask, rowId, 3);
      currIndex = svld1_gather_offset(
          predicateMask, hashes, svreinterpret_u64(rowOffset));
    }

    // calc tag
    svuint64_t currTagTmp =
        svlsr_n_u64_x(predicateMask, currIndex, kTagShiftBits);
    svuint64_t currTag = svorr_n_u64_z(
        predicateMask,
        currTagTmp,
        kTagMask);

    // Mask to slot index in table
    currIndex = svand_n_u64_z(predicateMask, currIndex, capacity_ - 1);
    svuint64_t htOffset = svlsl_n_u64_z(predicateMask, currIndex, 3);
    valVec = svld1_gather_u64offset_u64(
        predicateMask,
        reinterpret_cast<uint64_t*>(getValuePtr()),
        htOffset);

    emptyMask = svcmpeq_n_u64(predicateMask, valVec, 0);
    if (svptest_any(predicateMask, emptyMask)) {
      indexMask = get_uniq_mask2(
          emptyMask,
          currIndex); // Deduplicate slot indices for empty lanes
    } else {
      indexMask = emptyMask;
    }

    svbool_t toWriteMask = svand_z(predicateMask, indexMask, emptyMask);

    // Per-lane table indices
    uint64_t htIndices[kVectorWidth] = {0, 0, 0, 0};
    svst1(svptrue_b64(), htIndices, currIndex);

    uint64_t tags[kVectorWidth] = {0, 0, 0, 0};
    svst1(svptrue_b64(), tags, currTag);

    uint32_t flag = 0;
    __asm__("str %1, [%0]"
                         :
                         : "r"(&flag), "Upl"(toWriteMask)
                         : "memory");
    uint32_t flag1 = flag;
    while (flag1) {
      int32_t offset = __builtin_ctz(flag1);
      int32_t idx = offset / kBitsPerByte;
      uint64_t htIdx = htIndices[idx];

      getKeyPtr()[htIdx] = normalizedKeys[i + idx];
      getValuePtr()[htIdx] =
          insertEntryforSVE(lookup, htIdx, i + idx);
      groups[i + idx] = getValuePtr()[htIdx];
      getTagPtr()[htIdx] = tags[idx];

      flag1 &= (flag1 - 1);
    }


    uint32_t conflictFlag = ~flag & 0x01010101;

    // Conflict lanes: linear probe
    while (conflictFlag) {
      int32_t offset = __builtin_ctz(conflictFlag);
      int32_t idx = offset / kBitsPerByte;
      int32_t rowIdx = i + idx;

      bool processSuccess = false;
      int64_t htStartIdx = htIndices[idx];


      
      while (!processSuccess) {
        // Scan tags from htStartIdx (wrap at capacity)
        svbool_t tagPredicate = svwhilelt_b8(htStartIdx, capacity_);
        svuint8_t htTag =
            svld1_u8(tagPredicate, getTagPtr() + htStartIdx);

        svuint8_t conflictTag =
            svdup_lane(svreinterpret_u8_u64(currTag), offset);

        svbool_t matchZero = svcmpeq_n_u8(tagPredicate, htTag, 0);
        svbool_t conflictMatch = svcmpeq_u8(tagPredicate, htTag, conflictTag);
        uint32_t zeroMask = 0;
        uint32_t conflictMask = 0;
        __asm__("str %1, [%0]"
                             :
                             : "r"(&zeroMask), "Upl"(matchZero)
                             : "memory");
        __asm__("str %1, [%0]"
                             :
                             : "r"(&conflictMask), "Upl"(conflictMatch)
                             : "memory");
        int32_t zeroBefore = kInvalidIndex;
        int32_t htZeroIdx = kInvalidIndex;
        if (zeroMask) {
          zeroBefore = __builtin_ctz(zeroMask);
          htZeroIdx = htStartIdx + zeroBefore;
        }
        

        uint64_t currNormalizedKey = normalizedKeys[rowIdx];
        while (conflictMask) {
          int32_t conflictIdx = __builtin_ctz(conflictMask);
          //
          if (conflictIdx > zeroBefore) {
            // insert new
            getKeyPtr()[htZeroIdx] = currNormalizedKey;
            getValuePtr()[htZeroIdx] =
                insertEntryforSVE(lookup, htZeroIdx, rowIdx);
            groups[rowIdx] = getValuePtr()[htZeroIdx];
            getTagPtr()[htZeroIdx] = tags[idx];
            
            processSuccess = true;
            break;
          } else {
            // compare normalize key
            int32_t htIdx =
                htStartIdx + conflictIdx;
            if (currNormalizedKey == getKeyPtr()[htIdx]) {
              groups[rowIdx] = getValuePtr()[htIdx];
              
              processSuccess = true;
              break;
            }
          }
          conflictMask = conflictMask & (conflictMask - 1);
        }
        if (zeroMask && !processSuccess) {
          getKeyPtr()[htZeroIdx] = currNormalizedKey;
          getValuePtr()[htZeroIdx] =
              insertEntryforSVE(lookup, htZeroIdx, rowIdx);
          groups[rowIdx] = getValuePtr()[htZeroIdx];
          getTagPtr()[htZeroIdx] = tags[idx];
          processSuccess = true;
        }

        htStartIdx =
            capacity_ - htStartIdx > kProbeStep ? htStartIdx + kProbeStep : 0;
      }
      conflictFlag &= (conflictFlag - 1);
    }
    i += kVectorWidth;
  }
    predicateMask = svwhilelt_b64(i, numProbes);
    // load normalized keys and curr_index
    if (all) {
      currIndex = svld1(predicateMask, hashes + i);
    } else {
      rowId = svld1sw_s64(predicateMask, rows + i);
      svint64_t rowOffset = svlsl_n_s64_z(predicateMask, rowId, 3);
      currIndex = svld1_gather_offset(
          predicateMask, hashes, svreinterpret_u64(rowOffset));
    }

    // calc tag
    svuint64_t currTagTmp =
        svlsr_n_u64_x(predicateMask, currIndex, kTagShiftBits);
    svuint64_t currTag = svorr_n_u64_z(
        predicateMask,
        currTagTmp,
        kTagMask);

    // Mask to slot index in table
    currIndex = svand_n_u64_z(predicateMask, currIndex, capacity_ - 1);
    svuint64_t htOffset = svlsl_n_u64_z(predicateMask, currIndex, 3);
    valVec = svld1_gather_u64offset_u64(
        predicateMask,
        reinterpret_cast<uint64_t*>(getValuePtr()),
        htOffset);

    emptyMask = svcmpeq_n_u64(predicateMask, valVec, 0);
    if (svptest_any(predicateMask, emptyMask)) {
      indexMask = get_uniq_mask2(
          emptyMask,
          currIndex); // Deduplicate slot indices for empty lanes
    } else {
      indexMask = emptyMask;
    }

    svbool_t toWriteMask = svand_z(predicateMask, indexMask, emptyMask);

    // Per-lane table indices
    uint64_t htIndices[kVectorWidth] = {0, 0, 0, 0};
    svst1(svptrue_b64(), htIndices, currIndex);

    uint64_t tags[kVectorWidth] = {0, 0, 0, 0};
    svst1(svptrue_b64(), tags, currTag);

    uint32_t flag = 0;
    __asm__("str %1, [%0]"
                         :
                         : "r"(&flag), "Upl"(toWriteMask)
                         : "memory");
    uint32_t flag1 = flag;
    while (flag1) {
      int32_t offset = __builtin_ctz(flag1);
      int32_t idx = offset / kBitsPerByte;
      uint64_t htIdx = htIndices[idx];

      getKeyPtr()[htIdx] = normalizedKeys[i + idx];
      getValuePtr()[htIdx] =
          insertEntryforSVE(lookup, htIdx, i + idx);
      groups[i + idx] = getValuePtr()[htIdx];
      getTagPtr()[htIdx] = tags[idx];

      flag1 &= (flag1 - 1);
    }



  svbool_t conflictIndex = svnot_b_z(predicateMask, toWriteMask);
  uint32_t conflictFlag = 0;
  __asm__ __volatile__("str %1, [%0]" : : "r"(&conflictFlag), "Upl"(conflictIndex): "memory");
    // Conflict lanes: linear probe
    while (conflictFlag) {
      int32_t offset = __builtin_ctz(conflictFlag);
      int32_t idx = offset / kBitsPerByte;
      int32_t rowIdx = i + idx;

      bool processSuccess = false;
      int64_t htStartIdx = htIndices[idx];


      
      while (!processSuccess) {
        // Scan tags from htStartIdx (wrap at capacity)
        svbool_t tagPredicate = svwhilelt_b8(htStartIdx, capacity_);
        svuint8_t htTag =
            svld1_u8(tagPredicate, getTagPtr() + htStartIdx);

        svuint8_t conflictTag =
            svdup_lane(svreinterpret_u8_u64(currTag), offset);

        svbool_t matchZero = svcmpeq_n_u8(tagPredicate, htTag, 0);
        svbool_t conflictMatch = svcmpeq_u8(tagPredicate, htTag, conflictTag);
        uint32_t zeroMask = 0;
        uint32_t conflictMask = 0;
        __asm__("str %1, [%0]"
                             :
                             : "r"(&zeroMask), "Upl"(matchZero)
                             : "memory");
        __asm__("str %1, [%0]"
                             :
                             : "r"(&conflictMask), "Upl"(conflictMatch)
                             : "memory");
        int32_t zeroBefore = kInvalidIndex;
        int32_t htZeroIdx = kInvalidIndex;
        if (zeroMask) {
          zeroBefore = __builtin_ctz(zeroMask);
          htZeroIdx = htStartIdx + zeroBefore;
        }
        

        uint64_t currNormalizedKey = normalizedKeys[rowIdx];
        while (conflictMask) {
          int32_t conflictIdx = __builtin_ctz(conflictMask);
          //
          if (conflictIdx > zeroBefore) {
            // insert new
            getKeyPtr()[htZeroIdx] = currNormalizedKey;
            getValuePtr()[htZeroIdx] =
                insertEntryforSVE(lookup, htZeroIdx, rowIdx);
            groups[rowIdx] = getValuePtr()[htZeroIdx];
            getTagPtr()[htZeroIdx] = tags[idx];
            
            processSuccess = true;
            break;
          } else {
            // compare normalize key
            int32_t htIdx =
                htStartIdx + conflictIdx;
            if (currNormalizedKey == getKeyPtr()[htIdx]) {
              groups[rowIdx] = getValuePtr()[htIdx];
              
              processSuccess = true;
              break;
            }
          }
          conflictMask = conflictMask & (conflictMask - 1);
        }
        if (zeroMask && !processSuccess) {
          getKeyPtr()[htZeroIdx] = currNormalizedKey;
          getValuePtr()[htZeroIdx] =
              insertEntryforSVE(lookup, htZeroIdx, rowIdx);
          groups[rowIdx] = getValuePtr()[htZeroIdx];
          getTagPtr()[htZeroIdx] = tags[idx];
          processSuccess = true;
        }

        htStartIdx =
            capacity_ - htStartIdx > kProbeStep ? htStartIdx + kProbeStep : 0;
      }
      conflictFlag &= (conflictFlag - 1);
    }
}

template <bool ignoreNullKeys>
void HashTable<ignoreNullKeys>::insertForGroupBySve(
    char** groups,
    uint64_t* hashes,
    int32_t numGroups) {
  constexpr int32_t kVectorWidth = 4;
  constexpr int32_t kTagShiftBits = 38;
  constexpr uint64_t kTagMask = 0x80;
  constexpr int32_t kProbeStep = 32;
  constexpr int32_t kBitsPerByte = 8;
  constexpr int32_t kInvalidIndex = INT_MAX;
  constexpr int32_t kPrefetchDistance = 8;

  int32_t numProbes = numGroups;
  svuint64_t valVec = svdup_n_u64(0);
  svuint64_t currIndex = svdup_n_u64(0);
  svbool_t indexMask = svptrue_b64();
  svbool_t emptyMask = svptrue_b64();

  int32_t i = 0;
  svbool_t predicateMask = svptrue_b64();
  while (i + kVectorWidth < numProbes) {
    if (i + kVectorWidth + kPrefetchDistance < numProbes) {
      for (int32_t p = 0; p < kVectorWidth; ++p) {
        int32_t prefetchRow = i + kVectorWidth + kPrefetchDistance + p;
        uint64_t prefetchIdx = hashes[prefetchRow] & (capacity_ - 1);
        svprfb(svptrue_b8(), getTagPtr() + prefetchIdx, SV_PLDL1STRM);
        svprfd(svptrue_b64(), getValuePtr() + prefetchIdx, SV_PLDL1STRM);
        svprfd(svptrue_b64(), getKeyPtr() + prefetchIdx, SV_PLDL1STRM);
      }
    }

    currIndex = svld1(predicateMask, hashes + i);

    svuint64_t currTagTmp =
        svlsr_n_u64_x(predicateMask, currIndex, kTagShiftBits);
    svuint64_t currTag =
        svorr_n_u64_z(predicateMask, currTagTmp, kTagMask);

    currIndex = svand_n_u64_z(predicateMask, currIndex, capacity_ - 1);
    svuint64_t htOffset = svlsl_n_u64_z(predicateMask, currIndex, 3);
    valVec = svld1_gather_u64offset_u64(
        predicateMask,
        reinterpret_cast<uint64_t*>(getValuePtr()),
        htOffset);

    emptyMask = svcmpeq_n_u64(predicateMask, valVec, 0);
    if (svptest_any(predicateMask, emptyMask)) {
      indexMask = get_uniq_mask2(emptyMask, currIndex);
    } else {
      indexMask = emptyMask;
    }

    svbool_t toWriteMask = svand_z(predicateMask, indexMask, emptyMask);

    uint64_t htIndices[kVectorWidth] = {0, 0, 0, 0};
    svst1(svptrue_b64(), htIndices, currIndex);

    uint64_t tags[kVectorWidth] = {0, 0, 0, 0};
    svst1(svptrue_b64(), tags, currTag);

    uint32_t flag = 0;
    __asm__("str %1, [%0]" : : "r"(&flag), "Upl"(toWriteMask) : "memory");
    uint32_t flag1 = flag;
    while (flag1) {
      int32_t offset = __builtin_ctz(flag1);
      int32_t idx = offset / kBitsPerByte;
      uint64_t htIdx = htIndices[idx];

      getKeyPtr()[htIdx] = reinterpret_cast<uint64_t*>(groups[i + idx])[-1];
      getValuePtr()[htIdx] = groups[i + idx];
      getTagPtr()[htIdx] = tags[idx];

      flag1 &= (flag1 - 1);
    }

    uint32_t conflictFlag = ~flag & 0x01010101;
    while (conflictFlag) {
      int32_t offset = __builtin_ctz(conflictFlag);
      int32_t idx = offset / kBitsPerByte;
      int32_t rowIdx = i + idx;
      int64_t htStartIdx = htIndices[idx];

      while (true) {
        svbool_t tagPredicate = svwhilelt_b8(htStartIdx, capacity_);
        svuint8_t htTag = svld1_u8(tagPredicate, getTagPtr() + htStartIdx);
        svbool_t matchZero = svcmpeq_n_u8(tagPredicate, htTag, 0);

        uint32_t zeroMask = 0;
        __asm__("str %1, [%0]"
                :
                : "r"(&zeroMask), "Upl"(matchZero)
                : "memory");

        if (zeroMask) {
          int32_t zeroBefore = __builtin_ctz(zeroMask);
          int32_t htZeroIdx = htStartIdx + zeroBefore;

          getKeyPtr()[htZeroIdx] =
              reinterpret_cast<uint64_t*>(groups[rowIdx])[-1];
          getValuePtr()[htZeroIdx] = groups[rowIdx];
          getTagPtr()[htZeroIdx] = tags[idx];
          break;
        }

        htStartIdx = capacity_ - htStartIdx > kProbeStep ? htStartIdx + kProbeStep
                                                         : 0;
      }
      conflictFlag &= (conflictFlag - 1);
    }
    i += kVectorWidth;
  }

  predicateMask = svwhilelt_b64(i, numProbes);
  currIndex = svld1(predicateMask, hashes + i);

  svuint64_t currTagTmp =
      svlsr_n_u64_x(predicateMask, currIndex, kTagShiftBits);
  svuint64_t currTag = svorr_n_u64_z(predicateMask, currTagTmp, kTagMask);

  currIndex = svand_n_u64_z(predicateMask, currIndex, capacity_ - 1);
  svuint64_t htOffset = svlsl_n_u64_z(predicateMask, currIndex, 3);
  valVec = svld1_gather_u64offset_u64(
      predicateMask,
      reinterpret_cast<uint64_t*>(getValuePtr()),
      htOffset);

  emptyMask = svcmpeq_n_u64(predicateMask, valVec, 0);
  if (svptest_any(predicateMask, emptyMask)) {
    indexMask = get_uniq_mask2(emptyMask, currIndex);
  } else {
    indexMask = emptyMask;
  }

  svbool_t toWriteMask = svand_z(predicateMask, indexMask, emptyMask);

  uint64_t htIndices[kVectorWidth] = {0, 0, 0, 0};
  svst1(svptrue_b64(), htIndices, currIndex);

  uint64_t tags[kVectorWidth] = {0, 0, 0, 0};
  svst1(svptrue_b64(), tags, currTag);

  uint32_t flag = 0;
  __asm__("str %1, [%0]" : : "r"(&flag), "Upl"(toWriteMask) : "memory");
  uint32_t flag1 = flag;
  while (flag1) {
    int32_t offset = __builtin_ctz(flag1);
    int32_t idx = offset / kBitsPerByte;
    uint64_t htIdx = htIndices[idx];

    getKeyPtr()[htIdx] = reinterpret_cast<uint64_t*>(groups[i + idx])[-1];
    getValuePtr()[htIdx] = groups[i + idx];
    getTagPtr()[htIdx] = tags[idx];

    flag1 &= (flag1 - 1);
  }

  svbool_t conflictIndex = svnot_b_z(predicateMask, toWriteMask);
  uint32_t conflictFlag = 0;
  __asm__ __volatile__(
      "str %1, [%0]" : : "r"(&conflictFlag), "Upl"(conflictIndex) : "memory");
  while (conflictFlag) {
    int32_t offset = __builtin_ctz(conflictFlag);
    int32_t idx = offset / kBitsPerByte;
    int32_t rowIdx = i + idx;
    int64_t htStartIdx = htIndices[idx];

    while (true) {
      svbool_t tagPredicate = svwhilelt_b8(htStartIdx, capacity_);
      svuint8_t htTag = svld1_u8(tagPredicate, getTagPtr() + htStartIdx);
      svbool_t matchZero = svcmpeq_n_u8(tagPredicate, htTag, 0);

      uint32_t zeroMask = 0;
      __asm__("str %1, [%0]"
              :
              : "r"(&zeroMask), "Upl"(matchZero)
              : "memory");

      if (zeroMask) {
        int32_t zeroBefore = __builtin_ctz(zeroMask);
        int32_t htZeroIdx = htStartIdx + zeroBefore;

        getKeyPtr()[htZeroIdx] =
            reinterpret_cast<uint64_t*>(groups[rowIdx])[-1];
        getValuePtr()[htZeroIdx] = groups[rowIdx];
        getTagPtr()[htZeroIdx] = tags[idx];
        break;
      }

      htStartIdx = capacity_ - htStartIdx > kProbeStep ? htStartIdx + kProbeStep
                                                       : 0;
    }
    conflictFlag &= (conflictFlag - 1);
  }
}

template void HashTable<true>::groupNormalizedKeyProbeSVE(HashLookup&);
template void HashTable<false>::groupNormalizedKeyProbeSVE(HashLookup&);
template void HashTable<true>::insertForGroupBySve(char**, uint64_t*, int32_t);
template void HashTable<false>::insertForGroupBySve(char**, uint64_t*, int32_t);

} // namespace bytedance::bolt::exec
