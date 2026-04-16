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

#pragma once

#include <sstream>
#include <string>
namespace bytedance::bolt {

/**
 * Contains the types of vector encodings available for various classes of
 * vectors.
 */
namespace VectorEncoding {
/**
 * Provides an enumeration of vector encoding types.
 */
enum class Simple {
  BIASED,
  CONSTANT,
  DICTIONARY,
  FLAT,
  SEQUENCE,
  ROW,
  MAP,
  ARRAY,
  LAZY,
  FUNCTION,
  VARIANT
};

inline std::ostream& operator<<(
    std::ostream& out,
    const VectorEncoding::Simple& type) {
  switch (type) {
    case VectorEncoding::Simple::BIASED:
      return out << "BIASED";
    case VectorEncoding::Simple::CONSTANT:
      return out << "CONSTANT";
    case VectorEncoding::Simple::DICTIONARY:
      return out << "DICTIONARY";
    case VectorEncoding::Simple::FLAT:
      return out << "FLAT";
    case VectorEncoding::Simple::SEQUENCE:
      return out << "SEQUENCE";
    case VectorEncoding::Simple::ROW:
      return out << "ROW";
    case VectorEncoding::Simple::MAP:
      return out << "MAP";
    case VectorEncoding::Simple::ARRAY:
      return out << "ARRAY";
    case VectorEncoding::Simple::LAZY:
      return out << "LAZY";
    case VectorEncoding::Simple::FUNCTION:
      return out << "FUNCTION";
    case VectorEncoding::Simple::VARIANT:
      return out << "VARIANT";
  }
  return out;
}

inline std::string mapSimpleToName(const VectorEncoding::Simple& simple) {
  std::stringstream ss;
  ss << simple;
  return ss.str();
}

inline bool isFlat(VectorEncoding::Simple encoding) {
  return encoding == VectorEncoding::Simple::FLAT;
}

inline bool isConstant(VectorEncoding::Simple encoding) {
  return encoding == VectorEncoding::Simple::CONSTANT;
}

inline bool isLazy(VectorEncoding::Simple encoding) {
  return encoding == VectorEncoding::Simple::LAZY;
}

inline bool isDictionary(VectorEncoding::Simple encoding) {
  return encoding == VectorEncoding::Simple::DICTIONARY;
}

VectorEncoding::Simple mapNameToSimple(const std::string& name);
} // namespace VectorEncoding
} // namespace bytedance::bolt
