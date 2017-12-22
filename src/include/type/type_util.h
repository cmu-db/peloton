
//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// type_util.h
//
// Identification: /peloton/src/include/type/type_util.h
//
// Copyright (c) 2015, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <string.h>

#include "common/logger.h"
#include "type/type.h"

namespace peloton {
namespace type {

/**
 * Type Utility Functions
 */
class TypeUtil {
 public:
  /**
   * Use memcmp to evaluate two strings
   * This does not work with VARBINARY attributes.
   */
  static inline int CompareStrings(const char* str1, int len1, const char* str2,
                                   int len2) {
    PL_ASSERT(str1 != nullptr);
    PL_ASSERT(len1 >= 0);
    PL_ASSERT(str2 != nullptr);
    PL_ASSERT(len2 >= 0);
    // PAVLO: 2017-04-04
    // The reason why we use memcmp here is that our inputs are
    // not null-terminated strings, so we can't use strncmp
    int ret = memcmp(str1, str2, std::min(len1, len2));
    if (ret == 0 && len1 != len2) {
      ret = len1 - len2;
    }
    return ret;
  }

  /**
   * Perform CompareEquals directly on raw pointers.
   * We assume that the left and right values are the same type.
   * We also do not check for nulls here.
   *
   * <B>WARNING:</B> This is dangerous AF. Use at your own risk!!
   */
  static CmpBool CompareEqualsRaw(type::Type type, const char* left,
                                  const char* right, bool inlined) {
    CmpBool result = CmpBool::NULL_;
    switch (type.GetTypeId()) {
      case TypeId::BOOLEAN:
      case TypeId::TINYINT: {
        result = (*reinterpret_cast<const int8_t*>(left) ==
                          *reinterpret_cast<const int8_t*>(right)
                      ? CmpBool::TRUE
                      : CmpBool::FALSE);
        break;
      }
      case TypeId::SMALLINT: {
        result = (*reinterpret_cast<const int16_t*>(left) ==
                          *reinterpret_cast<const int16_t*>(right)
                      ? CmpBool::TRUE
                      : CmpBool::FALSE);
        break;
      }
      case TypeId::INTEGER: {
        result = (*reinterpret_cast<const int32_t*>(left) ==
                          *reinterpret_cast<const int32_t*>(right)
                      ? CmpBool::TRUE
                      : CmpBool::FALSE);
        break;
      }
      case TypeId::BIGINT: {
        result = (*reinterpret_cast<const int64_t*>(left) ==
                          *reinterpret_cast<const int64_t*>(right)
                      ? CmpBool::TRUE
                      : CmpBool::FALSE);
        break;
      }
      case TypeId::DECIMAL: {
        result = (*reinterpret_cast<const double*>(left) ==
                          *reinterpret_cast<const double*>(right)
                      ? CmpBool::TRUE
                      : CmpBool::FALSE);
        break;
      }
      case TypeId::TIMESTAMP:
      case TypeId::DATE: {
        result = (*reinterpret_cast<const uint64_t*>(left) ==
                          *reinterpret_cast<const uint64_t*>(right)
                      ? CmpBool::TRUE
                      : CmpBool::FALSE);
        break;
      }
      case TypeId::VARCHAR:
      case TypeId::VARBINARY: {
        const char* leftPtr = left;
        const char* rightPtr = right;
        if (inlined == false) {
          leftPtr = *reinterpret_cast<const char* const*>(left);
          rightPtr = *reinterpret_cast<const char* const*>(right);
        }
        if (leftPtr == nullptr || rightPtr == nullptr) {
          result = CmpBool::FALSE;
          break;
        }
        uint32_t leftLen = *reinterpret_cast<const uint32_t*>(leftPtr);
        uint32_t rightLen = *reinterpret_cast<const uint32_t*>(rightPtr);
        result = GetCmpBool(TypeUtil::CompareStrings(
                                leftPtr + sizeof(uint32_t), leftLen,
                                rightPtr + sizeof(uint32_t), rightLen) == 0);
        break;
      }
      default: { break; }
    }  // SWITCH
    return (result);
  }

  /**
   * Perform CompareLessThan directly on raw pointers.
   * We assume that the left and right values are the same type.
   * We also do not check for nulls here.
   *
   * <B>WARNING:</B> This is dangerous AF. Use at your own risk!!
   */
  static CmpBool CompareLessThanRaw(const type::Type type, const char* left,
                                    const char* right, bool inlined) {
    CmpBool result = CmpBool::NULL_;
    switch (type.GetTypeId()) {
      case TypeId::BOOLEAN:
      case TypeId::TINYINT: {
        result = (*reinterpret_cast<const int8_t*>(left) <
                          *reinterpret_cast<const int8_t*>(right)
                      ? CmpBool::TRUE
                      : CmpBool::FALSE);
        break;
      }
      case TypeId::SMALLINT: {
        result = (*reinterpret_cast<const int16_t*>(left) <
                          *reinterpret_cast<const int16_t*>(right)
                      ? CmpBool::TRUE
                      : CmpBool::FALSE);
        break;
      }
      case TypeId::INTEGER: {
        result = (*reinterpret_cast<const int32_t*>(left) <
                          *reinterpret_cast<const int32_t*>(right)
                      ? CmpBool::TRUE
                      : CmpBool::FALSE);
        break;
      }
      case TypeId::BIGINT: {
        result = (*reinterpret_cast<const int64_t*>(left) <
                          *reinterpret_cast<const int64_t*>(right)
                      ? CmpBool::TRUE
                      : CmpBool::FALSE);
        break;
      }
      case TypeId::DECIMAL: {
        result = (*reinterpret_cast<const double*>(left) <
                          *reinterpret_cast<const double*>(right)
                      ? CmpBool::TRUE
                      : CmpBool::FALSE);
        break;
      }
      case TypeId::TIMESTAMP:
      case TypeId::DATE: {
        result = (*reinterpret_cast<const uint64_t*>(left) <
                          *reinterpret_cast<const uint64_t*>(right)
                      ? CmpBool::TRUE
                      : CmpBool::FALSE);
        break;
      }
      case TypeId::VARCHAR:
      case TypeId::VARBINARY: {
        const char* leftPtr = left;
        const char* rightPtr = right;
        if (inlined == false) {
          leftPtr = *reinterpret_cast<const char* const*>(left);
          rightPtr = *reinterpret_cast<const char* const*>(right);
        }
        if (leftPtr == nullptr || rightPtr == nullptr) {
          result = CmpBool::FALSE;
          break;
        }
        uint32_t leftLen = *reinterpret_cast<const uint32_t*>(leftPtr);
        uint32_t rightLen = *reinterpret_cast<const uint32_t*>(rightPtr);
        result = GetCmpBool(TypeUtil::CompareStrings(
                                leftPtr + sizeof(uint32_t), leftLen,
                                rightPtr + sizeof(uint32_t), rightLen) < 0);
        break;
      }
      default: { break; }
    }  // SWITCH
    return (result);
  }

  /**
   * Perform CompareGreaterThan directly on raw pointers.
   * We assume that the left and right values are the same type.
   * We also do not check for nulls here.
   *
   * <B>WARNING:</B> This is dangerous AF. Use at your own risk!!
   */
  static CmpBool CompareGreaterThanRaw(const type::Type type, const char* left,
                                       const char* right, bool inlined) {
    CmpBool result = CmpBool::NULL_;
    switch (type.GetTypeId()) {
      case TypeId::BOOLEAN:
      case TypeId::TINYINT: {
        result = (*reinterpret_cast<const int8_t*>(left) >
                          *reinterpret_cast<const int8_t*>(right)
                      ? CmpBool::TRUE
                      : CmpBool::FALSE);
        break;
      }
      case TypeId::SMALLINT: {
        result = (*reinterpret_cast<const int16_t*>(left) >
                          *reinterpret_cast<const int16_t*>(right)
                      ? CmpBool::TRUE
                      : CmpBool::FALSE);
        break;
      }
      case TypeId::INTEGER: {
        result = (*reinterpret_cast<const int32_t*>(left) >
                          *reinterpret_cast<const int32_t*>(right)
                      ? CmpBool::TRUE
                      : CmpBool::FALSE);
        break;
      }
      case TypeId::BIGINT: {
        result = (*reinterpret_cast<const int64_t*>(left) >
                          *reinterpret_cast<const int64_t*>(right)
                      ? CmpBool::TRUE
                      : CmpBool::FALSE);
        break;
      }
      case TypeId::DECIMAL: {
        result = (*reinterpret_cast<const double*>(left) >
                          *reinterpret_cast<const double*>(right)
                      ? CmpBool::TRUE
                      : CmpBool::FALSE);
        break;
      }
      case TypeId::TIMESTAMP:
      case TypeId::DATE: {
        result = (*reinterpret_cast<const uint64_t*>(left) >
                          *reinterpret_cast<const uint64_t*>(right)
                      ? CmpBool::TRUE
                      : CmpBool::FALSE);
        break;
      }
      case TypeId::VARCHAR:
      case TypeId::VARBINARY: {
        const char* leftPtr = left;
        const char* rightPtr = right;
        if (inlined == false) {
          leftPtr = *reinterpret_cast<const char* const*>(left);
          rightPtr = *reinterpret_cast<const char* const*>(right);
        }
        if (leftPtr == nullptr || rightPtr == nullptr) {
          result = CmpBool::FALSE;
          break;
        }
        uint32_t leftLen = *reinterpret_cast<const uint32_t*>(leftPtr);
        uint32_t rightLen = *reinterpret_cast<const uint32_t*>(rightPtr);
        result = GetCmpBool(TypeUtil::CompareStrings(
                                leftPtr + sizeof(uint32_t), leftLen,
                                rightPtr + sizeof(uint32_t), rightLen) > 0);
        break;
      }
      default: { break; }
    }  // SWITCH
    return (result);
  }
};
}
}
