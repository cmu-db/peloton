
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

#include "type.h"

namespace peloton {
namespace type {

/**
 * Type Utility Functions
 */
class TypeUtil {
 public:
  /**
   * Perform CompareEquals directly on raw pointers.
   * Note: We assume that the left and right values are the same type.
   * We also do not check for nulls here.
   */
  static CmpBool CompareEqualsRaw(type::Type type, const char* left,
                                  const char* right) const {
    CmpBool result = CmpBool::CMP_NULL;
    switch (type) {
      case Type::BOOLEAN:
      case Type::TINYINT: {
        result = (*reinterpret_cast<const int8_t*>(left) ==
                          *reinterpret_cast<const int8_t*>(right)
                      ? CmpBool::CMP_TRUE
                      : CmpBool::CMP_FALSE);
        break;
      }
      case Type::SMALLINT: {
        result = (*reinterpret_cast<const int16_t*>(left) ==
                          *reinterpret_cast<const int16_t*>(right)
                      ? CmpBool::CMP_TRUE
                      : CmpBool::CMP_FALSE);
        break;
      }
      case Type::INTEGER: {
        result = (*reinterpret_cast<const int32_t*>(left) ==
                          *reinterpret_cast<const int32_t*>(right)
                      ? CmpBool::CMP_TRUE
                      : CmpBool::CMP_FALSE);
        break;
      }
      case Type::BIGINT: {
        result = (*reinterpret_cast<const int64_t*>(left) ==
                          *reinterpret_cast<const int64_t*>(right)
                      ? CmpBool::CMP_TRUE
                      : CmpBool::CMP_FALSE);
        break;
      }
      case Type::DECIMAL: {
        result = (*reinterpret_cast<const double>(left) ==
                          *reinterpret_cast<const double>(right)
                      ? CmpBool::CMP_TRUE
                      : CmpBool::CMP_FALSE);
        break;
      }
      case Type::TIMESTAMP:
      case Type::DATE: {
        result = (*reinterpret_cast<const uint64_t*>(left) ==
                          *reinterpret_cast<const uint64_t*>(right)
                      ? CmpBool::CMP_TRUE
                      : CmpBool::CMP_FALSE);
        break;
      }
      case Type::VARCHAR:
      case Type::VARBINARY: {
        // FIXME
        result = CmpBool::CMP_FALSE;
        break;
      }
      default: { break; }
    }  // SWITCH
    return (result);
  }

  /**
       * Perform CompareLessThan directly on raw pointers.
       * Note: We assume that the left and right values are the same type.
       * We also do not check for nulls here.
       */
  static CmpBool CompareLessThanRaw(type::Type type, const char* left,
                                    const char* right) const {
    CmpBool result = CmpBool::CMP_NULL;
    switch (type) {
      case Type::BOOLEAN:
      case Type::TINYINT: {
        result = (*reinterpret_cast<const int8_t*>(left) <
                          *reinterpret_cast<const int8_t*>(right)
                      ? CmpBool::CMP_TRUE
                      : CmpBool::CMP_FALSE);
        break;
      }
      case Type::SMALLINT: {
        result = (*reinterpret_cast<const int16_t*>(left) <
                          *reinterpret_cast<const int16_t*>(right)
                      ? CmpBool::CMP_TRUE
                      : CmpBool::CMP_FALSE);
        break;
      }
      case Type::INTEGER: {
        result = (*reinterpret_cast<const int32_t*>(left) <
                          *reinterpret_cast<const int32_t*>(right)
                      ? CmpBool::CMP_TRUE
                      : CmpBool::CMP_FALSE);
        break;
      }
      case Type::BIGINT: {
        result = (*reinterpret_cast<const int64_t*>(left) <
                          *reinterpret_cast<const int64_t*>(right)
                      ? CmpBool::CMP_TRUE
                      : CmpBool::CMP_FALSE);
        break;
      }
      case Type::DECIMAL: {
        result = (*reinterpret_cast<const double>(left) <
                          *reinterpret_cast<const double>(right)
                      ? CmpBool::CMP_TRUE
                      : CmpBool::CMP_FALSE);
        break;
      }
      case Type::TIMESTAMP:
      case Type::DATE: {
        result = (*reinterpret_cast<const uint64_t*>(left) <
                          *reinterpret_cast<const uint64_t*>(right)
                      ? CmpBool::CMP_TRUE
                      : CmpBool::CMP_FALSE);
        break;
      }
      case Type::VARCHAR:
      case Type::VARBINARY: {
        // FIXME
        result = CmpBool::CMP_FALSE;
        break;
      }
      default: { break; }
    }  // SWITCH
    return (result);
  }

  /**
     * Perform CompareGreaterThan directly on raw pointers.
     * Note: We assume that the left and right values are the same type.
     * We also do not check for nulls here.
     */
  static CmpBool CompareGreaterThanRaw(type::Type type, const char* left,
                                       const char* right) const {
    CmpBool result = CmpBool::CMP_NULL;
    switch (type) {
      case Type::BOOLEAN:
      case Type::TINYINT: {
        result = (*reinterpret_cast<const int8_t*>(left) >
                          *reinterpret_cast<const int8_t*>(right)
                      ? CmpBool::CMP_TRUE
                      : CmpBool::CMP_FALSE);
        break;
      }
      case Type::SMALLINT: {
        result = (*reinterpret_cast<const int16_t*>(left) >
                          *reinterpret_cast<const int16_t*>(right)
                      ? CmpBool::CMP_TRUE
                      : CmpBool::CMP_FALSE);
        break;
      }
      case Type::INTEGER: {
        result = (*reinterpret_cast<const int32_t*>(left) >
                          *reinterpret_cast<const int32_t*>(right)
                      ? CmpBool::CMP_TRUE
                      : CmpBool::CMP_FALSE);
        break;
      }
      case Type::BIGINT: {
        result = (*reinterpret_cast<const int64_t*>(left) >
                          *reinterpret_cast<const int64_t*>(right)
                      ? CmpBool::CMP_TRUE
                      : CmpBool::CMP_FALSE);
        break;
      }
      case Type::DECIMAL: {
        result = (*reinterpret_cast<const double>(left) >
                          *reinterpret_cast<const double>(right)
                      ? CmpBool::CMP_TRUE
                      : CmpBool::CMP_FALSE);
        break;
      }
      case Type::TIMESTAMP:
      case Type::DATE: {
        result = (*reinterpret_cast<const uint64_t*>(left) >
                          *reinterpret_cast<const uint64_t*>(right)
                      ? CmpBool::CMP_TRUE
                      : CmpBool::CMP_FALSE);
        break;
      }
      case Type::VARCHAR:
      case Type::VARBINARY: {
        // FIXME
        result = CmpBool::CMP_FALSE;
        break;
      }
      default: { break; }
    }  // SWITCH
    return (result);
  }
};
}
}
