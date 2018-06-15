//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// math_util.h
//
// Identification: src/include/util/math_util.h
//
// Copyright (c) 2015-2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include "common/macros.h"

namespace peloton {

/**
 * Math Utility Functions
 */
class MathUtil {
 public:
  /**
   * Performs a division of two integer values and rounds up the result.
   * Calculation is made using a trick with integer division.
   */
  static constexpr ALWAYS_INLINE inline size_t DivRoundUp(size_t numerator,
                                                          size_t denominator) {
    // division must be integer division
    return (numerator + denominator - 1) / denominator;
  }
};

}  // namespace peloton
