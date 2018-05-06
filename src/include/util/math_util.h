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
  template <typename type_t>
  static ALWAYS_INLINE inline type_t DivRoundUp(type_t numerator,
                                                type_t denominator) {
    static_assert(std::is_integral<type_t>::value,
                  "type of DivRoundUp must be integral type");

    // division must be integer division
    return (numerator + denominator - 1) / denominator;
  }
};

}  // namespace peloton
