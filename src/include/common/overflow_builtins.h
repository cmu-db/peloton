//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// overflow_builtins.h
//
// Identification: src/include/common/overflow_builtins.h
//
// Copyright (c) 2015-2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include "common/macros.h"

#include <limits>

//----------------------------------------------------------------------------//
// Fall back implementations if the gcc overflow builtins are not available.
//
// Documentation:
// https://gcc.gnu.org/onlinedocs/gcc/Integer-Overflow-Builtins.html
//----------------------------------------------------------------------------//

namespace peloton {

template <typename type_t>
static inline bool builtin_add_overflow(type_t a, type_t b, type_t *res) {
  *res = a + b;

  if (a >= 0 && b >= 0 && std::numeric_limits<type_t>::max() - a < b)
    return true;
  else if (a < 0 && b < 0 && std::numeric_limits<type_t>::min() - a > b)
    return true;

  return false;
}

template <typename type_t>
static inline bool builtin_sub_overflow(type_t a, type_t b, type_t *res) {
  *res = a - b;

  if (std::is_unsigned<type_t>::value)
    return b > a;
  else
    return ((((a ^ b)) & (*res ^ a)) & std::numeric_limits<type_t>::min()) != 0;
}

template <typename type_t>
static inline bool builtin_mul_overflow(type_t a, type_t b, type_t *res) {
  *res = a * b;

  if (a != 0 && *res / a != b) return true;

  return false;
}

#if !GCC_OVERFLOW_BUILTINS_DEFINED

template <typename type_t>
static inline bool __builtin_add_overflow(type_t a, type_t b, type_t *res) {
  return builtin_add_overflow(a, b, res);
}

template <typename type_t>
static inline bool __builtin_sub_overflow(type_t a, type_t b, type_t *res) {
  return builtin_sub_overflow(a, b, res);
}

template <typename type_t>
static inline bool __builtin_mul_overflow(type_t a, type_t b, type_t *res) {
  return builtin_mul_overflow(a, b, res);
}

#endif

}  // namespace peloton
