//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// values_runtime.cpp
//
// Identification: src/codegen/values_runtime.cpp
//
// Copyright (c) 2015-2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "codegen/values_runtime.h"

#include <type_traits>

#include "codegen/runtime_functions.h"
#include "codegen/type/type.h"
#include "type/abstract_pool.h"
#include "type/value.h"
#include "type/type_util.h"
#include "type/value_factory.h"
#include "type/value_peeker.h"

namespace peloton {
namespace codegen {

////////////////////////////////////////////////////////////////////////////////
///
/// Output functions
///
////////////////////////////////////////////////////////////////////////////////

namespace {

inline void SetValue(peloton::type::Value *val_ptr,
                     peloton::type::Value &&val) {
  new (val_ptr) peloton::type::Value(val);
}

}  // namespace

void ValuesRuntime::OutputBoolean(char *values, uint32_t idx, bool val,
                                  bool is_null) {
  auto *vals = reinterpret_cast<peloton::type::Value *>(values);
  if (is_null) {
    SetValue(&vals[idx], peloton::type::ValueFactory::GetNullValueByType(
                             peloton::type::TypeId::BOOLEAN));
  } else {
    SetValue(&vals[idx], peloton::type::ValueFactory::GetBooleanValue(val));
  }
}

void ValuesRuntime::OutputTinyInt(char *values, uint32_t idx, int8_t val) {
  auto *vals = reinterpret_cast<peloton::type::Value *>(values);
  SetValue(&vals[idx], peloton::type::ValueFactory::GetTinyIntValue(val));
}

void ValuesRuntime::OutputSmallInt(char *values, uint32_t idx, int16_t val) {
  auto *vals = reinterpret_cast<peloton::type::Value *>(values);
  SetValue(&vals[idx], peloton::type::ValueFactory::GetSmallIntValue(val));
}

void ValuesRuntime::OutputInteger(char *values, uint32_t idx, int32_t val) {
  auto *vals = reinterpret_cast<peloton::type::Value *>(values);
  SetValue(&vals[idx], peloton::type::ValueFactory::GetIntegerValue(val));
}

void ValuesRuntime::OutputBigInt(char *values, uint32_t idx, int64_t val) {
  auto *vals = reinterpret_cast<peloton::type::Value *>(values);
  SetValue(&vals[idx], peloton::type::ValueFactory::GetBigIntValue(val));
}

void ValuesRuntime::OutputDate(char *values, uint32_t idx, int32_t val) {
  auto *vals = reinterpret_cast<peloton::type::Value *>(values);
  SetValue(&vals[idx], peloton::type::ValueFactory::GetDateValue(val));
}

void ValuesRuntime::OutputTimestamp(char *values, uint32_t idx, int64_t val) {
  auto *vals = reinterpret_cast<peloton::type::Value *>(values);
  SetValue(&vals[idx], peloton::type::ValueFactory::GetTimestampValue(val));
}

void ValuesRuntime::OutputDecimal(char *values, uint32_t idx, double val) {
  auto *vals = reinterpret_cast<peloton::type::Value *>(values);
  SetValue(&vals[idx], peloton::type::ValueFactory::GetDecimalValue(val));
}

void ValuesRuntime::OutputVarchar(char *values, uint32_t idx, const char *str,
                                  uint32_t len) {
  auto *vals = reinterpret_cast<peloton::type::Value *>(values);
  SetValue(&vals[idx],
           peloton::type::ValueFactory::GetVarcharValue(str, len, false));
}

void ValuesRuntime::OutputVarbinary(char *values, uint32_t idx, const char *ptr,
                                    uint32_t len) {
  auto *vals = reinterpret_cast<peloton::type::Value *>(values);
  const auto *bin_ptr = reinterpret_cast<const unsigned char *>(ptr);
  SetValue(&vals[idx],
           peloton::type::ValueFactory::GetVarbinaryValue(bin_ptr, len, false));
}

////////////////////////////////////////////////////////////////////////////////
///
/// Input functions
///
////////////////////////////////////////////////////////////////////////////////

namespace {

/**
 * Skip all leading and trailing whitespace from the string bounded by the
 * provided pointers. This function will modify the input pointers to point to
 * the first non-space character at the start and end of the input string.
 *
 * @param[in,out] left A pointer to the leftmost character in the input string
 * @param[in,out] right A pointer to the rightmost character in the input string
 */
void TrimLeftRight(const char *&left, const char *&right) {
  while (*left == ' ') {
    left++;
  }
  while (right > left && *(right - 1) == ' ') {
    right--;
  }
}

/**
 * Convert the provided input string into a integral number. This function
 * handles leading whitespace and leading negative (-) or positive (+) signs.
 * Additionally, it performs a bounds check to ensure the number falls into the
 * valid range of numbers for the given type.
 *
 * @tparam T The integral type (int8_t, int16_t, int32_t, int64_t)
 * @param ptr A pointer to the start of the input string
 * @param len The length of the input string
 * @return The numeric interpretation of the input string
 */
template <typename T>
typename std::enable_if<std::is_integral<T>::value, T>::type ToNum(
    const char *ptr, uint32_t len) {
  if (len == 0) {
    RuntimeFunctions::ThrowInvalidInputStringException();
    __builtin_unreachable();
  }

  const char *start = ptr;
  const char *end = start + len;

  // Trim leading and trailing whitespace
  TrimLeftRight(start, end);

  // Check negative or positive sign
  bool negative = false;
  if (*start == '-') {
    negative = true;
    start++;
  } else if (*start == '+') {
    start++;
  }

  // Convert
  int64_t num = 0;
  while (start != end) {
    if (*start < '0' || *start > '9') {
      RuntimeFunctions::ThrowInvalidInputStringException();
      __builtin_unreachable();
    }

    num = (num * 10) + (*start - '0');

    start++;
  }

  // Negate number if we need to
  if (negative) {
    num = -num;
  }

  // Range check
  if (num <= std::numeric_limits<T>::min() ||
      num >= std::numeric_limits<T>::max()) {
    RuntimeFunctions::ThrowOverflowException();
    __builtin_unreachable();
  }

  // Done
  return static_cast<T>(num);
}

template <typename T>
typename std::enable_if<std::is_floating_point<T>::value, T>::type ToNum(
    const char *ptr, uint32_t len) {
  if (len == 0) {
    RuntimeFunctions::ThrowInvalidInputStringException();
    __builtin_unreachable();
  }

  // TODO(pmenon): Optimize me later
  char *end = nullptr;
  auto ret = std::strtod(ptr, &end);

  if (unlikely_branch(end == ptr)) {
    if (errno == ERANGE) {
      RuntimeFunctions::ThrowOverflowException();
      __builtin_unreachable();
    } else {
      RuntimeFunctions::ThrowInvalidInputStringException();
      __builtin_unreachable();
    }
  }

  // Done
  return static_cast<T>(ret);
}

}  // namespace

bool ValuesRuntime::InputBoolean(UNUSED_ATTRIBUTE const type::Type &type,
                                 const char *ptr, uint32_t len) {
  PELOTON_ASSERT(ptr != nullptr && "Input is assumed to be non-NULL");

  if (len == 0) {
    RuntimeFunctions::ThrowInvalidInputStringException();
    __builtin_unreachable();
  }

  const char *start = ptr, *end = ptr + len;

  // Trim leading and trailing whitespace
  TrimLeftRight(start, end);

  //
  uint64_t trimmed_len = end - start;

  // Check cases
  switch (*start) {
    case 't':
    case 'T': {
      static constexpr char kTrue[] = "true";
      std::cout << sizeof(kTrue) << std::endl;
      if (strncasecmp(start, kTrue, trimmed_len) == 0) {
        return true;
      }
      break;
    }
    case 'f':
    case 'F': {
      static constexpr char kFalse[] = "false";
      if (strncasecmp(start, kFalse, trimmed_len) == 0) {
        return false;
      }
      break;
    }
    case 'y':
    case 'Y': {
      static constexpr char kYes[] = "yes";
      if (strncasecmp(start, kYes, trimmed_len) == 0) {
        return true;
      }
      break;
    }
    case 'n':
    case 'N': {
      static constexpr char kNo[] = "no";
      if (strncasecmp(start, kNo, trimmed_len) == 0) {
        return false;
      }
      break;
    }
    case 'o':
    case 'O': {
      // 'o' not enough to distinguish between on/off
      static constexpr char kOff[] = "off";
      static constexpr char kOn[] = "on";
      if (strncasecmp(start, kOff, (trimmed_len > 3 ? trimmed_len : 3)) == 0) {
        return false;
      } else if (strncasecmp(start, kOn, (trimmed_len > 2 ? trimmed_len : 2)) ==
                 0) {
        return true;
      }
      break;
    }
    case '0': {
      if (trimmed_len == 1) {
        return false;
      } else {
        return true;
      }
    }
    case '1': {
      if (trimmed_len == 1) {
        return true;
      } else {
        return false;
      }
    }
    default: { break; }
  }

  // Error
  RuntimeFunctions::ThrowInvalidInputStringException();
  __builtin_unreachable();
}

int8_t ValuesRuntime::InputTinyInt(UNUSED_ATTRIBUTE const type::Type &type,
                                   const char *ptr, uint32_t len) {
  PELOTON_ASSERT(ptr != nullptr && "Input is assumed to be non-NULL");
  return ToNum<int8_t>(ptr, len);
}

int16_t ValuesRuntime::InputSmallInt(UNUSED_ATTRIBUTE const type::Type &type,
                                     const char *ptr, uint32_t len) {
  PELOTON_ASSERT(ptr != nullptr && "Input is assumed to be non-NULL");
  return ToNum<int16_t>(ptr, len);
}

int32_t ValuesRuntime::InputInteger(UNUSED_ATTRIBUTE const type::Type &type,
                                    const char *ptr, uint32_t len) {
  PELOTON_ASSERT(ptr != nullptr && "Input is assumed to be non-NULL");
  return ToNum<int32_t>(ptr, len);
}

int64_t ValuesRuntime::InputBigInt(UNUSED_ATTRIBUTE const type::Type &type,
                                   const char *ptr, uint32_t len) {
  PELOTON_ASSERT(ptr != nullptr && "Input is assumed to be non-NULL");
  return ToNum<int64_t>(ptr, len);
}

double ValuesRuntime::InputDecimal(UNUSED_ATTRIBUTE const type::Type &type,
                                   const char *ptr, uint32_t len) {
  PELOTON_ASSERT(ptr != nullptr && "Input is assumed to be non-NULL");
  return ToNum<double>(ptr, len);
}

////////////////////////////////////////////////////////////////////////////////
///
/// String comparison
///
////////////////////////////////////////////////////////////////////////////////

int32_t ValuesRuntime::CompareStrings(const char *str1, uint32_t len1,
                                      const char *str2, uint32_t len2) {
  return peloton::type::TypeUtil::CompareStrings(str1, len1, str2, len2);
}

void ValuesRuntime::WriteVarlen(const char *data, uint32_t len, char *buf,
                                peloton::type::AbstractPool &pool) {
  struct Varlen {
    uint32_t len;
    char data[0];
  };

  // Allocate memory for the Varlen object
  auto *area = static_cast<Varlen *>(pool.Allocate(sizeof(uint32_t) + len));

  // Populate it
  area->len = len;
  PELOTON_MEMCPY(area->data, data, len);

  // Store a pointer to the Varlen object into the target memory space
  *reinterpret_cast<Varlen **>(buf) = area;
}

}  // namespace codegen
}  // namespace peloton
