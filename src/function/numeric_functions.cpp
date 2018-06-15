//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// numeric_functions.cpp
//
// Identification: src/function/numeric_functions.cpp
//
// Copyright (c) 2015-2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "function/numeric_functions.h"

#include "codegen/type/type.h"
#include "codegen/runtime_functions.h"
#include "type/value.h"
#include "type/value_factory.h"

namespace peloton {
namespace function {

////////////////////////////////////////////////////////////////////////////////
///
/// Square root
///
////////////////////////////////////////////////////////////////////////////////

double NumericFunctions::ISqrt(uint32_t num) {
  return std::sqrt<uint32_t>(num);
}

double NumericFunctions::DSqrt(double num) { return std::sqrt(num); }

type::Value NumericFunctions::Sqrt(const std::vector<type::Value> &args) {
  PELOTON_ASSERT(args.size() == 1);
  if (args[0].IsNull()) {
    return type::ValueFactory::GetNullValueByType(type::TypeId::DECIMAL);
  }
  return args[0].Sqrt();
}

////////////////////////////////////////////////////////////////////////////////
///
/// Absolute value
///
////////////////////////////////////////////////////////////////////////////////

double NumericFunctions::Abs(const double args) { return fabs(args); }

// Get Abs of value
type::Value NumericFunctions::_Abs(const std::vector<type::Value> &args) {
  PELOTON_ASSERT(args.size() == 1);
  if (args[0].IsNull()) {
    return type::ValueFactory::GetNullValueByType(type::TypeId::DECIMAL);
  }
  switch (args[0].GetElementType()) {
    case type::TypeId::DECIMAL: {
      double result;
      result = Abs(args[0].GetAs<double>());
      return type::ValueFactory::GetDecimalValue(result);
    }
    case type::TypeId::INTEGER: {
      int32_t result;
      result = abs(args[0].GetAs<int32_t>());
      return type::ValueFactory::GetIntegerValue(result);
    }
    case type::TypeId::BIGINT: {
      int64_t result;
      result = std::abs(args[0].GetAs<int64_t>());
      return type::ValueFactory::GetBigIntValue(result);
    }
    case type::TypeId::SMALLINT: {
      int16_t result;
      result = abs(args[0].GetAs<int16_t>());
      return type::ValueFactory::GetSmallIntValue(result);
    }
    case type::TypeId::TINYINT: {
      int8_t result;
      result = abs(args[0].GetAs<int8_t>());
      return type::ValueFactory::GetTinyIntValue(result);
    }
    default: {
      return type::ValueFactory::GetNullValueByType(type::TypeId::DECIMAL);
    }
  }
}

////////////////////////////////////////////////////////////////////////////////
///
/// Ceiling value
///
////////////////////////////////////////////////////////////////////////////////

double NumericFunctions::Ceil(const double args) { return ceil(args); }

type::Value NumericFunctions::_Ceil(const std::vector<type::Value> &args) {
  PELOTON_ASSERT(args.size() == 1);
  if (args[0].IsNull()) {
    return type::ValueFactory::GetNullValueByType(type::TypeId::DECIMAL);
  }
  double result;
  switch (args[0].GetElementType()) {
    case type::TypeId::DECIMAL:
      result = Ceil(args[0].GetAs<double>());
      break;
    case type::TypeId::INTEGER:
      result = args[0].GetAs<int32_t>();
      break;
    case type::TypeId::BIGINT:
      result = args[0].GetAs<int64_t>();
      break;
    case type::TypeId::SMALLINT:
      result = args[0].GetAs<int16_t>();
      break;
    case type::TypeId::TINYINT:
      result = args[0].GetAs<int8_t>();
      break;
    default:
      return type::ValueFactory::GetNullValueByType(type::TypeId::DECIMAL);
  }
  return type::ValueFactory::GetDecimalValue(result);
}

////////////////////////////////////////////////////////////////////////////////
///
/// Floor value
///
////////////////////////////////////////////////////////////////////////////////

double NumericFunctions::Floor(const double val) { return floor(val); }

type::Value NumericFunctions::_Floor(const std::vector<type::Value> &args) {
  PELOTON_ASSERT(args.size() == 1);
  if (args[0].IsNull()) {
    return type::ValueFactory::GetNullValueByType(type::TypeId::DECIMAL);
  }
  double res;
  switch (args[0].GetElementType()) {
    case type::TypeId::DECIMAL:
      res = Floor(args[0].GetAs<double>());
      break;
    case type::TypeId::INTEGER:
      res = args[0].GetAs<int32_t>();
      break;
    case type::TypeId::BIGINT:
      res = args[0].GetAs<int64_t>();
      break;
    case type::TypeId::SMALLINT:
      res = args[0].GetAs<int16_t>();
      break;
    case type::TypeId::TINYINT:
      res = args[0].GetAs<int8_t>();
      break;
    default:
      return type::ValueFactory::GetNullValueByType(type::TypeId::DECIMAL);
  }
  return type::ValueFactory::GetDecimalValue(res);
}

////////////////////////////////////////////////////////////////////////////////
///
/// Rounding
///
////////////////////////////////////////////////////////////////////////////////

double NumericFunctions::Round(double arg) { return round(arg); }

type::Value NumericFunctions::_Round(const std::vector<type::Value> &args) {
  PELOTON_ASSERT(args.size() == 1);
  if (args[0].IsNull()) {
    return type::ValueFactory::GetNullValueByType(type::TypeId::DECIMAL);
  }
  return type::ValueFactory::GetDecimalValue(Round(args[0].GetAs<double>()));
}

////////////////////////////////////////////////////////////////////////////////
///
/// Input functions
///
////////////////////////////////////////////////////////////////////////////////

namespace {

/**
 * Convert the provided input string into an integral number. This function
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
T ParseInteger(const char *ptr, uint32_t len) {
  static_assert(std::is_integral<T>::value,
                "Must provide integer-type when calling ParseInteger");

  const char *start = ptr;
  const char *end = start + len;

  // Trim leading whitespace
  while (start < end && *start == ' ') {
    start++;
  }

  // Check negative or positive sign
  bool negative = false;
  if (*start == '-') {
    negative = true;
    start++;
  } else if (*start == '+') {
    start++;
  }

  // Convert
  uint64_t cutoff =
      static_cast<uint64_t>(negative ? -std::numeric_limits<int64_t>::min()
                                     : std::numeric_limits<int64_t>::max());
  uint64_t cutlimit = cutoff % 10;
  cutoff /= 10;

  uint64_t num = 0;
  while (start < end) {
    if (*start < '0' || *start > '9') {
      break;
    }

    uint32_t c = static_cast<uint32_t>(*start - '0');

    if (num > cutoff || (num == cutoff && c > cutlimit)) {
      goto overflow;
    }

    num = (num * 10) + c;

    start++;
  }

  // Trim trailing whitespace
  while (start < end && *start == ' ') {
    start++;
  }

  // If we haven't consumed everything at this point, it was an invalid input
  if (start < end) {
    goto invalid;
  }

  // Negate number if we need to
  if (negative) {
    num = -num;
  }

  // Range check
  if (static_cast<int64_t>(num) <= std::numeric_limits<T>::min() ||
      static_cast<int64_t>(num) >= std::numeric_limits<T>::max()) {
    goto overflow;
  }

  // Done
  return static_cast<T>(num);

overflow:
  codegen::RuntimeFunctions::ThrowOverflowException();
  __builtin_unreachable();

invalid:
  codegen::RuntimeFunctions::ThrowInvalidInputStringException();
  __builtin_unreachable();
}

}  // namespace

bool NumericFunctions::InputBoolean(
    UNUSED_ATTRIBUTE const codegen::type::Type &type, const char *ptr,
    uint32_t len) {
  PELOTON_ASSERT(ptr != nullptr && "Input is assumed to be non-NULL");

  if (len == 0) {
    codegen::RuntimeFunctions::ThrowInvalidInputStringException();
    __builtin_unreachable();
  }

  const char *start = ptr;
  const char *end = ptr + len;

  // Trim leading whitespace
  while (start < end && *start == ' ') {
    start++;
  }

  //
  uint64_t trimmed_len = end - start;

  // Check cases
  switch (*start) {
    case 't':
    case 'T': {
      static constexpr char kTrue[] = "true";
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
  codegen::RuntimeFunctions::ThrowInvalidInputStringException();
  __builtin_unreachable();
}

int8_t NumericFunctions::InputTinyInt(
    UNUSED_ATTRIBUTE const codegen::type::Type &type, const char *ptr,
    uint32_t len) {
  PELOTON_ASSERT(ptr != nullptr && "Input is assumed to be non-NULL");
  return ParseInteger<int8_t>(ptr, len);
}

int16_t NumericFunctions::InputSmallInt(
    UNUSED_ATTRIBUTE const codegen::type::Type &type, const char *ptr,
    uint32_t len) {
  PELOTON_ASSERT(ptr != nullptr && "Input is assumed to be non-NULL");
  return ParseInteger<int16_t>(ptr, len);
}

int32_t NumericFunctions::InputInteger(
    UNUSED_ATTRIBUTE const codegen::type::Type &type, const char *ptr,
    uint32_t len) {
  PELOTON_ASSERT(ptr != nullptr && "Input is assumed to be non-NULL");
  return ParseInteger<int32_t>(ptr, len);
}

int64_t NumericFunctions::InputBigInt(
    UNUSED_ATTRIBUTE const codegen::type::Type &type, const char *ptr,
    uint32_t len) {
  PELOTON_ASSERT(ptr != nullptr && "Input is assumed to be non-NULL");
  return ParseInteger<int64_t>(ptr, len);
}

double NumericFunctions::InputDecimal(
    UNUSED_ATTRIBUTE const codegen::type::Type &type, const char *ptr,
    uint32_t len) {
  PELOTON_ASSERT(ptr != nullptr && "Input is assumed to be non-NULL");
  if (len == 0) {
    codegen::RuntimeFunctions::ThrowInvalidInputStringException();
    __builtin_unreachable();
  }

  const char *start = ptr;
  const char *end = ptr + len;

  // We don't trim because std::strtod() does the trimming for us

  // TODO(pmenon): Optimize me later
  char *consumed_ptr = nullptr;
  double ret = std::strtod(ptr, &consumed_ptr);

  if (unlikely_branch(consumed_ptr == start)) {
    if (errno == ERANGE) {
      codegen::RuntimeFunctions::ThrowOverflowException();
      __builtin_unreachable();
    } else {
      codegen::RuntimeFunctions::ThrowInvalidInputStringException();
      __builtin_unreachable();
    }
  }

  // Eat the rest
  while (consumed_ptr < end && *consumed_ptr == ' ') {
    consumed_ptr++;
  }

  // If we haven't consumed everything at this point, it was an invalid input
  if (consumed_ptr < end) {
    codegen::RuntimeFunctions::ThrowInvalidInputStringException();
    __builtin_unreachable();
  }

  // Done
  return ret;
}

}  // namespace function
}  // namespace peloton
