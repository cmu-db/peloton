//===----------------------------------------------------------------------===//
//
//                         PelotonDB
//
// value.cpp
//
// Identification: src/backend/common/value.cpp
//
// Copyright (c) 2015, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "backend/common/value.h"

#include <cstdio>
#include <cstring>
#include <sstream>
#include <iostream>

namespace peloton {

// For x <op> y where x is an integer,
// promote x and y to s_intPromotionTable[y]
ValueType Value::IntPromotionTable[] = {
    VALUE_TYPE_INVALID,  // 0 invalid
    VALUE_TYPE_NULL,     // 1 null
    VALUE_TYPE_INVALID,  // 2 <unused>
    VALUE_TYPE_BIGINT,   // 3 tinyint
    VALUE_TYPE_BIGINT,   // 4 smallint
    VALUE_TYPE_BIGINT,   // 5 integer
    VALUE_TYPE_BIGINT,   // 6 bigint
    VALUE_TYPE_INVALID,  // 7 <unused>
    VALUE_TYPE_DOUBLE,   // 8 double
    VALUE_TYPE_INVALID,  // 9 varchar
    VALUE_TYPE_INVALID,  // 10 <unused>
    VALUE_TYPE_BIGINT,   // 11 timestamp

    // 12 - 21 unused
    VALUE_TYPE_INVALID, VALUE_TYPE_INVALID, VALUE_TYPE_INVALID,
    VALUE_TYPE_INVALID, VALUE_TYPE_INVALID, VALUE_TYPE_INVALID,
    VALUE_TYPE_INVALID, VALUE_TYPE_INVALID, VALUE_TYPE_INVALID,
    VALUE_TYPE_INVALID,

    VALUE_TYPE_DECIMAL,  // 22 decimal
    VALUE_TYPE_INVALID,  // 23 boolean
    VALUE_TYPE_INVALID,  // 24 address
};

// For x <op> y where x is a double
// promote x and y to s_doublePromotionTable[y]
ValueType Value::DoublePromotionTable[] = {
    VALUE_TYPE_INVALID,  // 0 invalid
    VALUE_TYPE_NULL,     // 1 null
    VALUE_TYPE_INVALID,  // 2 <unused>
    VALUE_TYPE_DOUBLE,   // 3 tinyint
    VALUE_TYPE_DOUBLE,   // 4 smallint
    VALUE_TYPE_DOUBLE,   // 5 integer
    VALUE_TYPE_DOUBLE,   // 6 bigint
    VALUE_TYPE_INVALID,  // 7 <unused>
    VALUE_TYPE_DOUBLE,   // 8 double
    VALUE_TYPE_INVALID,  // 9 varchar
    VALUE_TYPE_INVALID,  // 10 <unused>
    VALUE_TYPE_DOUBLE,   // 11 timestamp

    // 12 - 21 unused.
    VALUE_TYPE_INVALID, VALUE_TYPE_INVALID, VALUE_TYPE_INVALID,
    VALUE_TYPE_INVALID, VALUE_TYPE_INVALID, VALUE_TYPE_INVALID,
    VALUE_TYPE_INVALID, VALUE_TYPE_INVALID, VALUE_TYPE_INVALID,
    VALUE_TYPE_INVALID,

    VALUE_TYPE_INVALID,  // 22 decimal  (todo)
    VALUE_TYPE_INVALID,  // 23 boolean
    VALUE_TYPE_INVALID,  // 24 address
};

// for x <op> y where x is a decimal
// promote x and y to s_decimalPromotionTable[y]
ValueType Value::DecimalPromotionTable[] = {
    VALUE_TYPE_INVALID,  // 0 invalid
    VALUE_TYPE_NULL,     // 1 null
    VALUE_TYPE_INVALID,  // 2 <unused>
    VALUE_TYPE_DECIMAL,  // 3 tinyint
    VALUE_TYPE_DECIMAL,  // 4 smallint
    VALUE_TYPE_DECIMAL,  // 5 integer
    VALUE_TYPE_DECIMAL,  // 6 bigint
    VALUE_TYPE_INVALID,  // 7 <unused>
    VALUE_TYPE_INVALID,  // 8 double (todo)
    VALUE_TYPE_INVALID,  // 9 varchar
    VALUE_TYPE_INVALID,  // 10 <unused>
    VALUE_TYPE_DECIMAL,  // 11 timestamp

    // 12 - 21 unused. ick.
    VALUE_TYPE_INVALID, VALUE_TYPE_INVALID, VALUE_TYPE_INVALID,
    VALUE_TYPE_INVALID, VALUE_TYPE_INVALID, VALUE_TYPE_INVALID,
    VALUE_TYPE_INVALID, VALUE_TYPE_INVALID, VALUE_TYPE_INVALID,
    VALUE_TYPE_INVALID,

    VALUE_TYPE_DECIMAL,  // 22 decimal
    VALUE_TYPE_INVALID,  // 23 boolean
    VALUE_TYPE_INVALID,  // 24 address
};

TTInt Value::MaxDecimal(
    "9999999999"  // 10 digits
    "9999999999"  // 20 digits
    "9999999999"  // 30 digits
    "99999999");  // 38 digits

TTInt Value::MinDecimal(
    "-9999999999"  // 10 digits
    "9999999999"   // 20 digits
    "9999999999"   // 30 digits
    "99999999");   // 38 digits

//===--------------------------------------------------------------------===//
// Type Cast and Getters
//===--------------------------------------------------------------------===//

int64_t Value::CastAsBigIntAndGetValue() const {
  const ValueType type = GetValueType();
  if (IsNull()) {
    return INT64_NULL;
  }

  switch (type) {
    case VALUE_TYPE_NULL:
      return INT64_NULL;
    case VALUE_TYPE_TINYINT:
      return static_cast<int64_t>(GetTinyInt());
    case VALUE_TYPE_SMALLINT:
      return static_cast<int64_t>(GetSmallInt());
    case VALUE_TYPE_INTEGER:
      return static_cast<int64_t>(GetInteger());
    case VALUE_TYPE_ADDRESS:
      return GetBigInt();
    case VALUE_TYPE_BIGINT:
      return GetBigInt();
    case VALUE_TYPE_TIMESTAMP:
      return GetTimestamp();
    case VALUE_TYPE_DOUBLE:
      if (GetDouble() > (double)INT64_MAX || GetDouble() < (double)INT64_MIN) {
        throw ValueOutOfRangeException(GetDouble(), VALUE_TYPE_DOUBLE,
                                       VALUE_TYPE_BIGINT);
      }
      return static_cast<int64_t>(GetDouble());
    default:
      throw CastException(type, VALUE_TYPE_BIGINT);
      return 0;  // NOT REACHED
  }
}

int64_t Value::CastAsRawInt64AndGetValue() const {
  const ValueType type = GetValueType();

  switch (type) {
    case VALUE_TYPE_TINYINT:
      return static_cast<int64_t>(GetTinyInt());
    case VALUE_TYPE_SMALLINT:
      return static_cast<int64_t>(GetSmallInt());
    case VALUE_TYPE_INTEGER:
      return static_cast<int64_t>(GetInteger());
    case VALUE_TYPE_BIGINT:
      return GetBigInt();
    case VALUE_TYPE_TIMESTAMP:
      return GetTimestamp();
    default:
      throw CastException(type, VALUE_TYPE_BIGINT);
      return 0;  // NOT REACHED
  }
}

double Value::CastAsDoubleAndGetValue() const {
  const ValueType type = GetValueType();
  if (IsNull()) {
    return DOUBLE_MIN;
  }

  switch (type) {
    case VALUE_TYPE_NULL:
      return DOUBLE_MIN;
    case VALUE_TYPE_TINYINT:
      return static_cast<double>(GetTinyInt());
    case VALUE_TYPE_SMALLINT:
      return static_cast<double>(GetSmallInt());
    case VALUE_TYPE_INTEGER:
      return static_cast<double>(GetInteger());
    case VALUE_TYPE_ADDRESS:
      return static_cast<double>(GetBigInt());
    case VALUE_TYPE_BIGINT:
      return static_cast<double>(GetBigInt());
    case VALUE_TYPE_TIMESTAMP:
      return static_cast<double>(GetTimestamp());
    case VALUE_TYPE_DOUBLE:
      return GetDouble();
    case VALUE_TYPE_VARCHAR:
    case VALUE_TYPE_VARBINARY:
    case VALUE_TYPE_DECIMAL:
    default:
      throw CastException(type, VALUE_TYPE_DOUBLE);
      return 0;  // NOT REACHED
  }
}

TTInt Value::CastAsDecimalAndGetValue() const {
  const ValueType type = GetValueType();
  if (IsNull()) {
    TTInt retval;
    retval.SetMin();
    return retval;
  }

  switch (type) {
    case VALUE_TYPE_TINYINT:
    case VALUE_TYPE_SMALLINT:
    case VALUE_TYPE_INTEGER:
    case VALUE_TYPE_BIGINT:
    case VALUE_TYPE_TIMESTAMP: {
      int64_t value = CastAsBigIntAndGetValue();
      TTInt retval(value);
      retval *= Value::max_decimal_scale_factor;
      return retval;
    }
    case VALUE_TYPE_DECIMAL:
      return GetDecimal();
    case VALUE_TYPE_VARCHAR:
    case VALUE_TYPE_VARBINARY:
    default:
      throw CastException(type, VALUE_TYPE_DOUBLE);
      return 0;  // NOT REACHED
  }
}

Value Value::CastAsBigInt() const {
  Value retval(VALUE_TYPE_BIGINT);
  const ValueType type = GetValueType();
  if (IsNull()) {
    retval.SetNull();
    return retval;
  }
  switch (type) {
    case VALUE_TYPE_TINYINT:
      retval.GetBigInt() = static_cast<int64_t>(GetTinyInt());
      break;
    case VALUE_TYPE_SMALLINT:
      retval.GetBigInt() = static_cast<int64_t>(GetSmallInt());
      break;
    case VALUE_TYPE_INTEGER:
      retval.GetBigInt() = static_cast<int64_t>(GetInteger());
      break;
    case VALUE_TYPE_ADDRESS:
      retval.GetBigInt() = GetBigInt();
      break;
    case VALUE_TYPE_BIGINT:
      return *this;
    case VALUE_TYPE_TIMESTAMP:
      retval.GetBigInt() = GetTimestamp();
      break;
    case VALUE_TYPE_DOUBLE:
      if (GetDouble() > (double)INT64_MAX || GetDouble() < (double)INT64_MIN) {
        throw ValueOutOfRangeException(GetDouble(), VALUE_TYPE_DOUBLE,
                                       VALUE_TYPE_BIGINT);
      }
      retval.GetBigInt() = static_cast<int64_t>(GetDouble());
      break;
    case VALUE_TYPE_VARCHAR:
    case VALUE_TYPE_VARBINARY:
    case VALUE_TYPE_DECIMAL:
    default:
      throw CastException(type, VALUE_TYPE_BIGINT);
  }
  return retval;
}

Value Value::CastAsTimestamp() const {
  Value retval(VALUE_TYPE_TIMESTAMP);
  const ValueType type = GetValueType();
  if (IsNull()) {
    retval.SetNull();
    return retval;
  }
  switch (type) {
    case VALUE_TYPE_TINYINT:
      retval.GetTimestamp() = static_cast<int64_t>(GetTinyInt());
      break;
    case VALUE_TYPE_SMALLINT:
      retval.GetTimestamp() = static_cast<int64_t>(GetSmallInt());
      break;
    case VALUE_TYPE_INTEGER:
      retval.GetTimestamp() = static_cast<int64_t>(GetInteger());
      break;
    case VALUE_TYPE_BIGINT:
      retval.GetTimestamp() = GetBigInt();
      break;
    case VALUE_TYPE_TIMESTAMP:
      retval.GetTimestamp() = GetTimestamp();
      break;
    case VALUE_TYPE_DOUBLE:
      if (GetDouble() > (double)INT64_MAX || GetDouble() < (double)INT64_MIN) {
        throw ValueOutOfRangeException(GetDouble(), VALUE_TYPE_DOUBLE,
                                       VALUE_TYPE_BIGINT);
      }
      retval.GetTimestamp() = static_cast<int64_t>(GetDouble());
      break;
    case VALUE_TYPE_VARCHAR:
    case VALUE_TYPE_VARBINARY:
    case VALUE_TYPE_DECIMAL:
    default:
      throw CastException(type, VALUE_TYPE_TIMESTAMP);
  }
  return retval;
}

Value Value::CastAsInteger() const {
  Value retval(VALUE_TYPE_INTEGER);
  const ValueType type = GetValueType();
  if (IsNull()) {
    retval.SetNull();
    return retval;
  }
  switch (type) {
    case VALUE_TYPE_TINYINT:
      retval.GetInteger() = static_cast<int32_t>(GetTinyInt());
      break;
    case VALUE_TYPE_SMALLINT:
      retval.GetInteger() = static_cast<int32_t>(GetSmallInt());
      break;
    case VALUE_TYPE_INTEGER:
      return *this;
    case VALUE_TYPE_BIGINT:
      if (GetBigInt() > INT32_MAX || GetBigInt() < INT32_MIN) {
        throw ValueOutOfRangeException(GetBigInt(), VALUE_TYPE_BIGINT,
                                       VALUE_TYPE_INTEGER);
      }
      retval.GetInteger() = static_cast<int32_t>(GetBigInt());
      break;
    case VALUE_TYPE_TIMESTAMP:
      if (GetTimestamp() > INT32_MAX || GetTimestamp() < INT32_MIN) {
        throw ValueOutOfRangeException(GetTimestamp(), VALUE_TYPE_TIMESTAMP,
                                       VALUE_TYPE_INTEGER);
      }
      retval.GetInteger() = static_cast<int32_t>(GetTimestamp());
      break;
    case VALUE_TYPE_DOUBLE:
      if (GetDouble() > (double)INT32_MAX || GetDouble() < (double)INT32_MIN) {
        throw ValueOutOfRangeException(GetDouble(), VALUE_TYPE_DOUBLE,
                                       VALUE_TYPE_INTEGER);
      }
      retval.GetInteger() = static_cast<int32_t>(GetDouble());
      break;
    case VALUE_TYPE_VARCHAR:
    case VALUE_TYPE_VARBINARY:
    case VALUE_TYPE_DECIMAL:
    default:
      throw CastException(type, VALUE_TYPE_INTEGER);
  }
  return retval;
}

Value Value::CastAsSmallInt() const {
  Value retval(VALUE_TYPE_SMALLINT);
  const ValueType type = GetValueType();
  if (IsNull()) {
    retval.SetNull();
    return retval;
  }
  switch (type) {
    case VALUE_TYPE_TINYINT:
      retval.GetSmallInt() = static_cast<int16_t>(GetTinyInt());
      break;
    case VALUE_TYPE_SMALLINT:
      retval.GetSmallInt() = GetSmallInt();
      break;
    case VALUE_TYPE_INTEGER:
      if (GetInteger() > INT16_MAX || GetInteger() < INT16_MIN) {
        throw ValueOutOfRangeException((int64_t)GetInteger(),
                                       VALUE_TYPE_INTEGER, VALUE_TYPE_SMALLINT);
      }
      retval.GetSmallInt() = static_cast<int16_t>(GetInteger());
      break;
    case VALUE_TYPE_BIGINT:
      if (GetBigInt() > INT16_MAX || GetBigInt() < INT16_MIN) {
        throw ValueOutOfRangeException(GetBigInt(), VALUE_TYPE_BIGINT,
                                       VALUE_TYPE_SMALLINT);
      }
      retval.GetSmallInt() = static_cast<int16_t>(GetBigInt());
      break;
    case VALUE_TYPE_TIMESTAMP:
      if (GetTimestamp() > INT16_MAX || GetTimestamp() < INT16_MIN) {
        throw ValueOutOfRangeException(GetTimestamp(), VALUE_TYPE_BIGINT,
                                       VALUE_TYPE_SMALLINT);
      }
      retval.GetSmallInt() = static_cast<int16_t>(GetTimestamp());
      break;
    case VALUE_TYPE_DOUBLE:
      if (GetDouble() > (double)INT16_MAX || GetDouble() < (double)INT16_MIN) {
        throw ValueOutOfRangeException(GetDouble(), VALUE_TYPE_DOUBLE,
                                       VALUE_TYPE_SMALLINT);
      }
      retval.GetSmallInt() = static_cast<int16_t>(GetDouble());
      break;
    case VALUE_TYPE_VARCHAR:
    case VALUE_TYPE_VARBINARY:
    case VALUE_TYPE_DECIMAL:
    default:
      throw CastException(type, VALUE_TYPE_SMALLINT);
  }
  return retval;
}

Value Value::CastAsTinyInt() const {
  Value retval(VALUE_TYPE_TINYINT);
  const ValueType type = GetValueType();
  if (IsNull()) {
    retval.SetNull();
    return retval;
  }
  switch (type) {
    case VALUE_TYPE_TINYINT:
      retval.GetTinyInt() = GetTinyInt();
      break;
    case VALUE_TYPE_SMALLINT:
      if (GetSmallInt() > INT8_MAX || GetSmallInt() < INT8_MIN) {
        throw ValueOutOfRangeException((int64_t)GetSmallInt(),
                                       VALUE_TYPE_SMALLINT, VALUE_TYPE_TINYINT);
      }
      retval.GetTinyInt() = static_cast<int8_t>(GetSmallInt());
      break;
    case VALUE_TYPE_INTEGER:
      if (GetInteger() > INT8_MAX || GetInteger() < INT8_MIN) {
        throw ValueOutOfRangeException((int64_t)GetInteger(),
                                       VALUE_TYPE_INTEGER, VALUE_TYPE_TINYINT);
      }
      retval.GetTinyInt() = static_cast<int8_t>(GetInteger());
      break;
    case VALUE_TYPE_BIGINT:
      if (GetBigInt() > INT8_MAX || GetBigInt() < INT8_MIN) {
        throw ValueOutOfRangeException(GetBigInt(), VALUE_TYPE_BIGINT,
                                       VALUE_TYPE_TINYINT);
      }
      retval.GetTinyInt() = static_cast<int8_t>(GetBigInt());
      break;
    case VALUE_TYPE_TIMESTAMP:
      if (GetTimestamp() > INT8_MAX || GetTimestamp() < INT8_MIN) {
        throw ValueOutOfRangeException(GetTimestamp(), VALUE_TYPE_TIMESTAMP,
                                       VALUE_TYPE_TINYINT);
      }
      retval.GetTinyInt() = static_cast<int8_t>(GetTimestamp());
      break;
    case VALUE_TYPE_DOUBLE:
      if (GetDouble() > (double)INT8_MAX || GetDouble() < (double)INT8_MIN) {
        throw ValueOutOfRangeException(GetDouble(), VALUE_TYPE_DOUBLE,
                                       VALUE_TYPE_TINYINT);
      }
      retval.GetTinyInt() = static_cast<int8_t>(GetDouble());
      break;
    case VALUE_TYPE_VARCHAR:
    case VALUE_TYPE_VARBINARY:
    case VALUE_TYPE_DECIMAL:
    default:
      throw CastException(type, VALUE_TYPE_TINYINT);
  }
  return retval;
}

Value Value::CastAsDouble() const {
  Value retval(VALUE_TYPE_DOUBLE);
  const ValueType type = GetValueType();
  if (IsNull()) {
    retval.SetNull();
    return retval;
  }
  switch (type) {
    case VALUE_TYPE_TINYINT:
      retval.GetDouble() = static_cast<double>(GetTinyInt());
      break;
    case VALUE_TYPE_SMALLINT:
      retval.GetDouble() = static_cast<double>(GetSmallInt());
      break;
    case VALUE_TYPE_INTEGER:
      retval.GetDouble() = static_cast<double>(GetInteger());
      break;
    case VALUE_TYPE_BIGINT:
      retval.GetDouble() = static_cast<double>(GetBigInt());
      break;
    case VALUE_TYPE_TIMESTAMP:
      retval.GetDouble() = static_cast<double>(GetTimestamp());
      break;
    case VALUE_TYPE_DOUBLE:
      retval.GetDouble() = GetDouble();
      break;
    case VALUE_TYPE_DECIMAL:
      retval.GetDouble() = std::stod(GetDecimal().ToString());
      break;
    case VALUE_TYPE_VARCHAR:
    case VALUE_TYPE_VARBINARY:
    default:
      throw CastException(type, VALUE_TYPE_DOUBLE);
  }
  return retval;
}

Value Value::CastAsString() const {
  Value retval(VALUE_TYPE_VARCHAR);
  const ValueType type = GetValueType();
  if (IsNull()) {
    retval.SetNull();
    return retval;
  }
  // note: we allow binary conversion to strings to support
  // byte[] as string parameters...
  // In the future, it would be nice to check this is a decent string here...
  switch (type) {
    case VALUE_TYPE_VARCHAR:
    case VALUE_TYPE_VARBINARY:
      memcpy(retval.value_data, value_data, sizeof(value_data));
      break;
    default:
      throw CastException(type, VALUE_TYPE_VARCHAR);
  }
  return retval;
}

Value Value::CastAsBinary() const {
  Value retval(VALUE_TYPE_VARBINARY);
  const ValueType type = GetValueType();
  if (IsNull()) {
    retval.SetNull();
    return retval;
  }
  switch (type) {
    case VALUE_TYPE_VARBINARY:
      memcpy(retval.value_data, value_data, sizeof(value_data));
      break;
    default:
      throw CastException(type, VALUE_TYPE_VARBINARY);
  }
  return retval;
}

Value Value::CastAsDecimal() const {
  Value retval(VALUE_TYPE_DECIMAL);
  const ValueType type = GetValueType();
  if (IsNull()) {
    retval.SetNull();
    return retval;
  }
  switch (type) {
    case VALUE_TYPE_TINYINT:
    case VALUE_TYPE_SMALLINT:
    case VALUE_TYPE_INTEGER:
    case VALUE_TYPE_BIGINT: {
      int64_t rhsint = CastAsBigIntAndGetValue();
      TTInt retval(rhsint);
      retval *= Value::max_decimal_scale_factor;
      return GetDecimalValue(retval);
    }
    case VALUE_TYPE_DOUBLE: {
      // FIXME: Cast from a double to decimal,
      // for now, we can only cast it into an Int and then to a Decimal
      // but we lost precision here,
      int64_t rhsint = CastAsBigIntAndGetValue();
      TTInt retval(rhsint);
      retval *= Value::max_decimal_scale_factor;
      return GetDecimalValue(retval);
    }
    case VALUE_TYPE_DECIMAL:
      ::memcpy(retval.value_data, value_data, sizeof(TTInt));
      break;
    default:
      throw CastException(type, VALUE_TYPE_DECIMAL);
  }
  return retval;
}
Value Value::CastAs(ValueType type) const {
  if (GetValueType() == type) {
    return *this;
  }

  switch (type) {
    case VALUE_TYPE_TINYINT:
      return CastAsTinyInt();
    case VALUE_TYPE_SMALLINT:
      return CastAsSmallInt();
    case VALUE_TYPE_INTEGER:
      return CastAsInteger();
    case VALUE_TYPE_BIGINT:
      return CastAsBigInt();
    case VALUE_TYPE_TIMESTAMP:
      return CastAsTimestamp();
    case VALUE_TYPE_DOUBLE:
      return CastAsDouble();
    case VALUE_TYPE_VARCHAR:
      return CastAsString();
    case VALUE_TYPE_VARBINARY:
      return CastAsBinary();
    case VALUE_TYPE_DECIMAL:
      return CastAsDecimal();
    default:
      char message[128];
      snprintf(message, 128, "Type %d not a recognized type for Casting",
               (int)type);

      throw TypeMismatchException(message, GetValueType(), type);
  }
}

//===--------------------------------------------------------------------===//
// Type Comparison
//===--------------------------------------------------------------------===//

int Value::CompareAnyIntegerValue(const Value rhs) const {
  int64_t lhsValue, rhsValue;

  // Get the right hand side as a bigint
  if (rhs.GetValueType() != VALUE_TYPE_BIGINT)
    rhsValue = rhs.CastAsBigIntAndGetValue();
  else
    rhsValue = rhs.GetBigInt();

  // convert the left hand side
  switch (GetValueType()) {
    case VALUE_TYPE_TINYINT:
    case VALUE_TYPE_SMALLINT:
    case VALUE_TYPE_INTEGER:
    case VALUE_TYPE_TIMESTAMP:
      lhsValue = CastAsBigIntAndGetValue();
      break;
    case VALUE_TYPE_BIGINT:
      lhsValue = GetBigInt();
      break;
    default: {
      throw TypeMismatchException("non comparable types lhs '%d' rhs '%d'",
                                  GetValueType(), rhs.GetValueType());
    }
  }

  // do the comparison
  if (lhsValue == rhsValue) {
    return VALUE_COMPARE_EQUAL;
  } else if (lhsValue > rhsValue) {
    return VALUE_COMPARE_GREATERTHAN;
  } else {
    return VALUE_COMPARE_LESSTHAN;
  }
}

int Value::CompareDoubleValue(const Value rhs) const {
  switch (rhs.GetValueType()) {
    case VALUE_TYPE_DOUBLE: {
      const double lhsValue = GetDouble();
      const double rhsValue = rhs.GetDouble();
      if (lhsValue == rhsValue) {
        return VALUE_COMPARE_EQUAL;
      } else if (lhsValue > rhsValue) {
        return VALUE_COMPARE_GREATERTHAN;
      } else {
        return VALUE_COMPARE_LESSTHAN;
      }
    }
    case VALUE_TYPE_TINYINT:
    case VALUE_TYPE_SMALLINT:
    case VALUE_TYPE_INTEGER:
    case VALUE_TYPE_BIGINT:
    case VALUE_TYPE_TIMESTAMP: {
      const double lhsValue = GetDouble();
      const double rhsValue = rhs.CastAsDouble().GetDouble();
      if (lhsValue == rhsValue) {
        return VALUE_COMPARE_EQUAL;
      } else if (lhsValue > rhsValue) {
        return VALUE_COMPARE_GREATERTHAN;
      } else {
        return VALUE_COMPARE_LESSTHAN;
      }
    }
    case VALUE_TYPE_DECIMAL: {
      double val = rhs.CastAsDoubleAndGetValue();
      if (rhs.IsNegative()) {
        val *= -1;
      }
      return ((GetDouble() > val) - (GetDouble() < val));
    }
    default: {
      char message[128];
      snprintf(message, 128, "Type %s cannot be Cast for comparison to type %s",
               ValueTypeToString(rhs.GetValueType()).c_str(),
               ValueTypeToString(GetValueType()).c_str());
      throw Exception(message);
      // Not reached
      return 0;
    }
  }
}

int Value::CompareStringValue(const Value rhs) const {
  if ((rhs.GetValueType() != VALUE_TYPE_VARCHAR) &&
      (rhs.GetValueType() != VALUE_TYPE_VARBINARY)) {
    char message[128];
    snprintf(message, 128, "Type %s cannot be Cast for comparison to type %s",
             ValueTypeToString(rhs.GetValueType()).c_str(),
             ValueTypeToString(GetValueType()).c_str());
    throw Exception(message);
  }
  const char *left = reinterpret_cast<const char *>(GetObjectValue());
  const char *right = reinterpret_cast<const char *>(rhs.GetObjectValue());
  if (IsNull()) {
    if (rhs.IsNull()) {
      return VALUE_COMPARE_EQUAL;
    } else {
      return VALUE_COMPARE_LESSTHAN;
    }
  } else if (rhs.IsNull()) {
    return VALUE_COMPARE_GREATERTHAN;
  }
  const int32_t leftLength = GetObjectLength();
  const int32_t rightLength = rhs.GetObjectLength();
  const int result = ::strncmp(left, right, std::min(leftLength, rightLength));
  if (result == 0 && leftLength != rightLength) {
    if (leftLength > rightLength) {
      return VALUE_COMPARE_GREATERTHAN;
    } else {
      return VALUE_COMPARE_LESSTHAN;
    }
  } else if (result > 0) {
    return VALUE_COMPARE_GREATERTHAN;
  } else if (result < 0) {
    return VALUE_COMPARE_LESSTHAN;
  }

  return VALUE_COMPARE_EQUAL;
}

int Value::CompareBinaryValue(const Value rhs) const {
  if (rhs.GetValueType() != VALUE_TYPE_VARBINARY) {
    char message[128];
    snprintf(message, 128, "Type %s cannot be Cast for comparison to type %s",
             ValueTypeToString(rhs.GetValueType()).c_str(),
             ValueTypeToString(GetValueType()).c_str());
    throw Exception(message);
  }
  const char *left = reinterpret_cast<const char *>(GetObjectValue());
  const char *right = reinterpret_cast<const char *>(rhs.GetObjectValue());
  if (IsNull()) {
    if (rhs.IsNull()) {
      return VALUE_COMPARE_EQUAL;
    } else {
      return VALUE_COMPARE_LESSTHAN;
    }
  } else if (rhs.IsNull()) {
    return VALUE_COMPARE_GREATERTHAN;
  }
  const int32_t leftLength = GetObjectLength();
  const int32_t rightLength = rhs.GetObjectLength();
  const int result = ::memcmp(left, right, std::min(leftLength, rightLength));
  if (result == 0 && leftLength != rightLength) {
    if (leftLength > rightLength) {
      return VALUE_COMPARE_GREATERTHAN;
    } else {
      return VALUE_COMPARE_LESSTHAN;
    }
  } else if (result > 0) {
    return VALUE_COMPARE_GREATERTHAN;
  } else if (result < 0) {
    return VALUE_COMPARE_LESSTHAN;
  }

  return VALUE_COMPARE_EQUAL;
}

int Value::CompareDecimalValue(const Value rhs) const {
  switch (rhs.GetValueType()) {
    // create the equivalent decimal value
    case VALUE_TYPE_TINYINT:
    case VALUE_TYPE_SMALLINT:
    case VALUE_TYPE_INTEGER:
    case VALUE_TYPE_BIGINT: {
      const TTInt lhsValue = GetDecimal();
      const TTInt rhsValue = rhs.CastAsDecimalAndGetValue();

      if (lhsValue == rhsValue) {
        return VALUE_COMPARE_EQUAL;
      } else if (lhsValue > rhsValue) {
        return VALUE_COMPARE_GREATERTHAN;
      } else {
        return VALUE_COMPARE_LESSTHAN;
      }
    }
    case VALUE_TYPE_DECIMAL: {
      const TTInt lhsValue = GetDecimal();
      const TTInt rhsValue = rhs.GetDecimal();

      if (lhsValue == rhsValue) {
        return VALUE_COMPARE_EQUAL;
      } else if (lhsValue > rhsValue) {
        return VALUE_COMPARE_GREATERTHAN;
      } else {
        return VALUE_COMPARE_LESSTHAN;
      }
    }
    case VALUE_TYPE_DOUBLE: {
      const double lhsValue = CastAsDoubleAndGetValue();
      const double rhsValue = rhs.GetDouble();

      if (lhsValue == rhsValue) {
        return VALUE_COMPARE_EQUAL;
      } else if (lhsValue > rhsValue) {
        return VALUE_COMPARE_GREATERTHAN;
      } else {
        return VALUE_COMPARE_LESSTHAN;
      }
    }
    default: {
      char message[128];
      snprintf(message, 128, "Type %s cannot be Cast for comparison to type %s",
               ValueTypeToString(rhs.GetValueType()).c_str(),
               ValueTypeToString(GetValueType()).c_str());
      throw TypeMismatchException(message, GetValueType(), rhs.GetValueType());
      // Not reached
      return 0;
    }
  }
}

//===--------------------------------------------------------------------===//
// Type operators
//===--------------------------------------------------------------------===//

Value Value::OpEquals(const Value rhs) const {
  return Compare(rhs) == 0 ? GetTrue() : GetFalse();
}

Value Value::OpNotEquals(const Value rhs) const {
  return Compare(rhs) != 0 ? GetTrue() : GetFalse();
}

Value Value::OpLessThan(const Value rhs) const {
  return Compare(rhs) < 0 ? GetTrue() : GetFalse();
}

Value Value::OpLessThanOrEqual(const Value rhs) const {
  return Compare(rhs) <= 0 ? GetTrue() : GetFalse();
}

Value Value::OpGreaterThan(const Value rhs) const {
  return Compare(rhs) > 0 ? GetTrue() : GetFalse();
}

Value Value::OpGreaterThanOrEqual(const Value rhs) const {
  return Compare(rhs) >= 0 ? GetTrue() : GetFalse();
}

Value Value::OpMax(const Value rhs) const {
  const int value = Compare(rhs);
  if (value > 0) {
    return *this;
  } else {
    return rhs;
  }
}

Value Value::OpMin(const Value rhs) const {
  const int value = Compare(rhs);
  if (value < 0) {
    return *this;
  } else {
    return rhs;
  }
}

Value Value::GetNullValue(ValueType type) {
  Value retval(type);
  retval.SetNull();
  return retval;
}

bool Value::IsZero() const {
  const ValueType type = GetValueType();
  switch (type) {
    case VALUE_TYPE_TINYINT:
      return GetTinyInt() == 0;
    case VALUE_TYPE_SMALLINT:
      return GetSmallInt() == 0;
    case VALUE_TYPE_INTEGER:
      return GetInteger() == 0;
    case VALUE_TYPE_BIGINT:
    case VALUE_TYPE_TIMESTAMP:
      return GetBigInt() == 0;
    case VALUE_TYPE_DECIMAL:
      return GetDecimal().IsZero();
    default:
      throw IncompatibleTypeException(
          (int)type, "type %d is not a numeric type that implements isZero()");
  }
}

void Value::HashCombine(std::size_t &seed) const {
  const ValueType type = GetValueType();
  switch (type) {
    case VALUE_TYPE_TINYINT:
      boost::hash_combine(seed, GetTinyInt());
      break;
    case VALUE_TYPE_SMALLINT:
      boost::hash_combine(seed, GetSmallInt());
      break;
    case VALUE_TYPE_INTEGER:
      boost::hash_combine(seed, GetInteger());
      break;
    case VALUE_TYPE_BIGINT:
    case VALUE_TYPE_TIMESTAMP:
      boost::hash_combine(seed, GetBigInt());
      break;
    case VALUE_TYPE_DOUBLE:
      boost::hash_combine(seed, GetDouble());
      break;
    case VALUE_TYPE_VARCHAR: {
      if (GetObjectValue() == NULL) {
        boost::hash_combine(seed, std::string(""));
      } else {
        const int32_t length = GetObjectLength();
        boost::hash_combine(
            seed, std::string(reinterpret_cast<const char *>(GetObjectValue()),
                              length));
      }
      break;
    }
    case VALUE_TYPE_VARBINARY: {
      if (GetObjectValue() == NULL) {
        boost::hash_combine(seed, std::string(""));
      } else {
        const int32_t length = GetObjectLength();
        char *data = reinterpret_cast<char *>(GetObjectValue());
        for (int32_t i = 0; i < length; i++) boost::hash_combine(seed, data[i]);
      }
      break;
    }
    case VALUE_TYPE_DECIMAL:
      GetDecimal().hash(seed);
      break;
    default:
      throw UnknownTypeException((int)type, "unknown type %d");
  }
}

Value Value::OpIncrement() const {
  const ValueType type = GetValueType();
  Value retval(type);
  switch (type) {
    case VALUE_TYPE_TINYINT:
      if (GetTinyInt() == INT8_MAX) {
        throw NumericValueOutOfRangeException(
            "Incrementing this TinyInt results in a value out of range");
      }
      retval.GetTinyInt() = static_cast<int8_t>(GetTinyInt() + 1);
      break;
    case VALUE_TYPE_SMALLINT:
      if (GetSmallInt() == INT16_MAX) {
        throw NumericValueOutOfRangeException(
            "Incrementing this SmallInt results in a value out of range");
      }
      retval.GetSmallInt() = static_cast<int16_t>(GetSmallInt() + 1);
      break;
    case VALUE_TYPE_INTEGER:
      if (GetInteger() == INT32_MAX) {
        throw NumericValueOutOfRangeException(
            "Incrementing this Integer results in a value out of range");
      }
      retval.GetInteger() = GetInteger() + 1;
      break;
    case VALUE_TYPE_BIGINT:
    case VALUE_TYPE_TIMESTAMP:
      if (GetBigInt() == INT64_MAX) {
        throw NumericValueOutOfRangeException(
            "Incrementing this BigInt/Timestamp results in a value out of "
            "range");
      }
      retval.GetBigInt() = GetBigInt() + 1;
      break;
    case VALUE_TYPE_DOUBLE:
      retval.GetDouble() = GetDouble() + 1;
      break;
    default:
      throw IncompatibleTypeException((int)type,
                                      "type %d is not incrementable");
      break;
  }
  return retval;
}

Value Value::OpDecrement() const {
  const ValueType type = GetValueType();
  Value retval(type);
  switch (type) {
    case VALUE_TYPE_TINYINT:
      if (GetTinyInt() == PELOTON_INT8_MIN) {
        throw NumericValueOutOfRangeException(
            "Decrementing this TinyInt results in a value out of range");
      }
      retval.GetTinyInt() = static_cast<int8_t>(GetTinyInt() - 1);
      break;
    case VALUE_TYPE_SMALLINT:
      if (GetSmallInt() == PELOTON_INT16_MIN) {
        throw NumericValueOutOfRangeException(
            "Decrementing this SmallInt results in a value out of range");
      }
      retval.GetSmallInt() = static_cast<int16_t>(GetSmallInt() - 1);
      break;
    case VALUE_TYPE_INTEGER:
      if (GetInteger() == PELOTON_INT32_MIN) {
        throw NumericValueOutOfRangeException(
            "Decrementing this Integer results in a value out of range");
      }
      retval.GetInteger() = GetInteger() - 1;
      break;
    case VALUE_TYPE_BIGINT:
    case VALUE_TYPE_TIMESTAMP:
      if (GetBigInt() == PELOTON_INT64_MIN) {
        throw NumericValueOutOfRangeException(
            "Decrementing this BigInt/Timestamp results in a value out of "
            "range");
      }
      retval.GetBigInt() = GetBigInt() - 1;
      break;
    case VALUE_TYPE_DOUBLE:
      retval.GetDouble() = GetDouble() - 1;
      break;
    default:
      throw IncompatibleTypeException((int)type,
                                      "type %d is not decrementable");
      break;
  }
  return retval;
}

Value Value::OpSubtract(const Value rhs) const {
  ValueType vt = PromoteForOp(GetValueType(), rhs.GetValueType());
  switch (vt) {
    case VALUE_TYPE_TINYINT:
    case VALUE_TYPE_SMALLINT:
    case VALUE_TYPE_INTEGER:
    case VALUE_TYPE_BIGINT:
    case VALUE_TYPE_TIMESTAMP:
      return OpSubtractBigInts(CastAsBigIntAndGetValue(),
                               rhs.CastAsBigIntAndGetValue());

    case VALUE_TYPE_DOUBLE:
      return OpSubtractDoubles(CastAsDoubleAndGetValue(),
                               rhs.CastAsDoubleAndGetValue());

    case VALUE_TYPE_DECIMAL:
      return OpSubtractDecimals(CastAsDecimal(), rhs.CastAsDecimal());

    default:
      break;
  }
  throw TypeMismatchException("Promotion of %s and %s failed in Opsubtract.",
                              GetValueType(), rhs.GetValueType());
}

Value Value::OpAdd(const Value rhs) const {
  ValueType vt = PromoteForOp(GetValueType(), rhs.GetValueType());
  switch (vt) {
    case VALUE_TYPE_TINYINT:
    case VALUE_TYPE_SMALLINT:
    case VALUE_TYPE_INTEGER:
    case VALUE_TYPE_BIGINT:
    case VALUE_TYPE_TIMESTAMP:
      return OpAddBigInts(CastAsBigIntAndGetValue(),
                          rhs.CastAsBigIntAndGetValue());

    case VALUE_TYPE_DOUBLE:
      return OpAddDoubles(CastAsDoubleAndGetValue(),
                          rhs.CastAsDoubleAndGetValue());

    case VALUE_TYPE_DECIMAL:
      return OpAddDecimals(CastAsDecimal(), rhs.CastAsDecimal());

    default:
      break;
  }
  throw TypeMismatchException("Promotion of %s and %s failed in Opadd.",
                              GetValueType(), rhs.GetValueType());
}

Value Value::OpMultiply(const Value rhs) const {
  ValueType vt = PromoteForOp(GetValueType(), rhs.GetValueType());
  switch (vt) {
    case VALUE_TYPE_TINYINT:
    case VALUE_TYPE_SMALLINT:
    case VALUE_TYPE_INTEGER:
    case VALUE_TYPE_BIGINT:
    case VALUE_TYPE_TIMESTAMP:
      return OpMultiplyBigInts(CastAsBigIntAndGetValue(),
                               rhs.CastAsBigIntAndGetValue());

    case VALUE_TYPE_DOUBLE:
      return OpMultiplyDoubles(CastAsDoubleAndGetValue(),
                               rhs.CastAsDoubleAndGetValue());

    case VALUE_TYPE_DECIMAL:
      return OpMultiplyDecimals(*this, rhs);

    default:
      break;
  }
  throw TypeMismatchException("Promotion of %s and %s failed in Opmultiply.",
                              GetValueType(), rhs.GetValueType());
}

Value Value::OpDivide(const Value rhs) const {
  ValueType vt = PromoteForOp(GetValueType(), rhs.GetValueType());
  switch (vt) {
    case VALUE_TYPE_TINYINT:
    case VALUE_TYPE_SMALLINT:
    case VALUE_TYPE_INTEGER:
    case VALUE_TYPE_BIGINT:
    case VALUE_TYPE_TIMESTAMP:
      return OpDivideBigInts(CastAsBigIntAndGetValue(),
                             rhs.CastAsBigIntAndGetValue());

    case VALUE_TYPE_DOUBLE:
      return OpDivideDoubles(CastAsDoubleAndGetValue(),
                             rhs.CastAsDoubleAndGetValue());

    case VALUE_TYPE_DECIMAL:
      return OpDivideDecimals(CastAsDecimal(), rhs.CastAsDecimal());

    default:
      break;
  }

  throw TypeMismatchException("Promotion of %s and %s failed in Opdivide.",
                              GetValueType(), rhs.GetValueType());
}

Value Value::OpAddBigInts(const int64_t lhs, const int64_t rhs) const {
  if (lhs == INT64_NULL || rhs == INT64_NULL) return GetBigIntValue(INT64_NULL);
  // Scary overflow check
  if (((lhs ^ rhs) |
       (((lhs ^ (~(lhs ^ rhs) & (1L << (sizeof(int64_t) * CHAR_BIT - 1)))) +
         rhs) ^
        rhs)) >= 0) {
    char message[4096];
    ::snprintf(message, 4096, "Adding %jd and %jd will overflow BigInt storage",
               (intmax_t)lhs, (intmax_t)rhs);
    throw NumericValueOutOfRangeException(message);
  }
  return GetBigIntValue(lhs + rhs);
}

Value Value::OpSubtractBigInts(const int64_t lhs, const int64_t rhs) const {
  if (lhs == INT64_NULL || rhs == INT64_NULL) return GetBigIntValue(INT64_NULL);
  // Scary overflow check
  if (((lhs ^ rhs) &
       (((lhs ^ ((lhs ^ rhs) & (1L << (sizeof(int64_t) * CHAR_BIT - 1)))) -
         rhs) ^
        rhs)) < 0) {
    char message[4096];
    ::snprintf(message, 4096,
               "Subtracting %jd from %jd will overflow BigInt storage",
               (intmax_t)lhs, (intmax_t)rhs);
    throw NumericValueOutOfRangeException(message);
  }
  return GetBigIntValue(lhs - rhs);
}

Value Value::OpMultiplyBigInts(const int64_t lhs, const int64_t rhs) const {
  if (lhs == INT64_NULL || rhs == INT64_NULL) return GetBigIntValue(INT64_NULL);
  bool overflow = false;
  // Scary overflow check
  if (lhs > 0) {   /* lhs is positive */
    if (rhs > 0) { /* lhs and rhs are positive */
      if (lhs > (INT64_MAX / rhs)) {
        overflow = true;
      }
    }      /* end if lhs and rhs are positive */
    else { /* lhs positive, rhs non-positive */
      if (rhs < (INT64_MIN / lhs)) {
        overflow = true;
      }
    }              /* lhs positive, rhs non-positive */
  }                /* end if lhs is positive */
  else {           /* lhs is non-positive */
    if (rhs > 0) { /* lhs is non-positive, rhs is positive */
      if (lhs < (INT64_MIN / rhs)) {
        overflow = true;
      }
    }      /* end if lhs is non-positive, rhs is positive */
    else { /* lhs and rhs are non-positive */
      if ((lhs != 0) && (rhs < (INT64_MAX / lhs))) {
        overflow = true;
      }
    } /* end if lhs and rhs non-positive */
  }   /* end if lhs is non-positive */

  const int64_t result = lhs * rhs;

  if (result == INT64_NULL) {
    overflow = true;
  }

  if (overflow) {
    char message[4096];
    ::snprintf(message, 4096,
               "Multiplying %jd with %jd will overflow BigInt storage",
               (intmax_t)lhs, (intmax_t)rhs);
    throw NumericValueOutOfRangeException(message);
  }

  return GetBigIntValue(result);
}

Value Value::OpDivideBigInts(const int64_t lhs, const int64_t rhs) const {
  if (lhs == INT64_NULL || rhs == INT64_NULL) return GetBigIntValue(INT64_NULL);

  if (rhs == 0) {
    char message[4096];
    ::snprintf(message, 4096, "Attempted to divide %jd by 0", (intmax_t)lhs);
    throw DivideByZeroException(message);
  }

  /**
   * Because the smallest int64 value is used to represent null (and this is
   * checked for an handled above)
   * it isn't necessary to check for any kind of overflow since none is
   * possible.
   */
  return GetBigIntValue(int64_t(lhs / rhs));
}

Value Value::OpAddDoubles(const double lhs, const double rhs) const {
  if (lhs <= DOUBLE_NULL || rhs <= DOUBLE_NULL)
    return GetDoubleValue(DOUBLE_MIN);

  const double result = lhs + rhs;

  if (CHECK_FPE(result)) {
    char message[4096];
    ::snprintf(message, 4096,
               "Attempted to add %f with %f caused overflow/underflow or some "
               "other error. Result was %f",
               lhs, rhs, result);
    throw NumericValueOutOfRangeException(message);
  }
  return GetDoubleValue(result);
}

Value Value::OpSubtractDoubles(const double lhs, const double rhs) const {
  if (lhs <= DOUBLE_NULL || rhs <= DOUBLE_NULL)
    return GetDoubleValue(DOUBLE_MIN);

  const double result = lhs - rhs;

  if (CHECK_FPE(result)) {
    char message[4096];
    ::snprintf(message, 4096,
               "Attempted to subtract %f by %f caused overflow/underflow or "
               "some other error. Result was %f",
               lhs, rhs, result);
    throw NumericValueOutOfRangeException(message);
  }
  return GetDoubleValue(result);
}

Value Value::OpMultiplyDoubles(const double lhs, const double rhs) const {
  if (lhs <= DOUBLE_NULL || rhs <= DOUBLE_NULL)
    return GetDoubleValue(DOUBLE_MIN);

  const double result = lhs * rhs;

  if (CHECK_FPE(result)) {
    char message[4096];
    ::snprintf(message, 4096,
               "Attempted to multiply %f by %f caused overflow/underflow or "
               "some other error. Result was %f",
               lhs, rhs, result);
    throw NumericValueOutOfRangeException(message);
  }
  return GetDoubleValue(result);
}

Value Value::OpDivideDoubles(const double lhs, const double rhs) const {
  if (lhs <= DOUBLE_NULL || rhs <= DOUBLE_NULL)
    return GetDoubleValue(DOUBLE_MIN);

  const double result = lhs / rhs;

  if (CHECK_FPE(result)) {
    char message[4096];
    ::snprintf(message, 4096,
               "Attempted to divide %f by %f caused overflow/underflow or some "
               "other error. Result was %f",
               lhs, rhs, result);
    throw NumericValueOutOfRangeException(message);
  }
  return GetDoubleValue(result);
}

Value Value::OpAddDecimals(const Value lhs, const Value rhs) const {
  if ((lhs.GetValueType() != VALUE_TYPE_DECIMAL) ||
      (rhs.GetValueType() != VALUE_TYPE_DECIMAL)) {
    throw Exception("Non-decimal Value in decimal adder.");
  }

  if (lhs.IsNull() || rhs.IsNull()) {
    TTInt retval;
    retval.SetMin();
    return GetDecimalValue(retval);
  }

  TTInt retval(lhs.GetDecimal());
  if (retval.Add(rhs.GetDecimal()) || retval > Value::MaxDecimal ||
      retval < MinDecimal) {
    char message[4096];
    ::snprintf(message, 4096,
               "Attempted to add %s with %s causing overflow/underflow",
               lhs.CreateStringFromDecimal().c_str(),
               rhs.CreateStringFromDecimal().c_str());
    throw NumericValueOutOfRangeException(message);
  }

  return GetDecimalValue(retval);
}

Value Value::OpSubtractDecimals(const Value lhs, const Value rhs) const {
  if ((lhs.GetValueType() != VALUE_TYPE_DECIMAL) ||
      (rhs.GetValueType() != VALUE_TYPE_DECIMAL)) {
    throw Exception("Non-decimal Value in decimal subtract.");
  }

  if (lhs.IsNull() || rhs.IsNull()) {
    TTInt retval;
    retval.SetMin();
    return GetDecimalValue(retval);
  }

  TTInt retval(lhs.GetDecimal());
  if (retval.Sub(rhs.GetDecimal()) || retval > Value::MaxDecimal ||
      retval < Value::MinDecimal) {
    char message[4096];
    ::snprintf(message, 4096,
               "Attempted to subtract %s from %s causing overflow/underflow",
               rhs.CreateStringFromDecimal().c_str(),
               lhs.CreateStringFromDecimal().c_str());
    throw NumericValueOutOfRangeException(message);
  }

  return GetDecimalValue(retval);
}

// Serialize sign and value using radix point (no exponent).
std::string Value::CreateStringFromDecimal() const {
  assert(!IsNull());
  std::ostringstream buffer;
  TTInt scaledValue = GetDecimal();
  if (scaledValue.IsSign()) {
    buffer << '-';
  }
  TTInt whole(scaledValue);
  TTInt fractional(scaledValue);
  whole /= Value::max_decimal_scale_factor;
  fractional %= Value::max_decimal_scale_factor;
  if (whole.IsSign()) {
    whole.ChangeSign();
  }
  buffer << whole.ToString(10);
  buffer << '.';
  if (fractional.IsSign()) {
    fractional.ChangeSign();
  }
  std::string fractionalString = fractional.ToString(10);
  for (int ii = static_cast<int>(fractionalString.size());
       ii < Value::max_decimal_scale; ii++) {
    buffer << '0';
  }
  buffer << fractionalString;
  return buffer.str();
}

// Set a decimal value from a serialized representation
void Value::CreateDecimalFromString(const std::string &txt) {
  if (txt.length() == 0) {
    throw DecimalException("Empty string provided");
  }
  bool setSign = false;
  if (txt[0] == '-') {
    setSign = true;
  }

  // Check for invalid characters
  for (int ii = (setSign ? 1 : 0); ii < static_cast<int>(txt.size()); ii++) {
    if ((txt[ii] < '0' || txt[ii] > '9') && txt[ii] != '.') {
      char message[4096];
      snprintf(message, 4096, "Invalid characters in decimal string: %s",
               txt.c_str());
      throw DecimalException(message);
    }
  }

  std::size_t separatorPos = txt.find('.', 0);
  if (separatorPos == std::string::npos) {
    const std::string wholeString = txt.substr(setSign ? 1 : 0, txt.size());
    const std::size_t wholeStringSize = wholeString.size();
    if (wholeStringSize > 26) {
      throw DecimalException(
          "Maximum precision exceeded. "
          "Maximum of 26 digits to the left of the decimal point");
    }
    TTInt whole(wholeString);
    if (setSign) {
      whole.SetSign();
    }
    whole *= max_decimal_scale_factor;
    GetDecimal() = whole;
    return;
  }

  if (txt.find('.', separatorPos + 1) != std::string::npos) {
    throw DecimalException("Too many decimal points");
  }

  const std::string wholeString =
      txt.substr(setSign ? 1 : 0, separatorPos - (setSign ? 1 : 0));
  const std::size_t wholeStringSize = wholeString.size();
  if (wholeStringSize > 26) {
    throw DecimalException(
        "Maximum precision exceeded. "
        "Maximum of 26 digits to the left of the decimal point");
  }
  TTInt whole(wholeString);
  std::string fractionalString =
      txt.substr(separatorPos + 1, txt.size() - (separatorPos + 1));
  if (fractionalString.size() > 12) {
    throw DecimalException(
        "Maximum scale exceeded. "
        "Maximum of 12 digits to the right of the decimal point");
  }
  while (fractionalString.size() < Value::max_decimal_scale) {
    fractionalString.push_back('0');
  }
  TTInt fractional(fractionalString);

  whole *= max_decimal_scale_factor;
  whole += fractional;

  if (setSign) {
    whole.SetSign();
  }

  assert(sizeof(TTInt) == sizeof(value_data));

  GetDecimal() = whole;
}

/**
 * Avoid scaling both sides if possible. E.g, don't turn dec * 2 into
 * (dec * 2*kMaxScale*E-12). Then the result of simple multiplication
 * is a*b*E-24 and have to further multiply to get back to the assumed
 * E-12, which can overflow unnecessarily at the middle step.
 */
Value Value::OpMultiplyDecimals(const Value &lhs, const Value &rhs) const {
  if ((lhs.GetValueType() != VALUE_TYPE_DECIMAL) &&
      (rhs.GetValueType() != VALUE_TYPE_DECIMAL)) {
    throw DecimalException("No decimal Value in decimal multiply.");
  }

  if (lhs.IsNull() || rhs.IsNull()) {
    TTInt retval;
    retval.SetMin();
    return GetDecimalValue(retval);
  }

  if ((lhs.GetValueType() == VALUE_TYPE_DECIMAL) &&
      (rhs.GetValueType() == VALUE_TYPE_DECIMAL)) {
    TTLInt calc;
    calc.FromInt(lhs.GetDecimal());
    calc *= rhs.GetDecimal();
    calc /= Value::max_decimal_scale_factor;
    TTInt retval;
    if (retval.FromInt(calc) || retval > Value::MaxDecimal ||
        retval < MinDecimal) {
      char message[4096];
      snprintf(message, 4096,
               "Attempted to multiply %s by %s causing overflow/underflow. "
               "Unscaled result was %s",
               lhs.CreateStringFromDecimal().c_str(),
               rhs.CreateStringFromDecimal().c_str(),
               calc.ToString(10).c_str());
    }
    return GetDecimalValue(retval);
  } else if (lhs.GetValueType() != VALUE_TYPE_DECIMAL) {
    TTLInt calc;
    calc.FromInt(rhs.GetDecimal());
    calc *= lhs.CastAsDecimalAndGetValue();
    calc /= Value::max_decimal_scale_factor;
    TTInt retval;
    retval.FromInt(calc);
    if (retval.FromInt(calc) || retval > Value::MaxDecimal ||
        retval < MinDecimal) {
      char message[4096];
      snprintf(message, 4096,
               "Attempted to multiply %s by %s causing overflow/underflow. "
               "Unscaled result was %s",
               lhs.CreateStringFromDecimal().c_str(),
               rhs.CreateStringFromDecimal().c_str(),
               calc.ToString(10).c_str());
      throw DecimalException(message);
    }
    return GetDecimalValue(retval);
  } else {
    TTLInt calc;
    calc.FromInt(lhs.GetDecimal());
    calc *= rhs.CastAsDecimalAndGetValue();
    calc /= Value::max_decimal_scale_factor;
    TTInt retval;
    retval.FromInt(calc);
    if (retval.FromInt(calc) || retval > Value::MaxDecimal ||
        retval < MinDecimal) {
      char message[4096];
      snprintf(message, 4096,
               "Attempted to multiply %s by %s causing overflow/underflow. "
               "Unscaled result was %s",
               lhs.CreateStringFromDecimal().c_str(),
               rhs.CreateStringFromDecimal().c_str(),
               calc.ToString(10).c_str());
      throw DecimalException(message);
    }
    return GetDecimalValue(retval);
  }
}

/*
 * Divide two decimals and return a correctly scaled decimal.
 * A little cumbersome. Better algorithms welcome.
 *   (1) calculate the quotient and the remainder.
 *   (2) temporarily scale the remainder to 19 digits
 *   (3) divide out remainder to calculate digits after the radix point.
 *   (4) scale remainder to 12 digits (that's the default scale)
 *   (5) scale the quotient back to 19,12.
 *   (6) sum the scaled quotient and remainder.
 *   (7) construct the final decimal.
 */
Value Value::OpDivideDecimals(const Value lhs, const Value rhs) const {
  if ((lhs.GetValueType() != VALUE_TYPE_DECIMAL) ||
      (rhs.GetValueType() != VALUE_TYPE_DECIMAL)) {
    throw DecimalException("Non-decimal Value in decimal subtract.");
  }

  if (lhs.IsNull() || rhs.IsNull()) {
    TTInt retval;
    retval.SetMin();
    return GetDecimalValue(retval);
  }

  TTLInt calc;
  calc.FromInt(lhs.GetDecimal());
  calc *= Value::max_decimal_scale_factor;
  if (calc.Div(rhs.GetDecimal())) {
    char message[4096];
    snprintf(message, 4096,
             "Attempted to divide %s by %s causing overflow/underflow (or "
             "divide by zero)",
             lhs.CreateStringFromDecimal().c_str(),
             rhs.CreateStringFromDecimal().c_str());
    throw DecimalException(message);
  }
  TTInt retval;
  if (retval.FromInt(calc) || retval > Value::MaxDecimal ||
      retval < MinDecimal) {
    char message[4096];
    snprintf(
        message, 4096,
        "Attempted to divide %s by %s causing overflow. Unscaled result was %s",
        lhs.CreateStringFromDecimal().c_str(),
        rhs.CreateStringFromDecimal().c_str(), calc.ToString(10).c_str());
    throw DecimalException(message);
  }
  return GetDecimalValue(retval);
}

Value Value::GetMinValue(ValueType type) {
  switch (type) {
    case (VALUE_TYPE_TINYINT):
      return GetTinyIntValue(PELOTON_INT8_MIN);
      break;
    case (VALUE_TYPE_SMALLINT):
      return GetSmallIntValue(PELOTON_INT16_MIN);
      break;
    case (VALUE_TYPE_INTEGER):
      return GetIntegerValue(PELOTON_INT32_MIN);
      break;
      break;
    case (VALUE_TYPE_BIGINT):
      return GetBigIntValue(PELOTON_INT64_MIN);
      break;
    case (VALUE_TYPE_DOUBLE):
      return GetDoubleValue(-DBL_MAX);
      break;
    case (VALUE_TYPE_VARCHAR):
      return GetStringValue("", nullptr);
      break;
      break;
    case (VALUE_TYPE_VARBINARY):
      return GetBinaryValue("", nullptr);
      break;
    case (VALUE_TYPE_TIMESTAMP):
      return GetTimestampValue(PELOTON_INT64_MIN);
      break;
    case (VALUE_TYPE_DECIMAL):
      return GetDecimalValue(DECIMAL_MIN);
      break;
    case (VALUE_TYPE_BOOLEAN):
      return GetFalse();
      break;

    case (VALUE_TYPE_INVALID):
    case (VALUE_TYPE_NULL):
    case (VALUE_TYPE_ADDRESS):
    default: {
      throw UnknownTypeException((int)type, "Can't get min value for type");
    }
  }
}

//===--------------------------------------------------------------------===//
// Misc functions
//===--------------------------------------------------------------------===//

// Get a string representation of this value
std::ostream &operator<<(std::ostream &os, const Value &value) {
  os << value.GetInfo();
  return os;
}

// Return a string full of arcane and wonder.
std::string Value::GetInfo() const {
  const ValueType type = GetValueType();
  std::stringstream os;

  if (IsNull()) {
    os << "<NULL>";
    return os.str();
  }

  std::string out_val;
  const char *ptr;
  int64_t addr;

  os << GetTypeName(type) << "::";
  switch (type) {
    case VALUE_TYPE_TINYINT:
      os << static_cast<int32_t>(GetTinyInt());
      break;
    case VALUE_TYPE_SMALLINT:
      os << GetSmallInt();
      break;
    case VALUE_TYPE_INTEGER:
      os << GetInteger();
      break;
    case VALUE_TYPE_BIGINT:
    case VALUE_TYPE_TIMESTAMP:
      os << GetBigInt();
      break;
    case VALUE_TYPE_DOUBLE:
      os << GetDouble();
      break;
    case VALUE_TYPE_VARCHAR:
      ptr = reinterpret_cast<const char *>(GetObjectValue());
      // addr = reinterpret_cast<int64_t>(ptr);
      out_val = std::string(ptr, GetObjectLength());
      os << "[" << GetObjectLength() << "]";
      os << "\"" << out_val << "\"";
      // os << "\"" << out_val << "\"[@" << addr << "]";
      break;
    case VALUE_TYPE_VARBINARY:
      ptr = reinterpret_cast<const char *>(GetObjectValue());
      addr = reinterpret_cast<int64_t>(ptr);
      out_val = std::string(ptr, GetObjectLength());
      os << "[" << GetObjectLength() << "]";
      os << "-bin[@" << addr << "]";
      break;
    case VALUE_TYPE_DECIMAL:
      os << CreateStringFromDecimal();
      break;
    default:
      throw UnknownTypeException((int)type, "unknown type");
  }

  return os.str();
}

// Convert ValueType to a string. One might say that,
// strictly speaking, this has no business here.
std::string Value::GetTypeName(ValueType type) {
  std::string ret;
  switch (type) {
    case (VALUE_TYPE_TINYINT):
      ret = "tinyint";
      break;
    case (VALUE_TYPE_SMALLINT):
      ret = "smallint";
      break;
    case (VALUE_TYPE_INTEGER):
      ret = "integer";
      break;
    case (VALUE_TYPE_BIGINT):
      ret = "bigint";
      break;
    case (VALUE_TYPE_DOUBLE):
      ret = "double";
      break;
    case (VALUE_TYPE_VARCHAR):
      ret = "varchar";
      break;
    case (VALUE_TYPE_VARBINARY):
      ret = "varbinary";
      break;
    case (VALUE_TYPE_TIMESTAMP):
      ret = "timestamp";
      break;
    case (VALUE_TYPE_DECIMAL):
      ret = "decimal";
      break;
    case (VALUE_TYPE_INVALID):
      ret = "INVALID";
      break;
    case (VALUE_TYPE_NULL):
      ret = "NULL";
      break;
    case (VALUE_TYPE_BOOLEAN):
      ret = "boolean";
      break;
    case (VALUE_TYPE_ADDRESS):
      ret = "address";
      break;
    default: {
      char buffer[32];
      snprintf(buffer, 32, "UNKNOWN[%d]", type);
      ret = buffer;
    }
  }
  return (ret);
}

}  // End peloton namespace
