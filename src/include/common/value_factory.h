#pragma once

#include <stdexcept>
#include "common/array_value.h"
#include "common/boolean_value.h"
#include "common/decimal_value.h"
#include "common/macros.h"
#include "common/numeric_value.h"
#include "common/timestamp_value.h"
#include "common/varlen_value.h"
#include "common/serializer.h"

namespace peloton {
namespace common {

//===--------------------------------------------------------------------===//
// Value Factory
//===--------------------------------------------------------------------===//

class ValueFactory {
 public:
  static inline Value *Clone(const Value &src,
                             UNUSED_ATTRIBUTE VarlenPool *dataPool = nullptr) {
    return src.Copy();
  }

  static inline Value GetTinyIntValue(int8_t value) {
    return Value(Type::TINYINT, value);
  }

  static inline Value GetSmallIntValue(int16_t value) {
    return Value(Type::SMALLINT, value);
  }

  static inline Value GetIntegerValue(int32_t value) {
    return Value(Type::INTEGER, value);
  }

  static inline Value GetParameterOffsetValue(int32_t value) {
    return Value(Type::PARAMETER_OFFSET, value);
  }

  static inline Value GetBigIntValue(int64_t value) {
    return Value(Type::BIGINT, value);
  }

  static inline Value GetTimestampValue(int64_t value) {
    return Value(Type::TIMESTAMP, value);
  }

  static inline Value GetDoubleValue(double value) {
    return Value(Type::DECIMAL, value);
  }

  static inline Value GetBooleanValue(bool value) {
    return Value(Type::BOOLEAN, value);
  }

  static inline Value GetVarcharValue(
      const char *value, UNUSED_ATTRIBUTE VarlenPool *pool = nullptr) {
    std::string str(value);
    return Value(Type::VARCHAR, str);
  }

  static inline Value GetVarcharValue(
      const std::string &value, UNUSED_ATTRIBUTE VarlenPool *pool = nullptr) {
    return Value(Type::VARCHAR, value);
  }

  static inline Value GetVarbinaryValue(
      const std::string &value, UNUSED_ATTRIBUTE VarlenPool *pool = nullptr) {
    return Value(Type::VARBINARY, value);
  }

  static inline Value GetVarbinaryValue(
      const unsigned char *rawBuf, int32_t rawLength,
      UNUSED_ATTRIBUTE VarlenPool *pool = nullptr) {
    return Value(Type::VARBINARY, (const char *)rawBuf, rawLength);
  }

  static inline Value *GetNullValueByType(Type::TypeId type_id) {
    switch (type_id) {
      case Type::BOOLEAN:
        return new Value(type_id, PELOTON_BOOLEAN_NULL);
      case Type::TINYINT:
        return new Value(type_id, PELOTON_INT8_NULL);
      case Type::SMALLINT:
        return new Value(type_id, PELOTON_INT16_NULL);
      case Type::INTEGER:
        return new Value(type_id, PELOTON_INT32_NULL);
      case Type::BIGINT:
        return new Value(type_id, PELOTON_INT64_NULL);
      case Type::DECIMAL:
        return new Value(type_id, PELOTON_DECIMAL_NULL);
      case Type::TIMESTAMP:
        return new Value(type_id, PELOTON_TIMESTAMP_NULL);
      case Type::VARCHAR:
        return new Value(type_id, nullptr, 0);
      default:
        break;
    }
    throw Exception(EXCEPTION_TYPE_UNKNOWN_TYPE, "Unknown type.");
  }

  static inline Value *GetZeroValueByType(Type::TypeId type_id) {
    std::string zero_string("0");

    switch (type_id) {
      case Type::BOOLEAN:
        return new Value(type_id, 0);
      case Type::TINYINT:
        return new Value(type_id, 0);
      case Type::SMALLINT:
        return new Value(type_id, 0);
      case Type::INTEGER:
        return new Value(type_id, 0);
      case Type::BIGINT:
        return new Value(type_id, 0);
      case Type::DECIMAL:
        return new Value(type_id, (double)0);
      case Type::TIMESTAMP:
        return new Value(type_id, 0);
      case Type::VARCHAR:
        return new Value(type_id, zero_string);
      default:
        break;
    }
    throw Exception(EXCEPTION_TYPE_UNKNOWN_TYPE, "Unknown type.");
  }

  static inline Value *CastAsBigInt(const Value &value) {
    if (Type::GetInstance(Type::BIGINT)->IsCoercableFrom(value.GetTypeId())) {
      if (value.IsNull()) return new Value(Type::BIGINT, (int64_t)PELOTON_INT64_NULL);
      switch (value.GetTypeId()) {
        case Type::TINYINT:
          return new Value(Type::BIGINT, (int64_t)value.GetAs<int8_t>());
        case Type::SMALLINT:
          return new Value(Type::BIGINT, (int64_t)value.GetAs<int16_t>());
        case Type::INTEGER:
          return new Value(Type::BIGINT, (int64_t)value.GetAs<int32_t>());
        case Type::BIGINT:
          return new Value(Type::BIGINT, value.GetAs<int64_t>());
        case Type::DECIMAL: {
          if (value.GetAs<double>() > (double)PELOTON_INT64_MAX ||
              value.GetAs<double>() < (double)PELOTON_INT64_MIN)
            throw Exception(EXCEPTION_TYPE_OUT_OF_RANGE,
                            "Numeric value out of range.");
          return new Value(Type::BIGINT, (int64_t)value.GetAs<double>());
        }
        case Type::VARCHAR: {
          std::string str = value.ToString();
          int64_t bigint = 0;
          try {
            bigint = stoll(str);
          } catch (std::out_of_range &e) {
            throw Exception(EXCEPTION_TYPE_OUT_OF_RANGE,
                            "Numeric value out of range.");
          }
          if (bigint > PELOTON_INT64_MAX || bigint < PELOTON_INT64_MIN)
            throw Exception(EXCEPTION_TYPE_OUT_OF_RANGE,
                            "Numeric value out of range.");
          return new Value(Type::BIGINT, bigint);
        }
        default:
          break;
      }
    }
    throw Exception(Type::GetInstance(value.GetTypeId())->ToString() +
                    " is not coercable to BIGINT.");
  }

  static inline Value *CastAsInteger(const Value &value) {
    if (Type::GetInstance(Type::INTEGER)->IsCoercableFrom(value.GetTypeId())) {
      if (value.IsNull()) return new Value(Type::INTEGER, (int32_t)PELOTON_INT32_NULL);
      switch (value.GetTypeId()) {
        case Type::TINYINT:
          return new Value(Type::INTEGER, (int32_t)value.GetAs<int8_t>());
        case Type::SMALLINT:
          return new Value(Type::INTEGER, (int32_t)value.GetAs<int16_t>());
        case Type::INTEGER:
          return new Value(Type::INTEGER, value.GetAs<int32_t>());
        case Type::BIGINT: {
          if (value.GetAs<int64_t>() > (int64_t)PELOTON_INT32_MAX ||
              value.GetAs<int64_t>() < (int64_t)PELOTON_INT32_MIN)
            throw Exception(EXCEPTION_TYPE_OUT_OF_RANGE,
                            "Numeric value out of range.");
          return new Value(Type::INTEGER, (int32_t)value.GetAs<int64_t>());
        }
        case Type::DECIMAL: {
          if (value.GetAs<double>() > (double)PELOTON_INT32_MAX ||
              value.GetAs<double>() < (double)PELOTON_INT32_MIN)
            throw Exception(EXCEPTION_TYPE_OUT_OF_RANGE,
                            "Numeric value out of range.");
          return new Value(Type::INTEGER, (int32_t)value.GetAs<double>());
        }
        case Type::VARCHAR: {
          std::string str = value.ToString();
          int32_t integer = 0;
          try {
            integer = stoi(str);
          } catch (std::out_of_range &e) {
            throw Exception(EXCEPTION_TYPE_OUT_OF_RANGE,
                            "Numeric value out of range.");
          }
          if (integer > PELOTON_INT32_MAX || integer < PELOTON_INT32_MIN)
            throw Exception(EXCEPTION_TYPE_OUT_OF_RANGE,
                            "Numeric value out of range.");
          return new Value(Type::INTEGER, integer);
        }
        default:
          break;
      }
    }
    throw Exception(Type::GetInstance(value.GetTypeId())->ToString() +
                    " is not coercable to INTEGER.");
  }

  static inline Value *CastAsSmallInt(const Value &value) {
    if (Type::GetInstance(Type::SMALLINT)->IsCoercableFrom(value.GetTypeId())) {
      if (value.IsNull()) return new Value(Type::SMALLINT, (int16_t)PELOTON_INT16_NULL);
      switch (value.GetTypeId()) {
        case Type::TINYINT:
          return new Value(Type::SMALLINT, (int16_t)value.GetAs<int8_t>());
        case Type::SMALLINT:
          return new Value(Type::SMALLINT, value.GetAs<int16_t>());
        case Type::INTEGER: {
          if (value.GetAs<int32_t>() > (int32_t)PELOTON_INT16_MAX ||
              value.GetAs<int32_t>() < (int32_t)PELOTON_INT16_MIN)
            throw Exception(EXCEPTION_TYPE_OUT_OF_RANGE,
                            "Numeric value out of range.");
          return new Value(Type::SMALLINT, (int16_t)value.GetAs<int32_t>());
        }
        case Type::BIGINT: {
          if (value.GetAs<int64_t>() > (int64_t)PELOTON_INT16_MAX ||
              value.GetAs<int64_t>() < (int64_t)PELOTON_INT16_MIN)
            throw Exception(EXCEPTION_TYPE_OUT_OF_RANGE,
                            "Numeric value out of range.");
          return new Value(Type::SMALLINT, (int16_t)value.GetAs<int64_t>());
        }
        case Type::DECIMAL: {
          if (value.GetAs<double>() > (double)PELOTON_INT16_MAX ||
              value.GetAs<double>() < (double)PELOTON_INT16_MIN)
            throw Exception(EXCEPTION_TYPE_OUT_OF_RANGE,
                            "Numeric value out of range.");
          return new Value(Type::SMALLINT, (int16_t)value.GetAs<double>());
        }
        case Type::VARCHAR: {
          std::string str = value.ToString();
          int16_t smallint = 0;
          try {
            smallint = stoi(str);
          } catch (std::out_of_range &e) {
            throw Exception(EXCEPTION_TYPE_OUT_OF_RANGE,
                            "Numeric value out of range.");
          }
          if (smallint < PELOTON_INT16_MIN)
            throw Exception(EXCEPTION_TYPE_OUT_OF_RANGE,
                            "Numeric value out of range.");
          return new Value(Type::SMALLINT, smallint);
        }
        default:
          break;
      }
    }
    throw Exception(Type::GetInstance(value.GetTypeId())->ToString() +
                    " is not coercable to SMALLINT.");
  }

  static inline Value *CastAsTinyInt(const Value &value) {
    if (Type::GetInstance(Type::TINYINT)->IsCoercableFrom(value.GetTypeId())) {
      if (value.IsNull()) return new Value(Type::TINYINT, PELOTON_INT8_NULL);
      switch (value.GetTypeId()) {
        case Type::TINYINT:
          return new Value(Type::TINYINT, value.GetAs<int8_t>());
        case Type::SMALLINT: {
          if (value.GetAs<int16_t>() > PELOTON_INT8_MAX ||
              value.GetAs<int16_t>() < PELOTON_INT8_MIN)
            throw Exception(EXCEPTION_TYPE_OUT_OF_RANGE,
                            "Numeric value out of range.");
          return new Value(Type::TINYINT, (int8_t)value.GetAs<int16_t>());
        }
        case Type::INTEGER: {
          if (value.GetAs<int32_t>() > (int32_t)PELOTON_INT8_MAX ||
              value.GetAs<int32_t>() < (int32_t)PELOTON_INT8_MIN)
            throw Exception(EXCEPTION_TYPE_OUT_OF_RANGE,
                            "Numeric value out of range.");
          return new Value(Type::TINYINT, (int8_t)value.GetAs<int32_t>());
        }
        case Type::BIGINT: {
          if (value.GetAs<int64_t>() > (int64_t)PELOTON_INT8_MAX ||
              value.GetAs<int64_t>() < (int64_t)PELOTON_INT8_MIN)
            throw Exception(EXCEPTION_TYPE_OUT_OF_RANGE,
                            "Numeric value out of range.");
          return new Value(Type::TINYINT, (int8_t)value.GetAs<int64_t>());
        }
        case Type::DECIMAL: {
          if (value.GetAs<double>() > (double)PELOTON_INT8_MAX ||
              value.GetAs<double>() < (double)PELOTON_INT8_MIN)
            throw Exception(EXCEPTION_TYPE_OUT_OF_RANGE,
                            "Numeric value out of range.");
          return new Value(Type::TINYINT, (int8_t)value.GetAs<double>());
        }
        case Type::VARCHAR: {
          std::string str = value.ToString();
          int8_t tinyint = 0;
          try {
            tinyint = stoi(str);
          } catch (std::out_of_range &e) {
            throw Exception(EXCEPTION_TYPE_OUT_OF_RANGE,
                            "Numeric value out of range.");
          }
          if (tinyint < PELOTON_INT8_MIN)
            throw Exception(EXCEPTION_TYPE_OUT_OF_RANGE,
                            "Numeric value out of range.");
          return new Value(Type::TINYINT, tinyint);
        }
        default:
          break;
      }
    }
    throw Exception(Type::GetInstance(value.GetTypeId())->ToString() +
                    " is not coercable to TINYINT.");
  }

  static inline Value *CastAsDecimal(const Value &value) {
    if (Type::GetInstance(Type::DECIMAL)->IsCoercableFrom(value.GetTypeId())) {
      if (value.IsNull()) return new Value(Type::DECIMAL, (double)PELOTON_DECIMAL_NULL);
      switch (value.GetTypeId()) {
        case Type::TINYINT:
          return new Value(Type::DECIMAL, (double)value.GetAs<int8_t>());
        case Type::SMALLINT:
          return new Value(Type::DECIMAL, (double)value.GetAs<int16_t>());
        case Type::INTEGER:
          return new Value(Type::DECIMAL, (double)value.GetAs<int32_t>());
        case Type::BIGINT:
          return new Value(Type::DECIMAL, (double)value.GetAs<int64_t>());
        case Type::DECIMAL:
          return new Value(Type::DECIMAL, value.GetAs<double>());
        case Type::VARCHAR: {
          std::string str = value.ToString();
          double res = 0;
          try {
            res = stod(str);
          } catch (std::out_of_range &e) {
            throw Exception(EXCEPTION_TYPE_OUT_OF_RANGE,
                            "Numeric value out of range.");
          }
          if (res > PELOTON_DECIMAL_MAX || res < PELOTON_DECIMAL_MIN)
            throw Exception(EXCEPTION_TYPE_OUT_OF_RANGE,
                            "Numeric value out of range.");
          return new Value(Type::DECIMAL, res);
        }
        default:
          break;
      }
    }
    throw Exception(Type::GetInstance(value.GetTypeId())->ToString() +
                    " is not coercable to DECIMAL.");
  }

  static inline Value *CastAsVarchar(const Value &value) {
    if (Type::GetInstance(Type::VARCHAR)->IsCoercableFrom(value.GetTypeId())) {
      if (value.IsNull()) return new Value(Type::VARCHAR, nullptr, 0);
      switch (value.GetTypeId()) {
        case Type::BOOLEAN:
        case Type::TINYINT:
        case Type::SMALLINT:
        case Type::INTEGER:
        case Type::BIGINT:
        case Type::DECIMAL:
        case Type::TIMESTAMP:
        case Type::VARCHAR:
          return new Value(Type::VARCHAR, value.ToString());
        default:
          break;
      }
    }
    throw Exception(Type::GetInstance(value.GetTypeId())->ToString() +
                    " is not coercable to VARCHAR.");
  }

  static inline Value *CastAsTimestamp(const Value &value) {
    if (Type::GetInstance(Type::TIMESTAMP)->IsCoercableFrom(value.GetTypeId())) {
      if (value.IsNull()) return new Value(Type::TIMESTAMP, PELOTON_TIMESTAMP_NULL);
      switch (value.GetTypeId()) {
        case Type::TIMESTAMP:
          return new Value(Type::TIMESTAMP, value.GetAs<uint64_t>());
        case Type::VARCHAR: {
          std::string str = value.ToString();
          if (str.length() == 22)
            str = str.substr(0, 19) + ".000000" + str.substr(19, 3);
          if (str.length() != 29) throw Exception("Timestamp format error.");
          if (str[10] != ' ' || str[4] != '-' || str[7] != '-' ||
              str[13] != ':' || str[16] != ':' || str[19] != '.' ||
              (str[26] != '+' && str[26] != '-'))
            throw Exception("Timestamp format error.");
          bool isDigit[29] = {1, 1, 1, 1, 0, 1, 1, 0, 1, 1, 0, 1, 1, 0, 1,
                              1, 0, 1, 1, 0, 1, 1, 1, 1, 1, 1, 0, 1, 1};
          for (int i = 0; i < 29; i++) {
            if (isDigit[i])
              if (str[i] < '0' || str[i] > '9')
                throw Exception("Timestamp format error.");
          }
          int tz = 0;
          uint32_t year = 0;
          uint32_t month = 0;
          uint32_t day = 0;
          uint32_t hour = 0;
          uint32_t min = 0;
          uint32_t sec = 0;
          uint32_t micro = 0;
          uint64_t res = 0;
          if (sscanf(str.c_str(), "%4u-%2u-%2u %2u:%2u:%2u.%6u%3d", &year,
                     &month, &day, &hour, &min, &sec, &micro, &tz) != 8)
            throw Exception("Timestamp format error.");
          if (year > 9999 || month > 12 || day > 31 || hour > 23 || min > 59 ||
              sec > 59 || micro > 999999 || day == 0 || month == 0)
            throw Exception(EXCEPTION_TYPE_OUT_OF_RANGE,
                            "Timestamp value out of range.");
          uint32_t max_day[13] = {0,  31, 28, 31, 30, 31, 30,
                                  31, 31, 30, 31, 30, 31};
          uint32_t max_day_lunar[13] = {0,  31, 29, 31, 30, 31, 30,
                                        31, 31, 30, 31, 30, 31};
          if ((year % 4 == 0 && year % 100 != 0) || year % 400 == 0) {
            if (day > max_day_lunar[month])
              throw Exception(EXCEPTION_TYPE_OUT_OF_RANGE,
                              "Timestamp value out of range.");
          } else if (day > max_day[month])
            throw Exception(EXCEPTION_TYPE_OUT_OF_RANGE,
                            "Timestamp value out of range.");
          uint32_t timezone = tz + 12;
          if (tz > 26) throw Exception("Timestamp format error.");
          res += month;
          res *= 32;
          res += day;
          res *= 27;
          res += timezone;
          res *= 10000;
          res += year;
          res *= 100000;
          res += hour * 3600 + min * 60 + sec;
          res *= 1000000;
          res += micro;
          return new Value(Type::TIMESTAMP, res);
        }
        default:
          break;
      }
    }
    throw Exception(Type::GetInstance(value.GetTypeId())->ToString() +
                    " is not coercable to TIMESTAMP.");
  }

  static inline Value *CastAsBoolean(const Value &value) {
    if (Type::GetInstance(Type::BOOLEAN)->IsCoercableFrom(value.GetTypeId())) {
      if (value.IsNull()) return new Value(Type::BOOLEAN, PELOTON_BOOLEAN_NULL);
      switch (value.GetTypeId()) {
        case Type::BOOLEAN:
          return new Value(Type::BOOLEAN, value.GetAs<int8_t>());
        case Type::VARCHAR: {
          std::string str = value.ToString();
          if (str == "true" || str == "TURE" || str == "1")
            return new Value(Type::BOOLEAN, 1);
          else if (str == "flase" || str == "FALSE" || str == "0")
            return new Value(Type::BOOLEAN, 0);
          else
            throw Exception("Boolean value format error.");
        }
        default:
          break;
      }
    }
    throw Exception(Type::GetInstance(value.GetTypeId())->ToString() +
                    " is not coercable to BOOLEAN.");
  }
};

}  // namespace common
}  // namespace peloton
