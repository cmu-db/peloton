#pragma once

#include <algorithm>
#include <stdexcept>

#include "common/macros.h"
#include "type/boolean_type.h"
#include "type/decimal_type.h"
#include "type/numeric_type.h"
#include "type/serializer.h"
#include "type/timestamp_type.h"
#include "type/varlen_type.h"
#include "util/string_util.h"

namespace peloton {
namespace type {

//===--------------------------------------------------------------------===//
// Value Factory
//===--------------------------------------------------------------------===//

class ValueFactory {
 public:
  static inline Value Clone(const Value &src,
                            UNUSED_ATTRIBUTE AbstractPool *dataPool = nullptr) {
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

  static inline Value GetDecimalValue(double value) {
    return Value(Type::DECIMAL, value);
  }

  static inline Value GetBooleanValue(CmpBool value) {
    return Value(Type::BOOLEAN,
                 value == CMP_NULL ? PELOTON_BOOLEAN_NULL : (int8_t)value);
  }

  static inline Value GetBooleanValue(bool value) {
    return Value(Type::BOOLEAN, value);
  }

  static inline Value GetBooleanValue(int8_t value) {
    return Value(Type::BOOLEAN, value);
  }

  static inline Value GetVarcharValue(
      const char *value, bool manage_data,
      UNUSED_ATTRIBUTE AbstractPool *pool = nullptr) {
    return Value(Type::VARCHAR, value, value == nullptr ? 0 : strlen(value) + 1,
                 manage_data);
  }

  static inline Value GetVarcharValue(
      const std::string &value, UNUSED_ATTRIBUTE AbstractPool *pool = nullptr) {
    return Value(Type::VARCHAR, value);
  }

  static inline Value GetVarbinaryValue(
      const std::string &value, UNUSED_ATTRIBUTE AbstractPool *pool = nullptr) {
    return Value(Type::VARBINARY, value);
  }

  static inline Value GetVarbinaryValue(
      const unsigned char *rawBuf, int32_t rawLength, bool manage_data,
      UNUSED_ATTRIBUTE AbstractPool *pool = nullptr) {
    return Value(Type::VARBINARY, (const char *)rawBuf, rawLength, manage_data);
  }

  static inline Value GetNullValueByType(Type::TypeId type_id) {
    Value ret_value;
    switch (type_id) {
      case Type::BOOLEAN:
        ret_value = GetBooleanValue(PELOTON_BOOLEAN_NULL);
        break;
      case Type::TINYINT:
        ret_value = GetTinyIntValue(PELOTON_INT8_NULL);
        break;
      case Type::SMALLINT:
        ret_value = GetSmallIntValue(PELOTON_INT16_NULL);
        break;
      case Type::INTEGER:
        ret_value = GetIntegerValue(PELOTON_INT32_NULL);
        break;
      case Type::BIGINT:
        ret_value = GetBigIntValue(PELOTON_INT64_NULL);
        break;
      case Type::DECIMAL:
        ret_value = GetDecimalValue(PELOTON_DECIMAL_NULL);
        break;
      case Type::TIMESTAMP:
        ret_value = GetTimestampValue(PELOTON_TIMESTAMP_NULL);
        break;
      case Type::VARCHAR:
        ret_value = GetVarcharValue(nullptr, false, nullptr);
        break;
      case Type::VARBINARY:
        ret_value = GetVarbinaryValue(nullptr, 0, false, nullptr);
        break;
      default: {
        std::string msg =
            StringUtil::Format("Unknown Type '%d' for GetNullValueByType",
                               static_cast<int>(type_id));
        throw Exception(EXCEPTION_TYPE_UNKNOWN_TYPE, msg);
      }
    }
    ret_value.size_.len = PELOTON_VALUE_NULL;
    return ret_value;
  }

  static inline Value GetZeroValueByType(Type::TypeId type_id) {
    std::string zero_string("0");

    switch (type_id) {
      case Type::BOOLEAN:
        return GetBooleanValue(false);
      case Type::TINYINT:
        return GetTinyIntValue(0);
      case Type::SMALLINT:
        return GetSmallIntValue(0);
      case Type::INTEGER:
        return GetIntegerValue(0);
      case Type::BIGINT:
        return GetBigIntValue(0);
      case Type::DECIMAL:
        return GetDecimalValue((double)0);
      case Type::TIMESTAMP:
        return GetTimestampValue(0);
      case Type::VARCHAR:
        return GetVarcharValue(zero_string);
      case Type::VARBINARY:
        return GetVarbinaryValue(zero_string);
      default:
        break;
    }
    std::string msg = StringUtil::Format(
        "Unknown Type '%d' for GetZeroValueByType", static_cast<int>(type_id));
    throw Exception(EXCEPTION_TYPE_UNKNOWN_TYPE, msg);
  }

  static inline Value CastAsBigInt(const Value &value) {
    if (Type::GetInstance(Type::BIGINT)->IsCoercableFrom(value.GetTypeId())) {
      if (value.IsNull())
        return ValueFactory::GetBigIntValue((int64_t)PELOTON_INT64_NULL);
      switch (value.GetTypeId()) {
        case Type::TINYINT:
          return ValueFactory::GetBigIntValue((int64_t)value.GetAs<int8_t>());
        case Type::SMALLINT:
          return ValueFactory::GetBigIntValue((int64_t)value.GetAs<int16_t>());
        case Type::INTEGER:
          return ValueFactory::GetBigIntValue((int64_t)value.GetAs<int32_t>());
        case Type::BIGINT:
          return ValueFactory::GetBigIntValue(value.GetAs<int64_t>());
        case Type::DECIMAL: {
          if (value.GetAs<double>() > (double)PELOTON_INT64_MAX ||
              value.GetAs<double>() < (double)PELOTON_INT64_MIN)
            throw Exception(EXCEPTION_TYPE_OUT_OF_RANGE,
                            "Numeric value out of range.");
          return ValueFactory::GetBigIntValue((int64_t)value.GetAs<double>());
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
          return ValueFactory::GetBigIntValue(bigint);
        }
        default:
          break;
      }
    }
    throw Exception(Type::GetInstance(value.GetTypeId())->ToString() +
                    " is not coercable to BIGINT.");
  }

  static inline Value CastAsInteger(const Value &value) {
    if (Type::GetInstance(Type::INTEGER)->IsCoercableFrom(value.GetTypeId())) {
      if (value.IsNull())
        return ValueFactory::GetIntegerValue((int32_t)PELOTON_INT32_NULL);
      switch (value.GetTypeId()) {
        case Type::TINYINT:
          return ValueFactory::GetIntegerValue((int32_t)value.GetAs<int8_t>());
        case Type::SMALLINT:
          return ValueFactory::GetIntegerValue((int32_t)value.GetAs<int16_t>());
        case Type::INTEGER:
          return ValueFactory::GetIntegerValue(value.GetAs<int32_t>());
        case Type::BIGINT: {
          if (value.GetAs<int64_t>() > (int64_t)PELOTON_INT32_MAX ||
              value.GetAs<int64_t>() < (int64_t)PELOTON_INT32_MIN)
            throw Exception(EXCEPTION_TYPE_OUT_OF_RANGE,
                            "Numeric value out of range.");
          return ValueFactory::GetIntegerValue((int32_t)value.GetAs<int64_t>());
        }
        case Type::DECIMAL: {
          if (value.GetAs<double>() > (double)PELOTON_INT32_MAX ||
              value.GetAs<double>() < (double)PELOTON_INT32_MIN)
            throw Exception(EXCEPTION_TYPE_OUT_OF_RANGE,
                            "Numeric value out of range.");
          return ValueFactory::GetIntegerValue((int32_t)value.GetAs<double>());
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
          return ValueFactory::GetIntegerValue(integer);
        }
        default:
          break;
      }
    }
    throw Exception(Type::GetInstance(value.GetTypeId())->ToString() +
                    " is not coercable to INTEGER.");
  }

  static inline Value CastAsSmallInt(const Value &value) {
    if (Type::GetInstance(Type::SMALLINT)->IsCoercableFrom(value.GetTypeId())) {
      if (value.IsNull())
        return ValueFactory::GetSmallIntValue((int16_t)PELOTON_INT16_NULL);
      switch (value.GetTypeId()) {
        case Type::TINYINT:
          return ValueFactory::GetSmallIntValue((int16_t)value.GetAs<int8_t>());
        case Type::SMALLINT:
          return ValueFactory::GetSmallIntValue(value.GetAs<int16_t>());
        case Type::INTEGER: {
          if (value.GetAs<int32_t>() > (int32_t)PELOTON_INT16_MAX ||
              value.GetAs<int32_t>() < (int32_t)PELOTON_INT16_MIN)
            throw Exception(EXCEPTION_TYPE_OUT_OF_RANGE,
                            "Numeric value out of range.");
          return ValueFactory::GetSmallIntValue(
              (int16_t)value.GetAs<int32_t>());
        }
        case Type::BIGINT: {
          if (value.GetAs<int64_t>() > (int64_t)PELOTON_INT16_MAX ||
              value.GetAs<int64_t>() < (int64_t)PELOTON_INT16_MIN)
            throw Exception(EXCEPTION_TYPE_OUT_OF_RANGE,
                            "Numeric value out of range.");
          return ValueFactory::GetSmallIntValue(
              (int16_t)value.GetAs<int64_t>());
        }
        case Type::DECIMAL: {
          if (value.GetAs<double>() > (double)PELOTON_INT16_MAX ||
              value.GetAs<double>() < (double)PELOTON_INT16_MIN)
            throw Exception(EXCEPTION_TYPE_OUT_OF_RANGE,
                            "Numeric value out of range.");
          return ValueFactory::GetSmallIntValue((int16_t)value.GetAs<double>());
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
          return ValueFactory::GetSmallIntValue(smallint);
        }
        default:
          break;
      }
    }
    throw Exception(Type::GetInstance(value.GetTypeId())->ToString() +
                    " is not coercable to SMALLINT.");
  }

  static inline Value CastAsTinyInt(const Value &value) {
    if (Type::GetInstance(Type::TINYINT)->IsCoercableFrom(value.GetTypeId())) {
      if (value.IsNull())
        return ValueFactory::GetTinyIntValue(PELOTON_INT8_NULL);
      switch (value.GetTypeId()) {
        case Type::TINYINT:
          return ValueFactory::GetTinyIntValue(value.GetAs<int8_t>());
        case Type::SMALLINT: {
          if (value.GetAs<int16_t>() > PELOTON_INT8_MAX ||
              value.GetAs<int16_t>() < PELOTON_INT8_MIN)
            throw Exception(EXCEPTION_TYPE_OUT_OF_RANGE,
                            "Numeric value out of range.");
          return ValueFactory::GetTinyIntValue((int8_t)value.GetAs<int16_t>());
        }
        case Type::INTEGER: {
          if (value.GetAs<int32_t>() > (int32_t)PELOTON_INT8_MAX ||
              value.GetAs<int32_t>() < (int32_t)PELOTON_INT8_MIN)
            throw Exception(EXCEPTION_TYPE_OUT_OF_RANGE,
                            "Numeric value out of range.");
          return ValueFactory::GetTinyIntValue((int8_t)value.GetAs<int32_t>());
        }
        case Type::BIGINT: {
          if (value.GetAs<int64_t>() > (int64_t)PELOTON_INT8_MAX ||
              value.GetAs<int64_t>() < (int64_t)PELOTON_INT8_MIN)
            throw Exception(EXCEPTION_TYPE_OUT_OF_RANGE,
                            "Numeric value out of range.");
          return ValueFactory::GetTinyIntValue((int8_t)value.GetAs<int64_t>());
        }
        case Type::DECIMAL: {
          if (value.GetAs<double>() > (double)PELOTON_INT8_MAX ||
              value.GetAs<double>() < (double)PELOTON_INT8_MIN)
            throw Exception(EXCEPTION_TYPE_OUT_OF_RANGE,
                            "Numeric value out of range.");
          return ValueFactory::GetTinyIntValue((int8_t)value.GetAs<double>());
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
          return ValueFactory::GetTinyIntValue(tinyint);
        }
        default:
          break;
      }
    }
    throw Exception(Type::GetInstance(value.GetTypeId())->ToString() +
                    " is not coercable to TINYINT.");
  }

  static inline Value CastAsDecimal(const Value &value) {
    if (Type::GetInstance(Type::DECIMAL)->IsCoercableFrom(value.GetTypeId())) {
      if (value.IsNull())
        return ValueFactory::GetDecimalValue((double)PELOTON_DECIMAL_NULL);
      switch (value.GetTypeId()) {
        case Type::TINYINT:
          return ValueFactory::GetDecimalValue((double)value.GetAs<int8_t>());
        case Type::SMALLINT:
          return ValueFactory::GetDecimalValue((double)value.GetAs<int16_t>());
        case Type::INTEGER:
          return ValueFactory::GetDecimalValue((double)value.GetAs<int32_t>());
        case Type::BIGINT:
          return ValueFactory::GetDecimalValue((double)value.GetAs<int64_t>());
        case Type::DECIMAL:
          return ValueFactory::GetDecimalValue(value.GetAs<double>());
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
          return ValueFactory::GetDecimalValue(res);
        }
        default:
          break;
      }
    }
    throw Exception(Type::GetInstance(value.GetTypeId())->ToString() +
                    " is not coercable to DECIMAL.");
  }

  static inline Value CastAsVarchar(const Value &value) {
    if (Type::GetInstance(Type::VARCHAR)->IsCoercableFrom(value.GetTypeId())) {
      if (value.IsNull()) return ValueFactory::GetVarcharValue(nullptr, 0);
      switch (value.GetTypeId()) {
        case Type::BOOLEAN:
        case Type::TINYINT:
        case Type::SMALLINT:
        case Type::INTEGER:
        case Type::BIGINT:
        case Type::DECIMAL:
        case Type::TIMESTAMP:
        case Type::VARCHAR:
          return ValueFactory::GetVarcharValue(value.ToString());
        default:
          break;
      }
    }
    throw Exception(Type::GetInstance(value.GetTypeId())->ToString() +
                    " is not coercable to VARCHAR.");
  }

  static inline Value CastAsTimestamp(const Value &value) {
    if (Type::GetInstance(Type::TIMESTAMP)
            ->IsCoercableFrom(value.GetTypeId())) {
      if (value.IsNull())
        return ValueFactory::GetTimestampValue(PELOTON_TIMESTAMP_NULL);
      switch (value.GetTypeId()) {
        case Type::TIMESTAMP:
          return ValueFactory::GetTimestampValue(value.GetAs<uint64_t>());
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
          return ValueFactory::GetTimestampValue(res);
        }
        default:
          break;
      }
    }
    throw Exception(Type::GetInstance(value.GetTypeId())->ToString() +
                    " is not coercable to TIMESTAMP.");
  }

  static inline Value CastAsBoolean(const Value &value) {
    if (Type::GetInstance(Type::BOOLEAN)->IsCoercableFrom(value.GetTypeId())) {
      if (value.IsNull())
        return ValueFactory::GetBooleanValue(PELOTON_BOOLEAN_NULL);
      switch (value.GetTypeId()) {
        case Type::BOOLEAN:
          return ValueFactory::GetBooleanValue(value.GetAs<int8_t>());
        case Type::VARCHAR: {
          std::string str = value.ToString();
          std::transform(str.begin(), str.end(), str.begin(), ::tolower);
          if (str == "true" || str == "1")
            return ValueFactory::GetBooleanValue(true);
          else if (str == "false" || str == "0")
            return ValueFactory::GetBooleanValue(false);
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

}  // namespace type
}  // namespace peloton
