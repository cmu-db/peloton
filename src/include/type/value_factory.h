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
    return Value(TypeId::TINYINT, value);
  }

  static inline Value GetSmallIntValue(int16_t value) {
    return Value(TypeId::SMALLINT, value);
  }

  static inline Value GetIntegerValue(int32_t value) {
    return Value(TypeId::INTEGER, value);
  }

  static inline Value GetParameterOffsetValue(int32_t value) {
    return Value(TypeId::PARAMETER_OFFSET, value);
  }

  static inline Value GetBigIntValue(int64_t value) {
    return Value(TypeId::BIGINT, value);
  }

  static inline Value GetDateValue(uint32_t value) {
    return Value(TypeId::DATE, static_cast<int32_t>(value));
  }

  static inline Value GetTimestampValue(int64_t value) {
    return Value(TypeId::TIMESTAMP, value);
  }

  static inline Value GetDecimalValue(double value) {
    return Value(TypeId::DECIMAL, value);
  }

  static inline Value GetBooleanValue(CmpBool value) {
    return Value(TypeId::BOOLEAN,
                 value == CmpBool::NULL_ ? PELOTON_BOOLEAN_NULL : (int8_t)value);
  }

  static inline Value GetBooleanValue(bool value) {
    return Value(TypeId::BOOLEAN, value);
  }

  static inline Value GetBooleanValue(int8_t value) {
    return Value(TypeId::BOOLEAN, value);
  }

  static inline Value GetVarcharValue(
      const char *value, bool manage_data,
      UNUSED_ATTRIBUTE AbstractPool *pool = nullptr) {
    uint32_t len =
        static_cast<uint32_t>(value == nullptr ? 0u : strlen(value) + 1);
    return GetVarcharValue(value, len, manage_data);
  }

  static inline Value GetVarcharValue(
      const char *value, uint32_t len, bool manage_data,
      UNUSED_ATTRIBUTE AbstractPool *pool = nullptr) {
    return Value(TypeId::VARCHAR, value, len, manage_data);
  }

  static inline Value GetVarcharValue(
      const std::string &value, UNUSED_ATTRIBUTE AbstractPool *pool = nullptr) {
    return Value(TypeId::VARCHAR, value);
  }

  static inline Value GetVarbinaryValue(
      const std::string &value, UNUSED_ATTRIBUTE AbstractPool *pool = nullptr) {
    return Value(TypeId::VARBINARY, value);
  }

  static inline Value GetVarbinaryValue(
      const unsigned char *rawBuf, int32_t rawLength, bool manage_data,
      UNUSED_ATTRIBUTE AbstractPool *pool = nullptr) {
    return Value(TypeId::VARBINARY, (const char *)rawBuf, rawLength,
                 manage_data);
  }

  static inline Value GetNullValueByType(TypeId type_id) {
    Value ret_value;
    switch (type_id) {
      case TypeId::BOOLEAN:
        ret_value = GetBooleanValue(PELOTON_BOOLEAN_NULL);
        break;
      case TypeId::TINYINT:
        ret_value = GetTinyIntValue(PELOTON_INT8_NULL);
        break;
      case TypeId::SMALLINT:
        ret_value = GetSmallIntValue(PELOTON_INT16_NULL);
        break;
      case TypeId::INTEGER:
        ret_value = GetIntegerValue(PELOTON_INT32_NULL);
        break;
      case TypeId::BIGINT:
        ret_value = GetBigIntValue(PELOTON_INT64_NULL);
        break;
      case TypeId::DECIMAL:
        ret_value = GetDecimalValue(PELOTON_DECIMAL_NULL);
        break;
      case TypeId::TIMESTAMP:
        ret_value = GetTimestampValue(PELOTON_TIMESTAMP_NULL);
        break;
      case TypeId::DATE:
        ret_value = GetDateValue(PELOTON_DATE_NULL);
        break;
      case TypeId::VARCHAR:
        ret_value = GetVarcharValue(nullptr, false, nullptr);
        break;
      case TypeId::VARBINARY:
        ret_value = GetVarbinaryValue(nullptr, 0, false, nullptr);
        break;
      default: {
        std::string msg =
            StringUtil::Format("Type '%s' does not have a NULL value",
                               TypeIdToString(type_id).c_str());
        throw Exception(ExceptionType::UNKNOWN_TYPE, msg);
      }
    }
    ret_value.size_.len = PELOTON_VALUE_NULL;
    return ret_value;
  }

  static inline Value GetZeroValueByType(TypeId type_id) {
    std::string zero_string("0");

    switch (type_id) {
      case TypeId::BOOLEAN:
        return GetBooleanValue(false);
      case TypeId::TINYINT:
        return GetTinyIntValue(0);
      case TypeId::SMALLINT:
        return GetSmallIntValue(0);
      case TypeId::INTEGER:
        return GetIntegerValue(0);
      case TypeId::BIGINT:
        return GetBigIntValue(0);
      case TypeId::DECIMAL:
        return GetDecimalValue((double)0);
      case TypeId::TIMESTAMP:
        return GetTimestampValue(0);
      case TypeId::DATE:
        return GetDateValue(0);
      case TypeId::VARCHAR:
        return GetVarcharValue(zero_string);
      case TypeId::VARBINARY:
        return GetVarbinaryValue(zero_string);
      default:
        break;
    }
    std::string msg = StringUtil::Format(
        "Unknown Type '%d' for GetZeroValueByType", static_cast<int>(type_id));
    throw Exception(ExceptionType::UNKNOWN_TYPE, msg);
  }

  static inline Value CastAsBigInt(const Value &value) {
    if (Type::GetInstance(TypeId::BIGINT)->IsCoercableFrom(value.GetTypeId())) {
      if (value.IsNull())
        return ValueFactory::GetBigIntValue((int64_t)PELOTON_INT64_NULL);
      switch (value.GetTypeId()) {
        case TypeId::TINYINT:
          return ValueFactory::GetBigIntValue((int64_t)value.GetAs<int8_t>());
        case TypeId::SMALLINT:
          return ValueFactory::GetBigIntValue((int64_t)value.GetAs<int16_t>());
        case TypeId::INTEGER:
          return ValueFactory::GetBigIntValue((int64_t)value.GetAs<int32_t>());
        case TypeId::BIGINT:
          return ValueFactory::GetBigIntValue(value.GetAs<int64_t>());
        case TypeId::DECIMAL: {
          if (value.GetAs<double>() > (double)PELOTON_INT64_MAX ||
              value.GetAs<double>() < (double)PELOTON_INT64_MIN)
            throw Exception(ExceptionType::OUT_OF_RANGE,
                            "Numeric value out of range.");
          return ValueFactory::GetBigIntValue((int64_t)value.GetAs<double>());
        }
        case TypeId::VARCHAR: {
          std::string str = value.ToString();
          int64_t bigint = 0;
          try {
            bigint = stoll(str);
          } catch (std::out_of_range &e) {
            throw Exception(ExceptionType::OUT_OF_RANGE,
                            "Numeric value out of range.");
          }
          if (bigint > PELOTON_INT64_MAX || bigint < PELOTON_INT64_MIN)
            throw Exception(ExceptionType::OUT_OF_RANGE,
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
    if (Type::GetInstance(TypeId::INTEGER)
            ->IsCoercableFrom(value.GetTypeId())) {
      if (value.IsNull())
        return ValueFactory::GetIntegerValue((int32_t)PELOTON_INT32_NULL);
      switch (value.GetTypeId()) {
        case TypeId::TINYINT:
          return ValueFactory::GetIntegerValue((int32_t)value.GetAs<int8_t>());
        case TypeId::SMALLINT:
          return ValueFactory::GetIntegerValue((int32_t)value.GetAs<int16_t>());
        case TypeId::INTEGER:
          return ValueFactory::GetIntegerValue(value.GetAs<int32_t>());
        case TypeId::BIGINT: {
          if (value.GetAs<int64_t>() > (int64_t)PELOTON_INT32_MAX ||
              value.GetAs<int64_t>() < (int64_t)PELOTON_INT32_MIN)
            throw Exception(ExceptionType::OUT_OF_RANGE,
                            "Numeric value out of range.");
          return ValueFactory::GetIntegerValue((int32_t)value.GetAs<int64_t>());
        }
        case TypeId::DECIMAL: {
          if (value.GetAs<double>() > (double)PELOTON_INT32_MAX ||
              value.GetAs<double>() < (double)PELOTON_INT32_MIN)
            throw Exception(ExceptionType::OUT_OF_RANGE,
                            "Numeric value out of range.");
          return ValueFactory::GetIntegerValue((int32_t)value.GetAs<double>());
        }
        case TypeId::VARCHAR: {
          std::string str = value.ToString();
          int32_t integer = 0;
          try {
            integer = stoi(str);
          } catch (std::out_of_range &e) {
            throw Exception(ExceptionType::OUT_OF_RANGE,
                            "Numeric value out of range.");
          }
          if (integer > PELOTON_INT32_MAX || integer < PELOTON_INT32_MIN)
            throw Exception(ExceptionType::OUT_OF_RANGE,
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
    if (Type::GetInstance(TypeId::SMALLINT)
            ->IsCoercableFrom(value.GetTypeId())) {
      if (value.IsNull())
        return ValueFactory::GetSmallIntValue((int16_t)PELOTON_INT16_NULL);
      switch (value.GetTypeId()) {
        case TypeId::TINYINT:
          return ValueFactory::GetSmallIntValue((int16_t)value.GetAs<int8_t>());
        case TypeId::SMALLINT:
          return ValueFactory::GetSmallIntValue(value.GetAs<int16_t>());
        case TypeId::INTEGER: {
          if (value.GetAs<int32_t>() > (int32_t)PELOTON_INT16_MAX ||
              value.GetAs<int32_t>() < (int32_t)PELOTON_INT16_MIN)
            throw Exception(ExceptionType::OUT_OF_RANGE,
                            "Numeric value out of range.");
          return ValueFactory::GetSmallIntValue(
              (int16_t)value.GetAs<int32_t>());
        }
        case TypeId::BIGINT: {
          if (value.GetAs<int64_t>() > (int64_t)PELOTON_INT16_MAX ||
              value.GetAs<int64_t>() < (int64_t)PELOTON_INT16_MIN)
            throw Exception(ExceptionType::OUT_OF_RANGE,
                            "Numeric value out of range.");
          return ValueFactory::GetSmallIntValue(
              (int16_t)value.GetAs<int64_t>());
        }
        case TypeId::DECIMAL: {
          if (value.GetAs<double>() > (double)PELOTON_INT16_MAX ||
              value.GetAs<double>() < (double)PELOTON_INT16_MIN)
            throw Exception(ExceptionType::OUT_OF_RANGE,
                            "Numeric value out of range.");
          return ValueFactory::GetSmallIntValue((int16_t)value.GetAs<double>());
        }
        case TypeId::VARCHAR: {
          std::string str = value.ToString();
          int16_t smallint = 0;
          try {
            smallint = stoi(str);
          } catch (std::out_of_range &e) {
            throw Exception(ExceptionType::OUT_OF_RANGE,
                            "Numeric value out of range.");
          }
          if (smallint < PELOTON_INT16_MIN)
            throw Exception(ExceptionType::OUT_OF_RANGE,
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
    if (Type::GetInstance(TypeId::TINYINT)
            ->IsCoercableFrom(value.GetTypeId())) {
      if (value.IsNull())
        return ValueFactory::GetTinyIntValue(PELOTON_INT8_NULL);
      switch (value.GetTypeId()) {
        case TypeId::TINYINT:
          return ValueFactory::GetTinyIntValue(value.GetAs<int8_t>());
        case TypeId::SMALLINT: {
          if (value.GetAs<int16_t>() > PELOTON_INT8_MAX ||
              value.GetAs<int16_t>() < PELOTON_INT8_MIN)
            throw Exception(ExceptionType::OUT_OF_RANGE,
                            "Numeric value out of range.");
          return ValueFactory::GetTinyIntValue((int8_t)value.GetAs<int16_t>());
        }
        case TypeId::INTEGER: {
          if (value.GetAs<int32_t>() > (int32_t)PELOTON_INT8_MAX ||
              value.GetAs<int32_t>() < (int32_t)PELOTON_INT8_MIN)
            throw Exception(ExceptionType::OUT_OF_RANGE,
                            "Numeric value out of range.");
          return ValueFactory::GetTinyIntValue((int8_t)value.GetAs<int32_t>());
        }
        case TypeId::BIGINT: {
          if (value.GetAs<int64_t>() > (int64_t)PELOTON_INT8_MAX ||
              value.GetAs<int64_t>() < (int64_t)PELOTON_INT8_MIN)
            throw Exception(ExceptionType::OUT_OF_RANGE,
                            "Numeric value out of range.");
          return ValueFactory::GetTinyIntValue((int8_t)value.GetAs<int64_t>());
        }
        case TypeId::DECIMAL: {
          if (value.GetAs<double>() > (double)PELOTON_INT8_MAX ||
              value.GetAs<double>() < (double)PELOTON_INT8_MIN)
            throw Exception(ExceptionType::OUT_OF_RANGE,
                            "Numeric value out of range.");
          return ValueFactory::GetTinyIntValue((int8_t)value.GetAs<double>());
        }
        case TypeId::VARCHAR: {
          std::string str = value.ToString();
          int8_t tinyint = 0;
          try {
            tinyint = stoi(str);
          } catch (std::out_of_range &e) {
            throw Exception(ExceptionType::OUT_OF_RANGE,
                            "Numeric value out of range.");
          }
          if (tinyint < PELOTON_INT8_MIN)
            throw Exception(ExceptionType::OUT_OF_RANGE,
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
    if (Type::GetInstance(TypeId::DECIMAL)
            ->IsCoercableFrom(value.GetTypeId())) {
      if (value.IsNull())
        return ValueFactory::GetDecimalValue((double)PELOTON_DECIMAL_NULL);
      switch (value.GetTypeId()) {
        case TypeId::TINYINT:
          return ValueFactory::GetDecimalValue((double)value.GetAs<int8_t>());
        case TypeId::SMALLINT:
          return ValueFactory::GetDecimalValue((double)value.GetAs<int16_t>());
        case TypeId::INTEGER:
          return ValueFactory::GetDecimalValue((double)value.GetAs<int32_t>());
        case TypeId::BIGINT:
          return ValueFactory::GetDecimalValue((double)value.GetAs<int64_t>());
        case TypeId::DECIMAL:
          return ValueFactory::GetDecimalValue(value.GetAs<double>());
        case TypeId::VARCHAR: {
          std::string str = value.ToString();
          double res = 0;
          try {
            res = stod(str);
          } catch (std::out_of_range &e) {
            throw Exception(ExceptionType::OUT_OF_RANGE,
                            "Numeric value out of range.");
          }
          if (res > PELOTON_DECIMAL_MAX || res < PELOTON_DECIMAL_MIN)
            throw Exception(ExceptionType::OUT_OF_RANGE,
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
    if (Type::GetInstance(TypeId::VARCHAR)
            ->IsCoercableFrom(value.GetTypeId())) {
      if (value.IsNull()) return ValueFactory::GetVarcharValue(nullptr, 0);
      switch (value.GetTypeId()) {
        case TypeId::BOOLEAN:
        case TypeId::TINYINT:
        case TypeId::SMALLINT:
        case TypeId::INTEGER:
        case TypeId::BIGINT:
        case TypeId::DECIMAL:
        case TypeId::TIMESTAMP:
        case TypeId::VARCHAR:
          return ValueFactory::GetVarcharValue(value.ToString());
        default:
          break;
      }
    }
    throw Exception(Type::GetInstance(value.GetTypeId())->ToString() +
                    " is not coercable to VARCHAR.");
  }

  static inline Value CastAsTimestamp(const Value &value) {
    if (Type::GetInstance(TypeId::TIMESTAMP)
            ->IsCoercableFrom(value.GetTypeId())) {
      if (value.IsNull())
        return ValueFactory::GetTimestampValue(PELOTON_TIMESTAMP_NULL);
      switch (value.GetTypeId()) {
        case TypeId::TIMESTAMP:
          return ValueFactory::GetTimestampValue(value.GetAs<uint64_t>());
        case TypeId::VARCHAR: {
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
            throw Exception(ExceptionType::OUT_OF_RANGE,
                            "Timestamp value out of range.");
          uint32_t max_day[13] = {0,  31, 28, 31, 30, 31, 30,
                                  31, 31, 30, 31, 30, 31};
          uint32_t max_day_lunar[13] = {0,  31, 29, 31, 30, 31, 30,
                                        31, 31, 30, 31, 30, 31};
          if ((year % 4 == 0 && year % 100 != 0) || year % 400 == 0) {
            if (day > max_day_lunar[month])
              throw Exception(ExceptionType::OUT_OF_RANGE,
                              "Timestamp value out of range.");
          } else if (day > max_day[month])
            throw Exception(ExceptionType::OUT_OF_RANGE,
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

  static Value CastAsDate(const Value &value) {
    if (Type::GetInstance(TypeId::DATE)->IsCoercableFrom(value.GetTypeId())) {
      if (value.IsNull()) return ValueFactory::GetDateValue(PELOTON_DATE_NULL);
      switch (value.GetTypeId()) {
        case TypeId::DATE:
          return ValueFactory::GetDateValue(value.GetAs<uint32_t>());
        case TypeId::VARCHAR: {
          std::string str = value.ToString();
          if (str.length() != 10) throw Exception("Date format error.");
          uint32_t res = 0;
          // Format: YYYY-MM-DD
          uint32_t year = 0;
          uint32_t month = 0;
          uint32_t day = 0;
          if (sscanf(str.c_str(), "%4u-%2u-%2u", &year, &month, &day) != 3)
            throw Exception("Date format error.");
          if (year > 9999 || month > 12 || day > 31 || day == 0 || month == 0)
            throw Exception(ExceptionType::OUT_OF_RANGE,
                            "Date value out of range.");
          uint32_t max_day[13] = {0,  31, 28, 31, 30, 31, 30,
                                  31, 31, 30, 31, 30, 31};
          uint32_t max_day_lunar[13] = {0,  31, 29, 31, 30, 31, 30,
                                        31, 31, 30, 31, 30, 31};

          if ((year % 4 == 0 && year % 100 != 0) || year % 400 == 0) {
            if (day > max_day_lunar[month])
              throw Exception(ExceptionType::OUT_OF_RANGE,
                              "Date value out of range.");
          } else if (day > max_day[month])
            throw Exception(ExceptionType::OUT_OF_RANGE,
                            "Date value out of range.");
          bool isDigit[10] = {1, 1, 1, 1, 0, 1, 1, 0, 1, 1};
          for (int i = 0; i < 10; i++) {
            if (isDigit[i])
              if (str[i] < '0' || str[i] > '9')
                throw Exception("Date format error.");
          }
          res += year;
          res *= 10000;
          res += month * 100;
          res += day;
          return ValueFactory::GetDateValue(res);
        }
        default:
          break;
      }
    }
    throw Exception(Type::GetInstance(value.GetTypeId())->ToString() +
                    " is not coercable to DATE.");
  }

  static inline Value CastAsBoolean(const Value &value) {
    if (Type::GetInstance(TypeId::BOOLEAN)
            ->IsCoercableFrom(value.GetTypeId())) {
      if (value.IsNull())
        return ValueFactory::GetBooleanValue(PELOTON_BOOLEAN_NULL);
      switch (value.GetTypeId()) {
        case TypeId::BOOLEAN:
          return ValueFactory::GetBooleanValue(value.GetAs<int8_t>());
        case TypeId::VARCHAR: {
          std::string str = value.ToString();
          std::transform(str.begin(), str.end(), str.begin(), ::tolower);
          if (str == "true" || str == "1" || str == "t")
            return ValueFactory::GetBooleanValue(true);
          else if (str == "false" || str == "0" || str == "f")
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
