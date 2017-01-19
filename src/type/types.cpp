//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// types.cpp
//
// Identification: src/common/types.cpp
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "type/types.h"

#include <algorithm>
#include <cstring>
#include <sstream>

#include "common/exception.h"
#include "common/logger.h"
#include "common/macros.h"
#include "type/value_factory.h"
#include "util/string_util.h"

namespace peloton {

FileHandle INVALID_FILE_HANDLE;

// WARNING: It will limit scalability if tuples per tile group is too small,
// When a tile group is full, a new tile group needs to be allocated, until
// then no new insertion of new versions or tuples are available in the table.
int DEFAULT_TUPLES_PER_TILEGROUP = 1000;
int TEST_TUPLES_PER_TILEGROUP = 5;

// For threads
size_t QUERY_THREAD_COUNT = 1;
size_t LOGGING_THREAD_COUNT = 1;
size_t GC_THREAD_COUNT = 1;
size_t EPOCH_THREAD_COUNT = 1;

//===--------------------------------------------------------------------===//
// DatePart <--> String Utilities
//===--------------------------------------------------------------------===//
std::string DatePartTypeToString(DatePartType type) {
  // IMPORTANT: You should not include any of the duplicate plural DatePartTypes
  // in this switch statement, otherwise the compiler will throw an error.
  // For example, you will want to use 'DatePartType::SECOND' and not
  // 'DatePartType::SECONDS'. Make sure that none of the returned strings
  // include the 'S' suffix.
  switch (type) {
    case DatePartType::INVALID:
      return ("INVALID");
    case DatePartType::CENTURY:
      return "CENTURY";
    case DatePartType::DAY:
      return "DAY";
    case DatePartType::DECADE:
      return "DECADE";
    case DatePartType::DOW:
      return "DOW";
    case DatePartType::DOY:
      return "DOY";
    case DatePartType::HOUR:
      return "HOUR";
    case DatePartType::MICROSECOND:
      return "MICROSECOND";
    case DatePartType::MILLENNIUM:
      return "MILLENNIUM";
    case DatePartType::MILLISECOND:
      return "MILLISECOND";
    case DatePartType::MINUTE:
      return "MINUTE";
    case DatePartType::MONTH:
      return "MONTH";
    case DatePartType::QUARTER:
      return "QUARTER";
    case DatePartType::SECOND:
      return "SECOND";
    case DatePartType::WEEK:
      return "WEEK";
    case DatePartType::YEAR:
      return "YEAR";
    default: {
      throw ConversionException(
          StringUtil::Format("No string conversion for DatePart value '%d'",
                             static_cast<int>(type)));
    }
  }
  return ("INVALID");
}

DatePartType StringToDatePartType(const std::string& str) {
  std::string upper_str = StringUtil::Upper(str);
  if (upper_str == "INVALID") {
    return DatePartType::INVALID;
  } else if (upper_str == "CENTURY") {
    return DatePartType::CENTURY;
  } else if (upper_str == "DAY") {
    return DatePartType::DAY;
  } else if (upper_str == "DAYS") {
    return DatePartType::DAYS;
  } else if (upper_str == "DECADE") {
    return DatePartType::DECADE;
  } else if (upper_str == "DECADES") {
    return DatePartType::DECADES;
  } else if (upper_str == "DOW") {
    return DatePartType::DOW;
  } else if (upper_str == "DOY") {
    return DatePartType::DOY;
  } else if (upper_str == "HOUR") {
    return DatePartType::HOUR;
  } else if (upper_str == "HOURS") {
    return DatePartType::HOURS;
  } else if (upper_str == "MICROSECOND") {
    return DatePartType::MICROSECOND;
  } else if (upper_str == "MICROSECONDS") {
    return DatePartType::MICROSECONDS;
  } else if (upper_str == "MILLENNIUM") {
    return DatePartType::MILLENNIUM;
  } else if (upper_str == "MILLISECOND") {
    return DatePartType::MILLISECOND;
  } else if (upper_str == "MILLISECONDS") {
    return DatePartType::MILLISECONDS;
  } else if (upper_str == "MINUTE") {
    return DatePartType::MINUTE;
  } else if (upper_str == "MINUTES") {
    return DatePartType::MINUTES;
  } else if (upper_str == "MONTH") {
    return DatePartType::MONTH;
  } else if (upper_str == "MONTHS") {
    return DatePartType::MONTHS;
  } else if (upper_str == "QUARTER") {
    return DatePartType::QUARTER;
  } else if (upper_str == "QUARTERS") {
    return DatePartType::QUARTERS;
  } else if (upper_str == "SECOND") {
    return DatePartType::SECONDS;
  } else if (upper_str == "SECONDS") {
    return DatePartType::SECOND;
  } else if (upper_str == "WEEK") {
    return DatePartType::WEEK;
  } else if (upper_str == "WEEKS") {
    return DatePartType::WEEKS;
  } else if (upper_str == "YEAR") {
    return DatePartType::YEAR;
  } else if (upper_str == "YEARS") {
    return DatePartType::YEARS;
  } else {
    throw ConversionException(StringUtil::Format(
        "No DatePartType conversion from string '%s'", upper_str.c_str()));
  }
}

std::ostream& operator<<(std::ostream& os, const DatePartType& type) {
  os << DatePartTypeToString(type);
  return os;
}

//===--------------------------------------------------------------------===//
// BackendType <--> String Utilities
//===--------------------------------------------------------------------===//

std::string BackendTypeToString(BackendType type) {
  switch (type) {
    case (BackendType::MM):
      return "MM";
    case (BackendType::NVM):
      return "NVM";
    case (BackendType::SSD):
      return "SSD";
    case (BackendType::HDD):
      return "HDD";
    case (BackendType::INVALID):
      return "INVALID";
    default: {
      throw ConversionException(
          StringUtil::Format("No string conversion for BackendType value '%d'",
                             static_cast<int>(type)));
    }
  }
  return ("INVALID");
}

BackendType StringToBackendType(const std::string& str) {
  if (str == "INVALID") {
    return BackendType::INVALID;
  } else if (str == "MM") {
    return BackendType::MM;
  } else if (str == "NVM") {
    return BackendType::NVM;
  } else if (str == "SSD") {
    return BackendType::SSD;
  } else if (str == "HDD") {
    return BackendType::HDD;
  } else {
    throw ConversionException(StringUtil::Format(
        "No BackendType conversion from string '%s'", str.c_str()));
  }
  return BackendType::INVALID;
}

std::ostream& operator<<(std::ostream& os, const BackendType& type) {
  os << BackendTypeToString(type);
  return os;
}

//===--------------------------------------------------------------------===//
// Value <--> String Utilities
//===--------------------------------------------------------------------===//

std::string TypeIdToString(type::Type::TypeId type) {
  switch (type) {
    case type::Type::INVALID:
      return "INVALID";
    case type::Type::PARAMETER_OFFSET:
      return "PARAMETER_OFFSET";
    case type::Type::BOOLEAN:
      return "BOOLEAN";
    case type::Type::TINYINT:
      return "TINYINT";
    case type::Type::SMALLINT:
      return "SMALLINT";
    case type::Type::INTEGER:
      return "INTEGER";
    case type::Type::BIGINT:
      return "BIGINT";
    case type::Type::DECIMAL:
      return "DECIMAL";
    case type::Type::TIMESTAMP:
      return "TIMESTAMP";
    case type::Type::DATE:
      return "DATE";
    case type::Type::VARCHAR:
      return "VARCHAR";
    case type::Type::VARBINARY:
      return "VARBINARY";
    case type::Type::ARRAY:
      return "ARRAY";
    case type::Type::UDT:
      return "UDT";
    default: {
      throw ConversionException(
          StringUtil::Format("No string conversion for TypeId value '%d'",
                             static_cast<int>(type)));
    }
      return "INVALID";
  }
}

type::Type::TypeId StringToTypeId(const std::string& str) {
  std::string upper_str = StringUtil::Upper(str);
  if (upper_str == "INVALID") {
    return type::Type::INVALID;
  } else if (upper_str == "PARAMETER_OFFSET") {
    return type::Type::PARAMETER_OFFSET;
  } else if (upper_str == "BOOLEAN") {
    return type::Type::BOOLEAN;
  } else if (upper_str == "TINYINT") {
    return type::Type::TINYINT;
  } else if (upper_str == "SMALLINT") {
    return type::Type::SMALLINT;
  } else if (upper_str == "INTEGER") {
    return type::Type::INTEGER;
  } else if (upper_str == "BIGINT") {
    return type::Type::BIGINT;
  } else if (upper_str == "DECIMAL") {
    return type::Type::DECIMAL;
  } else if (upper_str == "TIMESTAMP") {
    return type::Type::TIMESTAMP;
  } else if (upper_str == "DATE") {
    return type::Type::DATE;
  } else if (upper_str == "VARCHAR") {
    return type::Type::VARCHAR;
  } else if (upper_str == "VARBINARY") {
    return type::Type::VARBINARY;
  } else if (upper_str == "ARRAY") {
    return type::Type::ARRAY;
  } else if (upper_str == "UDT") {
    return type::Type::UDT;
  } else {
    throw ConversionException(StringUtil::Format(
        "No TypeId conversion from string '%s'", upper_str.c_str()));
  }
  return type::Type::INVALID;
}

/** takes in 0-F, returns 0-15 */
int32_t HexCharToInt(char c) {
  c = static_cast<char>(toupper(c));
  if ((c < '0' || c > '9') && (c < 'A' || c > 'F')) {
    return -1;
  }
  int32_t retval;
  if (c >= 'A')
    retval = c - 'A' + 10;
  else
    retval = c - '0';

  PL_ASSERT(retval >= 0 && retval < 16);
  return retval;
}

bool HexDecodeToBinary(unsigned char* bufferdst, const char* hexString) {
  PL_ASSERT(hexString);
  size_t len = strlen(hexString);
  if ((len % 2) != 0) return false;
  uint32_t i;
  for (i = 0; i < len / 2; i++) {
    int32_t high = HexCharToInt(hexString[i * 2]);
    int32_t low = HexCharToInt(hexString[i * 2 + 1]);
    if ((high == -1) || (low == -1)) return false;
    int32_t result = high * 16 + low;

    PL_ASSERT(result >= 0 && result < 256);
    bufferdst[i] = static_cast<unsigned char>(result);
  }
  return true;
}

//===--------------------------------------------------------------------===//
// Statement - String Utilities
//===--------------------------------------------------------------------===//

std::string StatementTypeToString(StatementType type) {
  switch (type) {
    case StatementType::SELECT: {
      return "SELECT";
    }
    case StatementType::ALTER: {
      return "ALTER";
    }
    case StatementType::CREATE: {
      return "CREATE";
    }
    case StatementType::DELETE: {
      return "DELETE";
    }
    case StatementType::DROP: {
      return "DROP";
    }
    case StatementType::EXECUTE: {
      return "EXECUTE";
    }
    case StatementType::COPY: {
      return "COPY";
    }
    case StatementType::INSERT: {
      return "INSERT";
    }
    case StatementType::INVALID: {
      return "INVALID";
    }
    case StatementType::PREPARE: {
      return "PREPARE";
    }
    case StatementType::RENAME: {
      return "RENAME";
    }
    case StatementType::TRANSACTION: {
      return "TRANSACTION";
    }
    case StatementType::UPDATE: {
      return "UPDATE";
    }
    default: {
      throw ConversionException(StringUtil::Format(
          "No string conversion for StatementType value '%d'",
          static_cast<int>(type)));
    }
  }
  return "INVALID";
}

StatementType StringToStatementType(const std::string& str) {
  std::string upper_str = StringUtil::Upper(str);
  if (upper_str == "INVALID") {
    return StatementType::INVALID;
  } else if (upper_str == "SELECT") {
    return StatementType::SELECT;
  } else if (upper_str == "INSERT") {
    return StatementType::INSERT;
  } else if (upper_str == "UPDATE") {
    return StatementType::UPDATE;
  } else if (upper_str == "DELETE") {
    return StatementType::DELETE;
  } else if (upper_str == "CREATE") {
    return StatementType::CREATE;
  } else if (upper_str == "DROP") {
    return StatementType::DROP;
  } else if (upper_str == "PREPARE") {
    return StatementType::PREPARE;
  } else if (upper_str == "EXECUTE") {
    return StatementType::EXECUTE;
  } else if (upper_str == "RENAME") {
    return StatementType::RENAME;
  } else if (upper_str == "ALTER") {
    return StatementType::ALTER;
  } else if (upper_str == "TRANSACTION") {
    return StatementType::TRANSACTION;
  } else if (upper_str == "COPY") {
    return StatementType::COPY;
  } else {
    throw ConversionException(StringUtil::Format(
        "No StatementType conversion from string '%s'", upper_str.c_str()));
  }
  return StatementType::INVALID;
}
std::ostream& operator<<(std::ostream& os, const StatementType& type) {
  os << StatementTypeToString(type);
  return os;
}

//===--------------------------------------------------------------------===//
// Expression - String Utilities
//===--------------------------------------------------------------------===//

std::string ExpressionTypeToString(ExpressionType type) {
  switch (type) {
    case ExpressionType::INVALID: {
      return ("INVALID");
    }
    case ExpressionType::OPERATOR_PLUS: {
      return ("OPERATOR_PLUS");
    }
    case ExpressionType::OPERATOR_MINUS: {
      return ("OPERATOR_MINUS");
    }
    case ExpressionType::OPERATOR_MULTIPLY: {
      return ("OPERATOR_MULTIPLY");
    }
    case ExpressionType::OPERATOR_DIVIDE: {
      return ("OPERATOR_DIVIDE");
    }
    case ExpressionType::OPERATOR_CONCAT: {
      return ("OPERATOR_CONCAT");
    }
    case ExpressionType::OPERATOR_MOD: {
      return ("OPERATOR_MOD");
    }
    case ExpressionType::OPERATOR_CAST: {
      return ("OPERATOR_CAST");
    }
    case ExpressionType::OPERATOR_NOT: {
      return ("OPERATOR_NOT");
    }
    case ExpressionType::OPERATOR_IS_NULL: {
      return ("OPERATOR_IS_NULL");
    }
    case ExpressionType::OPERATOR_EXISTS: {
      return ("OPERATOR_EXISTS");
    }
    case ExpressionType::OPERATOR_UNARY_MINUS: {
      return ("OPERATOR_UNARY_MINUS");
    }
    case ExpressionType::COMPARE_EQUAL: {
      return ("COMPARE_EQUAL");
    }
    case ExpressionType::COMPARE_NOTEQUAL: {
      return ("COMPARE_NOTEQUAL");
    }
    case ExpressionType::COMPARE_LESSTHAN: {
      return ("COMPARE_LESSTHAN");
    }
    case ExpressionType::COMPARE_GREATERTHAN: {
      return ("COMPARE_GREATERTHAN");
    }
    case ExpressionType::COMPARE_LESSTHANOREQUALTO: {
      return ("COMPARE_LESSTHANOREQUALTO");
    }
    case ExpressionType::COMPARE_GREATERTHANOREQUALTO: {
      return ("COMPARE_GREATERTHANOREQUALTO");
    }
    case ExpressionType::COMPARE_LIKE: {
      return ("COMPARE_LIKE");
    }
    case ExpressionType::COMPARE_NOTLIKE: {
      return ("COMPARE_NOTLIKE");
    }
    case ExpressionType::COMPARE_IN: {
      return ("COMPARE_IN");
    }
    case ExpressionType::CONJUNCTION_AND: {
      return ("CONJUNCTION_AND");
    }
    case ExpressionType::CONJUNCTION_OR: {
      return ("CONJUNCTION_OR");
    }
    case ExpressionType::VALUE_CONSTANT: {
      return ("VALUE_CONSTANT");
    }
    case ExpressionType::VALUE_PARAMETER: {
      return ("VALUE_PARAMETER");
    }
    case ExpressionType::VALUE_TUPLE: {
      return ("VALUE_TUPLE");
    }
    case ExpressionType::VALUE_TUPLE_ADDRESS: {
      return ("VALUE_TUPLE_ADDRESS");
    }
    case ExpressionType::VALUE_NULL: {
      return ("VALUE_NULL");
    }
    case ExpressionType::VALUE_VECTOR: {
      return ("VALUE_VECTOR");
    }
    case ExpressionType::VALUE_SCALAR: {
      return ("VALUE_SCALAR");
    }
    case ExpressionType::AGGREGATE_COUNT: {
      return ("AGGREGATE_COUNT");
    }
    case ExpressionType::AGGREGATE_COUNT_STAR: {
      return ("AGGREGATE_COUNT_STAR");
    }
    case ExpressionType::AGGREGATE_SUM: {
      return ("AGGREGATE_SUM");
    }
    case ExpressionType::AGGREGATE_MIN: {
      return ("AGGREGATE_MIN");
    }
    case ExpressionType::AGGREGATE_MAX: {
      return ("AGGREGATE_MAX");
    }
    case ExpressionType::AGGREGATE_AVG: {
      return ("AGGREGATE_AVG");
    }
    case ExpressionType::FUNCTION: {
      return ("FUNCTION");
    }
    case ExpressionType::HASH_RANGE: {
      return ("HASH_RANGE");
    }
    case ExpressionType::OPERATOR_CASE_EXPR: {
      return ("OPERATOR_CASE_EXPR");
    }
    case ExpressionType::OPERATOR_NULLIF: {
      return ("OPERATOR_NULLIF");
    }
    case ExpressionType::OPERATOR_COALESCE: {
      return ("OPERATOR_COALESCE");
    }
    case ExpressionType::ROW_SUBQUERY: {
      return ("ROW_SUBQUERY");
    }
    case ExpressionType::SELECT_SUBQUERY: {
      return ("SELECT_SUBQUERY");
    }
    case ExpressionType::SUBSTR: {
      return ("SUBSTR");
    }
    case ExpressionType::ASCII: {
      return ("ASCII");
    }
    case ExpressionType::OCTET_LEN: {
      return ("OCTET_LEN");
    }
    case ExpressionType::CHAR: {
      return ("CHAR");
    }
    case ExpressionType::CHAR_LEN: {
      return ("CHAR_LEN");
    }
    case ExpressionType::SPACE: {
      return ("SPACE");
    }
    case ExpressionType::REPEAT: {
      return ("REPEAT");
    }
    case ExpressionType::POSITION: {
      return ("POSITION");
    }
    case ExpressionType::LEFT: {
      return ("LEFT");
    }
    case ExpressionType::RIGHT: {
      return ("RIGHT");
    }
    case ExpressionType::CONCAT: {
      return ("CONCAT");
    }
    case ExpressionType::LTRIM: {
      return ("LTRIM");
    }
    case ExpressionType::RTRIM: {
      return ("RTRIM");
    }
    case ExpressionType::BTRIM: {
      return ("BTRIM");
    }
    case ExpressionType::REPLACE: {
      return ("REPLACE");
    }
    case ExpressionType::OVERLAY: {
      return ("OVERLAY");
    }
    case ExpressionType::EXTRACT: {
      return ("EXTRACT");
    }
    case ExpressionType::DATE_TO_TIMESTAMP: {
      return ("DATE_TO_TIMESTAMP");
    }
    case ExpressionType::STAR: {
      return ("STAR");
    }
    case ExpressionType::PLACEHOLDER: {
      return ("PLACEHOLDER");
    }
    case ExpressionType::COLUMN_REF: {
      return ("COLUMN_REF");
    }
    case ExpressionType::FUNCTION_REF: {
      return ("FUNCTION_REF");
    }
    case ExpressionType::CAST: {
      return ("CAST");
    }
    default: {
      throw ConversionException(StringUtil::Format(
          "No string conversion for ExpressionType value '%d'",
          static_cast<int>(type)));
    } break;
  }
  return "INVALID";
}

ExpressionType ParserExpressionNameToExpressionType(const std::string& str) {
  std::string lower_str = str;
  std::transform(lower_str.begin(), lower_str.end(), lower_str.begin(),
                 ::tolower);
  if (str == "count") {
    return ExpressionType::AGGREGATE_COUNT;
  } else if (str == "sum") {
    return ExpressionType::AGGREGATE_SUM;
  } else if (str == "avg") {
    return ExpressionType::AGGREGATE_AVG;
  } else if (str == "max") {
    return ExpressionType::AGGREGATE_MAX;
  } else if (str == "min") {
    return ExpressionType::AGGREGATE_MIN;
  }
  return ExpressionType::INVALID;
}

ExpressionType StringToExpressionType(const std::string& str) {
  std::string upper_str = StringUtil::Upper(str);
  if (upper_str == "INVALID") {
    return ExpressionType::INVALID;
  } else if (upper_str == "OPERATOR_PLUS") {
    return ExpressionType::OPERATOR_PLUS;
  } else if (upper_str == "OPERATOR_MINUS") {
    return ExpressionType::OPERATOR_MINUS;
  } else if (upper_str == "OPERATOR_MULTIPLY") {
    return ExpressionType::OPERATOR_MULTIPLY;
  } else if (upper_str == "OPERATOR_DIVIDE") {
    return ExpressionType::OPERATOR_DIVIDE;
  } else if (upper_str == "OPERATOR_CONCAT") {
    return ExpressionType::OPERATOR_CONCAT;
  } else if (upper_str == "OPERATOR_MOD") {
    return ExpressionType::OPERATOR_MOD;
  } else if (upper_str == "OPERATOR_CAST") {
    return ExpressionType::OPERATOR_CAST;
  } else if (upper_str == "OPERATOR_NOT") {
    return ExpressionType::OPERATOR_NOT;
  } else if (upper_str == "OPERATOR_IS_NULL") {
    return ExpressionType::OPERATOR_IS_NULL;
  } else if (upper_str == "OPERATOR_EXISTS") {
    return ExpressionType::OPERATOR_EXISTS;
  } else if (upper_str == "OPERATOR_UNARY_MINUS") {
    return ExpressionType::OPERATOR_UNARY_MINUS;
  } else if (upper_str == "COMPARE_EQUAL") {
    return ExpressionType::COMPARE_EQUAL;
  } else if (upper_str == "COMPARE_NOTEQUAL") {
    return ExpressionType::COMPARE_NOTEQUAL;
  } else if (upper_str == "COMPARE_LESSTHAN") {
    return ExpressionType::COMPARE_LESSTHAN;
  } else if (upper_str == "COMPARE_GREATERTHAN") {
    return ExpressionType::COMPARE_GREATERTHAN;
  } else if (upper_str == "COMPARE_LESSTHANOREQUALTO") {
    return ExpressionType::COMPARE_LESSTHANOREQUALTO;
  } else if (upper_str == "COMPARE_GREATERTHANOREQUALTO") {
    return ExpressionType::COMPARE_GREATERTHANOREQUALTO;
  } else if (upper_str == "COMPARE_LIKE") {
    return ExpressionType::COMPARE_LIKE;
  } else if (upper_str == "COMPARE_NOTLIKE") {
    return ExpressionType::COMPARE_NOTLIKE;
  } else if (upper_str == "COMPARE_IN") {
    return ExpressionType::COMPARE_IN;
  } else if (upper_str == "CONJUNCTION_AND") {
    return ExpressionType::CONJUNCTION_AND;
  } else if (upper_str == "CONJUNCTION_OR") {
    return ExpressionType::CONJUNCTION_OR;
  } else if (upper_str == "VALUE_CONSTANT") {
    return ExpressionType::VALUE_CONSTANT;
  } else if (upper_str == "VALUE_PARAMETER") {
    return ExpressionType::VALUE_PARAMETER;
  } else if (upper_str == "VALUE_TUPLE") {
    return ExpressionType::VALUE_TUPLE;
  } else if (upper_str == "VALUE_TUPLE_ADDRESS") {
    return ExpressionType::VALUE_TUPLE_ADDRESS;
  } else if (upper_str == "VALUE_NULL") {
    return ExpressionType::VALUE_NULL;
  } else if (upper_str == "VALUE_VECTOR") {
    return ExpressionType::VALUE_VECTOR;
  } else if (upper_str == "VALUE_SCALAR") {
    return ExpressionType::VALUE_SCALAR;
  } else if (upper_str == "AGGREGATE_COUNT") {
    return ExpressionType::AGGREGATE_COUNT;
  } else if (upper_str == "AGGREGATE_COUNT_STAR") {
    return ExpressionType::AGGREGATE_COUNT_STAR;
  } else if (upper_str == "AGGREGATE_SUM") {
    return ExpressionType::AGGREGATE_SUM;
  } else if (upper_str == "AGGREGATE_MIN") {
    return ExpressionType::AGGREGATE_MIN;
  } else if (upper_str == "AGGREGATE_MAX") {
    return ExpressionType::AGGREGATE_MAX;
  } else if (upper_str == "AGGREGATE_AVG") {
    return ExpressionType::AGGREGATE_AVG;
  } else if (upper_str == "FUNCTION") {
    return ExpressionType::FUNCTION;
  } else if (upper_str == "HASH_RANGE") {
    return ExpressionType::HASH_RANGE;
  } else if (upper_str == "OPERATOR_CASE_EXPR") {
    return ExpressionType::OPERATOR_CASE_EXPR;
  } else if (upper_str == "OPERATOR_NULLIF") {
    return ExpressionType::OPERATOR_NULLIF;
  } else if (upper_str == "OPERATOR_COALESCE") {
    return ExpressionType::OPERATOR_COALESCE;
  } else if (upper_str == "ROW_SUBQUERY") {
    return ExpressionType::ROW_SUBQUERY;
  } else if (upper_str == "SELECT_SUBQUERY") {
    return ExpressionType::SELECT_SUBQUERY;
  } else if (upper_str == "SUBSTR") {
    return ExpressionType::SUBSTR;
  } else if (upper_str == "ASCII") {
    return ExpressionType::ASCII;
  } else if (upper_str == "OCTET_LEN") {
    return ExpressionType::OCTET_LEN;
  } else if (upper_str == "CHAR") {
    return ExpressionType::CHAR;
  } else if (upper_str == "CHAR_LEN") {
    return ExpressionType::CHAR_LEN;
  } else if (upper_str == "SPACE") {
    return ExpressionType::SPACE;
  } else if (upper_str == "REPEAT") {
    return ExpressionType::REPEAT;
  } else if (upper_str == "POSITION") {
    return ExpressionType::POSITION;
  } else if (upper_str == "LEFT") {
    return ExpressionType::LEFT;
  } else if (upper_str == "RIGHT") {
    return ExpressionType::RIGHT;
  } else if (upper_str == "CONCAT") {
    return ExpressionType::CONCAT;
  } else if (upper_str == "LTRIM") {
    return ExpressionType::LTRIM;
  } else if (upper_str == "RTRIM") {
    return ExpressionType::RTRIM;
  } else if (upper_str == "BTRIM") {
    return ExpressionType::BTRIM;
  } else if (upper_str == "REPLACE") {
    return ExpressionType::REPLACE;
  } else if (upper_str == "OVERLAY") {
    return ExpressionType::OVERLAY;
  } else if (upper_str == "EXTRACT") {
    return ExpressionType::EXTRACT;
  } else if (upper_str == "DATE_TO_TIMESTAMP") {
    return ExpressionType::DATE_TO_TIMESTAMP;
  } else if (upper_str == "STAR") {
    return ExpressionType::STAR;
  } else if (upper_str == "PLACEHOLDER") {
    return ExpressionType::PLACEHOLDER;
  } else if (upper_str == "COLUMN_REF") {
    return ExpressionType::COLUMN_REF;
  } else if (upper_str == "FUNCTION_REF") {
    return ExpressionType::FUNCTION_REF;
  } else if (upper_str == "CAST") {
    return ExpressionType::CAST;
  } else {
    throw ConversionException(StringUtil::Format(
        "No ExpressionType conversion from string '%s'", upper_str.c_str()));
  }
  return ExpressionType::INVALID;
}

std::ostream& operator<<(std::ostream& os, const ExpressionType& type) {
  os << ExpressionTypeToString(type);
  return os;
}

//===--------------------------------------------------------------------===//
// Index Method Type - String Utilities
//===--------------------------------------------------------------------===//

std::string IndexTypeToString(IndexType type) {
  switch (type) {
    case IndexType::INVALID: {
      return "INVALID";
    }
    case IndexType::BWTREE: {
      return "BWTREE";
    }
    case IndexType::HASH: {
      return "HASH";
    }
    default: {
      throw ConversionException(
          StringUtil::Format("No string conversion for IndexType value '%d'",
                             static_cast<int>(type)));
    }
  }
  return "INVALID";
}

IndexType StringToIndexType(const std::string& str) {
  std::string upper_str = StringUtil::Upper(str);
  if (upper_str == "INVALID") {
    return IndexType::INVALID;
  } else if (upper_str == "BWTREE") {
    return IndexType::BWTREE;
  } else if (upper_str == "HASH") {
    return IndexType::HASH;
  } else {
    throw ConversionException(StringUtil::Format(
        "No IndexType conversion from string '%s'", upper_str.c_str()));
  }
  return IndexType::INVALID;
}

std::ostream& operator<<(std::ostream& os, const IndexType& type) {
  os << IndexTypeToString(type);
  return os;
}

//===--------------------------------------------------------------------===//
// IndexConstraintType - String Utilities
//===--------------------------------------------------------------------===//

std::string IndexConstraintTypeToString(IndexConstraintType type) {
  switch (type) {
    case IndexConstraintType::INVALID: {
      return "INVALID";
    }
    case IndexConstraintType::DEFAULT: {
      return "NORMAL";
    }
    case IndexConstraintType::PRIMARY_KEY: {
      return "PRIMARY_KEY";
    }
    case IndexConstraintType::UNIQUE: {
      return "UNIQUE";
    }
    default: {
      throw ConversionException(StringUtil::Format(
          "No string conversion for IndexConstraintType value '%d'",
          static_cast<int>(type)));
    }
  }
  return "INVALID";
}

IndexConstraintType StringToIndexConstraintType(const std::string& str) {
  std::string upper_str = StringUtil::Upper(str);
  if (upper_str == "INVALID") {
    return IndexConstraintType::INVALID;
  } else if (upper_str == "NORMAL") {
    return IndexConstraintType::DEFAULT;
  } else if (upper_str == "PRIMARY_KEY") {
    return IndexConstraintType::PRIMARY_KEY;
  } else if (upper_str == "UNIQUE") {
    return IndexConstraintType::UNIQUE;
  } else {
    throw ConversionException(
        StringUtil::Format("No IndexConstraintType conversion from string '%s'",
                           upper_str.c_str()));
  }
  return IndexConstraintType::INVALID;
}

std::ostream& operator<<(std::ostream& os, const IndexConstraintType& type) {
  os << IndexConstraintTypeToString(type);
  return os;
}

//===--------------------------------------------------------------------===//
// HybridScanType - String Utilities
//===--------------------------------------------------------------------===//

std::string HybridScanTypeToString(HybridScanType type) {
  switch (type) {
    case HybridScanType::INVALID: {
      return "INVALID";
    }
    case HybridScanType::SEQUENTIAL: {
      return "SEQUENTIAL";
    }
    case HybridScanType::INDEX: {
      return "INDEX";
    }
    case HybridScanType::HYBRID: {
      return "HYBRID";
    }
    default: {
      throw ConversionException(StringUtil::Format(
          "No string conversion for HybridScanType value '%d'",
          static_cast<int>(type)));
    }
  }
  return "INVALID";
}

HybridScanType StringToHybridScanType(const std::string& str) {
  std::string upper_str = StringUtil::Upper(str);
  if (upper_str == "INVALID") {
    return HybridScanType::INVALID;
  } else if (upper_str == "SEQUENTIAL") {
    return HybridScanType::SEQUENTIAL;
  } else if (upper_str == "INDEX") {
    return HybridScanType::INDEX;
  } else if (upper_str == "HYBRID") {
    return HybridScanType::HYBRID;
  } else {
    throw ConversionException(StringUtil::Format(
        "No HybridScanType conversion from string '%s'", upper_str.c_str()));
  }
  return HybridScanType::INVALID;
}

std::ostream& operator<<(std::ostream& os, const HybridScanType& type) {
  os << HybridScanTypeToString(type);
  return os;
}

//===--------------------------------------------------------------------===//
// Plan Node - String Utilities
//===--------------------------------------------------------------------===//

std::string PlanNodeTypeToString(PlanNodeType type) {
  switch (type) {
    case PlanNodeType::INVALID: {
      return ("INVALID");
    }
    case PlanNodeType::ABSTRACT_SCAN: {
      return ("ABSTRACT_SCAN");
    }
    case PlanNodeType::SEQSCAN: {
      return ("SEQSCAN");
    }
    case PlanNodeType::INDEXSCAN: {
      return ("INDEXSCAN");
    }
    case PlanNodeType::NESTLOOP: {
      return ("NESTLOOP");
    }
    case PlanNodeType::NESTLOOPINDEX: {
      return ("NESTLOOPINDEX");
    }
    case PlanNodeType::MERGEJOIN: {
      return ("MERGEJOIN");
    }
    case PlanNodeType::HASHJOIN: {
      return ("HASHJOIN");
    }
    case PlanNodeType::UPDATE: {
      return ("UPDATE");
    }
    case PlanNodeType::INSERT: {
      return ("INSERT");
    }
    case PlanNodeType::DELETE: {
      return ("DELETE");
    }
    case PlanNodeType::DROP: {
      return ("DROP");
    }
    case PlanNodeType::CREATE: {
      return ("CREATE");
    }
    case PlanNodeType::SEND: {
      return ("SEND");
    }
    case PlanNodeType::RECEIVE: {
      return ("RECEIVE");
    }
    case PlanNodeType::PRINT: {
      return ("PRINT");
    }
    case PlanNodeType::AGGREGATE: {
      return ("AGGREGATE");
    }
    case PlanNodeType::UNION: {
      return ("UNION");
    }
    case PlanNodeType::ORDERBY: {
      return ("ORDERBY");
    }
    case PlanNodeType::PROJECTION: {
      return ("PROJECTION");
    }
    case PlanNodeType::MATERIALIZE: {
      return ("MATERIALIZE");
    }
    case PlanNodeType::LIMIT: {
      return ("LIMIT");
    }
    case PlanNodeType::DISTINCT: {
      return ("DISTINCT");
    }
    case PlanNodeType::SETOP: {
      return ("SETOP");
    }
    case PlanNodeType::APPEND: {
      return ("APPEND");
    }
    case PlanNodeType::AGGREGATE_V2: {
      return ("AGGREGATE_V2");
    }
    case PlanNodeType::HASH: {
      return ("HASH");
    }
    case PlanNodeType::RESULT: {
      return ("RESULT");
    }
    case PlanNodeType::COPY: {
      return ("COPY");
    }
    case PlanNodeType::MOCK: {
      return ("MOCK");
    }
    default: {
      throw ConversionException(
          StringUtil::Format("No string conversion for PlanNodeType value '%d'",
                             static_cast<int>(type)));
    }
  }
  return "INVALID";
}

PlanNodeType StringToPlanNodeType(const std::string& str) {
  std::string upper_str = StringUtil::Upper(str);
  if (upper_str == "INVALID") {
    return PlanNodeType::INVALID;
  } else if (upper_str == "ABSTRACT_SCAN") {
    return PlanNodeType::ABSTRACT_SCAN;
  } else if (upper_str == "SEQSCAN") {
    return PlanNodeType::SEQSCAN;
  } else if (upper_str == "INDEXSCAN") {
    return PlanNodeType::INDEXSCAN;
  } else if (upper_str == "NESTLOOP") {
    return PlanNodeType::NESTLOOP;
  } else if (upper_str == "NESTLOOPINDEX") {
    return PlanNodeType::NESTLOOPINDEX;
  } else if (upper_str == "MERGEJOIN") {
    return PlanNodeType::MERGEJOIN;
  } else if (upper_str == "HASHJOIN") {
    return PlanNodeType::HASHJOIN;
  } else if (upper_str == "UPDATE") {
    return PlanNodeType::UPDATE;
  } else if (upper_str == "INSERT") {
    return PlanNodeType::INSERT;
  } else if (upper_str == "DELETE") {
    return PlanNodeType::DELETE;
  } else if (upper_str == "DROP") {
    return PlanNodeType::DROP;
  } else if (upper_str == "CREATE") {
    return PlanNodeType::CREATE;
  } else if (upper_str == "SEND") {
    return PlanNodeType::SEND;
  } else if (upper_str == "RECEIVE") {
    return PlanNodeType::RECEIVE;
  } else if (upper_str == "PRINT") {
    return PlanNodeType::PRINT;
  } else if (upper_str == "AGGREGATE") {
    return PlanNodeType::AGGREGATE;
  } else if (upper_str == "UNION") {
    return PlanNodeType::UNION;
  } else if (upper_str == "ORDERBY") {
    return PlanNodeType::ORDERBY;
  } else if (upper_str == "PROJECTION") {
    return PlanNodeType::PROJECTION;
  } else if (upper_str == "MATERIALIZE") {
    return PlanNodeType::MATERIALIZE;
  } else if (upper_str == "LIMIT") {
    return PlanNodeType::LIMIT;
  } else if (upper_str == "DISTINCT") {
    return PlanNodeType::DISTINCT;
  } else if (upper_str == "SETOP") {
    return PlanNodeType::SETOP;
  } else if (upper_str == "APPEND") {
    return PlanNodeType::APPEND;
  } else if (upper_str == "AGGREGATE_V2") {
    return PlanNodeType::AGGREGATE_V2;
  } else if (upper_str == "HASH") {
    return PlanNodeType::HASH;
  } else if (upper_str == "RESULT") {
    return PlanNodeType::RESULT;
  } else if (upper_str == "COPY") {
    return PlanNodeType::COPY;
  } else if (upper_str == "MOCK") {
    return PlanNodeType::MOCK;
  } else {
    throw ConversionException(StringUtil::Format(
        "No PlanNodeType conversion from string '%s'", upper_str.c_str()));
  }
  return PlanNodeType::INVALID;
}

std::ostream& operator<<(std::ostream& os, const PlanNodeType& type) {
  os << PlanNodeTypeToString(type);
  return os;
}

//===--------------------------------------------------------------------===//
// Plan Node - String Utilities
//===--------------------------------------------------------------------===//

std::string ParseNodeTypeToString(ParseNodeType type) {
  switch (type) {
    case ParseNodeType::INVALID: {
      return "INVALID";
    }
    case ParseNodeType::SCAN: {
      return "SCAN";
    }
    case ParseNodeType::CREATE: {
      return "CREATE";
    }
    case ParseNodeType::DROP: {
      return "DROP";
    }
    case ParseNodeType::UPDATE: {
      return "UPDATE";
    }
    case ParseNodeType::INSERT: {
      return "INSERT";
    }
    case ParseNodeType::DELETE: {
      return "DELETE";
    }
    case ParseNodeType::PREPARE: {
      return "PREPARE";
    }
    case ParseNodeType::EXECUTE: {
      return "EXECUTE";
    }
    case ParseNodeType::SELECT: {
      return "SELECT";
    }
    case ParseNodeType::JOIN_EXPR: {
      return "JOIN_EXPR";
    }
    case ParseNodeType::TABLE: {
      return "TABLE";
    }
    case ParseNodeType::MOCK: {
      return "MOCK";
    }
    default: {
      throw ConversionException(StringUtil::Format(
          "No string conversion for ParseNodeType value '%d'",
          static_cast<int>(type)));
    }
  }
  return "INVALID";
}

ParseNodeType StringToParseNodeType(const std::string& str) {
  std::string upper_str = StringUtil::Upper(str);
  if (upper_str == "INVALID") {
    return ParseNodeType::INVALID;
  } else if (upper_str == "SCAN") {
    return ParseNodeType::SCAN;
  } else if (upper_str == "CREATE") {
    return ParseNodeType::CREATE;
  } else if (upper_str == "DROP") {
    return ParseNodeType::DROP;
  } else if (upper_str == "UPDATE") {
    return ParseNodeType::UPDATE;
  } else if (upper_str == "INSERT") {
    return ParseNodeType::INSERT;
  } else if (upper_str == "DELETE") {
    return ParseNodeType::DELETE;
  } else if (upper_str == "PREPARE") {
    return ParseNodeType::PREPARE;
  } else if (upper_str == "EXECUTE") {
    return ParseNodeType::EXECUTE;
  } else if (upper_str == "SELECT") {
    return ParseNodeType::SELECT;
  } else if (upper_str == "JOIN_EXPR") {
    return ParseNodeType::JOIN_EXPR;
  } else if (upper_str == "TABLE") {
    return ParseNodeType::TABLE;
  } else if (upper_str == "MOCK") {
    return ParseNodeType::MOCK;
  } else {
    throw ConversionException(StringUtil::Format(
        "No ParseNodeType conversion from string '%s'", upper_str.c_str()));
  }
  return ParseNodeType::INVALID;
}

std::ostream& operator<<(std::ostream& os, const ParseNodeType& type) {
  os << ParseNodeTypeToString(type);
  return os;
}

//===--------------------------------------------------------------------===//
// JoinType - String Utilities
//===--------------------------------------------------------------------===//

std::string JoinTypeToString(JoinType type) {
  switch (type) {
    case JoinType::INVALID: {
      return "INVALID";
    }
    case JoinType::LEFT: {
      return "LEFT";
    }
    case JoinType::RIGHT: {
      return "RIGHT";
    }
    case JoinType::INNER: {
      return "INNER";
    }
    case JoinType::OUTER: {
      return "OUTER";
    }
    case JoinType::SEMI: {
      return "SEMI";
    }
    default: {
      throw ConversionException(
          StringUtil::Format("No string conversion for JoinType value '%d'",
                             static_cast<int>(type)));
    }
  }
  return "INVALID";
}

JoinType StringToJoinType(const std::string& str) {
  std::string upper_str = StringUtil::Upper(str);
  if (upper_str == "INVALID") {
    return JoinType::INVALID;
  } else if (upper_str == "LEFT") {
    return JoinType::LEFT;
  } else if (upper_str == "RIGHT") {
    return JoinType::RIGHT;
  } else if (upper_str == "INNER") {
    return JoinType::INNER;
  } else if (upper_str == "OUTER") {
    return JoinType::OUTER;
  } else if (upper_str == "SEMI") {
    return JoinType::SEMI;
  } else {
    throw ConversionException(StringUtil::Format(
        "No JoinType conversion from string '%s'", upper_str.c_str()));
  }
  return JoinType::INVALID;
}

std::ostream& operator<<(std::ostream& os, const JoinType& type) {
  os << JoinTypeToString(type);
  return os;
}

//===--------------------------------------------------------------------===//
// ResultType - String Utilities
//===--------------------------------------------------------------------===//

std::string ResultTypeToString(ResultType type) {
  switch (type) {
    case ResultType::INVALID: {
      return ("INVALID");
    }
    case ResultType::SUCCESS: {
      return ("SUCCESS");
    }
    case ResultType::FAILURE: {
      return ("FAILURE");
    }
    case ResultType::ABORTED: {
      return ("ABORTED");
    }
    case ResultType::NOOP: {
      return ("NOOP");
    }
    case ResultType::UNKNOWN: {
      return ("UNKNOWN");
    }
    default: {
      throw ConversionException(
          StringUtil::Format("No string conversion for ResultType value '%d'",
                             static_cast<int>(type)));
    }
  }
  return "INVALID";
}

ResultType StringToResultType(const std::string& str) {
  std::string upper_str = StringUtil::Upper(str);
  if (upper_str == "INVALID") {
    return ResultType::INVALID;
  } else if (upper_str == "SUCCESS") {
    return ResultType::SUCCESS;
  } else if (upper_str == "FAILURE") {
    return ResultType::FAILURE;
  } else if (upper_str == "ABORTED") {
    return ResultType::ABORTED;
  } else if (upper_str == "NOOP") {
    return ResultType::NOOP;
  } else if (upper_str == "UNKNOWN") {
    return ResultType::UNKNOWN;
  } else {
    throw ConversionException(StringUtil::Format(
        "No ResultType conversion from string '%s'", upper_str.c_str()));
  }
  return ResultType::INVALID;
}

std::ostream& operator<<(std::ostream& os, const ResultType& type) {
  os << ResultTypeToString(type);
  return os;
}

//===--------------------------------------------------------------------===//
// Constraint Type - String Utilities
//===--------------------------------------------------------------------===//

std::string ConstraintTypeToString(ConstraintType type) {
  switch (type) {
    case CONSTRAINT_TYPE_INVALID: {
      return ("INVALID");
    }
    case CONSTRAINT_TYPE_NULL: {
      return ("NULL");
    }
    case CONSTRAINT_TYPE_NOTNULL: {
      return ("NOTNULL");
    }
    case CONSTRAINT_TYPE_DEFAULT: {
      return ("DEFAULT");
    }
    case CONSTRAINT_TYPE_CHECK: {
      return ("CHECK");
    }
    case CONSTRAINT_TYPE_PRIMARY: {
      return ("PRIMARY");
    }
    case CONSTRAINT_TYPE_UNIQUE: {
      return ("UNIQUE");
    }
    case CONSTRAINT_TYPE_FOREIGN: {
      return ("FOREIGN");
    }
    case CONSTRAINT_TYPE_EXCLUSION: {
      return ("EXCLUSION");
    }
    default: {
      throw ConversionException(StringUtil::Format(
          "No string conversion for ConstraintType value '%d'",
          static_cast<int>(type)));
    }
  }
  return "INVALID";
}

ConstraintType StringToConstraintType(const std::string& str) {
  std::string upper_str = StringUtil::Upper(str);
  if (upper_str == "INVALID") {
    return CONSTRAINT_TYPE_INVALID;
  } else if (upper_str == "NULL") {
    return CONSTRAINT_TYPE_NULL;
  } else if (upper_str == "NOTNULL") {
    return CONSTRAINT_TYPE_NOTNULL;
  } else if (upper_str == "DEFAULT") {
    return CONSTRAINT_TYPE_DEFAULT;
  } else if (upper_str == "CHECK") {
    return CONSTRAINT_TYPE_CHECK;
  } else if (upper_str == "PRIMARY") {
    return CONSTRAINT_TYPE_PRIMARY;
  } else if (upper_str == "UNIQUE") {
    return CONSTRAINT_TYPE_UNIQUE;
  } else if (upper_str == "FOREIGN") {
    return CONSTRAINT_TYPE_FOREIGN;
  } else if (upper_str == "EXCLUSION") {
    return CONSTRAINT_TYPE_EXCLUSION;
  } else {
    throw ConversionException(StringUtil::Format(
        "No ConstraintType conversion from string '%s'", upper_str.c_str()));
  }
  return CONSTRAINT_TYPE_INVALID;
}

//===--------------------------------------------------------------------===//
// Log Types - String Utilities
//===--------------------------------------------------------------------===//

std::string LoggingTypeToString(LoggingType type) {
  switch (type) {
    case LOGGING_TYPE_INVALID:
      return "INVALID";

    // WAL Based
    case LOGGING_TYPE_NVM_WAL:
      return "NVM_WAL";
    case LOGGING_TYPE_SSD_WAL:
      return "SSD_WAL";
    case LOGGING_TYPE_HDD_WAL:
      return "HDD_WAL";

    // WBL Based
    case LOGGING_TYPE_NVM_WBL:
      return "NVM_WBL";
    case LOGGING_TYPE_SSD_WBL:
      return "SSD_WBL";
    case LOGGING_TYPE_HDD_WBL:
      return "HDD_WBL";

    default: {
      throw ConversionException(
          StringUtil::Format("No string conversion for LoggingType value '%d'",
                             static_cast<int>(type)));
    }
  }
  return "INVALID";
}

LoggingType StringToLoggingType(const std::string& str) {
  std::string upper_str = StringUtil::Upper(str);
  if (upper_str == "INVALID") {
    return LOGGING_TYPE_INVALID;
  } else if (upper_str == "NVM_WAL") {
    return LOGGING_TYPE_NVM_WAL;
  } else if (upper_str == "SSD_WAL") {
    return LOGGING_TYPE_SSD_WAL;
  } else if (upper_str == "HDD_WAL") {
    return LOGGING_TYPE_HDD_WAL;
  } else if (upper_str == "NVM_WBL") {
    return LOGGING_TYPE_NVM_WBL;
  } else if (upper_str == "SSD_WBL") {
    return LOGGING_TYPE_SSD_WBL;
  } else if (upper_str == "HDD_WBL") {
    return LOGGING_TYPE_HDD_WBL;
  } else {
    throw ConversionException(StringUtil::Format(
        "No LoggingType conversion from string '%s'", upper_str.c_str()));
  }
  return LOGGING_TYPE_INVALID;
}

std::string LoggingStatusTypeToString(LoggingStatusType type) {
  switch (type) {
    case LOGGING_STATUS_TYPE_INVALID: {
      return "INVALID";
    }
    case LOGGING_STATUS_TYPE_STANDBY: {
      return "STANDBY";
    }
    case LOGGING_STATUS_TYPE_RECOVERY: {
      return "RECOVERY";
    }
    case LOGGING_STATUS_TYPE_LOGGING: {
      return "LOGGING";
    }
    case LOGGING_STATUS_TYPE_TERMINATE: {
      return "TERMINATE";
    }
    case LOGGING_STATUS_TYPE_SLEEP: {
      return "SLEEP";
    }
    default: {
      throw ConversionException(StringUtil::Format(
          "No string conversion for LoggingStatusType value '%d'",
          static_cast<int>(type)));
    }
  }
  return "INVALID";
}

LoggingStatusType StringToLoggingStatusType(const std::string& str) {
  std::string upper_str = StringUtil::Upper(str);
  if (upper_str == "INVALID") {
    return LOGGING_STATUS_TYPE_INVALID;
  } else if (upper_str == "STANDBY") {
    return LOGGING_STATUS_TYPE_STANDBY;
  } else if (upper_str == "RECOVERY") {
    return LOGGING_STATUS_TYPE_RECOVERY;
  } else if (upper_str == "LOGGING") {
    return LOGGING_STATUS_TYPE_LOGGING;
  } else if (upper_str == "TERMINATE") {
    return LOGGING_STATUS_TYPE_TERMINATE;
  } else if (upper_str == "SLEEP") {
    return LOGGING_STATUS_TYPE_SLEEP;
  } else {
    throw ConversionException(StringUtil::Format(
        "No LoggingStatusType conversion from string '%s'", upper_str.c_str()));
  }
  return LOGGING_STATUS_TYPE_INVALID;
}

std::string LoggerTypeToString(LoggerType type) {
  switch (type) {
    case LOGGER_TYPE_INVALID: {
      return "INVALID";
    }
    case LOGGER_TYPE_FRONTEND: {
      return "FRONTEND";
    }
    case LOGGER_TYPE_BACKEND: {
      return "BACKEND";
    }
    default: {
      throw ConversionException(
          StringUtil::Format("No string conversion for LoggerType value '%d'",
                             static_cast<int>(type)));
    }
  }
  return "INVALID";
}

LoggerType StringToLoggerType(const std::string& str) {
  std::string upper_str = StringUtil::Upper(str);
  if (upper_str == "INVALID") {
    return LOGGER_TYPE_INVALID;
  } else if (upper_str == "FRONTEND") {
    return LOGGER_TYPE_FRONTEND;
  } else if (upper_str == "BACKEND") {
    return LOGGER_TYPE_BACKEND;
  } else {
    throw ConversionException(StringUtil::Format(
        "No LoggerType conversion from string '%s'", upper_str.c_str()));
  }
  return LOGGER_TYPE_INVALID;
}

std::string LogRecordTypeToString(LogRecordType type) {
  switch (type) {
    case LOGRECORD_TYPE_INVALID: {
      return "INVALID";
    }
    case LOGRECORD_TYPE_TRANSACTION_BEGIN: {
      return "TRANSACTION_BEGIN";
    }
    case LOGRECORD_TYPE_TRANSACTION_COMMIT: {
      return "TRANSACTION_COMMIT";
    }
    case LOGRECORD_TYPE_TRANSACTION_END: {
      return "TRANSACTION_END";
    }
    case LOGRECORD_TYPE_TRANSACTION_ABORT: {
      return "TRANSACTION_ABORT";
    }
    case LOGRECORD_TYPE_TRANSACTION_DONE: {
      return "TRANSACTION_DONE";
    }
    case LOGRECORD_TYPE_TUPLE_INSERT: {
      return "TUPLE_INSERT";
    }
    case LOGRECORD_TYPE_TUPLE_DELETE: {
      return "TUPLE_DELETE";
    }
    case LOGRECORD_TYPE_TUPLE_UPDATE: {
      return "TUPLE_UPDATE";
    }
    case LOGRECORD_TYPE_WAL_TUPLE_INSERT: {
      return "WAL_TUPLE_INSERT";
    }
    case LOGRECORD_TYPE_WAL_TUPLE_DELETE: {
      return "WAL_TUPLE_DELETE";
    }
    case LOGRECORD_TYPE_WAL_TUPLE_UPDATE: {
      return "WAL_TUPLE_UPDATE";
    }
    case LOGRECORD_TYPE_WBL_TUPLE_INSERT: {
      return "WBL_TUPLE_INSERT";
    }
    case LOGRECORD_TYPE_WBL_TUPLE_DELETE: {
      return "WBL_TUPLE_DELETE";
    }
    case LOGRECORD_TYPE_WBL_TUPLE_UPDATE: {
      return "WBL_TUPLE_UPDATE";
    }
    case LOGRECORD_TYPE_ITERATION_DELIMITER: {
      return "ITERATION_DELIMITER";
    }
    default: {
      throw ConversionException(StringUtil::Format(
          "No string conversion for LogRecordType value '%d'",
          static_cast<int>(type)));
    }
  }
  return "INVALID";
}

LogRecordType StringToLogRecordType(const std::string& str) {
  std::string upper_str = StringUtil::Upper(str);
  if (upper_str == "INVALID") {
    return LOGRECORD_TYPE_INVALID;
  } else if (upper_str == "TRANSACTION_BEGIN") {
    return LOGRECORD_TYPE_TRANSACTION_BEGIN;
  } else if (upper_str == "TRANSACTION_COMMIT") {
    return LOGRECORD_TYPE_TRANSACTION_COMMIT;
  } else if (upper_str == "TRANSACTION_END") {
    return LOGRECORD_TYPE_TRANSACTION_END;
  } else if (upper_str == "TRANSACTION_ABORT") {
    return LOGRECORD_TYPE_TRANSACTION_ABORT;
  } else if (upper_str == "TRANSACTION_DONE") {
    return LOGRECORD_TYPE_TRANSACTION_DONE;
  } else if (upper_str == "TUPLE_INSERT") {
    return LOGRECORD_TYPE_TUPLE_INSERT;
  } else if (upper_str == "TUPLE_DELETE") {
    return LOGRECORD_TYPE_TUPLE_DELETE;
  } else if (upper_str == "TUPLE_UPDATE") {
    return LOGRECORD_TYPE_TUPLE_UPDATE;
  } else if (upper_str == "WAL_TUPLE_INSERT") {
    return LOGRECORD_TYPE_WAL_TUPLE_INSERT;
  } else if (upper_str == "WAL_TUPLE_DELETE") {
    return LOGRECORD_TYPE_WAL_TUPLE_DELETE;
  } else if (upper_str == "WAL_TUPLE_UPDATE") {
    return LOGRECORD_TYPE_WAL_TUPLE_UPDATE;
  } else if (upper_str == "WBL_TUPLE_INSERT") {
    return LOGRECORD_TYPE_WBL_TUPLE_INSERT;
  } else if (upper_str == "WBL_TUPLE_DELETE") {
    return LOGRECORD_TYPE_WBL_TUPLE_DELETE;
  } else if (upper_str == "WBL_TUPLE_UPDATE") {
    return LOGRECORD_TYPE_WBL_TUPLE_UPDATE;
  } else if (upper_str == "ITERATION_DELIMITER") {
    return LOGRECORD_TYPE_ITERATION_DELIMITER;
  } else {
    throw ConversionException(StringUtil::Format(
        "No LogRecordType conversion from string '%s'", upper_str.c_str()));
  }
  return LOGRECORD_TYPE_INVALID;
}

type::Type::TypeId PostgresValueTypeToPelotonValueType(PostgresValueType type) {
  switch (type) {
    case PostgresValueType::BOOLEAN:
      return type::Type::BOOLEAN;

    case PostgresValueType::SMALLINT:
      return type::Type::SMALLINT;
    case PostgresValueType::INTEGER:
      return type::Type::INTEGER;
    case PostgresValueType::BIGINT:
      return type::Type::BIGINT;
    case PostgresValueType::REAL:
      return type::Type::DECIMAL;
    case PostgresValueType::DOUBLE:
      return type::Type::DECIMAL;

    case PostgresValueType::BPCHAR:
    case PostgresValueType::BPCHAR2:
    case PostgresValueType::VARCHAR:
    case PostgresValueType::VARCHAR2:
    case PostgresValueType::TEXT:
      return type::Type::VARCHAR;

    case PostgresValueType::DATE:
    case PostgresValueType::TIMESTAMPS:
    case PostgresValueType::TIMESTAMPS2:
      return type::Type::TIMESTAMP;

    case PostgresValueType::DECIMAL:
      return type::Type::DECIMAL;
    default:
      throw ConversionException(StringUtil::Format(
          "No TypeId conversion for PostgresValueType value '%d'",
          static_cast<int>(type)));
  }
  return type::Type::INVALID;
}

ConstraintType PostgresConstraintTypeToPelotonConstraintType(
    PostgresConstraintType type) {
  ConstraintType constraintType = CONSTRAINT_TYPE_INVALID;

  switch (type) {
    case POSTGRES_CONSTRAINT_NULL:
      constraintType = CONSTRAINT_TYPE_NULL;
      break;

    case POSTGRES_CONSTRAINT_NOTNULL:
      constraintType = CONSTRAINT_TYPE_NOTNULL;
      break;

    case POSTGRES_CONSTRAINT_DEFAULT:
      constraintType = CONSTRAINT_TYPE_DEFAULT;
      break;

    case POSTGRES_CONSTRAINT_CHECK:
      constraintType = CONSTRAINT_TYPE_CHECK;
      break;

    case POSTGRES_CONSTRAINT_PRIMARY:
      constraintType = CONSTRAINT_TYPE_PRIMARY;
      break;

    case POSTGRES_CONSTRAINT_UNIQUE:
      constraintType = CONSTRAINT_TYPE_UNIQUE;
      break;

    case POSTGRES_CONSTRAINT_FOREIGN:
      constraintType = CONSTRAINT_TYPE_FOREIGN;
      break;

    case POSTGRES_CONSTRAINT_EXCLUSION:
      constraintType = CONSTRAINT_TYPE_EXCLUSION;
      break;

    default:
      throw ConversionException(StringUtil::Format(
          "No ConstraintType conversion for PostgresConstraintType value '%d'",
          static_cast<int>(type)));
      break;
  }
  return constraintType;
}

//===--------------------------------------------------------------------===//
// Network Message types
//===--------------------------------------------------------------------===//
std::string SqlStateErrorCodeToString(SqlStateErrorCode code) {
  switch (code) {
    case SERIALIZATION_ERROR:
      return "40001";
    default:
      return "INVALID";
  }
}

}  // End peloton namespace
