//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// internal_types.cpp
//
// Identification: src/common/internal_types.cpp
//
// Copyright (c) 2015-2017, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <algorithm>
#include <cstring>
#include <sstream>

#include "common/exception.h"
#include "common/internal_types.h"
#include "common/logger.h"
#include "common/macros.h"
#include "type/value_factory.h"
#include "parser/sql_statement.h"
#include "parser/statements.h"
#include "util/string_util.h"

namespace peloton {

FileHandle INVALID_FILE_HANDLE;

// WARNING: It will limit scalability if tuples per tile group is too small,
// When a tile group is full, a new tile group needs to be allocated, until
// then no new insertion of new versions or tuples are available in the table.
int DEFAULT_TUPLES_PER_TILEGROUP = 1000;
int TEST_TUPLES_PER_TILEGROUP = 5;

// For threads
size_t CONNECTION_THREAD_COUNT = 1;
size_t LOGGING_THREAD_COUNT = 1;
size_t GC_THREAD_COUNT = 1;
size_t EPOCH_THREAD_COUNT = 1;
size_t MAX_CONCURRENCY = 10;

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

DatePartType StringToDatePartType(const std::string &str) {
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

std::ostream &operator<<(std::ostream &os, const DatePartType &type) {
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

BackendType StringToBackendType(const std::string &str) {
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

std::ostream &operator<<(std::ostream &os, const BackendType &type) {
  os << BackendTypeToString(type);
  return os;
}

//===--------------------------------------------------------------------===//
// Value <--> String Utilities
//===--------------------------------------------------------------------===//

std::string TypeIdToString(type::TypeId type) {
  switch (type) {
    case type::TypeId::INVALID:
      return "INVALID";
    case type::TypeId::PARAMETER_OFFSET:
      return "PARAMETER_OFFSET";
    case type::TypeId::BOOLEAN:
      return "BOOLEAN";
    case type::TypeId::TINYINT:
      return "TINYINT";
    case type::TypeId::SMALLINT:
      return "SMALLINT";
    case type::TypeId::INTEGER:
      return "INTEGER";
    case type::TypeId::BIGINT:
      return "BIGINT";
    case type::TypeId::DECIMAL:
      return "DECIMAL";
    case type::TypeId::TIMESTAMP:
      return "TIMESTAMP";
    case type::TypeId::DATE:
      return "DATE";
    case type::TypeId::VARCHAR:
      return "VARCHAR";
    case type::TypeId::VARBINARY:
      return "VARBINARY";
    case type::TypeId::ARRAY:
      return "ARRAY";
    case type::TypeId::UDT:
      return "UDT";
    default: {
      throw ConversionException(
          StringUtil::Format("No string conversion for TypeId value '%d'",
                             static_cast<int>(type)));
    }
      return "INVALID";
  }
}

type::TypeId StringToTypeId(const std::string &str) {
  std::string upper_str = StringUtil::Upper(str);
  if (upper_str == "INVALID") {
    return type::TypeId::INVALID;
  } else if (upper_str == "PARAMETER_OFFSET") {
    return type::TypeId::PARAMETER_OFFSET;
  } else if (upper_str == "BOOLEAN") {
    return type::TypeId::BOOLEAN;
  } else if (upper_str == "TINYINT") {
    return type::TypeId::TINYINT;
  } else if (upper_str == "SMALLINT") {
    return type::TypeId::SMALLINT;
  } else if (upper_str == "INTEGER") {
    return type::TypeId::INTEGER;
  } else if (upper_str == "BIGINT") {
    return type::TypeId::BIGINT;
  } else if (upper_str == "DECIMAL") {
    return type::TypeId::DECIMAL;
  } else if (upper_str == "TIMESTAMP") {
    return type::TypeId::TIMESTAMP;
  } else if (upper_str == "DATE") {
    return type::TypeId::DATE;
  } else if (upper_str == "VARCHAR") {
    return type::TypeId::VARCHAR;
  } else if (upper_str == "VARBINARY") {
    return type::TypeId::VARBINARY;
  } else if (upper_str == "ARRAY") {
    return type::TypeId::ARRAY;
  } else if (upper_str == "UDT") {
    return type::TypeId::UDT;
  } else {
    throw ConversionException(StringUtil::Format(
        "No TypeId conversion from string '%s'", upper_str.c_str()));
  }
  return type::TypeId::INVALID;
}

std::string TypeIdArrayToString(const std::vector<type::TypeId> &types) {
  std::string result = "";
  for (auto type : types) {
    if (result != "") result.append(",");
    result.append(TypeIdToString(type));
  }
  return result;
}

// Get argument type vector from its string representation
// e.g. "integer,boolean" --> vector{TypeId::INTEGER, TypeId::BOOLEAN}
std::vector<type::TypeId> StringToTypeArray(const std::string &types) {
  std::vector<type::TypeId> result;
  std::istringstream stream(types);
  std::string type;
  while (getline(stream, type, ',')) {
    result.push_back(StringToTypeId(type));
  }
  return result;
}

//===--------------------------------------------------------------------===//
// CreateType - String Utilities
//===--------------------------------------------------------------------===//

std::string CreateTypeToString(CreateType type) {
  switch (type) {
    case CreateType::INVALID: {
      return "INVALID";
    }
    case CreateType::DB: {
      return "DB";
    }
    case CreateType::TABLE: {
      return "TABLE";
    }
    case CreateType::INDEX: {
      return "INDEX";
    }
    case CreateType::CONSTRAINT: {
      return "CONSTRAINT";
    }
    case CreateType::TRIGGER: {
      return "TRIGGER";
    }
    default: {
      throw ConversionException(
          StringUtil::Format("No string conversion for CreateType value '%d'",
                             static_cast<int>(type)));
    }
  }
  return "INVALID";
}

CreateType StringToCreateType(const std::string &str) {
  std::string upper_str = StringUtil::Upper(str);
  if (upper_str == "INVALID") {
    return CreateType::INVALID;
  } else if (upper_str == "DB") {
    return CreateType::DB;
  } else if (upper_str == "TABLE") {
    return CreateType::TABLE;
  } else if (upper_str == "INDEX") {
    return CreateType::INDEX;
  } else if (upper_str == "CONSTRAINT") {
    return CreateType::CONSTRAINT;
  } else if (upper_str == "TRIGGER") {
    return CreateType::TRIGGER;
  } else {
    throw ConversionException(StringUtil::Format(
        "No CreateType conversion from string '%s'", upper_str.c_str()));
  }
  return CreateType::INVALID;
}
std::ostream &operator<<(std::ostream &os, const CreateType &type) {
  os << CreateTypeToString(type);
  return os;
}

std::string DropTypeToString(DropType type) {
  switch (type) {
    case DropType::INVALID: {
      return "INVALID";
    }
    case DropType::DB: {
      return "DB";
    }
    case DropType::TABLE: {
      return "TABLE";
    }
    case DropType::INDEX: {
      return "INDEX";
    }
    case DropType::CONSTRAINT: {
      return "CONSTRAINT";
    }
    case DropType::TRIGGER: {
      return "TRIGGER";
    }
    default: {
      throw ConversionException(
          StringUtil::Format("No string conversion for DropType value '%d'",
                             static_cast<int>(type)));
    }
  }
  return "INVALID";
}

DropType StringToDropType(const std::string &str) {
  std::string upper_str = StringUtil::Upper(str);
  if (upper_str == "INVALID") {
    return DropType::INVALID;
  } else if (upper_str == "DB") {
    return DropType::DB;
  } else if (upper_str == "TABLE") {
    return DropType::TABLE;
  } else if (upper_str == "INDEX") {
    return DropType::INDEX;
  } else if (upper_str == "CONSTRAINT") {
    return DropType::CONSTRAINT;
  } else if (upper_str == "TRIGGER") {
    return DropType::TRIGGER;
  } else {
    throw ConversionException(StringUtil::Format(
        "No DropType conversion from string '%s'", upper_str.c_str()));
  }
  return DropType::INVALID;
}
std::ostream &operator<<(std::ostream &os, const DropType &type) {
  os << DropTypeToString(type);
  return os;
}

//===--------------------------------------------------------------------===//
// Statement - String Utilities
//===--------------------------------------------------------------------===//

std::string StatementTypeToString(StatementType type) {
  switch (type) {
    case StatementType::INVALID: {
      return "INVALID";
    }
    case StatementType::SELECT: {
      return "SELECT";
    }
    case StatementType::INSERT: {
      return "INSERT";
    }
    case StatementType::UPDATE: {
      return "UPDATE";
    }
    case StatementType::CREATE_FUNC: {
      return "CREATE_FUNC";
    }
    case StatementType::DELETE: {
      return "DELETE";
    }
    case StatementType::CREATE: {
      return "CREATE";
    }
    case StatementType::DROP: {
      return "DROP";
    }
    case StatementType::PREPARE: {
      return "PREPARE";
    }
    case StatementType::EXECUTE: {
      return "EXECUTE";
    }
    case StatementType::RENAME: {
      return "RENAME";
    }
    case StatementType::ALTER: {
      return "ALTER";
    }
    case StatementType::TRANSACTION: {
      return "TRANSACTION";
    }
    case StatementType::COPY: {
      return "COPY";
    }
    case StatementType::ANALYZE: {
      return "ANALYZE";
    }
    case StatementType::VARIABLE_SET: {
      return "SET";
    }
    default: {
      throw ConversionException(StringUtil::Format(
          "No string conversion for StatementType value '%d'",
          static_cast<int>(type)));
    }
  }
  return "INVALID";
}

StatementType StringToStatementType(const std::string &str) {
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
  } else if (upper_str == "CREATE_FUNC") {
    return StatementType::CREATE_FUNC;
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
  } else if (upper_str == "ANALYZE") {
    return StatementType::ANALYZE;
  } else {
    throw ConversionException(StringUtil::Format(
        "No StatementType conversion from string '%s'", upper_str.c_str()));
  }
  return StatementType::INVALID;
}
std::ostream &operator<<(std::ostream &os, const StatementType &type) {
  os << StatementTypeToString(type);
  return os;
}

//===--------------------------------------------------------------------===//
// QueryType - String Utilities
//===--------------------------------------------------------------------===//
std::string QueryTypeToString(QueryType query_type) {
  switch(query_type) {
    case QueryType::QUERY_BEGIN:return "BEGIN";
    case QueryType::QUERY_COMMIT:return "COMMIT";
    case QueryType::QUERY_ROLLBACK:return "ROLLBACK";
    case QueryType::QUERY_CREATE_DB:return "CREATE DATABASE";
    case QueryType::QUERY_CREATE_INDEX: return "CREATE INDEX";
    case QueryType::QUERY_CREATE_TABLE: return "CREATE TABLE";
    case QueryType::QUERY_CREATE_TRIGGER: return "CREATE TRIGGER";
    case QueryType::QUERY_CREATE_SCHEMA: return "CREATE SCHEMA";
    case QueryType::QUERY_CREATE_VIEW: return "CREATE VIEW";
    case QueryType::QUERY_DROP:return "DROP";
    case QueryType::QUERY_INSERT:return "INSERT";
    case QueryType::QUERY_SET: return "SET";
    case QueryType::QUERY_SHOW: return "SHOW";
    case QueryType::QUERY_UPDATE: return "UPDATE";
    case QueryType::QUERY_ALTER: return "ALTER";
    case QueryType::QUERY_DELETE: return "DELETE";
    case QueryType::QUERY_COPY: return "COPY";
    case QueryType::QUERY_ANALYZE: return "ANALYZE";
    case QueryType::QUERY_RENAME: return "RENAME";
    case QueryType::QUERY_PREPARE: return "PREPARE";
    case QueryType::QUERY_EXECUTE: return "EXECUTE";
    case QueryType::QUERY_SELECT: return "SELECT";
    case QueryType::QUERY_OTHER: default: return "OTHER";
  }
}

QueryType StringToQueryType(const std::string &str) {
  static std::unordered_map<std::string, QueryType> querytype_string_map {
      {"BEGIN", QueryType::QUERY_BEGIN}, {"COMMIT", QueryType::QUERY_COMMIT},
      {"ROLLBACK", QueryType::QUERY_ROLLBACK}, {"CREATE DATABASE", QueryType::QUERY_CREATE_DB},
      {"CREATE INDEX", QueryType::QUERY_CREATE_INDEX}, {"CREATE TABLE", QueryType::QUERY_CREATE_TABLE},
      {"DROP", QueryType::QUERY_DROP}, {"INSERT", QueryType::QUERY_INSERT},
      {"SET", QueryType::QUERY_SET}, {"SHOW", QueryType::QUERY_SHOW},
      {"SHOW", QueryType::QUERY_SHOW}, {"UPDATE", QueryType::QUERY_UPDATE},
      {"ALTER", QueryType::QUERY_ALTER}, {"DELETE", QueryType::QUERY_DELETE},
      {"COPY", QueryType::QUERY_COPY}, {"ANALYZE", QueryType::QUERY_ANALYZE},
      {"RENAME", QueryType::QUERY_RENAME}, {"PREPARE", QueryType::QUERY_PREPARE},
      {"EXECUTE", QueryType::QUERY_EXECUTE}, {"SELECT", QueryType::QUERY_SELECT},
      {"CREATE TRIGGER", QueryType::QUERY_CREATE_TRIGGER}, {"CREATE SCHEMA", QueryType::QUERY_CREATE_SCHEMA},
      {"CREATE VIEW", QueryType::QUERY_CREATE_VIEW}, {"OTHER", QueryType::QUERY_OTHER},
  };
  std::unordered_map<std::string, QueryType>::iterator it  = querytype_string_map.find(str);
  if (it != querytype_string_map.end()) {
    return it -> second;
  } else {
    return QueryType::QUERY_INVALID;
  }
}

QueryType StatementTypeToQueryType(StatementType stmt_type, const parser::SQLStatement* sql_stmt) {
  LOG_TRACE("%s", StatementTypeToString(stmt_type).c_str());
  static std::unordered_map<StatementType, QueryType, EnumHash<StatementType>> type_map {
      {StatementType::EXECUTE, QueryType::QUERY_EXECUTE},
      {StatementType::PREPARE, QueryType::QUERY_PREPARE},
      {StatementType::INSERT, QueryType::QUERY_INSERT},
      {StatementType::UPDATE, QueryType::QUERY_UPDATE},
      {StatementType::DELETE, QueryType::QUERY_DELETE},
      {StatementType::COPY, QueryType::QUERY_COPY},
      {StatementType::ANALYZE, QueryType::QUERY_ANALYZE},
      {StatementType::ALTER, QueryType::QUERY_ALTER},
      {StatementType::DROP, QueryType::QUERY_DROP},
      {StatementType::SELECT, QueryType::QUERY_SELECT},
      {StatementType::VARIABLE_SET, QueryType::QUERY_SET},
  };
  QueryType query_type = QueryType::QUERY_OTHER;
  std::unordered_map<StatementType, QueryType, EnumHash<StatementType>>::iterator it  = type_map.find(stmt_type);
  if (it != type_map.end()) {
    query_type = it -> second;
  } else {
    switch(stmt_type) {
      case StatementType::TRANSACTION: {
        switch (static_cast<const parser::TransactionStatement*>(sql_stmt) ->type) {
          case parser::TransactionStatement::CommandType::kBegin:query_type = QueryType::QUERY_BEGIN;
            break;
          case parser::TransactionStatement::CommandType::kCommit:query_type = QueryType::QUERY_COMMIT;
            break;
          case parser::TransactionStatement::CommandType::kRollback:query_type = QueryType::QUERY_ROLLBACK;
            break;
        }
        break;
      }
      case StatementType::CREATE: {
        switch (static_cast<const parser::CreateStatement*>(sql_stmt) ->type) {
          case parser::CreateStatement::CreateType::kDatabase:query_type = QueryType::QUERY_CREATE_DB;
            break;
          case parser::CreateStatement::CreateType::kIndex:query_type = QueryType::QUERY_CREATE_INDEX;
            break;
          case parser::CreateStatement::CreateType::kTable:query_type = QueryType::QUERY_CREATE_TABLE;
            break;
          case parser::CreateStatement::CreateType::kTrigger:query_type = QueryType::QUERY_CREATE_TRIGGER;
            break;
          case parser::CreateStatement::CreateType::kSchema:query_type = QueryType::QUERY_CREATE_SCHEMA;
            break;
          case parser::CreateStatement::CreateType::kView:query_type = QueryType::QUERY_CREATE_VIEW;
            break;
        }
        break;
      }
      default:
        query_type = QueryType::QUERY_OTHER;
    }
  }
  return query_type;
}

//===--------------------------------------------------------------------===//
// PostgresValueType - String Utilities
//===--------------------------------------------------------------------===//

std::string PostgresValueTypeToString(PostgresValueType type) {
  switch (type) {
    case PostgresValueType::INVALID: {
      return "INVALID";
    }
    case PostgresValueType::TINYINT: {
      return "TINYINT";
    }
    case PostgresValueType::SMALLINT: {
      return "SMALLINT";
    }
    case PostgresValueType::INTEGER: {
      return "INTEGER";
    }
    case PostgresValueType::VARBINARY: {
      return "VARBINARY";
    }
    case PostgresValueType::BIGINT: {
      return "BIGINT";
    }
    case PostgresValueType::REAL: {
      return "REAL";
    }
    case PostgresValueType::DOUBLE: {
      return "DOUBLE";
    }
    case PostgresValueType::TEXT: {
      return "TEXT";
    }
    case PostgresValueType::BPCHAR: {
      return "BPCHAR";
    }
    case PostgresValueType::BPCHAR2: {
      return "BPCHAR2";
    }
    case PostgresValueType::VARCHAR: {
      return "VARCHAR";
    }
    case PostgresValueType::VARCHAR2: {
      return "VARCHAR2";
    }
    case PostgresValueType::DATE: {
      return "DATE";
    }
    case PostgresValueType::TIMESTAMPS: {
      return "TIMESTAMPS";
    }
    case PostgresValueType::TIMESTAMPS2: {
      return "TIMESTAMPS2";
    }
    case PostgresValueType::TEXT_ARRAY: {
      return "TEXT_ARRAY";
    }
    case PostgresValueType::INT2_ARRAY: {
      return "INT2_ARRAY";
    }
    case PostgresValueType::INT4_ARRAY: {
      return "INT4_ARRAY";
    }
    case PostgresValueType::OID_ARRAY: {
      return "OID_ARRAY";
    }
    case PostgresValueType::FLOADT4_ARRAY: {
      return "FLOADT4_ARRAY";
    }
    case PostgresValueType::DECIMAL: {
      return "DECIMAL";
    }
    default: {
      throw ConversionException(
          StringUtil::Format("No string conversion for PostgresValueType value '%d'",
                             static_cast<int>(type)));
    }
  }
  return "INVALID";
}

PostgresValueType StringToPostgresValueType(const std::string &str) {
  std::string upper_str = StringUtil::Upper(str);
  if (upper_str == "INVALID") {
    return PostgresValueType::INVALID;
  } else if (upper_str == "BOOLEAN") {
    return PostgresValueType::BOOLEAN;
  } else if (upper_str == "TINYINT") {
    return PostgresValueType::TINYINT;
  } else if (upper_str == "SMALLINT") {
    return PostgresValueType::SMALLINT;
  } else if (upper_str == "INTEGER") {
    return PostgresValueType::INTEGER;
  } else if (upper_str == "VARBINARY") {
    return PostgresValueType::VARBINARY;
  } else if (upper_str == "BIGINT") {
    return PostgresValueType::BIGINT;
  } else if (upper_str == "REAL") {
    return PostgresValueType::REAL;
  } else if (upper_str == "DOUBLE") {
    return PostgresValueType::DOUBLE;
  } else if (upper_str == "TEXT") {
    return PostgresValueType::TEXT;
  } else if (upper_str == "BPCHAR") {
    return PostgresValueType::BPCHAR;
  } else if (upper_str == "BPCHAR2") {
    return PostgresValueType::BPCHAR2;
  } else if (upper_str == "VARCHAR") {
    return PostgresValueType::VARCHAR;
  } else if (upper_str == "VARCHAR2") {
    return PostgresValueType::VARCHAR2;
  } else if (upper_str == "DATE") {
    return PostgresValueType::DATE;
  } else if (upper_str == "TIMESTAMPS") {
    return PostgresValueType::TIMESTAMPS;
  } else if (upper_str == "TIMESTAMPS2") {
    return PostgresValueType::TIMESTAMPS2;
  } else if (upper_str == "TEXT_ARRAY") {
    return PostgresValueType::TEXT_ARRAY;
  } else if (upper_str == "INT2_ARRAY") {
    return PostgresValueType::INT2_ARRAY;
  } else if (upper_str == "INT4_ARRAY") {
    return PostgresValueType::INT4_ARRAY;
  } else if (upper_str == "OID_ARRAY") {
    return PostgresValueType::OID_ARRAY;
  } else if (upper_str == "FLOADT4_ARRAY") {
    return PostgresValueType::FLOADT4_ARRAY;
  } else if (upper_str == "DECIMAL") {
    return PostgresValueType::DECIMAL;
  } else {
    throw ConversionException(StringUtil::Format(
    "No PostgresValueType conversion from string '%s'", upper_str.c_str()));
  }
  return PostgresValueType::INVALID;
}

std::ostream &operator<<(std::ostream &os, const PostgresValueType &type) {
  os << PostgresValueTypeToString(type);
  return os;
}

//===--------------------------------------------------------------------===//
// Expression - String Utilities
//===--------------------------------------------------------------------===//

std::string ExpressionTypeToString(ExpressionType type, bool short_str) {
  switch (type) {
    case ExpressionType::INVALID: {
      return ("INVALID");
    }
    case ExpressionType::OPERATOR_PLUS: {
      return short_str ? "+" : ("OPERATOR_PLUS");
    }
    case ExpressionType::OPERATOR_MINUS: {
      return short_str ? "-" : ("OPERATOR_MINUS");
    }
    case ExpressionType::OPERATOR_MULTIPLY: {
      return short_str ? "*" : ("OPERATOR_MULTIPLY");
    }
    case ExpressionType::OPERATOR_DIVIDE: {
      return short_str ? "/" : ("OPERATOR_DIVIDE");
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
      return short_str ? "=" : ("COMPARE_EQUAL");
    }
    case ExpressionType::COMPARE_NOTEQUAL: {
      return short_str ? "!=" : ("COMPARE_NOTEQUAL");
    }
    case ExpressionType::COMPARE_LESSTHAN: {
      return short_str ? "<" : ("COMPARE_LESSTHAN");
    }
    case ExpressionType::COMPARE_GREATERTHAN: {
      return short_str ? ">" : ("COMPARE_GREATERTHAN");
    }
    case ExpressionType::COMPARE_LESSTHANOREQUALTO: {
      return short_str ? "<=" : ("COMPARE_LESSTHANOREQUALTO");
    }
    case ExpressionType::COMPARE_GREATERTHANOREQUALTO: {
      return short_str ? ">=" : ("COMPARE_GREATERTHANOREQUALTO");
    }
    case ExpressionType::COMPARE_LIKE: {
      return short_str ? "~~" : ("COMPARE_LIKE");
    }
    case ExpressionType::COMPARE_NOTLIKE: {
      return short_str ? "!~~" : ("COMPARE_NOTLIKE");
    }
    case ExpressionType::COMPARE_IN: {
      return ("COMPARE_IN");
    }
    case ExpressionType::COMPARE_DISTINCT_FROM: {
      return ("COMPARE_DISTINCT_FROM");
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

ExpressionType ParserExpressionNameToExpressionType(const std::string &str) {
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

ExpressionType StringToExpressionType(const std::string &str) {
  std::string upper_str = StringUtil::Upper(str);
  if (upper_str == "INVALID") {
    return ExpressionType::INVALID;
  } else if (upper_str == "OPERATOR_PLUS" || upper_str == "+") {
    return ExpressionType::OPERATOR_PLUS;
  } else if (upper_str == "OPERATOR_MINUS" || upper_str == "-") {
    return ExpressionType::OPERATOR_MINUS;
  } else if (upper_str == "OPERATOR_MULTIPLY" || upper_str == "*") {
    return ExpressionType::OPERATOR_MULTIPLY;
  } else if (upper_str == "OPERATOR_DIVIDE" || upper_str == "/") {
    return ExpressionType::OPERATOR_DIVIDE;
  } else if (upper_str == "OPERATOR_CONCAT" || upper_str == "||") {
    return ExpressionType::OPERATOR_CONCAT;
  } else if (upper_str == "OPERATOR_MOD" || upper_str == "%") {
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
  } else if (upper_str == "COMPARE_EQUAL" || upper_str == "=") {
    return ExpressionType::COMPARE_EQUAL;
  } else if (upper_str == "COMPARE_NOTEQUAL" || upper_str == "!=" ||
             upper_str == "<>") {
    return ExpressionType::COMPARE_NOTEQUAL;
  } else if (upper_str == "COMPARE_LESSTHAN" || upper_str == "<") {
    return ExpressionType::COMPARE_LESSTHAN;
  } else if (upper_str == "COMPARE_GREATERTHAN" || upper_str == ">") {
    return ExpressionType::COMPARE_GREATERTHAN;
  } else if (upper_str == "COMPARE_LESSTHANOREQUALTO" || upper_str == "<=") {
    return ExpressionType::COMPARE_LESSTHANOREQUALTO;
  } else if (upper_str == "COMPARE_GREATERTHANOREQUALTO" || upper_str == ">=") {
    return ExpressionType::COMPARE_GREATERTHANOREQUALTO;
  } else if (upper_str == "COMPARE_LIKE" || upper_str == "~~") {
    return ExpressionType::COMPARE_LIKE;
  } else if (upper_str == "COMPARE_NOTLIKE" || upper_str == "!~~") {
    return ExpressionType::COMPARE_NOTLIKE;
  } else if (upper_str == "COMPARE_IN") {
    return ExpressionType::COMPARE_IN;
  } else if (upper_str == "COMPARE_DISTINCT_FROM") {
    return ExpressionType::COMPARE_DISTINCT_FROM;
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

std::ostream &operator<<(std::ostream &os, const ExpressionType &type) {
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
    case IndexType::SKIPLIST: {
      return "SKIPLIST";
    }
    default: {
      throw ConversionException(
          StringUtil::Format("No string conversion for IndexType value '%d'",
                             static_cast<int>(type)));
    }
  }
  return "INVALID";
}

IndexType StringToIndexType(const std::string &str) {
  std::string upper_str = StringUtil::Upper(str);
  if (upper_str == "INVALID") {
    return IndexType::INVALID;
  } else if (upper_str == "BTREE") {
    return IndexType::BWTREE;
  } else if (upper_str == "BWTREE") {
    return IndexType::BWTREE;
  } else if (upper_str == "HASH") {
    return IndexType::HASH;
  } else if (upper_str == "SKIPLIST") {
    return IndexType::SKIPLIST;
  } else {
    throw ConversionException(StringUtil::Format(
        "No IndexType conversion from string '%s'", upper_str.c_str()));
  }
  return IndexType::INVALID;
}

std::ostream &operator<<(std::ostream &os, const IndexType &type) {
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

IndexConstraintType StringToIndexConstraintType(const std::string &str) {
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

std::ostream &operator<<(std::ostream &os, const IndexConstraintType &type) {
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

HybridScanType StringToHybridScanType(const std::string &str) {
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

std::ostream &operator<<(std::ostream &os, const HybridScanType &type) {
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
    case PlanNodeType::CREATE_FUNC: {
      return ("CREATE_FUNC");
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
    case PlanNodeType::POPULATE_INDEX: {
      return ("POPULATE_INDEX");
    }
    case PlanNodeType::ANALYZE: {
      return ("ANALYZE");
    }
    default: {
      throw ConversionException(
          StringUtil::Format("No string conversion for PlanNodeType value '%d'",
                             static_cast<int>(type)));
    }
  }
  return "INVALID";
}

PlanNodeType StringToPlanNodeType(const std::string &str) {
  std::string upper_str = StringUtil::Upper(str);
  if (upper_str == "INVALID") {
    return PlanNodeType::INVALID;
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
  } else if (upper_str == "ANALYZE") {
    return PlanNodeType::ANALYZE;
  } else {
    throw ConversionException(StringUtil::Format(
        "No PlanNodeType conversion from string '%s'", upper_str.c_str()));
  }
  return PlanNodeType::INVALID;
}

std::ostream &operator<<(std::ostream &os, const PlanNodeType &type) {
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

ParseNodeType StringToParseNodeType(const std::string &str) {
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

std::ostream &operator<<(std::ostream &os, const ParseNodeType &type) {
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

JoinType StringToJoinType(const std::string &str) {
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

std::ostream &operator<<(std::ostream &os, const JoinType &type) {
  os << JoinTypeToString(type);
  return os;
}

//===--------------------------------------------------------------------===//
// AggregateType - String Utilities
//===--------------------------------------------------------------------===//
std::string AggregateTypeToString(AggregateType type) {
  switch (type) {
    case AggregateType::INVALID: {
      return "INVALID";
    }
    case AggregateType::SORTED: {
      return "SORTED";
    }
    case AggregateType::HASH: {
      return "HASH";
    }
    case AggregateType::PLAIN: {
      return "PLAIN";
    }
    default: {
      throw ConversionException(StringUtil::Format(
          "No string conversion for AggregateType value '%d'",
          static_cast<int>(type)));
    }
  }
  return "INVALID";
}

AggregateType StringToAggregateType(const std::string &str) {
  std::string upper_str = StringUtil::Upper(str);
  if (upper_str == "INVALID") {
    return AggregateType::INVALID;
  } else if (upper_str == "SORTED") {
    return AggregateType::SORTED;
  } else if (upper_str == "HASH") {
    return AggregateType::HASH;
  } else if (upper_str == "PLAIN") {
    return AggregateType::PLAIN;
  } else {
    throw ConversionException(StringUtil::Format(
        "No AggregateType conversion from string '%s'", upper_str.c_str()));
  }
  return AggregateType::INVALID;
}

std::ostream &operator<<(std::ostream &os, const AggregateType &type) {
  os << AggregateTypeToString(type);
  return os;
}

// ------------------------------------------------------------------
// QuantifierType - String Utilities
// ------------------------------------------------------------------

std::string QuantifierTypeToString(QuantifierType type) {
  switch (type) {
    case QuantifierType::NONE: {
      return "NONE";
    }
    case QuantifierType::ANY: {
      return "ANY";
    }
    case QuantifierType::ALL: {
      return "ALL";
    }
    default: {
      throw ConversionException(StringUtil::Format(
          "No string conversion for QuantifierType value '%d'",
          static_cast<int>(type)));
    }
  }
  return "NONE";
}

QuantifierType StringToQuantifierType(const std::string &str) {
  std::string upper_str = StringUtil::Upper(str);
  if (upper_str == "NONE") {
    return QuantifierType::NONE;
  } else if (upper_str == "ANY") {
    return QuantifierType::ANY;
  } else if (upper_str == "ALL") {
    return QuantifierType::ALL;
  } else {
    throw ConversionException(StringUtil::Format(
        "No QuantifierType conversion from string '%s'", upper_str.c_str()));
  }
  return QuantifierType::NONE;
}

std::ostream &operator<<(std::ostream &os, const QuantifierType &type) {
  os << QuantifierTypeToString(type);
  return os;
}

//===--------------------------------------------------------------------===//
// TableReferenceType - String Utilities
//===--------------------------------------------------------------------===//

std::string TableReferenceTypeToString(TableReferenceType type) {
  switch (type) {
    case TableReferenceType::INVALID: {
      return "INVALID";
    }
    case TableReferenceType::NAME: {
      return "NAME";
    };
    case TableReferenceType::SELECT: {
      return "SELECT";
    }
    case TableReferenceType::JOIN: {
      return "JOIN";
    }
    case TableReferenceType::CROSS_PRODUCT: {
      return "CROSS_PRODUCT";
    }
    default: {
      throw ConversionException(StringUtil::Format(
          "No string conversion for TableReferenceType value '%d'",
          static_cast<int>(type)));
    }
  }
  return "INVALID";
}

TableReferenceType StringToTableReferenceType(const std::string &str) {
  std::string upper_str = StringUtil::Upper(str);
  if (upper_str == "INVALID") {
    return TableReferenceType::INVALID;
  } else if (upper_str == "NAME") {
    return TableReferenceType::NAME;
  } else if (upper_str == "SELECT") {
    return TableReferenceType::SELECT;
  } else if (upper_str == "JOIN") {
    return TableReferenceType::JOIN;
  } else if (upper_str == "CROSS_PRODUCT") {
    return TableReferenceType::CROSS_PRODUCT;
  } else {
    throw ConversionException(
        StringUtil::Format("No TableReferenceType conversion from string '%s'",
                           upper_str.c_str()));
  }
  return TableReferenceType::INVALID;
}

std::ostream &operator<<(std::ostream &os, const TableReferenceType &type) {
  os << TableReferenceTypeToString(type);
  return os;
}

//===--------------------------------------------------------------------===//
// InsertType - String Utilities
//===--------------------------------------------------------------------===//

std::string InsertTypeToString(InsertType type) {
  switch (type) {
    case InsertType::INVALID: {
      return "INVALID";
    }
    case InsertType::VALUES: {
      return "VALUES";
    }
    case InsertType::SELECT: {
      return "SELECT";
    }
    default: {
      throw ConversionException(
          StringUtil::Format("No string conversion for InsertType value '%d'",
                             static_cast<int>(type)));
    }
  }
  return "INVALID";
}

InsertType StringToInsertType(const std::string &str) {
  std::string upper_str = StringUtil::Upper(str);
  if (upper_str == "INVALID") {
    return InsertType::INVALID;
  } else if (upper_str == "VALUES") {
    return InsertType::VALUES;
  } else if (upper_str == "SELECT") {
    return InsertType::SELECT;
  } else {
    throw ConversionException(StringUtil::Format(
        "No InsertType conversion from string '%s'", upper_str.c_str()));
  }
  return InsertType::INVALID;
}

std::ostream &operator<<(std::ostream &os, const InsertType &type) {
  os << InsertTypeToString(type);
  return os;
}

//===--------------------------------------------------------------------===//
// CopyType - String Utilities
//===--------------------------------------------------------------------===//

std::string CopyTypeToString(CopyType type) {
  switch (type) {
    case CopyType::INVALID: {
      return "INVALID";
    }
    case CopyType::IMPORT_CSV: {
      return "IMPORT_CSV";
    }
    case CopyType::IMPORT_TSV: {
      return "IMPORT_TSV";
    }
    case CopyType::EXPORT_CSV: {
      return "EXPORT_CSV";
    }
    case CopyType::EXPORT_STDOUT: {
      return "EXPORT_STDOUT";
    }
    case CopyType::EXPORT_OTHER: {
      return "EXPORT_OTHER";
    }
    default: {
      throw ConversionException(
          StringUtil::Format("No string conversion for CopyType value '%d'",
                             static_cast<int>(type)));
    }
  }
  return "INVALID";
}

CopyType StringToCopyType(const std::string &str) {
  std::string upper_str = StringUtil::Upper(str);
  if (upper_str == "INVALID") {
    return CopyType::INVALID;
  } else if (upper_str == "IMPORT_CSV") {
    return CopyType::IMPORT_CSV;
  } else if (upper_str == "IMPORT_TSV") {
    return CopyType::IMPORT_TSV;
  } else if (upper_str == "EXPORT_CSV") {
    return CopyType::EXPORT_CSV;
  } else if (upper_str == "EXPORT_STDOUT") {
    return CopyType::EXPORT_STDOUT;
  } else if (upper_str == "EXPORT_OTHER") {
    return CopyType::EXPORT_OTHER;
  } else {
    throw ConversionException(StringUtil::Format(
        "No CopyType conversion from string '%s'", upper_str.c_str()));
  }
  return CopyType::INVALID;
}

std::ostream &operator<<(std::ostream &os, const CopyType &type) {
  os << CopyTypeToString(type);
  return os;
}

//===--------------------------------------------------------------------===//
// PayloadType - String Utilities
//===--------------------------------------------------------------------===//

std::string PayloadTypeToString(PayloadType type) {
  switch (type) {
    case PayloadType::INVALID: {
      return ("INVALID");
    }
    case PayloadType::CLIENT_REQUEST: {
      return ("CLIENT_REQUEST");
    }
    case PayloadType::CLIENT_RESPONSE: {
      return ("CLIENT_RESPONSE");
    }
    case PayloadType::STOP: {
      return ("STOP");
    }
    default: {
      throw ConversionException(
          StringUtil::Format("No string conversion for PayloadType value '%d'",
                             static_cast<int>(type)));
    }
  }
  return "INVALID";
}

PayloadType StringToPayloadType(const std::string &str) {
  std::string upper_str = StringUtil::Upper(str);
  if (upper_str == "INVALID") {
    return PayloadType::INVALID;
  } else if (upper_str == "CLIENT_REQUEST") {
    return PayloadType::CLIENT_REQUEST;
  } else if (upper_str == "CLIENT_RESPONSE") {
    return PayloadType::CLIENT_RESPONSE;
  } else if (upper_str == "STOP") {
    return PayloadType::STOP;
  } else {
    throw ConversionException(StringUtil::Format(
        "No PayloadType conversion from string '%s'", upper_str.c_str()));
  }
  return PayloadType::INVALID;
}

std::ostream &operator<<(std::ostream &os, const PayloadType &type) {
  os << PayloadTypeToString(type);
  return os;
}

//===--------------------------------------------------------------------===//
// TaskPriorityType - String Utilities
//===--------------------------------------------------------------------===//

std::string TaskPriorityTypeToString(TaskPriorityType type) {
  switch (type) {
    case TaskPriorityType::INVALID: {
      return "INVALID";
    }
    case TaskPriorityType::LOW: {
      return "LOW";
    }
    case TaskPriorityType::NORMAL: {
      return "NORMAL";
    }
    case TaskPriorityType::HIGH: {
      return "HIGH";
    }
    default: {
      throw ConversionException(StringUtil::Format(
          "No string conversion for TaskPriorityType value '%d'",
          static_cast<int>(type)));
    }
  }
  return "INVALID";
}

TaskPriorityType StringToTaskPriorityType(const std::string &str) {
  std::string upper_str = StringUtil::Upper(str);
  if (upper_str == "INVALID") {
    return TaskPriorityType::INVALID;
  } else if (upper_str == "LOW") {
    return TaskPriorityType::LOW;
  } else if (upper_str == "NORMAL") {
    return TaskPriorityType::NORMAL;
  } else if (upper_str == "HIGH") {
    return TaskPriorityType::HIGH;
  } else {
    throw ConversionException(StringUtil::Format(
        "No TaskPriorityType conversion from string '%s'", upper_str.c_str()));
  }
  return TaskPriorityType::INVALID;
}

std::ostream &operator<<(std::ostream &os, const TaskPriorityType &type) {
  os << TaskPriorityTypeToString(type);
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
    case ResultType::QUEUING: {
      return ("QUEUING");
    }
    default: {
      throw ConversionException(
          StringUtil::Format("No string conversion for ResultType value '%d'",
                             static_cast<int>(type)));
    }
  }
  return "INVALID";
}

ResultType StringToResultType(const std::string &str) {
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
  } else if (upper_str == "QUEUING") {
    return ResultType::QUEUING;
  } else {
    throw ConversionException(StringUtil::Format(
        "No ResultType conversion from string '%s'", upper_str.c_str()));
  }
  return ResultType::INVALID;
}

std::ostream &operator<<(std::ostream &os, const ResultType &type) {
  os << ResultTypeToString(type);
  return os;
}

//===--------------------------------------------------------------------===//
// Constraint Type - String Utilities
//===--------------------------------------------------------------------===//

std::string ConstraintTypeToString(ConstraintType type) {
  switch (type) {
    case ConstraintType::INVALID: {
      return ("INVALID");
    }
    case ConstraintType::NOT_NULL: {
      return ("NOT_NULL");
    }
    case ConstraintType::NOTNULL: {
      return ("NOTNULL");
    }
    case ConstraintType::DEFAULT: {
      return ("DEFAULT");
    }
    case ConstraintType::CHECK: {
      return ("CHECK");
    }
    case ConstraintType::PRIMARY: {
      return ("PRIMARY");
    }
    case ConstraintType::UNIQUE: {
      return ("UNIQUE");
    }
    case ConstraintType::FOREIGN: {
      return ("FOREIGN");
    }
    case ConstraintType::EXCLUSION: {
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

ConstraintType StringToConstraintType(const std::string &str) {
  std::string upper_str = StringUtil::Upper(str);
  if (upper_str == "INVALID") {
    return ConstraintType::INVALID;
  } else if (upper_str == "NOT_NULL") {
    return ConstraintType::NOT_NULL;
  } else if (upper_str == "NOTNULL") {
    return ConstraintType::NOTNULL;
  } else if (upper_str == "DEFAULT") {
    return ConstraintType::DEFAULT;
  } else if (upper_str == "CHECK") {
    return ConstraintType::CHECK;
  } else if (upper_str == "PRIMARY") {
    return ConstraintType::PRIMARY;
  } else if (upper_str == "UNIQUE") {
    return ConstraintType::UNIQUE;
  } else if (upper_str == "FOREIGN") {
    return ConstraintType::FOREIGN;
  } else if (upper_str == "EXCLUSION") {
    return ConstraintType::EXCLUSION;
  } else {
    throw ConversionException(StringUtil::Format(
        "No ConstraintType conversion from string '%s'", upper_str.c_str()));
  }
  return ConstraintType::INVALID;
}

std::ostream &operator<<(std::ostream &os, const ConstraintType &type) {
  os << ConstraintTypeToString(type);
  return os;
}

//===--------------------------------------------------------------------===//
// SetOpType - String Utilities
//===--------------------------------------------------------------------===//

std::string SetOpTypeToString(SetOpType type) {
  switch (type) {
    case SetOpType::INVALID: {
      return "INVALID";
    }
    case SetOpType::INTERSECT: {
      return "INTERSECT";
    }
    case SetOpType::INTERSECT_ALL: {
      return "INTERSECT_ALL";
    }
    case SetOpType::EXCEPT: {
      return "EXCEPT";
    }
    case SetOpType::EXCEPT_ALL: {
      return "EXCEPT_ALL";
    }
    default: {
      throw ConversionException(
          StringUtil::Format("No string conversion for SetOpType value '%d'",
                             static_cast<int>(type)));
    }
  }
  return "INVALID";
}

SetOpType StringToSetOpType(const std::string &str) {
  std::string upper_str = StringUtil::Upper(str);
  if (upper_str == "INVALID") {
    return SetOpType::INVALID;
  } else if (upper_str == "INTERSECT") {
    return SetOpType::INTERSECT;
  } else if (upper_str == "INTERSECT_ALL") {
    return SetOpType::INTERSECT_ALL;
  } else if (upper_str == "EXCEPT") {
    return SetOpType::EXCEPT;
  } else if (upper_str == "EXCEPT_ALL") {
    return SetOpType::EXCEPT_ALL;
  } else {
    throw ConversionException(StringUtil::Format(
        "No SetOpType conversion from string '%s'", upper_str.c_str()));
  }
  return SetOpType::INVALID;
}

std::ostream &operator<<(std::ostream &os, const SetOpType &type) {
  os << SetOpTypeToString(type);
  return os;
}

//===--------------------------------------------------------------------===//
// Concurrency Control Types
//===--------------------------------------------------------------------===//

std::string ProtocolTypeToString(ProtocolType type) {
  switch (type) {
    case ProtocolType::INVALID: {
      return "INVALID";
    }
    case ProtocolType::TIMESTAMP_ORDERING: {
      return "TIMESTAMP_ORDERING";
    }
    default: {
      throw ConversionException(
          StringUtil::Format("No string conversion for ProtocolType value '%d'",
                             static_cast<int>(type)));
    }
  }
  return "INVALID";
}

ProtocolType StringToProtocolType(const std::string &str) {
  std::string upper_str = StringUtil::Upper(str);
  if (upper_str == "INVALID") {
    return ProtocolType::INVALID;
  } else if (upper_str == "TIMESTAMP_ORDERING") {
    return ProtocolType::TIMESTAMP_ORDERING;
  } else {
    throw ConversionException(StringUtil::Format(
        "No ProtocolType conversion from string '%s'", upper_str.c_str()));
  }
  return ProtocolType::INVALID;
}

std::ostream &operator<<(std::ostream &os, const ProtocolType &type) {
  os << ProtocolTypeToString(type);
  return os;
}

//===--------------------------------------------------------------------===//
// Epoch Types
//===--------------------------------------------------------------------===//

std::string EpochTypeToString(EpochType type) {
  switch (type) {
    case EpochType::INVALID: {
      return "INVALID";
    }
    case EpochType::DECENTRALIZED_EPOCH: {
      return "DECENTRALIZED_EPOCH";
    }
    default: {
      throw ConversionException(
          StringUtil::Format("No string conversion for EpochType value '%d'",
                             static_cast<int>(type)));
    }
  }
  return "INVALID";
}

EpochType StringToEpochType(const std::string &str) {
  std::string upper_str = StringUtil::Upper(str);
  if (upper_str == "INVALID") {
    return EpochType::INVALID;
  } else if (upper_str == "DECENTRALIZED_EPOCH") {
    return EpochType::DECENTRALIZED_EPOCH;
  } else {
    throw ConversionException(StringUtil::Format(
        "No EpochType conversion from string '%s'", upper_str.c_str()));
  }
  return EpochType::INVALID;
}

std::ostream &operator<<(std::ostream &os, const EpochType &type) {
  os << EpochTypeToString(type);
  return os;
}

std::string TimestampTypeToString(TimestampType type) {
  switch (type) {
    case TimestampType::INVALID: {
      return "INVALID";
    }
    case TimestampType::SNAPSHOT_READ: {
      return "SNAPSHOT_READ";
    }
    case TimestampType::READ: {
      return "READ";
    }
    case TimestampType::COMMIT: {
      return "COMMIT";
    }
    default: {
      throw ConversionException(StringUtil::Format(
          "No string conversion for TimestampType value '%d'",
          static_cast<int>(type)));
    }
  }
  return "INVALID";
}

TimestampType StringToTimestampType(const std::string &str) {
  std::string upper_str = StringUtil::Upper(str);
  if (upper_str == "INVALID") {
    return TimestampType::INVALID;
  } else if (upper_str == "SNAPSHOT_READ") {
    return TimestampType::SNAPSHOT_READ;
  } else if (upper_str == "READ") {
    return TimestampType::READ;
  } else if (upper_str == "COMMIT") {
    return TimestampType::COMMIT;
  } else {
    throw ConversionException(StringUtil::Format(
        "No TimestampType conversion from string '%s'", upper_str.c_str()));
  }
  return TimestampType::INVALID;
}

std::ostream &operator<<(std::ostream &os, const TimestampType &type) {
  os << TimestampTypeToString(type);
  return os;
}

//===--------------------------------------------------------------------===//
// Visibility Types
//===--------------------------------------------------------------------===//

std::string VisibilityTypeToString(VisibilityType type) {
  switch (type) {
    case VisibilityType::INVALID: {
      return "INVALID";
    }
    case VisibilityType::INVISIBLE: {
      return "INVISIBLE";
    }
    case VisibilityType::DELETED: {
      return "DELETED";
    }
    case VisibilityType::OK: {
      return "OK";
    }
    default: {
      throw ConversionException(StringUtil::Format(
          "No string conversion for VisibilityType value '%d'",
          static_cast<int>(type)));
    }
  }
  return "INVALID";
}

VisibilityType StringToVisibilityType(const std::string &str) {
  std::string upper_str = StringUtil::Upper(str);
  if (upper_str == "INVALID") {
    return VisibilityType::INVALID;
  } else if (upper_str == "INVISIBLE") {
    return VisibilityType::INVISIBLE;
  } else if (upper_str == "DELETED") {
    return VisibilityType::DELETED;
  } else if (upper_str == "OK") {
    return VisibilityType::OK;
  } else {
    throw ConversionException(StringUtil::Format(
        "No VisibilityType conversion from string '%s'", upper_str.c_str()));
  }
  return VisibilityType::INVALID;
}

std::ostream &operator<<(std::ostream &os, const VisibilityType &type) {
  os << VisibilityTypeToString(type);
  return os;
}

std::string VisibilityIdTypeToString(VisibilityIdType type) {
  switch (type) {
    case VisibilityIdType::INVALID: {
      return "INVALID";
    }
    case VisibilityIdType::READ_ID: {
      return "READ_ID";
    }
    case VisibilityIdType::COMMIT_ID: {
      return "COMMIT_ID";
    }
    default: {
      throw ConversionException(StringUtil::Format(
          "No string conversion for VisibilityIdType value '%d'",
          static_cast<int>(type)));
    }
  }
  return "INVALID";
}

VisibilityIdType StringToVisibilityIdType(const std::string &str) {
  std::string upper_str = StringUtil::Upper(str);
  if (upper_str == "INVALID") {
    return VisibilityIdType::INVALID;
  } else if (upper_str == "READ_ID") {
    return VisibilityIdType::READ_ID;
  } else if (upper_str == "COMMIT_ID") {
    return VisibilityIdType::COMMIT_ID;
  } else {
    throw ConversionException(StringUtil::Format(
        "No VisibilityIdType conversion from string '%s'", upper_str.c_str()));
  }
  return VisibilityIdType::INVALID;
}

std::ostream &operator<<(std::ostream &os, const VisibilityIdType &type) {
  os << VisibilityIdTypeToString(type);
  return os;
}

//===--------------------------------------------------------------------===//
// Isolation Levels
//===--------------------------------------------------------------------===//

std::string IsolationLevelTypeToString(IsolationLevelType type) {
  switch (type) {
    case IsolationLevelType::INVALID: {
      return "INVALID";
    }
    case IsolationLevelType::SERIALIZABLE: {
      return "SERIALIZABLE";
    }
    case IsolationLevelType::SNAPSHOT: {
      return "SNAPSHOT";
    }
    case IsolationLevelType::REPEATABLE_READS: {
      return "REPEATABLE_READS";
    }
    case IsolationLevelType::READ_COMMITTED: {
      return "READ_COMMITTED";
    }
    case IsolationLevelType::READ_ONLY: {
      return "READ_ONLY";
    }
    default: {
      throw ConversionException(StringUtil::Format(
          "No string conversion for IsolationLevelType value '%d'",
          static_cast<int>(type)));
    }
  }
  return "INVALID";
}

IsolationLevelType StringToIsolationLevelType(const std::string &str) {
  std::string upper_str = StringUtil::Upper(str);
  if (upper_str == "INVALID") {
    return IsolationLevelType::INVALID;
  } else if (upper_str == "SERIALIZABLE") {
    return IsolationLevelType::SERIALIZABLE;
  } else if (upper_str == "SNAPSHOT") {
    return IsolationLevelType::SNAPSHOT;
  } else if (upper_str == "REPEATABLE_READS") {
    return IsolationLevelType::REPEATABLE_READS;
  } else if (upper_str == "READ_COMMITTED") {
    return IsolationLevelType::READ_COMMITTED;
  } else if (upper_str == "READ_ONLY") {
    return IsolationLevelType::READ_ONLY;
  } else {
    throw ConversionException(
        StringUtil::Format("No IsolationLevelType conversion from string '%s'",
                           upper_str.c_str()));
  }
  return IsolationLevelType::INVALID;
}

std::ostream &operator<<(std::ostream &os, const IsolationLevelType &type) {
  os << IsolationLevelTypeToString(type);
  return os;
}

//===--------------------------------------------------------------------===//
// Conflict Avoidance types
//===--------------------------------------------------------------------===//

std::string ConflictAvoidanceTypeToString(ConflictAvoidanceType type) {
  switch (type) {
    case ConflictAvoidanceType::INVALID: {
      return "INVALID";
    }
    case ConflictAvoidanceType::WAIT: {
      return "WAIT";
    }
    case ConflictAvoidanceType::ABORT: {
      return "ABORT";
    }
    default: {
      throw ConversionException(StringUtil::Format(
          "No string conversion for ConflictAvoidanceType value '%d'",
          static_cast<int>(type)));
    }
  }
  return "INVALID";
}

ConflictAvoidanceType StringToConflictAvoidanceType(const std::string &str) {
  std::string upper_str = StringUtil::Upper(str);
  if (upper_str == "INVALID") {
    return ConflictAvoidanceType::INVALID;
  } else if (upper_str == "WAIT") {
    return ConflictAvoidanceType::WAIT;
  } else if (upper_str == "ABORT") {
    return ConflictAvoidanceType::ABORT;
  } else {
    throw ConversionException(StringUtil::Format(
        "No ConflictAvoidanceType conversion from string '%s'",
        upper_str.c_str()));
  }
  return ConflictAvoidanceType::INVALID;
}

std::ostream &operator<<(std::ostream &os, const ConflictAvoidanceType &type) {
  os << ConflictAvoidanceTypeToString(type);
  return os;
}

//===--------------------------------------------------------------------===//
// Garbage Collection Types
//===--------------------------------------------------------------------===//

std::string GarbageCollectionTypeToString(GarbageCollectionType type) {
  switch (type) {
    case GarbageCollectionType::INVALID: {
      return "INVALID";
    }
    case GarbageCollectionType::OFF: {
      return "OFF";
    }
    case GarbageCollectionType::ON: {
      return "ON";
    }
    default: {
      throw ConversionException(StringUtil::Format(
          "No string conversion for GarbageCollectionType value '%d'",
          static_cast<int>(type)));
    }
  }
  return "INVALID";
}

GarbageCollectionType StringToGarbageCollectionType(const std::string &str) {
  std::string upper_str = StringUtil::Upper(str);
  if (upper_str == "INVALID") {
    return GarbageCollectionType::INVALID;
  } else if (upper_str == "OFF") {
    return GarbageCollectionType::OFF;
  } else if (upper_str == "ON") {
    return GarbageCollectionType::ON;
  } else {
    throw ConversionException(StringUtil::Format(
        "No GarbageCollectionType conversion from string '%s'",
        upper_str.c_str()));
  }
  return GarbageCollectionType::INVALID;
}

std::ostream &operator<<(std::ostream &os, const GarbageCollectionType &type) {
  os << GarbageCollectionTypeToString(type);
  return os;
}

//===--------------------------------------------------------------------===//
// LoggingType - String Utilities
//===--------------------------------------------------------------------===//

std::string LoggingTypeToString(LoggingType type) {
  switch (type) {
    case LoggingType::INVALID: {
      return "INVALID";
    }
    case LoggingType::OFF: {
      return "OFF";
    }
    case LoggingType::ON: {
      return "ON";
    }
    default: {
      throw ConversionException(
          StringUtil::Format("No string conversion for LoggingType value '%d'",
                             static_cast<int>(type)));
    }
  }
  return "INVALID";
}

LoggingType StringToLoggingType(const std::string &str) {
  std::string upper_str = StringUtil::Upper(str);
  if (upper_str == "INVALID") {
    return LoggingType::INVALID;
  } else if (upper_str == "OFF") {
    return LoggingType::OFF;
  } else if (upper_str == "ON") {
    return LoggingType::ON;
  } else {
    throw ConversionException(StringUtil::Format(
        "No LoggingType conversion from string '%s'", upper_str.c_str()));
  }
  return LoggingType::INVALID;
}

std::ostream &operator<<(std::ostream &os, const LoggingType &type) {
  os << LoggingTypeToString(type);
  return os;
}

std::string LogRecordTypeToString(LogRecordType type) {
  switch (type) {
    case LogRecordType::INVALID: {
      return "INVALID";
    }
    case LogRecordType::TRANSACTION_BEGIN: {
      return "TRANSACTION_BEGIN";
    }
    case LogRecordType::TRANSACTION_COMMIT: {
      return "TRANSACTION_COMMIT";
    }
    case LogRecordType::TUPLE_INSERT: {
      return "TUPLE_INSERT";
    }
    case LogRecordType::TUPLE_DELETE: {
      return "TUPLE_DELETE";
    }
    case LogRecordType::TUPLE_UPDATE: {
      return "TUPLE_UPDATE";
    }
    case LogRecordType::EPOCH_BEGIN: {
      return "EPOCH_BEGIN";
    }
    case LogRecordType::EPOCH_END: {
      return "EPOCH_END";
    }
    default: {
      throw ConversionException(StringUtil::Format(
          "No string conversion for LogRecordType value '%d'",
          static_cast<int>(type)));
    }
  }
  return "INVALID";
}

LogRecordType StringToLogRecordType(const std::string &str) {
  std::string upper_str = StringUtil::Upper(str);
  if (upper_str == "INVALID") {
    return LogRecordType::INVALID;
  } else if (upper_str == "TRANSACTION_BEGIN") {
    return LogRecordType::TRANSACTION_BEGIN;
  } else if (upper_str == "TRANSACTION_COMMIT") {
    return LogRecordType::TRANSACTION_COMMIT;
  } else if (upper_str == "TUPLE_INSERT") {
    return LogRecordType::TUPLE_INSERT;
  } else if (upper_str == "TUPLE_DELETE") {
    return LogRecordType::TUPLE_DELETE;
  } else if (upper_str == "TUPLE_UPDATE") {
    return LogRecordType::TUPLE_UPDATE;
  } else if (upper_str == "EPOCH_BEGIN") {
    return LogRecordType::EPOCH_BEGIN;
  } else if (upper_str == "EPOCH_END") {
    return LogRecordType::EPOCH_END;
  } else {
    throw ConversionException(StringUtil::Format(
        "No LogRecordType conversion from string '%s'", upper_str.c_str()));
  }
  return LogRecordType::INVALID;
}

std::ostream &operator<<(std::ostream &os, const LogRecordType &type) {
  os << LogRecordTypeToString(type);
  return os;
}

//===--------------------------------------------------------------------===//
// CheckpointingType - String Utilities
//===--------------------------------------------------------------------===//

std::string CheckpointingTypeToString(CheckpointingType type) {
  switch (type) {
    case CheckpointingType::INVALID: {
      return "INVALID";
    }
    case CheckpointingType::OFF: {
      return "OFF";
    }
    case CheckpointingType::ON: {
      return "ON";
    }
    default: {
      throw ConversionException(StringUtil::Format(
          "No string conversion for CheckpointingType value '%d'",
          static_cast<int>(type)));
    }
  }
  return "INVALID";
}

CheckpointingType StringToCheckpointingType(const std::string &str) {
  std::string upper_str = StringUtil::Upper(str);
  if (upper_str == "INVALID") {
    return CheckpointingType::INVALID;
  } else if (upper_str == "OFF") {
    return CheckpointingType::OFF;
  } else if (upper_str == "ON") {
    return CheckpointingType::ON;
  } else {
    throw ConversionException(StringUtil::Format(
        "No CheckpointingType conversion from string '%s'", upper_str.c_str()));
  }
  return CheckpointingType::INVALID;
}

std::ostream &operator<<(std::ostream &os, const CheckpointingType &type) {
  os << CheckpointingTypeToString(type);
  return os;
}

std::string LayoutTypeToString(LayoutType type) {
  switch (type) {
    case LayoutType::INVALID: {
      return "INVALID";
    }
    case LayoutType::ROW: {
      return "ROW";
    }
    case LayoutType::COLUMN: {
      return "COLUMN";
    }
    case LayoutType::HYBRID: {
      return "HYBRID";
    }
    default: {
      throw ConversionException(
          StringUtil::Format("No string conversion for LayoutType value '%d'",
                             static_cast<int>(type)));
    }
  }
  return "INVALID";
}

std::ostream &operator<<(std::ostream &os, const LayoutType &type) {
  os << LayoutTypeToString(type);
  return os;
}

type::TypeId PostgresValueTypeToPelotonValueType(PostgresValueType type) {
  switch (type) {
    case PostgresValueType::BOOLEAN:
      return type::TypeId::BOOLEAN;

    case PostgresValueType::SMALLINT:
      return type::TypeId::SMALLINT;
    case PostgresValueType::INTEGER:
      return type::TypeId::INTEGER;
    case PostgresValueType::BIGINT:
      return type::TypeId::BIGINT;
    case PostgresValueType::REAL:
      return type::TypeId::DECIMAL;
    case PostgresValueType::DOUBLE:
      return type::TypeId::DECIMAL;

    case PostgresValueType::BPCHAR:
    case PostgresValueType::BPCHAR2:
    case PostgresValueType::VARCHAR:
    case PostgresValueType::VARCHAR2:
    case PostgresValueType::TEXT:
      return type::TypeId::VARCHAR;

    case PostgresValueType::DATE:
    case PostgresValueType::TIMESTAMPS:
    case PostgresValueType::TIMESTAMPS2:
      return type::TypeId::TIMESTAMP;

    case PostgresValueType::DECIMAL:
      return type::TypeId::DECIMAL;
    default:
      throw ConversionException(StringUtil::Format(
          "No TypeId conversion for PostgresValueType value '%d'",
          static_cast<int>(type)));
  }
  return type::TypeId::INVALID;
}

ConstraintType PostgresConstraintTypeToPelotonConstraintType(
    PostgresConstraintType type) {
  ConstraintType constraintType = ConstraintType::INVALID;

  switch (type) {
    case PostgresConstraintType::NOT_NULL:
      constraintType = ConstraintType::NOT_NULL;
      break;

    case PostgresConstraintType::NOTNULL:
      constraintType = ConstraintType::NOTNULL;
      break;

    case PostgresConstraintType::DEFAULT:
      constraintType = ConstraintType::DEFAULT;
      break;

    case PostgresConstraintType::CHECK:
      constraintType = ConstraintType::CHECK;
      break;

    case PostgresConstraintType::PRIMARY:
      constraintType = ConstraintType::PRIMARY;
      break;

    case PostgresConstraintType::UNIQUE:
      constraintType = ConstraintType::UNIQUE;
      break;

    case PostgresConstraintType::FOREIGN:
      constraintType = ConstraintType::FOREIGN;
      break;

    case PostgresConstraintType::EXCLUSION:
      constraintType = ConstraintType::EXCLUSION;
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
// EntityType - String Utilities
//===--------------------------------------------------------------------===//

std::string EntityTypeToString(EntityType type) {
  switch (type) {
    case EntityType::INVALID: {
      return "INVALID";
    }
    case EntityType::TABLE: {
      return "TABLE";
    }
    case EntityType::SCHEMA: {
      return "SCHEMA";
    }
    case EntityType::INDEX: {
      return "INDEX";
    }
    case EntityType::VIEW: {
      return "VIEW";
    }
    case EntityType::PREPARED_STATEMENT: {
      return "PREPARED_STATEMENT";
    }
    default: {
      throw ConversionException(
          StringUtil::Format("No string conversion for EntityType value '%d'",
                             static_cast<int>(type)));
    }
  }
  return "INVALID";
}

EntityType StringToEntityType(const std::string &str) {
  std::string upper_str = StringUtil::Upper(str);
  if (upper_str == "INVALID") {
    return EntityType::INVALID;
  } else if (upper_str == "TABLE") {
    return EntityType::TABLE;
  } else if (upper_str == "SCHEMA") {
    return EntityType::SCHEMA;
  } else if (upper_str == "INDEX") {
    return EntityType::INDEX;
  } else if (upper_str == "VIEW") {
    return EntityType::VIEW;
  } else if (upper_str == "PREPARED_STATEMENT") {
    return EntityType::PREPARED_STATEMENT;
  } else {
    throw ConversionException(StringUtil::Format(
        "No EntityType conversion from string '%s'", upper_str.c_str()));
  }
  return EntityType::INVALID;
}

std::ostream &operator<<(std::ostream &os, const EntityType &type) {
  os << EntityTypeToString(type);
  return os;
}

//===--------------------------------------------------------------------===//
// RWType - String Utilities
//===--------------------------------------------------------------------===//

std::string RWTypeToString(RWType type) {
  switch (type) {
    case RWType::INVALID: {
      return "INVALID";
    }
    case RWType::READ: {
      return "READ";
    }
    case RWType::READ_OWN: {
      return "READ_OWN";
    }
    case RWType::UPDATE: {
      return "UPDATE";
    }
    case RWType::INSERT: {
      return "INSERT";
    }
    case RWType::DELETE: {
      return "DELETE";
    }
    case RWType::INS_DEL: {
      return "INS_DEL";
    }
    default: {
      throw ConversionException(
          StringUtil::Format("No string conversion for RWType value '%d'",
                             static_cast<int>(type)));
    }
  }
  return "INVALID";
}

RWType StringToRWType(const std::string &str) {
  std::string upper_str = StringUtil::Upper(str);
  if (upper_str == "INVALID") {
    return RWType::INVALID;
  } else if (upper_str == "READ") {
    return RWType::READ;
  } else if (upper_str == "READ_OWN") {
    return RWType::READ_OWN;
  } else if (upper_str == "UPDATE") {
    return RWType::UPDATE;
  } else if (upper_str == "INSERT") {
    return RWType::INSERT;
  } else if (upper_str == "DELETE") {
    return RWType::DELETE;
  } else if (upper_str == "INS_DEL") {
    return RWType::INS_DEL;
  } else {
    throw ConversionException(StringUtil::Format(
        "No RWType conversion from string '%s'", upper_str.c_str()));
  }
  return RWType::INVALID;
}

std::ostream &operator<<(std::ostream &os, const RWType &type) {
  os << RWTypeToString(type);
  return os;
}

//===--------------------------------------------------------------------===//
// GCVersionType - String Utilities
//===--------------------------------------------------------------------===//

std::string GCVersionTypeToString(GCVersionType type) {
  switch (type) {
    case GCVersionType::INVALID: {
      return "INVALID";
    }
    case GCVersionType::COMMIT_UPDATE: {
      return "COMMIT_UPDATE";
    }
    case GCVersionType::COMMIT_DELETE: {
      return "COMMIT_DELETE";
    }
    case GCVersionType::COMMIT_INS_DEL: {
      return "COMMIT_INS_DEL";
    }
    case GCVersionType::ABORT_UPDATE: {
      return "ABORT_UPDATE";
    }
    case GCVersionType::ABORT_DELETE: {
      return "ABORT_DELETE";
    }
    case GCVersionType::ABORT_INSERT: {
      return "ABORT_INSERT";
    }
    case GCVersionType::ABORT_INS_DEL: {
      return "ABORT_INS_DEL";
    }
    default: {
      throw ConversionException(StringUtil::Format(
          "No string conversion for GCVersionType value '%d'",
          static_cast<int>(type)));
    }
  }
  return "INVALID";
}

GCVersionType StringToGCVersionType(const std::string &str) {
  std::string upper_str = StringUtil::Upper(str);
  if (upper_str == "INVALID") {
    return GCVersionType::INVALID;
  } else if (upper_str == "COMMIT_UPDATE") {
    return GCVersionType::COMMIT_UPDATE;
  } else if (upper_str == "COMMIT_DELETE") {
    return GCVersionType::COMMIT_DELETE;
  } else if (upper_str == "COMMIT_INS_DEL") {
    return GCVersionType::COMMIT_INS_DEL;
  } else if (upper_str == "ABORT_UPDATE") {
    return GCVersionType::ABORT_UPDATE;
  } else if (upper_str == "ABORT_DELETE") {
    return GCVersionType::ABORT_DELETE;
  } else if (upper_str == "ABORT_INSERT") {
    return GCVersionType::ABORT_INSERT;
  } else if (upper_str == "ABORT_INS_DEL") {
    return GCVersionType::ABORT_INS_DEL;
  } else {
    throw ConversionException(StringUtil::Format(
        "No GCVersionType conversion from string '%s'", upper_str.c_str()));
  }
  return GCVersionType::INVALID;
}

std::ostream &operator<<(std::ostream &os, const GCVersionType &type) {
  os << GCVersionTypeToString(type);
  return os;
}

//===--------------------------------------------------------------------===//
// Optimizer
//===--------------------------------------------------------------------===//

std::string PropertyTypeToString(PropertyType type) {
  switch (type) {
    case PropertyType::INVALID: {
      return "INVALID";
    }
    case PropertyType::COLUMNS: {
      return "COLUMNS";
    }
    case PropertyType::DISTINCT: {
      return "DISTINCT";
    }
    case PropertyType::SORT: {
      return "SORT";
    }
    case PropertyType::LIMIT: {
      return "LIMIT";
    }
    default: {
      throw ConversionException(
          StringUtil::Format("No string conversion for PropertyType value '%d'",
                             static_cast<int>(type)));
    }
  }
  return "INVALID";
}

PropertyType StringToPropertyType(const std::string &str) {
  std::string upper_str = StringUtil::Upper(str);
  if (upper_str == "INVALID") {
    return PropertyType::INVALID;
  } else if (upper_str == "COLUMNS") {
    return PropertyType::COLUMNS;
  } else if (upper_str == "DISTINCT") {
    return PropertyType::DISTINCT;
  } else if (upper_str == "SORT") {
    return PropertyType::SORT;
  } else if (upper_str == "LIMIT") {
    return PropertyType::LIMIT;
  } else {
    throw ConversionException(StringUtil::Format(
        "No PropertyType conversion from string '%s'", upper_str.c_str()));
  }
  return PropertyType::INVALID;
}

std::ostream &operator<<(std::ostream &os, const PropertyType &type) {
  os << PropertyTypeToString(type);
  return os;
}

//===--------------------------------------------------------------------===//
// Network Message types
//===--------------------------------------------------------------------===//
std::string SqlStateErrorCodeToString(SqlStateErrorCode code) {
  switch (code) {
    case SqlStateErrorCode::SERIALIZATION_ERROR:
      return "40001";
    default:
      return "INVALID";
  }
}

std::string OperatorIdToString(OperatorId op_id) {
  switch (op_id) {
    case OperatorId::Negation:
      return "Negation";
    case OperatorId::Abs:
      return "Abs";
    case OperatorId::Add:
      return "Add";
    case OperatorId::Sub:
      return "Sub";
    case OperatorId::Mul:
      return "Mul";
    case OperatorId::Div:
      return "Div";
    case OperatorId::Mod:
      return "Mod";
    case OperatorId::LogicalAnd:
      return "LogicalAnd";
    case OperatorId::LogicalOr:
      return "LogicalOr";
    case OperatorId::Ascii:
      return "Ascii";
    case OperatorId::Chr:
      return "Chr";
    case OperatorId::Concat:
      return "Concat";
    case OperatorId::Substr:
      return "Substr";
    case OperatorId::CharLength:
      return "CharLength";
    case OperatorId::OctetLength:
      return "OctetLength";
    case OperatorId::Length:
      return "Length";
    case OperatorId::Repeat:
      return "Repeat";
    case OperatorId::Replace:
      return "Replace";
    case OperatorId::LTrim:
      return "LTrim";
    case OperatorId::RTrim:
      return "RTrim";
    case OperatorId::BTrim:
      return "BTrim";
    case OperatorId::Sqrt:
      return "Sqrt";
    case OperatorId::DatePart:
      return "DatePart";
    case OperatorId::Floor:
      return "Floor";
    case OperatorId::Like:
      return "Like";
    case OperatorId::DateTrunc:
      return "DateTrunc";
    default: {
      throw Exception{StringUtil::Format("Invalid operator ID: %u", op_id)};
    }
  }
}

}  // namespace peloton
