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

#include "common/types.h"
#include "common/exception.h"
#include "common/logger.h"
#include "common/macros.h"
#include "common/value_factory.h"

#include <algorithm>
#include <cstring>
#include <sstream>

namespace peloton {

ItemPointer INVALID_ITEMPOINTER;

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
// BackendType <--> String Utilities
//===--------------------------------------------------------------------===//

std::string BackendTypeToString(BackendType type) {
  std::string ret;

  switch (type) {
    case (BACKEND_TYPE_MM):
      return "MM";
    case (BACKEND_TYPE_NVM):
      return "NVM";
    case (BACKEND_TYPE_SSD):
      return "SSD";
    case (BACKEND_TYPE_HDD):
      return "HDD";
    case (BACKEND_TYPE_INVALID):
      return "INVALID";
    default: { return "UNKNOWN " + std::to_string(type); }
  }
  return (ret);
}

BackendType StringToBackendType(std::string str) {
  if (str == "INVALID") {
    return BACKEND_TYPE_INVALID;
  } else if (str == "MM") {
    return BACKEND_TYPE_MM;
  } else if (str == "NVM") {
    return BACKEND_TYPE_NVM;
  } else if (str == "SSD") {
    return BACKEND_TYPE_SSD;
  } else if (str == "HDD") {
    return BACKEND_TYPE_HDD;
  }
  return BACKEND_TYPE_INVALID;
}

//===--------------------------------------------------------------------===//
// Value <--> String Utilities
//===--------------------------------------------------------------------===//

std::string TypeIdToString(common::Type::TypeId type) {
  switch (type) {
    case common::Type::INVALID:
      return "INVALID";
    case common::Type::PARAMETER_OFFSET:
      return "PARAMETER_OFFSET";
    case common::Type::BOOLEAN:
      return "BOOLEAN";
    case common::Type::TINYINT:
      return "TINYINT";
    case common::Type::SMALLINT:
      return "SMALLINT";
    case common::Type::INTEGER:
      return "INTEGER";
    case common::Type::BIGINT:
      return "BIGINT";
    case common::Type::DECIMAL:
      return "DECIMAL";
    case common::Type::TIMESTAMP:
      return "TIMESTAMP";
    case common::Type::DATE:
      return "DATE";
    case common::Type::VARCHAR:
      return "VARCHAR";
    case common::Type::VARBINARY:
      return "VARBINARY";
    case common::Type::ARRAY:
      return "ARRAY";
    case common::Type::UDT:
      return "UDT";
    default: {
      throw ConversionException("No string conversion for TypeId");  // FIXME
    }
      return "INVALID";
  }
}

common::Type::TypeId StringToTypeId(const std::string& str) {
  if (str == "INVALID") {
    return common::Type::INVALID;
  } else if (str == "PARAMETER_OFFSET") {
    return common::Type::PARAMETER_OFFSET;
  } else if (str == "BOOLEAN") {
    return common::Type::BOOLEAN;
  } else if (str == "TINYINT") {
    return common::Type::TINYINT;
  } else if (str == "SMALLINT") {
    return common::Type::SMALLINT;
  } else if (str == "INTEGER") {
    return common::Type::INTEGER;
  } else if (str == "BIGINT") {
    return common::Type::BIGINT;
  } else if (str == "DECIMAL") {
    return common::Type::DECIMAL;
  } else if (str == "TIMESTAMP") {
    return common::Type::TIMESTAMP;
  } else if (str == "DATE") {
    return common::Type::DATE;
  } else if (str == "VARCHAR") {
    return common::Type::VARCHAR;
  } else if (str == "VARBINARY") {
    return common::Type::VARBINARY;
  } else if (str == "ARRAY") {
    return common::Type::ARRAY;
  } else if (str == "UDT") {
    return common::Type::UDT;
  } else {
    throw ConversionException("No conversion from string '" + str + "'");
  }
  return common::Type::INVALID;
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

bool AtomicUpdateItemPointer(ItemPointer* src_ptr, const ItemPointer& value) {
  PL_ASSERT(sizeof(ItemPointer) == sizeof(int64_t));
  int64_t* cast_src_ptr = reinterpret_cast<int64_t*>((void*)src_ptr);
  int64_t* cast_value_ptr = reinterpret_cast<int64_t*>((void*)&value);
  return __sync_bool_compare_and_swap(cast_src_ptr, *cast_src_ptr,
                                      *cast_value_ptr);
}

//===--------------------------------------------------------------------===//
// Statement - String Utilities
//===--------------------------------------------------------------------===//

std::string StatementTypeToString(StatementType type) {
  switch (type) {
    case STATEMENT_TYPE_SELECT: {
      return "SELECT";
    }
    case STATEMENT_TYPE_ALTER: {
      return "ALTER";
    }
    case STATEMENT_TYPE_CREATE: {
      return "CREATE";
    }
    case STATEMENT_TYPE_DELETE: {
      return "DELETE";
    }
    case STATEMENT_TYPE_DROP: {
      return "DROP";
    }
    case STATEMENT_TYPE_EXECUTE: {
      return "EXECUTE";
    }
    case STATEMENT_TYPE_COPY: {
      return "COPY";
    }
    case STATEMENT_TYPE_INSERT: {
      return "INSERT";
    }
    case STATEMENT_TYPE_INVALID: {
      return "INVALID";
    }
    case STATEMENT_TYPE_PREPARE: {
      return "PREPARE";
    }
    case STATEMENT_TYPE_RENAME: {
      return "RENAME";
    }
    case STATEMENT_TYPE_TRANSACTION: {
      return "TRANSACTION";
    }
    case STATEMENT_TYPE_UPDATE: {
      return "UPDATE";
    }
    default: {
      throw ConversionException("No conversion from StatementType");  // FIXME
    }
  }
  return "NOT A KNOWN TYPE - INVALID";
}

StatementType StringToStatementType(const std::string& str) {
  if (str == "INVALID") {
    return STATEMENT_TYPE_INVALID;
  } else if (str == "SELECT") {
    return STATEMENT_TYPE_SELECT;
  } else if (str == "INSERT") {
    return STATEMENT_TYPE_INSERT;
  } else if (str == "UPDATE") {
    return STATEMENT_TYPE_UPDATE;
  } else if (str == "DELETE") {
    return STATEMENT_TYPE_DELETE;
  } else if (str == "CREATE") {
    return STATEMENT_TYPE_CREATE;
  } else if (str == "DROP") {
    return STATEMENT_TYPE_DROP;
  } else if (str == "PREPARE") {
    return STATEMENT_TYPE_PREPARE;
  } else if (str == "EXECUTE") {
    return STATEMENT_TYPE_EXECUTE;
  } else if (str == "RENAME") {
    return STATEMENT_TYPE_RENAME;
  } else if (str == "ALTER") {
    return STATEMENT_TYPE_ALTER;
  } else if (str == "TRANSACTION") {
    return STATEMENT_TYPE_TRANSACTION;
  } else if (str == "COPY") {
    return STATEMENT_TYPE_COPY;
  } else {
    throw ConversionException("No conversion from string '" + str + "'");
  }
  return STATEMENT_TYPE_INVALID;
}

//===--------------------------------------------------------------------===//
// Expression - String Utilities
//===--------------------------------------------------------------------===//

std::string ExpressionTypeToString(ExpressionType type) {
  switch (type) {
    case EXPRESSION_TYPE_INVALID: {
      return ("INVALID");
    }
    case EXPRESSION_TYPE_OPERATOR_PLUS: {
      return ("OPERATOR_PLUS");
    }
    case EXPRESSION_TYPE_OPERATOR_MINUS: {
      return ("OPERATOR_MINUS");
    }
    case EXPRESSION_TYPE_OPERATOR_MULTIPLY: {
      return ("OPERATOR_MULTIPLY");
    }
    case EXPRESSION_TYPE_OPERATOR_DIVIDE: {
      return ("OPERATOR_DIVIDE");
    }
    case EXPRESSION_TYPE_OPERATOR_CONCAT: {
      return ("OPERATOR_CONCAT");
    }
    case EXPRESSION_TYPE_OPERATOR_MOD: {
      return ("OPERATOR_MOD");
    }
    case EXPRESSION_TYPE_OPERATOR_CAST: {
      return ("OPERATOR_CAST");
    }
    case EXPRESSION_TYPE_OPERATOR_NOT: {
      return ("OPERATOR_NOT");
    }
    case EXPRESSION_TYPE_OPERATOR_IS_NULL: {
      return ("OPERATOR_IS_NULL");
    }
    case EXPRESSION_TYPE_OPERATOR_EXISTS: {
      return ("OPERATOR_EXISTS");
    }
    case EXPRESSION_TYPE_OPERATOR_UNARY_MINUS: {
      return ("OPERATOR_UNARY_MINUS");
    }
    case EXPRESSION_TYPE_COMPARE_EQUAL: {
      return ("COMPARE_EQUAL");
    }
    case EXPRESSION_TYPE_COMPARE_NOTEQUAL: {
      return ("COMPARE_NOTEQUAL");
    }
    case EXPRESSION_TYPE_COMPARE_LESSTHAN: {
      return ("COMPARE_LESSTHAN");
    }
    case EXPRESSION_TYPE_COMPARE_GREATERTHAN: {
      return ("COMPARE_GREATERTHAN");
    }
    case EXPRESSION_TYPE_COMPARE_LESSTHANOREQUALTO: {
      return ("COMPARE_LESSTHANOREQUALTO");
    }
    case EXPRESSION_TYPE_COMPARE_GREATERTHANOREQUALTO: {
      return ("COMPARE_GREATERTHANOREQUALTO");
    }
    case EXPRESSION_TYPE_COMPARE_LIKE: {
      return ("COMPARE_LIKE");
    }
    case EXPRESSION_TYPE_COMPARE_NOTLIKE: {
      return ("COMPARE_NOTLIKE");
    }
    case EXPRESSION_TYPE_COMPARE_IN: {
      return ("COMPARE_IN");
    }
    case EXPRESSION_TYPE_CONJUNCTION_AND: {
      return ("CONJUNCTION_AND");
    }
    case EXPRESSION_TYPE_CONJUNCTION_OR: {
      return ("CONJUNCTION_OR");
    }
    case EXPRESSION_TYPE_VALUE_CONSTANT: {
      return ("VALUE_CONSTANT");
    }
    case EXPRESSION_TYPE_VALUE_PARAMETER: {
      return ("VALUE_PARAMETER");
    }
    case EXPRESSION_TYPE_VALUE_TUPLE: {
      return ("VALUE_TUPLE");
    }
    case EXPRESSION_TYPE_VALUE_TUPLE_ADDRESS: {
      return ("VALUE_TUPLE_ADDRESS");
    }
    case EXPRESSION_TYPE_VALUE_NULL: {
      return ("VALUE_NULL");
    }
    case EXPRESSION_TYPE_VALUE_VECTOR: {
      return ("VALUE_VECTOR");
    }
    case EXPRESSION_TYPE_VALUE_SCALAR: {
      return ("VALUE_SCALAR");
    }
    case EXPRESSION_TYPE_AGGREGATE_COUNT: {
      return ("AGGREGATE_COUNT");
    }
    case EXPRESSION_TYPE_AGGREGATE_COUNT_STAR: {
      return ("AGGREGATE_COUNT_STAR");
    }
    case EXPRESSION_TYPE_AGGREGATE_SUM: {
      return ("AGGREGATE_SUM");
    }
    case EXPRESSION_TYPE_AGGREGATE_MIN: {
      return ("AGGREGATE_MIN");
    }
    case EXPRESSION_TYPE_AGGREGATE_MAX: {
      return ("AGGREGATE_MAX");
    }
    case EXPRESSION_TYPE_AGGREGATE_AVG: {
      return ("AGGREGATE_AVG");
    }
    case EXPRESSION_TYPE_FUNCTION: {
      return ("FUNCTION");
    }
    case EXPRESSION_TYPE_HASH_RANGE: {
      return ("HASH_RANGE");
    }
    case EXPRESSION_TYPE_OPERATOR_CASE_EXPR: {
      return ("OPERATOR_CASE_EXPR");
    }
    case EXPRESSION_TYPE_OPERATOR_NULLIF: {
      return ("OPERATOR_NULLIF");
    }
    case EXPRESSION_TYPE_OPERATOR_COALESCE: {
      return ("OPERATOR_COALESCE");
    }
    case EXPRESSION_TYPE_ROW_SUBQUERY: {
      return ("ROW_SUBQUERY");
    }
    case EXPRESSION_TYPE_SELECT_SUBQUERY: {
      return ("SELECT_SUBQUERY");
    }
    case EXPRESSION_TYPE_SUBSTR: {
      return ("SUBSTR");
    }
    case EXPRESSION_TYPE_ASCII: {
      return ("ASCII");
    }
    case EXPRESSION_TYPE_OCTET_LEN: {
      return ("OCTET_LEN");
    }
    case EXPRESSION_TYPE_CHAR: {
      return ("CHAR");
    }
    case EXPRESSION_TYPE_CHAR_LEN: {
      return ("CHAR_LEN");
    }
    case EXPRESSION_TYPE_SPACE: {
      return ("SPACE");
    }
    case EXPRESSION_TYPE_REPEAT: {
      return ("REPEAT");
    }
    case EXPRESSION_TYPE_POSITION: {
      return ("POSITION");
    }
    case EXPRESSION_TYPE_LEFT: {
      return ("LEFT");
    }
    case EXPRESSION_TYPE_RIGHT: {
      return ("RIGHT");
    }
    case EXPRESSION_TYPE_CONCAT: {
      return ("CONCAT");
    }
    case EXPRESSION_TYPE_LTRIM: {
      return ("LTRIM");
    }
    case EXPRESSION_TYPE_RTRIM: {
      return ("RTRIM");
    }
    case EXPRESSION_TYPE_BTRIM: {
      return ("BTRIM");
    }
    case EXPRESSION_TYPE_REPLACE: {
      return ("REPLACE");
    }
    case EXPRESSION_TYPE_OVERLAY: {
      return ("OVERLAY");
    }
    case EXPRESSION_TYPE_EXTRACT: {
      return ("EXTRACT");
    }
    case EXPRESSION_TYPE_DATE_TO_TIMESTAMP: {
      return ("DATE_TO_TIMESTAMP");
    }
    case EXPRESSION_TYPE_STAR: {
      return ("STAR");
    }
    case EXPRESSION_TYPE_PLACEHOLDER: {
      return ("PLACEHOLDER");
    }
    case EXPRESSION_TYPE_COLUMN_REF: {
      return ("COLUMN_REF");
    }
    case EXPRESSION_TYPE_FUNCTION_REF: {
      return ("FUNCTION_REF");
    }
    case EXPRESSION_TYPE_CAST: {
      return ("CAST");
    }
    default: {
      throw ConversionException("No conversion from ExpressionType");  // FIXME
    } break;
  }
  return "INVALID";
}

ExpressionType ParserExpressionNameToExpressionType(const std::string& str) {
  std::string lower_str = str;
  std::transform(lower_str.begin(), lower_str.end(), lower_str.begin(),
                 ::tolower);
  if (str == "count") {
    return EXPRESSION_TYPE_AGGREGATE_COUNT;
  } else if (str == "sum") {
    return EXPRESSION_TYPE_AGGREGATE_SUM;
  } else if (str == "avg") {
    return EXPRESSION_TYPE_AGGREGATE_AVG;
  } else if (str == "max") {
    return EXPRESSION_TYPE_AGGREGATE_MAX;
  } else if (str == "min") {
    return EXPRESSION_TYPE_AGGREGATE_MIN;
  }
  return EXPRESSION_TYPE_INVALID;
}

ExpressionType StringToExpressionType(const std::string& str) {
  if (str == "INVALID") {
    return EXPRESSION_TYPE_INVALID;
  } else if (str == "OPERATOR_PLUS") {
    return EXPRESSION_TYPE_OPERATOR_PLUS;
  } else if (str == "OPERATOR_MINUS") {
    return EXPRESSION_TYPE_OPERATOR_MINUS;
  } else if (str == "OPERATOR_MULTIPLY") {
    return EXPRESSION_TYPE_OPERATOR_MULTIPLY;
  } else if (str == "OPERATOR_DIVIDE") {
    return EXPRESSION_TYPE_OPERATOR_DIVIDE;
  } else if (str == "OPERATOR_CONCAT") {
    return EXPRESSION_TYPE_OPERATOR_CONCAT;
  } else if (str == "OPERATOR_MOD") {
    return EXPRESSION_TYPE_OPERATOR_MOD;
  } else if (str == "OPERATOR_CAST") {
    return EXPRESSION_TYPE_OPERATOR_CAST;
  } else if (str == "OPERATOR_NOT") {
    return EXPRESSION_TYPE_OPERATOR_NOT;
  } else if (str == "OPERATOR_IS_NULL") {
    return EXPRESSION_TYPE_OPERATOR_IS_NULL;
  } else if (str == "OPERATOR_EXISTS") {
    return EXPRESSION_TYPE_OPERATOR_EXISTS;
  } else if (str == "OPERATOR_UNARY_MINUS") {
    return EXPRESSION_TYPE_OPERATOR_UNARY_MINUS;
  } else if (str == "COMPARE_EQUAL") {
    return EXPRESSION_TYPE_COMPARE_EQUAL;
  } else if (str == "COMPARE_NOTEQUAL") {
    return EXPRESSION_TYPE_COMPARE_NOTEQUAL;
  } else if (str == "COMPARE_LESSTHAN") {
    return EXPRESSION_TYPE_COMPARE_LESSTHAN;
  } else if (str == "COMPARE_GREATERTHAN") {
    return EXPRESSION_TYPE_COMPARE_GREATERTHAN;
  } else if (str == "COMPARE_LESSTHANOREQUALTO") {
    return EXPRESSION_TYPE_COMPARE_LESSTHANOREQUALTO;
  } else if (str == "COMPARE_GREATERTHANOREQUALTO") {
    return EXPRESSION_TYPE_COMPARE_GREATERTHANOREQUALTO;
  } else if (str == "COMPARE_LIKE") {
    return EXPRESSION_TYPE_COMPARE_LIKE;
  } else if (str == "COMPARE_NOTLIKE") {
    return EXPRESSION_TYPE_COMPARE_NOTLIKE;
  } else if (str == "COMPARE_IN") {
    return EXPRESSION_TYPE_COMPARE_IN;
  } else if (str == "CONJUNCTION_AND") {
    return EXPRESSION_TYPE_CONJUNCTION_AND;
  } else if (str == "CONJUNCTION_OR") {
    return EXPRESSION_TYPE_CONJUNCTION_OR;
  } else if (str == "VALUE_CONSTANT") {
    return EXPRESSION_TYPE_VALUE_CONSTANT;
  } else if (str == "VALUE_PARAMETER") {
    return EXPRESSION_TYPE_VALUE_PARAMETER;
  } else if (str == "VALUE_TUPLE") {
    return EXPRESSION_TYPE_VALUE_TUPLE;
  } else if (str == "VALUE_TUPLE_ADDRESS") {
    return EXPRESSION_TYPE_VALUE_TUPLE_ADDRESS;
  } else if (str == "VALUE_NULL") {
    return EXPRESSION_TYPE_VALUE_NULL;
  } else if (str == "VALUE_VECTOR") {
    return EXPRESSION_TYPE_VALUE_VECTOR;
  } else if (str == "VALUE_SCALAR") {
    return EXPRESSION_TYPE_VALUE_SCALAR;
  } else if (str == "AGGREGATE_COUNT") {
    return EXPRESSION_TYPE_AGGREGATE_COUNT;
  } else if (str == "AGGREGATE_COUNT_STAR") {
    return EXPRESSION_TYPE_AGGREGATE_COUNT_STAR;
  } else if (str == "AGGREGATE_SUM") {
    return EXPRESSION_TYPE_AGGREGATE_SUM;
  } else if (str == "AGGREGATE_MIN") {
    return EXPRESSION_TYPE_AGGREGATE_MIN;
  } else if (str == "AGGREGATE_MAX") {
    return EXPRESSION_TYPE_AGGREGATE_MAX;
  } else if (str == "AGGREGATE_AVG") {
    return EXPRESSION_TYPE_AGGREGATE_AVG;
  } else if (str == "FUNCTION") {
    return EXPRESSION_TYPE_FUNCTION;
  } else if (str == "HASH_RANGE") {
    return EXPRESSION_TYPE_HASH_RANGE;
  } else if (str == "OPERATOR_CASE_EXPR") {
    return EXPRESSION_TYPE_OPERATOR_CASE_EXPR;
  } else if (str == "OPERATOR_NULLIF") {
    return EXPRESSION_TYPE_OPERATOR_NULLIF;
  } else if (str == "OPERATOR_COALESCE") {
    return EXPRESSION_TYPE_OPERATOR_COALESCE;
  } else if (str == "ROW_SUBQUERY") {
    return EXPRESSION_TYPE_ROW_SUBQUERY;
  } else if (str == "SELECT_SUBQUERY") {
    return EXPRESSION_TYPE_SELECT_SUBQUERY;
  } else if (str == "SUBSTR") {
    return EXPRESSION_TYPE_SUBSTR;
  } else if (str == "ASCII") {
    return EXPRESSION_TYPE_ASCII;
  } else if (str == "OCTET_LEN") {
    return EXPRESSION_TYPE_OCTET_LEN;
  } else if (str == "CHAR") {
    return EXPRESSION_TYPE_CHAR;
  } else if (str == "CHAR_LEN") {
    return EXPRESSION_TYPE_CHAR_LEN;
  } else if (str == "SPACE") {
    return EXPRESSION_TYPE_SPACE;
  } else if (str == "REPEAT") {
    return EXPRESSION_TYPE_REPEAT;
  } else if (str == "POSITION") {
    return EXPRESSION_TYPE_POSITION;
  } else if (str == "LEFT") {
    return EXPRESSION_TYPE_LEFT;
  } else if (str == "RIGHT") {
    return EXPRESSION_TYPE_RIGHT;
  } else if (str == "CONCAT") {
    return EXPRESSION_TYPE_CONCAT;
  } else if (str == "LTRIM") {
    return EXPRESSION_TYPE_LTRIM;
  } else if (str == "RTRIM") {
    return EXPRESSION_TYPE_RTRIM;
  } else if (str == "BTRIM") {
    return EXPRESSION_TYPE_BTRIM;
  } else if (str == "REPLACE") {
    return EXPRESSION_TYPE_REPLACE;
  } else if (str == "OVERLAY") {
    return EXPRESSION_TYPE_OVERLAY;
  } else if (str == "EXTRACT") {
    return EXPRESSION_TYPE_EXTRACT;
  } else if (str == "DATE_TO_TIMESTAMP") {
    return EXPRESSION_TYPE_DATE_TO_TIMESTAMP;
  } else if (str == "STAR") {
    return EXPRESSION_TYPE_STAR;
  } else if (str == "PLACEHOLDER") {
    return EXPRESSION_TYPE_PLACEHOLDER;
  } else if (str == "COLUMN_REF") {
    return EXPRESSION_TYPE_COLUMN_REF;
  } else if (str == "FUNCTION_REF") {
    return EXPRESSION_TYPE_FUNCTION_REF;
  } else if (str == "CAST") {
    return EXPRESSION_TYPE_CAST;
  } else {
    throw ConversionException("No conversion from string '" + str + "'");
  }
  return EXPRESSION_TYPE_INVALID;
}

//===--------------------------------------------------------------------===//
// Index Method Type - String Utilities
//===--------------------------------------------------------------------===//

std::string IndexTypeToString(IndexType type) {
  switch (type) {
    case INDEX_TYPE_INVALID: {
      return "INVALID";
    }
    case INDEX_TYPE_BTREE: {
      return "BTREE";
    }
    case INDEX_TYPE_BWTREE: {
      return "BWTREE";
    }
    case INDEX_TYPE_HASH: {
      return "HASH";
    }
  }
  return "INVALID";
}

IndexType StringToIndexType(const std::string& str) {
  if (str == "INVALID") {
    return INDEX_TYPE_INVALID;
  } else if (str == "BTREE") {
    return INDEX_TYPE_BTREE;
  } else if (str == "BWTREE") {
    return INDEX_TYPE_BWTREE;
  } else if (str == "HASH") {
    return INDEX_TYPE_HASH;
  } else {
    throw ConversionException("No conversion from string '" + str + "'");
  }
  return INDEX_TYPE_INVALID;
}

//===--------------------------------------------------------------------===//
// Index  Type - String Utilities
//===--------------------------------------------------------------------===//

std::string IndexConstraintTypeToString(IndexConstraintType type) {
  switch (type) {
    case INDEX_CONSTRAINT_TYPE_INVALID: {
      return "INVALID";
    }
    case INDEX_CONSTRAINT_TYPE_DEFAULT: {
      return "NORMAL";
    }
    case INDEX_CONSTRAINT_TYPE_PRIMARY_KEY: {
      return "PRIMARY_KEY";
    }
    case INDEX_CONSTRAINT_TYPE_UNIQUE: {
      return "UNIQUE";
    }
    default: {
      throw ConversionException(
          "No conversion from IndexConstraintType");  // FIXME
    }
  }
  return "INVALID";
}

IndexConstraintType StringToIndexConstraintType(const std::string& str) {
  if (str == "INVALID") {
    return INDEX_CONSTRAINT_TYPE_INVALID;
  } else if (str == "NORMAL") {
    return INDEX_CONSTRAINT_TYPE_DEFAULT;
  } else if (str == "PRIMARY_KEY") {
    return INDEX_CONSTRAINT_TYPE_PRIMARY_KEY;
  } else if (str == "UNIQUE") {
    return INDEX_CONSTRAINT_TYPE_UNIQUE;
  } else {
    throw ConversionException("No conversion from string '" + str + "'");
  }
  return INDEX_CONSTRAINT_TYPE_INVALID;
}
//===--------------------------------------------------------------------===//
// Plan Node - String Utilities
//===--------------------------------------------------------------------===//

std::string PlanNodeTypeToString(PlanNodeType type) {
  switch (type) {
    case PLAN_NODE_TYPE_INVALID: {
      return ("INVALID");
    }
    case PLAN_NODE_TYPE_ABSTRACT_SCAN: {
      return ("ABSTRACT_SCAN");
    }
    case PLAN_NODE_TYPE_SEQSCAN: {
      return ("SEQSCAN");
    }
    case PLAN_NODE_TYPE_INDEXSCAN: {
      return ("INDEXSCAN");
    }
    case PLAN_NODE_TYPE_NESTLOOP: {
      return ("NESTLOOP");
    }
    case PLAN_NODE_TYPE_NESTLOOPINDEX: {
      return ("NESTLOOPINDEX");
    }
    case PLAN_NODE_TYPE_MERGEJOIN: {
      return ("MERGEJOIN");
    }
    case PLAN_NODE_TYPE_HASHJOIN: {
      return ("HASHJOIN");
    }
    case PLAN_NODE_TYPE_UPDATE: {
      return ("UPDATE");
    }
    case PLAN_NODE_TYPE_INSERT: {
      return ("INSERT");
    }
    case PLAN_NODE_TYPE_DELETE: {
      return ("DELETE");
    }
    case PLAN_NODE_TYPE_DROP: {
      return ("DROP");
    }
    case PLAN_NODE_TYPE_CREATE: {
      return ("CREATE");
    }
    case PLAN_NODE_TYPE_SEND: {
      return ("SEND");
    }
    case PLAN_NODE_TYPE_RECEIVE: {
      return ("RECEIVE");
    }
    case PLAN_NODE_TYPE_PRINT: {
      return ("PRINT");
    }
    case PLAN_NODE_TYPE_AGGREGATE: {
      return ("AGGREGATE");
    }
    case PLAN_NODE_TYPE_UNION: {
      return ("UNION");
    }
    case PLAN_NODE_TYPE_ORDERBY: {
      return ("ORDERBY");
    }
    case PLAN_NODE_TYPE_PROJECTION: {
      return ("PROJECTION");
    }
    case PLAN_NODE_TYPE_MATERIALIZE: {
      return ("MATERIALIZE");
    }
    case PLAN_NODE_TYPE_LIMIT: {
      return ("LIMIT");
    }
    case PLAN_NODE_TYPE_DISTINCT: {
      return ("DISTINCT");
    }
    case PLAN_NODE_TYPE_SETOP: {
      return ("SETOP");
    }
    case PLAN_NODE_TYPE_APPEND: {
      return ("APPEND");
    }
    case PLAN_NODE_TYPE_AGGREGATE_V2: {
      return ("AGGREGATE_V2");
    }
    case PLAN_NODE_TYPE_HASH: {
      return ("HASH");
    }
    case PLAN_NODE_TYPE_RESULT: {
      return ("RESULT");
    }
    case PLAN_NODE_TYPE_COPY: {
      return ("COPY");
    }
    case PLAN_NODE_TYPE_MOCK: {
      return ("MOCK");
    }
    default: {
      throw ConversionException("No conversion from PlanNodeType");  // FIXME
    }
  }
  return "INVALID";
}

PlanNodeType StringToPlanNodeType(const std::string& str) {
  if (str == "INVALID") {
    return PLAN_NODE_TYPE_INVALID;
  } else if (str == "ABSTRACT_SCAN") {
    return PLAN_NODE_TYPE_ABSTRACT_SCAN;
  } else if (str == "SEQSCAN") {
    return PLAN_NODE_TYPE_SEQSCAN;
  } else if (str == "INDEXSCAN") {
    return PLAN_NODE_TYPE_INDEXSCAN;
  } else if (str == "NESTLOOP") {
    return PLAN_NODE_TYPE_NESTLOOP;
  } else if (str == "NESTLOOPINDEX") {
    return PLAN_NODE_TYPE_NESTLOOPINDEX;
  } else if (str == "MERGEJOIN") {
    return PLAN_NODE_TYPE_MERGEJOIN;
  } else if (str == "HASHJOIN") {
    return PLAN_NODE_TYPE_HASHJOIN;
  } else if (str == "UPDATE") {
    return PLAN_NODE_TYPE_UPDATE;
  } else if (str == "INSERT") {
    return PLAN_NODE_TYPE_INSERT;
  } else if (str == "DELETE") {
    return PLAN_NODE_TYPE_DELETE;
  } else if (str == "DROP") {
    return PLAN_NODE_TYPE_DROP;
  } else if (str == "CREATE") {
    return PLAN_NODE_TYPE_CREATE;
  } else if (str == "SEND") {
    return PLAN_NODE_TYPE_SEND;
  } else if (str == "RECEIVE") {
    return PLAN_NODE_TYPE_RECEIVE;
  } else if (str == "PRINT") {
    return PLAN_NODE_TYPE_PRINT;
  } else if (str == "AGGREGATE") {
    return PLAN_NODE_TYPE_AGGREGATE;
  } else if (str == "UNION") {
    return PLAN_NODE_TYPE_UNION;
  } else if (str == "ORDERBY") {
    return PLAN_NODE_TYPE_ORDERBY;
  } else if (str == "PROJECTION") {
    return PLAN_NODE_TYPE_PROJECTION;
  } else if (str == "MATERIALIZE") {
    return PLAN_NODE_TYPE_MATERIALIZE;
  } else if (str == "LIMIT") {
    return PLAN_NODE_TYPE_LIMIT;
  } else if (str == "DISTINCT") {
    return PLAN_NODE_TYPE_DISTINCT;
  } else if (str == "SETOP") {
    return PLAN_NODE_TYPE_SETOP;
  } else if (str == "APPEND") {
    return PLAN_NODE_TYPE_APPEND;
  } else if (str == "AGGREGATE_V2") {
    return PLAN_NODE_TYPE_AGGREGATE_V2;
  } else if (str == "HASH") {
    return PLAN_NODE_TYPE_HASH;
  } else if (str == "RESULT") {
    return PLAN_NODE_TYPE_RESULT;
  } else if (str == "COPY") {
    return PLAN_NODE_TYPE_COPY;
  } else if (str == "MOCK") {
    return PLAN_NODE_TYPE_MOCK;
  } else {
    throw ConversionException("No conversion from string '" + str + "'");
  }
  return PLAN_NODE_TYPE_INVALID;
}

//===--------------------------------------------------------------------===//
// Plan Node - String Utilities
//===--------------------------------------------------------------------===//

std::string ParseNodeTypeToString(ParseNodeType type) {
  switch (type) {
    case PARSE_NODE_TYPE_INVALID: {
      return "INVALID";
    }
    case PARSE_NODE_TYPE_SCAN: {
      return "SCAN";
    }
    case PARSE_NODE_TYPE_CREATE: {
      return "CREATE";
    }
    case PARSE_NODE_TYPE_DROP: {
      return "DROP";
    }
    case PARSE_NODE_TYPE_UPDATE: {
      return "UPDATE";
    }
    case PARSE_NODE_TYPE_INSERT: {
      return "INSERT";
    }
    case PARSE_NODE_TYPE_DELETE: {
      return "DELETE";
    }
    case PARSE_NODE_TYPE_PREPARE: {
      return "PREPARE";
    }
    case PARSE_NODE_TYPE_EXECUTE: {
      return "EXECUTE";
    }
    case PARSE_NODE_TYPE_SELECT: {
      return "SELECT";
    }
    case PARSE_NODE_TYPE_JOIN_EXPR: {
      return "JOIN_EXPR";
    }
    case PARSE_NODE_TYPE_TABLE: {
      return "TABLE";
    }
    case PARSE_NODE_TYPE_MOCK: {
      return "MOCK";
    }
    default: {
      throw ConversionException("No conversion from ParseNodeType");  // FIXME
    }
  }
  return "INVALID";
}

ParseNodeType StringToParseNodeType(const std::string& str) {
  if (str == "INVALID") {
    return PARSE_NODE_TYPE_INVALID;
  } else if (str == "SCAN") {
    return PARSE_NODE_TYPE_SCAN;
  } else if (str == "CREATE") {
    return PARSE_NODE_TYPE_CREATE;
  } else if (str == "DROP") {
    return PARSE_NODE_TYPE_DROP;
  } else if (str == "UPDATE") {
    return PARSE_NODE_TYPE_UPDATE;
  } else if (str == "INSERT") {
    return PARSE_NODE_TYPE_INSERT;
  } else if (str == "DELETE") {
    return PARSE_NODE_TYPE_DELETE;
  } else if (str == "PREPARE") {
    return PARSE_NODE_TYPE_PREPARE;
  } else if (str == "EXECUTE") {
    return PARSE_NODE_TYPE_EXECUTE;
  } else if (str == "SELECT") {
    return PARSE_NODE_TYPE_SELECT;
  } else if (str == "JOIN_EXPR") {
    return PARSE_NODE_TYPE_JOIN_EXPR;
  } else if (str == "TABLE") {
    return PARSE_NODE_TYPE_TABLE;
  } else if (str == "MOCK") {
    return PARSE_NODE_TYPE_MOCK;
  } else {
    throw ConversionException("No conversion from string '" + str + "'");
  }
  return PARSE_NODE_TYPE_INVALID;
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
      throw ConversionException("No conversion from ConstraintType");  // FIXME
    }
  }
  return "INVALID";
}

ConstraintType StringToConstraintType(const std::string& str) {
  if (str == "INVALID") {
    return CONSTRAINT_TYPE_INVALID;
  } else if (str == "NULL") {
    return CONSTRAINT_TYPE_NULL;
  } else if (str == "NOTNULL") {
    return CONSTRAINT_TYPE_NOTNULL;
  } else if (str == "DEFAULT") {
    return CONSTRAINT_TYPE_DEFAULT;
  } else if (str == "CHECK") {
    return CONSTRAINT_TYPE_CHECK;
  } else if (str == "PRIMARY") {
    return CONSTRAINT_TYPE_PRIMARY;
  } else if (str == "UNIQUE") {
    return CONSTRAINT_TYPE_UNIQUE;
  } else if (str == "FOREIGN") {
    return CONSTRAINT_TYPE_FOREIGN;
  } else if (str == "EXCLUSION") {
    return CONSTRAINT_TYPE_EXCLUSION;
  } else {
    throw ConversionException("No conversion from string '" + str + "'");
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

    default:
      LOG_ERROR("Invalid logging_type :: %d", type);
      exit(EXIT_FAILURE);
  }
  return "INVALID";
}

std::string LoggingStatusToString(LoggingStatus type) {
  switch (type) {
    case LOGGING_STATUS_TYPE_INVALID: {
      return "INVALID";
    }
    case LOGGING_STATUS_TYPE_STANDBY: {
      return "LOGGING_STATUS_TYPE_STANDBY";
    }
    case LOGGING_STATUS_TYPE_RECOVERY: {
      return "LOGGING_STATUS_TYPE_RECOVERY";
    }
    case LOGGING_STATUS_TYPE_LOGGING: {
      return "LOGGING_STATUS_TYPE_ONGOING";
    }
    case LOGGING_STATUS_TYPE_TERMINATE: {
      return "LOGGING_STATUS_TYPE_TERMINATE";
    }
    case LOGGING_STATUS_TYPE_SLEEP: {
      return "LOGGING_STATUS_TYPE_SLEEP";
    }
  }
  return "INVALID";
}

std::string LoggerTypeToString(LoggerType type) {
  switch (type) {
    case LOGGER_TYPE_INVALID: {
      return "INVALID";
    }
    case LOGGER_TYPE_FRONTEND: {
      return "LOGGER_TYPE_FRONTEND";
    }
    case LOGGER_TYPE_BACKEND: {
      return "LOGGER_TYPE_BACKEND";
    }
  }
  return "INVALID";
}

std::string LogRecordTypeToString(LogRecordType type) {
  switch (type) {
    case LOGRECORD_TYPE_INVALID: {
      return "INVALID";
    }
    case LOGRECORD_TYPE_TRANSACTION_BEGIN: {
      return "LOGRECORD_TYPE_TRANSACTION_BEGIN";
    }
    case LOGRECORD_TYPE_TRANSACTION_COMMIT: {
      return "LOGRECORD_TYPE_TRANSACTION_COMMIT";
    }
    case LOGRECORD_TYPE_TRANSACTION_END: {
      return "LOGRECORD_TYPE_TRANSACTION_END";
    }
    case LOGRECORD_TYPE_TRANSACTION_ABORT: {
      return "LOGRECORD_TYPE_TRANSACTION_ABORT";
    }
    case LOGRECORD_TYPE_TRANSACTION_DONE: {
      return "LOGRECORD_TYPE_TRANSACTION_DONE";
    }
    case LOGRECORD_TYPE_TUPLE_INSERT: {
      return "LOGRECORD_TYPE_TUPLE_INSERT";
    }
    case LOGRECORD_TYPE_TUPLE_DELETE: {
      return "LOGRECORD_TYPE_TUPLE_DELETE";
    }
    case LOGRECORD_TYPE_TUPLE_UPDATE: {
      return "LOGRECORD_TYPE_TUPLE_UPDATE";
    }
    case LOGRECORD_TYPE_WAL_TUPLE_INSERT: {
      return "LOGRECORD_TYPE_WAL_TUPLE_INSERT";
    }
    case LOGRECORD_TYPE_WAL_TUPLE_DELETE: {
      return "LOGRECORD_TYPE_WAL_TUPLE_DELETE";
    }
    case LOGRECORD_TYPE_WAL_TUPLE_UPDATE: {
      return "LOGRECORD_TYPE_WAL_TUPLE_UPDATE";
    }
    case LOGRECORD_TYPE_WBL_TUPLE_INSERT: {
      return "LOGRECORD_TYPE_WBL_TUPLE_INSERT";
    }
    case LOGRECORD_TYPE_WBL_TUPLE_DELETE: {
      return "LOGRECORD_TYPE_WBL_TUPLE_DELETE";
    }
    case LOGRECORD_TYPE_WBL_TUPLE_UPDATE: {
      return "LOGRECORD_TYPE_WBL_TUPLE_UPDATE";
    }
    case LOGRECORD_TYPE_ITERATION_DELIMITER: {
      return "LOGRECORD_TYPE_ITERATION_DELIMITER";
    }
  }
  return "INVALID";
}

common::Type::TypeId PostgresValueTypeToPelotonValueType(
    PostgresValueType PostgresValType) {
  switch (PostgresValType) {
    case POSTGRES_VALUE_TYPE_BOOLEAN:
      return common::Type::BOOLEAN;

    case POSTGRES_VALUE_TYPE_SMALLINT:
      return common::Type::SMALLINT;
    case POSTGRES_VALUE_TYPE_INTEGER:
      return common::Type::INTEGER;
    case POSTGRES_VALUE_TYPE_BIGINT:
      return common::Type::BIGINT;
    case POSTGRES_VALUE_TYPE_REAL:
      return common::Type::DECIMAL;
    case POSTGRES_VALUE_TYPE_DOUBLE:
      return common::Type::DECIMAL;

    case POSTGRES_VALUE_TYPE_BPCHAR:
    case POSTGRES_VALUE_TYPE_BPCHAR2:
    case POSTGRES_VALUE_TYPE_VARCHAR:
    case POSTGRES_VALUE_TYPE_VARCHAR2:
    case POSTGRES_VALUE_TYPE_TEXT:
      return common::Type::VARCHAR;

    case POSTGRES_VALUE_TYPE_DATE:
    case POSTGRES_VALUE_TYPE_TIMESTAMPS:
    case POSTGRES_VALUE_TYPE_TIMESTAMPS2:
      return common::Type::TIMESTAMP;

    case POSTGRES_VALUE_TYPE_DECIMAL:
      return common::Type::DECIMAL;
    default:
      LOG_TRACE("INVALID VALUE TYPE : %d ", PostgresValType);
      return common::Type::INVALID;
      break;
  }
}

ConstraintType PostgresConstraintTypeToPelotonConstraintType(
    PostgresConstraintType PostgresConstrType) {
  ConstraintType constraintType = CONSTRAINT_TYPE_INVALID;

  switch (PostgresConstrType) {
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
      LOG_ERROR("INVALID CONSTRAINT TYPE : %d ", PostgresConstrType);
      break;
  }
  return constraintType;
}

std::string QuantifierTypeToString(QuantifierType type) {
  switch (type) {
    case QUANTIFIER_TYPE_NONE: {
      return "NONE";
    }
    case QUANTIFIER_TYPE_ANY: {
      return "ANY";
    }
    case QUANTIFIER_TYPE_ALL: {
      return "ALL";
    }
  }
  return "INVALID";
}

QuantifierType StringToQuantifierType(std::string str) {
  if (str == "ANY") {
    return QUANTIFIER_TYPE_ANY;
  } else if (str == "ALL") {
    return QUANTIFIER_TYPE_ALL;
  }
  return QUANTIFIER_TYPE_NONE;
}

}  // End peloton namespace
