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
#include "common/value_factory.h"
#include "common/macros.h"

#include <sstream>
#include <cstring>
#include <algorithm>

namespace peloton {

ItemPointer INVALID_ITEMPOINTER;

FileHandle INVALID_FILE_HANDLE;

// WARNING: It will limit scalability if tupers per tile group is too small,
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
// Type utilities
//===--------------------------------------------------------------------===//
/*
bool IsNumeric(ValueType type) {
  switch (type) {
    case VALUE_TYPE_TINYINT:
    case VALUE_TYPE_SMALLINT:
    case VALUE_TYPE_INTEGER:
    case VALUE_TYPE_BIGINT:
    case VALUE_TYPE_DECIMAL:
    case VALUE_TYPE_DOUBLE:
      return true;
    case VALUE_TYPE_VARCHAR:
    case VALUE_TYPE_VARBINARY:
    case VALUE_TYPE_DATE:
    case VALUE_TYPE_TIMESTAMP:
    case VALUE_TYPE_NULL:
    case VALUE_TYPE_INVALID:
    case VALUE_TYPE_ARRAY:
      return false;
    default:
      throw Exception("IsNumeric");
  }
  throw Exception("IsNumeric");
}

bool IsIntegralType(ValueType type) {
  switch (type) {
    case VALUE_TYPE_TINYINT:
    case VALUE_TYPE_SMALLINT:
    case VALUE_TYPE_INTEGER:
    case VALUE_TYPE_BIGINT:
      return true;
    case VALUE_TYPE_REAL:
    case VALUE_TYPE_DOUBLE:
    case VALUE_TYPE_VARCHAR:
    case VALUE_TYPE_VARBINARY:
    case VALUE_TYPE_DATE:
    case VALUE_TYPE_TIMESTAMP:
    case VALUE_TYPE_NULL:
    case VALUE_TYPE_DECIMAL:
    case VALUE_TYPE_ARRAY:
      return false;
    default:
      throw Exception("IsIntegralType");
  }
  throw Exception("IsIntegralType");
}

// Works only for fixed-length types
std::size_t GetTypeSize(ValueType type) {
  switch (type) {
    case VALUE_TYPE_TINYINT:
      return 1;
    case VALUE_TYPE_SMALLINT:
      return 2;
    case VALUE_TYPE_INTEGER:
      return 4;
    case VALUE_TYPE_BIGINT:
      return 8;
    case VALUE_TYPE_REAL:
    case VALUE_TYPE_DOUBLE:
      return 8;
    case VALUE_TYPE_VARCHAR:
      return 0;
    case VALUE_TYPE_VARBINARY:
      return 0;
    case VALUE_TYPE_DATE:
      return 4;
    case VALUE_TYPE_TIMESTAMP:
      return 8;
    case VALUE_TYPE_DECIMAL:
      return sizeof(TTInt);
    case VALUE_TYPE_INVALID:
      return 0;
    case VALUE_TYPE_NULL:
      return 0;
    default: { return 0; }
  }
}
*/
//===--------------------------------------------------------------------===//
// BackendType <--> String Utilities
//===--------------------------------------------------------------------===//

std::string BackendTypeToString(BackendType type) {
  std::string ret;

  switch (type) {
    case(BACKEND_TYPE_MM) :
      return "MM";
    case(BACKEND_TYPE_NVM) :
      return "NVM";
    case(BACKEND_TYPE_SSD) :
      return "SSD";
    case(BACKEND_TYPE_HDD) :
      return "HDD";
    case(BACKEND_TYPE_INVALID) :
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
/*
std::string ValueTypeToString(ValueType type) {
  switch (type) {
    case VALUE_TYPE_INVALID:
      return "INVALID";
    case VALUE_TYPE_NULL:
      return "NULL";
    case VALUE_TYPE_TINYINT:
      return "TINYINT";
    case VALUE_TYPE_SMALLINT:
      return "SMALLINT";
    case VALUE_TYPE_INTEGER:
      return "INTEGER";
    case VALUE_TYPE_BIGINT:
      return "BIGINT";
    case VALUE_TYPE_REAL:
      return "REAL";
    case VALUE_TYPE_DOUBLE:
      return "DOUBLE";
    case VALUE_TYPE_VARCHAR:
      return "VARCHAR";
    case VALUE_TYPE_VARBINARY:
      return "VARBINARY";
    case VALUE_TYPE_DATE:
      return "DATE";
    case VALUE_TYPE_TIMESTAMP:
      return "TIMESTAMP";
    case VALUE_TYPE_DECIMAL:
      return "DECIMAL";
    default:
      return "INVALID";
  }
}

ValueType StringToValueType(std::string str) {
  if (str == "INVALID") {
    return VALUE_TYPE_INVALID;
  } else if (str == "NULL") {
    return VALUE_TYPE_NULL;
  } else if (str == "TINYINT") {
    return VALUE_TYPE_TINYINT;
  } else if (str == "SMALLINT") {
    return VALUE_TYPE_SMALLINT;
  } else if (str == "INTEGER") {
    return VALUE_TYPE_INTEGER;
  } else if (str == "BIGINT") {
    return VALUE_TYPE_BIGINT;
  } else if (str == "DOUBLE") {
    return VALUE_TYPE_DOUBLE;
  } else if (str == "STRING") {
    return VALUE_TYPE_VARCHAR;
  } else if (str == "VARBINARY") {
    return VALUE_TYPE_VARBINARY;
  } else if (str == "DATE") {
    return VALUE_TYPE_DATE;
  } else if (str == "TIMESTAMP") {
    return VALUE_TYPE_TIMESTAMP;
  } else if (str == "DECIMAL") {
    return VALUE_TYPE_DECIMAL;
  } else {
    throw ConversionException("No conversion from string :" + str);
  }
  return VALUE_TYPE_INVALID;
}

ValueType PostgresStringToValueType(std::string str) {
  if (str == "int4" || str == "int")
    return VALUE_TYPE_INTEGER;
  else if (str == "int2" || str == "smallint")
    return VALUE_TYPE_SMALLINT;
  else if (str == "date")
    return VALUE_TYPE_DATE;
  else if (str == "timestamp" || str == "timestamptz")
    return VALUE_TYPE_TIMESTAMP;
  else if (str == "float" || str == "float4" || str == "float8" ||
           str == "double")
    return VALUE_TYPE_DOUBLE;
  else if (str == "text" || str == "varchar" || str == "char")
    return VALUE_TYPE_VARCHAR;
  else if (str == "numeric" || str == "decimal")
    return VALUE_TYPE_DECIMAL;
  else
    return VALUE_TYPE_INVALID;

  return VALUE_TYPE_INVALID;
}
*/
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

bool IsBasedOnWriteAheadLogging(const LoggingType& logging_type) {
  bool status = false;

  switch (logging_type) {
    case LOGGING_TYPE_NVM_WAL:
    case LOGGING_TYPE_SSD_WAL:
    case LOGGING_TYPE_HDD_WAL:
      status = true;
      break;

    default:
      status = false;
      break;
  }

  return status;
}

bool IsBasedOnWriteBehindLogging(const LoggingType& logging_type) {
  bool status = true;

  switch (logging_type) {
    case LOGGING_TYPE_NVM_WBL:
    case LOGGING_TYPE_SSD_WBL:
    case LOGGING_TYPE_HDD_WBL:
      status = true;
      break;

    default:
      status = false;
      break;
  }

  return status;
}

BackendType GetBackendType(const LoggingType& logging_type) {
  // Default backend type
  BackendType backend_type = BACKEND_TYPE_MM;

  switch (logging_type) {
    case LOGGING_TYPE_NVM_WBL:
      backend_type = BACKEND_TYPE_NVM;
      break;

    case LOGGING_TYPE_SSD_WBL:
      backend_type = BACKEND_TYPE_SSD;
      break;

    case LOGGING_TYPE_HDD_WBL:
      backend_type = BACKEND_TYPE_HDD;
      break;

    case LOGGING_TYPE_NVM_WAL:
    case LOGGING_TYPE_SSD_WAL:
    case LOGGING_TYPE_HDD_WAL:
      backend_type = BACKEND_TYPE_MM;
      break;

    default:
      break;
  }

  return backend_type;
}

bool AtomicUpdateItemPointer(ItemPointer* src_ptr, const ItemPointer& value) {
  PL_ASSERT(sizeof(ItemPointer) == sizeof(int64_t));
  int64_t* cast_src_ptr = reinterpret_cast<int64_t*>((void*)src_ptr);
  int64_t* cast_value_ptr = reinterpret_cast<int64_t*>((void*)&value);
  return __sync_bool_compare_and_swap(cast_src_ptr, *cast_src_ptr, *cast_value_ptr);
}

//===--------------------------------------------------------------------===//
// Statement - String Utilities
//===--------------------------------------------------------------------===//

std::string StatementTypeToString(StatementType type) {
  switch (type) {
    case STATEMENT_TYPE_SELECT: { return "SELECT"; }
    case STATEMENT_TYPE_ALTER: { return "ALTER"; }
    case STATEMENT_TYPE_CREATE: { return "CREATE"; }
    case STATEMENT_TYPE_DELETE: { return "DELETE"; }
    case STATEMENT_TYPE_DROP: { return "DROP"; }
    case STATEMENT_TYPE_EXECUTE: { return "EXECUTE"; }
    case STATEMENT_TYPE_COPY: { return "COPY"; }
    case STATEMENT_TYPE_INSERT: { return "INSERT"; }
    case STATEMENT_TYPE_INVALID: { return "INVALID"; }
    case STATEMENT_TYPE_PREPARE: { return "PREPARE"; }
    case STATEMENT_TYPE_RENAME: { return "RENAME"; }
    case STATEMENT_TYPE_TRANSACTION: { return "TRANSACTION"; }
    case STATEMENT_TYPE_UPDATE: { return "UPDATE"; }
  }
  return "NOT A KNOWN TYPE - INVALID";
}

//===--------------------------------------------------------------------===//
// Expression - String Utilities
//===--------------------------------------------------------------------===//

std::string ExpressionTypeToString(ExpressionType type) {
  switch (type) {
    case EXPRESSION_TYPE_INVALID: { return "INVALID"; }
    case EXPRESSION_TYPE_OPERATOR_PLUS: { return "OPERATOR_PLUS"; }
    case EXPRESSION_TYPE_OPERATOR_MINUS: { return "OPERATOR_MINUS"; }
    case EXPRESSION_TYPE_OPERATOR_UNARY_MINUS: {
      return "OPERATOR_UNARY_MINUS";
    }

    case EXPRESSION_TYPE_OPERATOR_CASE_EXPR: { return "OPERATOR_CASE_EXPR"; }
    case EXPRESSION_TYPE_OPERATOR_MULTIPLY: { return "OPERATOR_MULTIPLY"; }
    case EXPRESSION_TYPE_OPERATOR_DIVIDE: { return "OPERATOR_DIVIDE"; }
    case EXPRESSION_TYPE_OPERATOR_CONCAT: { return "OPERATOR_CONCAT"; }
    case EXPRESSION_TYPE_OPERATOR_MOD: { return "OPERATOR_MOD"; }
    case EXPRESSION_TYPE_OPERATOR_CAST: { return "OPERATOR_CAST"; }
    case EXPRESSION_TYPE_OPERATOR_NOT: { return "OPERATOR_NOT"; }
    case EXPRESSION_TYPE_OPERATOR_IS_NULL: { return "OPERATOR_IS_NULL"; }
    case EXPRESSION_TYPE_OPERATOR_EXISTS: { return "OPERATOR_EXISTS"; }
    case EXPRESSION_TYPE_COMPARE_EQUAL: { return "COMPARE_EQUAL"; }
    case EXPRESSION_TYPE_COMPARE_NOTEQUAL: { return "COMPARE_NOT_EQUAL"; }
    case EXPRESSION_TYPE_COMPARE_LESSTHAN: { return "COMPARE_LESSTHAN"; }
    case EXPRESSION_TYPE_COMPARE_GREATERTHAN: { return "COMPARE_GREATERTHAN"; }
    case EXPRESSION_TYPE_COMPARE_LESSTHANOREQUALTO: {
      return "COMPARE_LESSTHANOREQUALTO";
    }
    case EXPRESSION_TYPE_COMPARE_GREATERTHANOREQUALTO: {
      return "COMPARE_GREATERTHANOREQUALTO";
    }
    case EXPRESSION_TYPE_CONJUNCTION_AND: { return "CONJUNCTION_AND"; }
    case EXPRESSION_TYPE_CONJUNCTION_OR: { return "CONJUNCTION_OR"; }
    case EXPRESSION_TYPE_VALUE_CONSTANT: { return "VALUE_CONSTANT"; }
    case EXPRESSION_TYPE_VALUE_PARAMETER: { return "VALUE_PARAMETER"; }
    case EXPRESSION_TYPE_VALUE_TUPLE: { return "VALUE_TUPLE"; }
    case EXPRESSION_TYPE_AGGREGATE_COUNT: { return "AGGREGATE_COUNT"; }
    case EXPRESSION_TYPE_AGGREGATE_COUNT_STAR: {
      return "AGGREGATE_COUNT_STAR";
    }
    case EXPRESSION_TYPE_AGGREGATE_SUM: { return "AGGREGATE_SUM"; }
    case EXPRESSION_TYPE_AGGREGATE_MIN: { return "AGGREGATE_MIN"; }
    case EXPRESSION_TYPE_AGGREGATE_MAX: { return "AGGREGATE_MAX"; }
    case EXPRESSION_TYPE_AGGREGATE_AVG: { return "AGGREGATE_AVG"; }    
    case EXPRESSION_TYPE_PLACEHOLDER: { return "PLACEHOLDER"; }
    case EXPRESSION_TYPE_COLUMN_REF: { return "COLUMN_REF"; }
    case EXPRESSION_TYPE_FUNCTION_REF: { return "FUNCTION_REF"; }
    case EXPRESSION_TYPE_CAST: { return "CAST"; }
    case EXPRESSION_TYPE_STAR: { return "STAR"; }
    case EXPRESSION_TYPE_SUBSTR: { return "SUBSTRING"; }
    case EXPRESSION_TYPE_ASCII: { return "ASCII"; }
    case EXPRESSION_TYPE_OCTET_LEN: { return "OCTET_LENGTH"; }
    case EXPRESSION_TYPE_CHAR: { return "CHAR"; }
    case EXPRESSION_TYPE_CHAR_LEN: { return "CHAR_LEN"; }
    case EXPRESSION_TYPE_SPACE: { return "SPACE"; }
    case EXPRESSION_TYPE_REPEAT: { return "REPEAT"; }
    case EXPRESSION_TYPE_POSITION: { return "POSITION"; }
    case EXPRESSION_TYPE_LEFT: { return "LEFT"; }
    case EXPRESSION_TYPE_RIGHT: { return "RIGHT"; }
    case EXPRESSION_TYPE_CONCAT: { return "CONCAT"; }
    case EXPRESSION_TYPE_LTRIM: { return "L_TRIM"; }
    case EXPRESSION_TYPE_RTRIM: { return "R_TRIM"; }
    case EXPRESSION_TYPE_BTRIM: { return "B_TRIM"; }
    case EXPRESSION_TYPE_REPLACE: { return "REPLACE"; }
    case EXPRESSION_TYPE_OVERLAY: { return "OVERLAY"; }
    case EXPRESSION_TYPE_EXTRACT: { return "EXTRACT"; }
    case EXPRESSION_TYPE_DATE_TO_TIMESTAMP: { return "DATE_TO_TIMESTAMP"; }
    default:
      break;
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
  }
  return EXPRESSION_TYPE_INVALID;
}

//===--------------------------------------------------------------------===//
// Index Method Type - String Utilities
//===--------------------------------------------------------------------===//

std::string IndexTypeToString(IndexType type) {
  switch (type) {
    case INDEX_TYPE_INVALID: { return "INVALID"; }
    case INDEX_TYPE_BTREE: { return "BTREE"; }
    case INDEX_TYPE_BWTREE: { return "BWTREE"; }
    case INDEX_TYPE_HASH: { return "HASH"; }
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
  }
  return INDEX_TYPE_INVALID;
}

//===--------------------------------------------------------------------===//
// Index  Type - String Utilities
//===--------------------------------------------------------------------===//

std::string IndexConstraintTypeToString(IndexConstraintType type) {
  switch (type) {
    case INDEX_CONSTRAINT_TYPE_INVALID: { return "INVALID"; }
    case INDEX_CONSTRAINT_TYPE_DEFAULT: { return "NORMAL"; }
    case INDEX_CONSTRAINT_TYPE_PRIMARY_KEY: { return "PRIMARY_KEY"; }
    case INDEX_CONSTRAINT_TYPE_UNIQUE: { return "UNIQUE"; }
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
  }
  return INDEX_CONSTRAINT_TYPE_INVALID;
}
//===--------------------------------------------------------------------===//
// Plan Node - String Utilities
//===--------------------------------------------------------------------===//

std::string PlanNodeTypeToString(PlanNodeType type) {
  switch (type) {
    case PLAN_NODE_TYPE_INVALID: { return "INVALID"; }
    case PLAN_NODE_TYPE_ABSTRACT_SCAN: { return "ABSTRACT_SCAN"; }
    case PLAN_NODE_TYPE_SEQSCAN: { return "SEQSCAN"; }
    case PLAN_NODE_TYPE_INDEXSCAN: { return "INDEXSCAN"; }
    case PLAN_NODE_TYPE_NESTLOOP: { return "NESTLOOP"; }
    case PLAN_NODE_TYPE_NESTLOOPINDEX: { return "NESTLOOPINDEX"; }
    case PLAN_NODE_TYPE_MERGEJOIN: { return "MERGEJOIN"; }
    case PLAN_NODE_TYPE_HASHJOIN: { return "HASHJOIN"; }
    case PLAN_NODE_TYPE_UPDATE: { return "UPDATE"; }
    case PLAN_NODE_TYPE_INSERT: { return "INSERT"; }
    case PLAN_NODE_TYPE_DELETE: { return "DELETE"; }
    case PLAN_NODE_TYPE_COPY: { return "COPY"; }
    case PLAN_NODE_TYPE_SEND: { return "SEND"; }
    case PLAN_NODE_TYPE_RECEIVE: { return "RECEIVE"; }
    case PLAN_NODE_TYPE_PRINT: { return "PRINT"; }
    case PLAN_NODE_TYPE_AGGREGATE: { return "AGGREGATE"; }
    case PLAN_NODE_TYPE_UNION: { return "UNION"; }
    case PLAN_NODE_TYPE_ORDERBY: { return "RECEIVE"; }
    case PLAN_NODE_TYPE_PROJECTION: { return "PROJECTION"; }
    case PLAN_NODE_TYPE_MATERIALIZE: { return "MATERIALIZE"; }
    case PLAN_NODE_TYPE_LIMIT: { return "LIMIT"; }
    case PLAN_NODE_TYPE_DISTINCT: { return "DISTINCT"; }
    case PLAN_NODE_TYPE_SETOP: { return "SETOP"; }
    case PLAN_NODE_TYPE_APPEND: { return "APPEND"; }
    case PLAN_NODE_TYPE_RESULT: { return "RESULT"; }
    case PLAN_NODE_TYPE_AGGREGATE_V2: { return "AGGREGATE_V2"; }
    case PLAN_NODE_TYPE_MOCK: { return "MOCK"; }
    case PLAN_NODE_TYPE_HASH: { return "HASH"; }
    case PLAN_NODE_TYPE_DROP: { return "DROP"; }
    case PLAN_NODE_TYPE_CREATE: { return "CREATE"; }
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
  } else if (str == "UPDATE") {
    return PLAN_NODE_TYPE_UPDATE;
  } else if (str == "INSERT") {
    return PLAN_NODE_TYPE_INSERT;
  } else if (str == "DELETE") {
    return PLAN_NODE_TYPE_DELETE;
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
  }
  return PLAN_NODE_TYPE_INVALID;
}

//===--------------------------------------------------------------------===//
// Plan Node - String Utilities
//===--------------------------------------------------------------------===//

std::string ParseNodeTypeToString(ParseNodeType type) {
  switch (type) {
    case PARSE_NODE_TYPE_INVALID: { return "INVALID"; }
    case PARSE_NODE_TYPE_SCAN: { return "SCAN"; }
    case PARSE_NODE_TYPE_CREATE: { return "CREATE"; }
    case PARSE_NODE_TYPE_DROP: { return "DROP"; }
    case PARSE_NODE_TYPE_UPDATE: { return "UPDATE"; }
    case PARSE_NODE_TYPE_INSERT: { return "INSERT"; }
    case PARSE_NODE_TYPE_DELETE: { return "DELETE"; }
    case PARSE_NODE_TYPE_PREPARE: { return "PREPARE"; }
    case PARSE_NODE_TYPE_EXECUTE: { return "EXECUTE"; }
    case PARSE_NODE_TYPE_SELECT: { return "SELECT"; }
    case PARSE_NODE_TYPE_JOIN_EXPR: { return "JOIN_EXPR"; }
    case PARSE_NODE_TYPE_TABLE: { return "TABLE"; }
    case PARSE_NODE_TYPE_MOCK: { return "MOCK"; }
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
  }
  return PARSE_NODE_TYPE_INVALID;
}

//===--------------------------------------------------------------------===//
// Constraint Type - String Utilities
//===--------------------------------------------------------------------===//

std::string ConstraintTypeToString(ConstraintType type) {
  switch (type) {
    case CONSTRAINT_TYPE_INVALID: { return "INVALID"; }
    case CONSTRAINT_TYPE_NULL: { return "NULL"; }
    case CONSTRAINT_TYPE_NOTNULL: { return "NOTNULL"; }
    case CONSTRAINT_TYPE_DEFAULT: { return "DEFAULT"; }
    case CONSTRAINT_TYPE_CHECK: { return "CHECK"; }
    case CONSTRAINT_TYPE_PRIMARY: { return "PRIMARY_KEY"; }
    case CONSTRAINT_TYPE_UNIQUE: { return "UNIQUE"; }
    case CONSTRAINT_TYPE_FOREIGN: { return "FOREIGN_KEY"; }
    case CONSTRAINT_TYPE_EXCLUSION: { return "EXCLUSION"; }
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
  } else if (str == "CHECK") {
    return CONSTRAINT_TYPE_DEFAULT;
  } else if (str == "DEFAULT") {
    return CONSTRAINT_TYPE_UNIQUE;
  } else if (str == "PRIMARY_KEY") {
    return CONSTRAINT_TYPE_CHECK;
  } else if (str == "UNIQUE") {
    return CONSTRAINT_TYPE_PRIMARY;
  } else if (str == "FOREIGN_KEY") {
    return CONSTRAINT_TYPE_FOREIGN;
  } else if (str == "EXCLUSION") {
    return CONSTRAINT_TYPE_EXCLUSION;
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
    case LOGGING_STATUS_TYPE_INVALID: { return "INVALID"; }
    case LOGGING_STATUS_TYPE_STANDBY: { return "LOGGING_STATUS_TYPE_STANDBY"; }
    case LOGGING_STATUS_TYPE_RECOVERY: {
      return "LOGGING_STATUS_TYPE_RECOVERY";
    }
    case LOGGING_STATUS_TYPE_LOGGING: { return "LOGGING_STATUS_TYPE_ONGOING"; }
    case LOGGING_STATUS_TYPE_TERMINATE: {
      return "LOGGING_STATUS_TYPE_TERMINATE";
    }
    case LOGGING_STATUS_TYPE_SLEEP: { return "LOGGING_STATUS_TYPE_SLEEP"; }
  }
  return "INVALID";
}

std::string LoggerTypeToString(LoggerType type) {
  switch (type) {
    case LOGGER_TYPE_INVALID: { return "INVALID"; }
    case LOGGER_TYPE_FRONTEND: { return "LOGGER_TYPE_FRONTEND"; }
    case LOGGER_TYPE_BACKEND: { return "LOGGER_TYPE_BACKEND"; }
  }
  return "INVALID";
}

std::string LogRecordTypeToString(LogRecordType type) {
  switch (type) {
    case LOGRECORD_TYPE_INVALID: { return "INVALID"; }
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
    case LOGRECORD_TYPE_TUPLE_INSERT: { return "LOGRECORD_TYPE_TUPLE_INSERT"; }
    case LOGRECORD_TYPE_TUPLE_DELETE: { return "LOGRECORD_TYPE_TUPLE_DELETE"; }
    case LOGRECORD_TYPE_TUPLE_UPDATE: { return "LOGRECORD_TYPE_TUPLE_UPDATE"; }
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
    case QUANTIFIER_TYPE_NONE: { return "NONE"; }
    case QUANTIFIER_TYPE_ANY: { return "ANY"; }
    case QUANTIFIER_TYPE_ALL: { return "ALL"; }
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
