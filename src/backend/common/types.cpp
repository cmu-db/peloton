//===----------------------------------------------------------------------===//
//
//                         PelotonDB
//
// types.cpp
//
// Identification: src/backend/common/types.cpp
//
// Copyright (c) 2015, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "backend/common/types.h"
#include "backend/common/exception.h"
#include "backend/common/logger.h"
#include "backend/common/value_factory.h"

#include <sstream>
#include <cstring>

namespace peloton {

ItemPointer INVALID_ITEMPOINTER;

//===--------------------------------------------------------------------===//
// Type utilities
//===--------------------------------------------------------------------===//

/** Testing utility */
bool IsNumeric(ValueType type) {
    switch (type) {
      case (VALUE_TYPE_TINYINT):
      case (VALUE_TYPE_SMALLINT):
      case (VALUE_TYPE_INTEGER):
      case (VALUE_TYPE_BIGINT):
      case (VALUE_TYPE_DECIMAL):
      case (VALUE_TYPE_DOUBLE):
        return true;
      break;
      case (VALUE_TYPE_VARCHAR):
      case (VALUE_TYPE_VARBINARY):
      case (VALUE_TYPE_TIMESTAMP):
      case (VALUE_TYPE_NULL):
      case (VALUE_TYPE_INVALID):
      case (VALUE_TYPE_ARRAY):
        return false;
      default:
          throw Exception();
    }
    throw Exception();
}

/** Used in index optimization **/
bool IsIntegralType(ValueType type) {
    switch (type) {
      case (VALUE_TYPE_TINYINT):
      case (VALUE_TYPE_SMALLINT):
      case (VALUE_TYPE_INTEGER):
      case (VALUE_TYPE_BIGINT):
        return true;
      break;
      case (VALUE_TYPE_DOUBLE):
      case (VALUE_TYPE_VARCHAR):
      case (VALUE_TYPE_VARBINARY):
      case (VALUE_TYPE_TIMESTAMP):
      case (VALUE_TYPE_NULL):
      case (VALUE_TYPE_DECIMAL):
      case (VALUE_TYPE_ARRAY):
        return false;
      default:
          throw Exception();
    }
    throw Exception();
}

Value GetRandomValue(ValueType type) {
    switch (type) {
        case VALUE_TYPE_TIMESTAMP:
            return ValueFactory::GetTimestampValue(static_cast<int64_t>(time(NULL)));
        case VALUE_TYPE_TINYINT:
            return ValueFactory::GetTinyIntValue(static_cast<int8_t>(rand() % 128));
        case VALUE_TYPE_SMALLINT:
            return ValueFactory::GetSmallIntValue(static_cast<int16_t>(rand() % 32768));
        case VALUE_TYPE_INTEGER:
            return ValueFactory::GetIntegerValue(rand() % (1 << 31));
        case VALUE_TYPE_BIGINT:
            return ValueFactory::GetBigIntValue(rand());
        case VALUE_TYPE_DOUBLE:
            return ValueFactory::GetDoubleValue((rand() % 10000) / (double)(rand() % 10000));
        case VALUE_TYPE_VARCHAR: {
            int length = (rand() % 10);
            char characters[11];
            for (int ii = 0; ii < length; ii++) {
                characters[ii] = (char)(32 + (rand() % 94)); //printable characters
            }
            characters[length] = '\0';
            //printf("Characters are \"%s\"\n", characters);
            return ValueFactory::GetStringValue(std::string(characters));
        }
        case VALUE_TYPE_VARBINARY: {
            int length = (rand() % 16);
            unsigned char bytes[17];
            for (int ii = 0; ii < length; ii++) {
                bytes[ii] = (unsigned char)rand() % 256; //printable characters
            }
            bytes[length] = '\0';
            //printf("Characters are \"%s\"\n", characters);
            return ValueFactory::GetBinaryValue(bytes, length);
        }
            break;
        case VALUE_TYPE_ARRAY:
        default: {
            throw Exception("Attempted to get a random value of unsupported value type %d", type);
        }
    }
    throw Exception();
}


// Works only for fixed-length types
std::size_t GetTypeSize(ValueType type) {
  switch (type) {
    case (VALUE_TYPE_TINYINT):
      return 1;
    case (VALUE_TYPE_SMALLINT):
      return 2;
    case (VALUE_TYPE_INTEGER):
      return 4;
    case (VALUE_TYPE_BIGINT):
      return 8;
    case (VALUE_TYPE_DOUBLE):
      return 8;
    case (VALUE_TYPE_VARCHAR):
      return 0;
    case (VALUE_TYPE_VARBINARY):
      return 0;
    case (VALUE_TYPE_TIMESTAMP):
      return 8;
    case (VALUE_TYPE_DECIMAL):
      return 0;
    case (VALUE_TYPE_INVALID):
      return 0;
    case (VALUE_TYPE_NULL):
      return 0;
    default: { return 0; }
  }
}

//===--------------------------------------------------------------------===//
// BackendType <--> String Utilities
//===--------------------------------------------------------------------===//

std::string BackendTypeToString(BackendType type) {
  std::string ret;

  switch (type) {
    case (BACKEND_TYPE_VM):
      return "VOLATILE";
    case (BACKEND_TYPE_NVM):
      return "NON-VOLATILE";
    case (BACKEND_TYPE_INVALID):
      return "INVALID";
    default: {
      char buffer[32];
      ::snprintf(buffer, 32, "UNKNOWN[%d] ", type);
      ret = buffer;
    }
  }
  return (ret);
}

BackendType StringToBackendType(std::string str) {
  if (str == "INVALID") {
    return BACKEND_TYPE_INVALID;
  } else if (str == "VOLATILE") {
    return BACKEND_TYPE_VM;
  } else if (str == "NON-VOLATILE") {
    return BACKEND_TYPE_NVM;
  }
  return BACKEND_TYPE_INVALID;
}

//===--------------------------------------------------------------------===//
// Value <--> String Utilities
//===--------------------------------------------------------------------===//

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
    case VALUE_TYPE_DOUBLE:
      return "DOUBLE";
    case VALUE_TYPE_VARCHAR:
      return "VARCHAR";
    case VALUE_TYPE_VARBINARY:
      return "VARBINARY";
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
  } else if (str == "TIMESTAMP") {
    return VALUE_TYPE_TIMESTAMP;
  } else if (str == "DECIMAL") {
    return VALUE_TYPE_DECIMAL;
  } else {
    throw ConversionException("No conversion from string :" + str);
  }
  return VALUE_TYPE_INVALID;
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

  assert(retval >= 0 && retval < 16);
  return retval;
}

bool HexDecodeToBinary(unsigned char *bufferdst, const char *hexString) {
  assert(hexString);
  size_t len = strlen(hexString);
  if ((len % 2) != 0) return false;
  uint32_t i;
  for (i = 0; i < len / 2; i++) {
    int32_t high = HexCharToInt(hexString[i * 2]);
    int32_t low = HexCharToInt(hexString[i * 2 + 1]);
    if ((high == -1) || (low == -1)) return false;
    int32_t result = high * 16 + low;

    assert(result >= 0 && result < 256);
    bufferdst[i] = static_cast<unsigned char>(result);
  }
  return true;
}

//===--------------------------------------------------------------------===//
// Expression - String Utilities
//===--------------------------------------------------------------------===//

std::string ExpressionTypeToString(ExpressionType type)
{
    switch (type) {
    case EXPRESSION_TYPE_INVALID: {
        return "INVALID";
    }
    case EXPRESSION_TYPE_OPERATOR_PLUS: {
        return "OPERATOR_PLUS";
    }
    case EXPRESSION_TYPE_OPERATOR_MINUS: {
        return "OPERATOR_MINUS";
    }
    case EXPRESSION_TYPE_OPERATOR_MULTIPLY: {
        return "OPERATOR_MULTIPLY";
    }
    case EXPRESSION_TYPE_OPERATOR_DIVIDE: {
        return "OPERATOR_DIVIDE";
    }
    case EXPRESSION_TYPE_OPERATOR_CONCAT: {
        return "OPERATOR_CONCAT";
    }
    case EXPRESSION_TYPE_OPERATOR_MOD: {
        return "OPERATOR_MOD";
    }
    case EXPRESSION_TYPE_OPERATOR_CAST: {
        return "OPERATOR_CAST";
    }
    case EXPRESSION_TYPE_OPERATOR_NOT: {
        return "OPERATOR_NOT";
    }
    case EXPRESSION_TYPE_OPERATOR_IS_NULL: {
        return "OPERATOR_IS_NULL";
    }
    case EXPRESSION_TYPE_OPERATOR_EXISTS: {
        return "OPERATOR_EXISTS";
    }
    case EXPRESSION_TYPE_COMPARE_EQUAL: {
        return "COMPARE_EQUAL";
    }
    case EXPRESSION_TYPE_COMPARE_NOTEQUAL: {
        return "COMPARE_NOT_EQUAL";
    }
    case EXPRESSION_TYPE_COMPARE_LESSTHAN: {
        return "COMPARE_LESSTHAN";
    }
    case EXPRESSION_TYPE_COMPARE_GREATERTHAN: {
        return "COMPARE_GREATERTHAN";
    }
    case EXPRESSION_TYPE_COMPARE_LESSTHANOREQUALTO: {
        return "COMPARE_LESSTHANOREQUALTO";
    }
    case EXPRESSION_TYPE_COMPARE_GREATERTHANOREQUALTO: {
        return "COMPARE_GREATERTHANOREQUALTO";
    }
    case EXPRESSION_TYPE_COMPARE_LIKE: {
        return "COMPARE_LIKE";
    }
    case EXPRESSION_TYPE_COMPARE_IN: {
        return "COMPARE_IN";
    }
    case EXPRESSION_TYPE_CONJUNCTION_AND: {
        return "CONJUNCTION_AND";
    }
    case EXPRESSION_TYPE_CONJUNCTION_OR: {
        return "CONJUNCTION_OR";
    }
    case EXPRESSION_TYPE_VALUE_CONSTANT: {
        return "VALUE_CONSTANT";
    }
    case EXPRESSION_TYPE_VALUE_PARAMETER: {
        return "VALUE_PARAMETER";
    }
    case EXPRESSION_TYPE_VALUE_TUPLE: {
        return "VALUE_TUPLE";
    }
    case EXPRESSION_TYPE_VALUE_TUPLE_ADDRESS: {
        return "VALUE_TUPLE_ADDRESS";
    }
    case EXPRESSION_TYPE_VALUE_SCALAR: {
        return "VALUE_SCALAR";
    }
    case EXPRESSION_TYPE_VALUE_NULL: {
        return "VALUE_NULL";
    }
    case EXPRESSION_TYPE_AGGREGATE_COUNT: {
        return "AGGREGATE_COUNT";
    }
    case EXPRESSION_TYPE_AGGREGATE_COUNT_STAR: {
        return "AGGREGATE_COUNT_STAR";
    }
    case EXPRESSION_TYPE_AGGREGATE_APPROX_COUNT_DISTINCT: {
        return "AGGREGATE_APPROX_COUNT_DISTINCT";
    }
    case EXPRESSION_TYPE_AGGREGATE_VALS_TO_HYPERLOGLOG: {
        return "AGGREGATE_VALS_TO_HYPERLOGLOG";
    }
    case EXPRESSION_TYPE_AGGREGATE_HYPERLOGLOGS_TO_CARD: {
        return "AGGREGATE_HYPERLOGLOGS_TO_CARD";
    }
    case EXPRESSION_TYPE_AGGREGATE_SUM: {
        return "AGGREGATE_SUM";
    }
    case EXPRESSION_TYPE_AGGREGATE_MIN: {
        return "AGGREGATE_MIN";
    }
    case EXPRESSION_TYPE_AGGREGATE_MAX: {
        return "AGGREGATE_MAX";
    }
    case EXPRESSION_TYPE_AGGREGATE_AVG: {
        return "AGGREGATE_AVG";
    }
    case EXPRESSION_TYPE_FUNCTION: {
        return "FUNCTION";
    }
    case EXPRESSION_TYPE_VALUE_VECTOR: {
        return "VALUE_VECTOR";
    }
    case EXPRESSION_TYPE_HASH_RANGE: {
        return "HASH_RANGE";
    }
    case EXPRESSION_TYPE_OPERATOR_CASE_WHEN: {
        return "OPERATOR_CASE_WHEN";
    }
    case EXPRESSION_TYPE_OPERATOR_ALTERNATIVE: {
        return "OPERATOR_ALTERNATIVE";
    }
    case EXPRESSION_TYPE_ROW_SUBQUERY: {
        return "ROW_SUBQUERY";
    }
    case EXPRESSION_TYPE_SELECT_SUBQUERY: {
        return "SELECT_SUBQUERY";
    }
    }
    return "INVALID";
}

ExpressionType StringToExpressionType(std::string str )
{
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
    } else if (str == "COMPARE_LIKE") {
        return EXPRESSION_TYPE_COMPARE_LIKE;
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
    } else if (str == "VALUE_SCALAR") {
        return EXPRESSION_TYPE_VALUE_SCALAR;
    } else if (str == "VALUE_NULL") {
        return EXPRESSION_TYPE_VALUE_NULL;
    } else if (str == "AGGREGATE_COUNT") {
        return EXPRESSION_TYPE_AGGREGATE_COUNT;
    } else if (str == "AGGREGATE_COUNT_STAR") {
        return EXPRESSION_TYPE_AGGREGATE_COUNT_STAR;
    } else if (str == "AGGREGATE_APPROX_COUNT_DISTINCT") {
        return EXPRESSION_TYPE_AGGREGATE_APPROX_COUNT_DISTINCT;
    } else if (str == "AGGREGATE_VALS_TO_HYPERLOGLOG") {
        return EXPRESSION_TYPE_AGGREGATE_VALS_TO_HYPERLOGLOG;
    } else if (str == "AGGREGATE_HYPERLOGLOGS_TO_CARD") {
        return EXPRESSION_TYPE_AGGREGATE_HYPERLOGLOGS_TO_CARD;
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
    } else if (str == "VALUE_VECTOR") {
        return EXPRESSION_TYPE_VALUE_VECTOR;
    } else if (str == "HASH_RANGE") {
        return EXPRESSION_TYPE_HASH_RANGE;
    } else if (str == "OPERATOR_CASE_WHEN") {
        return EXPRESSION_TYPE_OPERATOR_CASE_WHEN;
    } else if (str == "OPERATOR_ALTERNATIVE") {
        return EXPRESSION_TYPE_OPERATOR_ALTERNATIVE;
    } else if (str == "ROW_SUBQUERY") {
        return EXPRESSION_TYPE_ROW_SUBQUERY;
    } else if (str == "SELECT_SUBQUERY") {
        return EXPRESSION_TYPE_SELECT_SUBQUERY;
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
  }
  return "INVALID";
}

IndexType StringToIndexType(std::string str) {
  if (str == "INVALID") {
    return INDEX_TYPE_INVALID;
  } else if (str == "BTREE") {
    return INDEX_TYPE_BTREE;
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
  }
  return "INVALID";
}

IndexConstraintType StringToIndexConstraintType(std::string str) {
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
    case PLAN_NODE_TYPE_INVALID: {
      return "INVALID";
    }
    case PLAN_NODE_TYPE_ABSTRACT_SCAN: {
      return "ABSTRACT_SCAN";
    }
    case PLAN_NODE_TYPE_SEQSCAN: {
      return "SEQSCAN";
    }
    case PLAN_NODE_TYPE_INDEXSCAN: {
      return "INDEXSCAN";
    }
    case PLAN_NODE_TYPE_NESTLOOP: {
      return "NESTLOOP";
    }
    case PLAN_NODE_TYPE_NESTLOOPINDEX: {
      return "NESTLOOPINDEX";
    }
    case PLAN_NODE_TYPE_MERGEJOIN: {
      return "MERGEJOIN";
    }
    case PLAN_NODE_TYPE_UPDATE: {
      return "UPDATE";
    }
    case PLAN_NODE_TYPE_INSERT: {
      return "DELETE";
    }
    case PLAN_NODE_TYPE_DELETE: {
      return "DELETE";
    }
    case PLAN_NODE_TYPE_SEND: {
      return "SEND";
    }
    case PLAN_NODE_TYPE_RECEIVE: {
      return "RECEIVE";
    }
    case PLAN_NODE_TYPE_PRINT: {
      return "PRINT";
    }
    case PLAN_NODE_TYPE_AGGREGATE: {
      return "AGGREGATE";
    }
    case PLAN_NODE_TYPE_HASHAGGREGATE: {
      return "HASHAGGREGATE";
    }
    case PLAN_NODE_TYPE_UNION: {
      return "UNION";
    }
    case PLAN_NODE_TYPE_ORDERBY: {
      return "RECEIVE";
    }
    case PLAN_NODE_TYPE_PROJECTION: {
      return "PROJECTION";
    }
    case PLAN_NODE_TYPE_MATERIALIZE: {
      return "MATERIALIZE";
    }
    case PLAN_NODE_TYPE_LIMIT: {
      return "LIMIT";
    }
    case PLAN_NODE_TYPE_DISTINCT: {
      return "DISTINCT";
    }
    case PLAN_NODE_TYPE_SETOP: {
      return "SETOP";
    }
    case PLAN_NODE_TYPE_APPEND: {
      return "APPEND";
    }
    case PLAN_NODE_TYPE_RESULT: {
      return "RESULT";
    }
    case PLAN_NODE_TYPE_AGGREGATE_V2: {
      return "AGGREGATE_V2";
    }
  }
  return "INVALID";
}

PlanNodeType StringToPlanNodeType(std::string str) {
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
  } else if (str == "HASHAGGREGATE") {
    return PLAN_NODE_TYPE_HASHAGGREGATE;
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
// Constraint Type - String Utilities
//===--------------------------------------------------------------------===//

std::string ConstraintTypeToString(ConstraintType type) {
  switch (type) {
    case CONSTRAINT_TYPE_INVALID: {
      return "INVALID";
    }
    case CONSTRAINT_TYPE_NULL: {
      return "NULL";
    }
    case CONSTRAINT_TYPE_NOTNULL: {
      return "NOTNULL";
    }
    case CONSTRAINT_TYPE_DEFAULT: {
      return "DEFAULT";
    }
    case CONSTRAINT_TYPE_CHECK: {
      return "CHECK";
    }
    case CONSTRAINT_TYPE_PRIMARY: {
      return "PRIMARY_KEY";
    }
    case CONSTRAINT_TYPE_UNIQUE: {
      return "UNIQUE";
    }
    case CONSTRAINT_TYPE_FOREIGN: {
      return "FOREIGN_KEY";
    }
    case CONSTRAINT_TYPE_EXCLUSION: {
      return "EXCLUSION";
    }
  }
  return "INVALID";
}

ConstraintType StringToConstraintType(std::string str) {
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
    case LOGGING_TYPE_INVALID: {
      return "INVALID";
    }
    case LOGGING_TYPE_STDOUT: {
      return "LOGGING_TYPE_STDOUT";
    }
    case LOGGING_TYPE_ARIES: {
      return "LOGGING_TYPE_ARIES";
    }
    case LOGGING_TYPE_PELOTON: {
      return "LOGGING_TYPE_PELOTON";
    }
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
    case LOGGING_STATUS_TYPE_ONGOING: {
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
    case LOGRECORD_TYPE_TUPLE_INSERT: {
      return "LOGRECORD_TYPE_TUPLE_INSERT";
    }
    case LOGRECORD_TYPE_TUPLE_DELETE: {
      return "LOGRECORD_TYPE_TUPLE_DELETE";
    }
    case LOGRECORD_TYPE_TUPLE_UPDATE: {
      return "LOGRECORD_TYPE_TUPLE_UPDATE";
    }
  }
  return "INVALID";
}

ValueType PostgresValueTypeToPelotonValueType(
    PostgresValueType PostgresValType) {
  ValueType valueType = VALUE_TYPE_INVALID;

  switch (PostgresValType) {
    case POSTGRES_VALUE_TYPE_BOOLEAN:
      valueType = VALUE_TYPE_BOOLEAN;
      break;

    /* INTEGER */
    case POSTGRES_VALUE_TYPE_SMALLINT:
      valueType = VALUE_TYPE_SMALLINT;
      break;
    case POSTGRES_VALUE_TYPE_INTEGER:
      valueType = VALUE_TYPE_INTEGER;
      break;
    case POSTGRES_VALUE_TYPE_BIGINT:
      valueType = VALUE_TYPE_BIGINT;
      break;

    /* DOUBLE */
    case POSTGRES_VALUE_TYPE_DOUBLE:
      valueType = VALUE_TYPE_DOUBLE;
      break;

    /* CHAR */
    case POSTGRES_VALUE_TYPE_BPCHAR:
    case POSTGRES_VALUE_TYPE_BPCHAR2:
    case POSTGRES_VALUE_TYPE_VARCHAR:
    case POSTGRES_VALUE_TYPE_VARCHAR2:
    case POSTGRES_VALUE_TYPE_TEXT:
      valueType = VALUE_TYPE_VARCHAR;
      break;

    /* TIMESTAMPS */
    case POSTGRES_VALUE_TYPE_TIMESTAMPS:
    case POSTGRES_VALUE_TYPE_TIMESTAMPS2:
      valueType = VALUE_TYPE_TIMESTAMP;
      break;

    /* DECIMAL */
    case POSTGRES_VALUE_TYPE_DECIMAL:
      valueType = VALUE_TYPE_DECIMAL;
      break;

    /* INVALID VALUE TYPE */
    default:
      LOG_WARN("INVALID VALUE TYPE : %d \n", PostgresValType);
      valueType = VALUE_TYPE_INVALID;
      break;
  }
  return valueType;
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
      fprintf(stderr, "INVALID CONSTRAINT TYPE : %d \n", PostgresConstrType);
      break;
  }
  return constraintType;
}

std::string QuantifierTypeToString(QuantifierType type)
{
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

QuantifierType StringToQuantifierType(std::string str)
{
    if (str == "ANY") {
        return QUANTIFIER_TYPE_ANY;
    } else if (str == "ALL") {
        return QUANTIFIER_TYPE_ALL;
    }
    return QUANTIFIER_TYPE_NONE;
}

}  // End peloton namespace
