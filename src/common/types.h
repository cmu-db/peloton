/*-------------------------------------------------------------------------
 *
 * types.h
 * file description
 *
 * Copyright(c) 2015, CMU
 *
 * /n-store/src/common/types.h
 *
 *-------------------------------------------------------------------------
 */

#pragma once

#include <string>
#include <cstdint>
#include <climits>
#include <limits>

namespace nstore {

//===--------------------------------------------------------------------===//
// NULL-related Constants
//===--------------------------------------------------------------------===//

/// NULL
#define INT8_NULL         INT8_MIN
#define INT16_NULL        INT16_MIN
#define INT32_NULL        INT32_MIN
#define INT64_NULL        INT64_MIN

/// Minimum value user can represent that is not NULL
#define NSTORE_INT8_MIN 		INT8_NULL  + 1
#define NSTORE_INT16_MIN 		INT16_NULL + 1
#define NSTORE_INT32_MIN 		INT32_NULL + 1
#define NSTORE_INT64_MIN 		INT64_NULL + 1
#define DECIMAL_MIN 	-9999999
#define DECIMAL_MAX 	9999999

/// Float/Double less than these values are NULL
#define FLOAT_NULL 		-3.4e+38f
#define DOUBLE_NULL 	-1.7E+308

// Values to be substituted as NULL
#define FLOAT_MIN 		-3.40282347e+38f
#define DOUBLE_MIN 		-1.7976931348623157E+308

// Objects (i.e., VARCHAR) with length prefix of -1 are NULL
#define OBJECTLENGTH_NULL			 -1
#define VALUE_COMPARE_LESSTHAN 		 -1
#define VALUE_COMPARE_EQUAL 		  0
#define VALUE_COMPARE_GREATERTHAN 	  1

//===--------------------------------------------------------------------===//
// Other Constants
//===--------------------------------------------------------------------===//

#define VARCHAR_LENGTH_SHORT 	16
#define VARCHAR_LENGTH_MID  	256
#define VARCHAR_LENGTH_LONG  	4096

//===--------------------------------------------------------------------===//
// Value types
// This file defines all the types that we will support
// We do not allow for user-defined types, nor do we try to do anything dynamic.
//===--------------------------------------------------------------------===//

enum ValueType {
  VALUE_TYPE_INVALID = 0, 	// invalid value type
  VALUE_TYPE_NULL = 1, 		// NULL type

  VALUE_TYPE_TINYINT = 3, 	// 1 byte integer
  VALUE_TYPE_SMALLINT = 4,	// 2 bytes integer
  VALUE_TYPE_INTEGER = 5,		// 4 bytes integer
  VALUE_TYPE_BIGINT = 6, 		// 8 bytes integer
  VALUE_TYPE_DOUBLE = 8, 		// 8 bytes floating number
  VALUE_TYPE_VARCHAR = 9, 	// variable length chars
  VALUE_TYPE_TIMESTAMP = 11, 	// 8 bytes integer
  VALUE_TYPE_DECIMAL = 22, 	// decimal(p,s)
  VALUE_TYPE_BOOLEAN = 23,
  VALUE_TYPE_ADDRESS = 24,
  VALUE_TYPE_VARBINARY = 25, 	// variable length bytes
};

//===--------------------------------------------------------------------===//
// Predicate Expression Operation Types
//===--------------------------------------------------------------------===//

enum ExpressionType {
  EXPRESSION_TYPE_INVALID                     = 0,

  //===--------------------------------------------------------------------===//
  // Arithmetic Operators
  //===--------------------------------------------------------------------===//

  EXPRESSION_TYPE_OPERATOR_PLUS                   = 1, // left + right (both must be number. implicitly casted)
  EXPRESSION_TYPE_OPERATOR_MINUS                  = 2, // left - right (both must be number. implicitly casted)
  EXPRESSION_TYPE_OPERATOR_MULTIPLY               = 3, // left * right (both must be number. implicitly casted)
  EXPRESSION_TYPE_OPERATOR_DIVIDE                 = 4, // left / right (both must be number. implicitly casted)
  EXPRESSION_TYPE_OPERATOR_CONCAT                 = 5, // left || right (both must be char/varchar)
  EXPRESSION_TYPE_OPERATOR_MOD                    = 6, // left % right (both must be integer)
  EXPRESSION_TYPE_OPERATOR_CAST                   = 7, // explicitly cast left as right (right is integer in ValueType enum)
  EXPRESSION_TYPE_OPERATOR_NOT                    = 8, // logical not operator

  //===--------------------------------------------------------------------===//
  // Comparison Operators
  //===--------------------------------------------------------------------===//

  EXPRESSION_TYPE_COMPARE_EQUAL                   = 10, // equal operator between left and right
  EXPRESSION_TYPE_COMPARE_NOTEQUAL                = 11, // inequal operator between left and right
  EXPRESSION_TYPE_COMPARE_LESSTHAN                = 12, // less than operator between left and right
  EXPRESSION_TYPE_COMPARE_GREATERTHAN             = 13, // greater than operator between left and right
  EXPRESSION_TYPE_COMPARE_LESSTHANOREQUALTO       = 14, // less than equal operator between left and right
  EXPRESSION_TYPE_COMPARE_GREATERTHANOREQUALTO    = 15, // greater than equal operator between left and right
  EXPRESSION_TYPE_COMPARE_LIKE                    = 16, // LIKE operator (left LIKE right). both children must be string.

  //===--------------------------------------------------------------------===//
  // Conjunction Operators
  //===--------------------------------------------------------------------===//

  EXPRESSION_TYPE_CONJUNCTION_AND                 = 20,
  EXPRESSION_TYPE_CONJUNCTION_OR                  = 21,

  //===--------------------------------------------------------------------===//
  // Values
  //===--------------------------------------------------------------------===//

  EXPRESSION_TYPE_VALUE_CONSTANT                  = 30,
  EXPRESSION_TYPE_VALUE_PARAMETER                 = 31,
  EXPRESSION_TYPE_VALUE_TUPLE                     = 32,
  EXPRESSION_TYPE_VALUE_TUPLE_ADDRESS             = 33,
  EXPRESSION_TYPE_VALUE_NULL                      = 34,

  //===--------------------------------------------------------------------===//
  // Aggregates
  //===--------------------------------------------------------------------===//

  EXPRESSION_TYPE_AGGREGATE_COUNT                 = 40,
  EXPRESSION_TYPE_AGGREGATE_COUNT_STAR            = 41,
  EXPRESSION_TYPE_AGGREGATE_SUM                   = 42,
  EXPRESSION_TYPE_AGGREGATE_MIN                   = 43,
  EXPRESSION_TYPE_AGGREGATE_MAX                   = 44,
  EXPRESSION_TYPE_AGGREGATE_AVG                   = 45,
  EXPRESSION_TYPE_AGGREGATE_WEIGHTED_AVG          = 46

};

//===--------------------------------------------------------------------===//
// Storage Backend Types
//===--------------------------------------------------------------------===//

enum BackendType {
  BACKEND_TYPE_INVALID = 0, 		// invalid backend type

  BACKEND_TYPE_VM = 1, 			// on volatile memory
  BACKEND_TYPE_NVM = 2 			// on non-volatile memory
};

//===--------------------------------------------------------------------===//
// Index Types
//===--------------------------------------------------------------------===//

enum IndexType {
  INDEX_TYPE_INVALID = 0,     // invalid index type

  INDEX_TYPE_BTREE_MULTIMAP = 1,       // array
  INDEX_TYPE_ORDERED_MAP = 2  // ordered map
};


//===--------------------------------------------------------------------===//
// Type definitions.
//===--------------------------------------------------------------------===//

// For offsets and other similar larger domain types

typedef uint64_t id_t;

static const id_t INVALID_ID = std::numeric_limits<id_t>::max();

static const id_t MAX_ID = std::numeric_limits<id_t>::max() - 1;

// For catalog and other similar smaller domain types

typedef uint16_t oid_t;

static const oid_t INVALID_OID = 0;

// For transaction id

typedef uint64_t txn_id_t;

static const txn_id_t INVALID_TXN_ID = 0;

static const txn_id_t START_TXN_ID = 1;

static const txn_id_t MAX_TXN_ID = std::numeric_limits<txn_id_t>::max();

// For commit id

typedef uint64_t cid_t;

static const cid_t INVALID_CID = 0;

static const cid_t START_CID = 1;

static const cid_t MAX_CID = std::numeric_limits<cid_t>::max();

//===--------------------------------------------------------------------===//
// ItemPointer
//===--------------------------------------------------------------------===//

struct ItemPointer {
  oid_t block;
  oid_t offset;

  ItemPointer()
  : block(INVALID_OID), offset(INVALID_OID) {
  }

  ItemPointer(oid_t block, oid_t offset)
  : block(block), offset(offset){
  }

};

//===--------------------------------------------------------------------===//
// Utilities
//===--------------------------------------------------------------------===//

std::string GetValueTypeName(ValueType type);

std::string GetBackendTypeName(BackendType type);

std::string ValueToString(ValueType type);

ValueType StringToValue(std::string str );

/// Works only for fixed-length types
std::size_t GetTypeSize(ValueType type);

bool IsNumeric(ValueType type);

bool HexDecodeToBinary(unsigned char *bufferdst, const char *hexString);

std::string ExpressionToString(ExpressionType type);

ExpressionType StringToExpression(std::string str);

} // End nstore namespace
