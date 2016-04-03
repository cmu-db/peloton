//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// types.h
//
// Identification: src/backend/common/types.h
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <string>
#include <cstdint>
#include <climits>
#include <limits>
#include <cassert>

//===--------------------------------------------------------------------===//
// GUC Variables
//===--------------------------------------------------------------------===//

// Logging type
// LOGGING_TYPE_AAA_BBB
// Data stored in AAA
// Log stored in BBB
enum LoggingType {
  LOGGING_TYPE_INVALID = 0,

  // Based on write ahead logging
  LOGGING_TYPE_DRAM_NVM = 10,
  LOGGING_TYPE_DRAM_HDD = 11,

  // Based on write behind logging
  LOGGING_TYPE_NVM_NVM = 20,
  LOGGING_TYPE_NVM_HDD = 21,

  LOGGING_TYPE_HDD_NVM = 30,
  LOGGING_TYPE_HDD_HDD = 31,
};

enum CheckpointType {
  CHECKPOINT_TYPE_INVALID = 0,
  CHECKPOINT_TYPE_NORMAL  = 1,
};
//===--------------------------------------------------------------------===//
// Filesystem directories
//===--------------------------------------------------------------------===//

#define NVM_DIR "/mnt/pmfs/"
#define HDD_DIR "/data/"

#define TMP_DIR "/tmp/"

namespace peloton {

// forward declare
class Value;

//===--------------------------------------------------------------------===//
// NULL-related Constants
//===--------------------------------------------------------------------===//

// We use these values for NULL to make it compact and fast
// "-1" is a hack to shut up gcc warning

/// NULL
#define INT8_NULL INT8_MIN
#define INT16_NULL INT16_MIN
#define INT32_NULL INT32_MIN
#define INT64_NULL INT64_MIN

/// Minimum value user can represent that is not NULL
#define PELOTON_INT8_MIN INT8_NULL + 1
#define PELOTON_INT16_MIN INT16_NULL + 1
#define PELOTON_INT32_MIN INT32_NULL + 1
#define PELOTON_INT64_MIN INT64_NULL + 1
#define DECIMAL_MIN -9999999
#define DECIMAL_MAX 9999999

/// Float/Double less than these values are NULL
#define FLOAT_NULL -3.4e+38f
#define DOUBLE_NULL -1.7E+308

// Values to be substituted as NULL
#define FLOAT_MIN -3.40282347e+38f
#define DOUBLE_MIN -1.7976931348623157E+308

// Objects (i.e., VARCHAR) with length prefix of -1 are NULL
#define OBJECTLENGTH_NULL -1

#define VALUE_COMPARE_LESSTHAN -1
#define VALUE_COMPARE_EQUAL 0
#define VALUE_COMPARE_GREATERTHAN 1
#define VALUE_COMPARE_INVALID -2
#define VALUE_COMPARE_NO_EQUAL \
  -3  // assigned when comparing array list and no element matching.

#define DEFAULT_DB_ID 12345
#define DEFAULT_DB_NAME "default"

#define DEFAULT_TUPLES_PER_TILEGROUP 1000

// TODO: Use ThreadLocalPool ?
// This needs to be >= the VoltType.MAX_VALUE_LENGTH defined in java, currently
// 1048576.
// The rationale for making it any larger would be to allow calculating wider
// "temp" values
// for use in situations where they are not being stored as column values
#define POOLED_MAX_VALUE_LENGTH 1048576

//===--------------------------------------------------------------------===//
// Other Constants
//===--------------------------------------------------------------------===//

#define VARCHAR_LENGTH_SHORT 16
#define VARCHAR_LENGTH_MID 256
#define VARCHAR_LENGTH_LONG 4096

//===--------------------------------------------------------------------===//
// Port to OSX
//===---------------------------
#ifdef __APPLE__
#define off64_t off_t
#define MAP_ANONYMOUS MAP_ANON
#endif

//===--------------------------------------------------------------------===//
// Value types
// This file defines all the types that we will support
// We do not allow for user-defined types, nor do we try to do anything dynamic.
//===--------------------------------------------------------------------===//

enum PostgresValueType {
  POSTGRES_VALUE_TYPE_INVALID = -1,
  POSTGRES_VALUE_TYPE_BOOLEAN = 16,

  POSTGRES_VALUE_TYPE_SMALLINT = 21,
  POSTGRES_VALUE_TYPE_INTEGER = 23,
  POSTGRES_VALUE_TYPE_BIGINT = 20,
  POSTGRES_VALUE_TYPE_REAL = 700,
  POSTGRES_VALUE_TYPE_DOUBLE = 701,

  POSTGRES_VALUE_TYPE_TEXT = 25,

  POSTGRES_VALUE_TYPE_BPCHAR = 1042,
  POSTGRES_VALUE_TYPE_BPCHAR2 = 1014,

  POSTGRES_VALUE_TYPE_VARCHAR = 1015,
  POSTGRES_VALUE_TYPE_VARCHAR2 = 1043,

  POSTGRES_VALUE_TYPE_DATE = 1082,
  POSTGRES_VALUE_TYPE_TIMESTAMPS = 1114,
  POSTGRES_VALUE_TYPE_TIMESTAMPS2 = 1184,

  POSTGRES_VALUE_TYPE_TEXT_ARRAY = 1009,     // TEXTARRAYOID in postgres code
  POSTGRES_VALUE_TYPE_INT2_ARRAY = 1005,     // INT2ARRAYOID in postgres code
  POSTGRES_VALUE_TYPE_INT4_ARRAY = 1007,     // INT4ARRAYOID in postgres code
  POSTGRES_VALUE_TYPE_OID_ARRAY = 1028,      // OIDARRAYOID in postgres code
  POSTGRES_VALUE_TYPE_FLOADT4_ARRAY = 1021,  // FLOADT4ARRAYOID in postgres code

  POSTGRES_VALUE_TYPE_DECIMAL = 1700

};

enum ValueType {
  VALUE_TYPE_INVALID = 0,  // a column should never be this type!
  VALUE_TYPE_NULL = 1,     // and they should never be this either!
  VALUE_TYPE_FOR_DIAGNOSTICS_ONLY_NUMERIC =
      2,  // Java planner only. Not used in EE.

  // Column Types
  VALUE_TYPE_TINYINT = 3,     // 1 byte int
  VALUE_TYPE_SMALLINT = 4,    // 2 bytes int
  VALUE_TYPE_INTEGER = 5,     // 4 bytes int
  VALUE_TYPE_BIGINT = 6,      // 8 bytes int
  VALUE_TYPE_REAL = 7,        // 4 bytes floating, called float in C/C++
  VALUE_TYPE_DOUBLE = 8,      // 8 bytes floating, called FLOAT in java
  VALUE_TYPE_VARCHAR = 9,     // variable length chars
  VALUE_TYPE_DATE = 10,       // 4 bytes int
  VALUE_TYPE_TIMESTAMP = 11,  // 8 bytes int
  VALUE_TYPE_DECIMAL = 22,    // decimal(p,s)
  VALUE_TYPE_BOOLEAN =
      23,  // used internally by the planner and EE. Not a valid user data type
  VALUE_TYPE_ADDRESS = 24,    // generated by certain expression code in the EE
  VALUE_TYPE_VARBINARY = 25,  // variable length bytes
  VALUE_TYPE_ARRAY =
      -99,  // an array of elements whose type is indicated elsewhere.
};

//===--------------------------------------------------------------------===//
// Predicate Expression Operation Types
//===--------------------------------------------------------------------===//

enum ExpressionType {
  EXPRESSION_TYPE_INVALID = 0,

  // -----------------------------
  // Arithmetic Operators
  // Implicit Numeric Casting: Trying to implement SQL-92.
  // Implicit Character Casting: Trying to implement SQL-92, but not easy...
  // Anyway, use explicit EXPRESSION_TYPE_OPERATOR_CAST if you could.
  // -----------------------------
  EXPRESSION_TYPE_OPERATOR_PLUS =
      1,  // left + right (both must be number. implicitly casted)
  EXPRESSION_TYPE_OPERATOR_MINUS =
      2,  // left - right (both must be number. implicitly casted)
  EXPRESSION_TYPE_OPERATOR_MULTIPLY =
      3,  // left * right (both must be number. implicitly casted)
  EXPRESSION_TYPE_OPERATOR_DIVIDE =
      4,  // left / right (both must be number. implicitly casted)
  EXPRESSION_TYPE_OPERATOR_CONCAT =
      5,  // left || right (both must be char/varchar)
  EXPRESSION_TYPE_OPERATOR_MOD = 6,  // left % right (both must be integer)
  EXPRESSION_TYPE_OPERATOR_CAST =
      7,  // explicitly cast left as right (right is integer in ValueType enum)
  EXPRESSION_TYPE_OPERATOR_NOT = 8,      // logical not operator
  EXPRESSION_TYPE_OPERATOR_IS_NULL = 9,  // is null test.
  EXPRESSION_TYPE_OPERATOR_EXISTS = 18,  // exists test.
  EXPRESSION_TYPE_OPERATOR_UNARY_MINUS = 50,

  // -----------------------------
  // Comparison Operators
  // -----------------------------
  EXPRESSION_TYPE_COMPARE_EQUAL = 10,  // equal operator between left and right
  EXPRESSION_TYPE_COMPARE_NOTEQUAL =
      11,  // inequal operator between left and right
  EXPRESSION_TYPE_COMPARE_LESSTHAN =
      12,  // less than operator between left and right
  EXPRESSION_TYPE_COMPARE_GREATERTHAN =
      13,  // greater than operator between left and right
  EXPRESSION_TYPE_COMPARE_LESSTHANOREQUALTO =
      14,  // less than equal operator between left and right
  EXPRESSION_TYPE_COMPARE_GREATERTHANOREQUALTO =
      15,  // greater than equal operator between left and right
  EXPRESSION_TYPE_COMPARE_LIKE =
      16,  // LIKE operator (left LIKE right). both children must be string.
  EXPRESSION_TYPE_COMPARE_NOTLIKE = 17,  // NOT LIKE operator (left NOT LIKE
                                         // right). both children must be
                                         // string.
  EXPRESSION_TYPE_COMPARE_IN =
      19,  // IN operator [left IN (right1, right2, ...)]

  // -----------------------------
  // Conjunction Operators
  // -----------------------------
  EXPRESSION_TYPE_CONJUNCTION_AND = 20,
  EXPRESSION_TYPE_CONJUNCTION_OR = 21,

  // -----------------------------
  // Values
  // -----------------------------
  EXPRESSION_TYPE_VALUE_CONSTANT = 30,
  EXPRESSION_TYPE_VALUE_PARAMETER = 31,
  EXPRESSION_TYPE_VALUE_TUPLE = 32,
  EXPRESSION_TYPE_VALUE_TUPLE_ADDRESS = 33,
  EXPRESSION_TYPE_VALUE_NULL = 34,
  EXPRESSION_TYPE_VALUE_VECTOR = 35,
  EXPRESSION_TYPE_VALUE_SCALAR = 36,

  // -----------------------------
  // Aggregates
  // -----------------------------
  EXPRESSION_TYPE_AGGREGATE_COUNT = 40,
  EXPRESSION_TYPE_AGGREGATE_COUNT_STAR = 41,
  EXPRESSION_TYPE_AGGREGATE_SUM = 42,
  EXPRESSION_TYPE_AGGREGATE_MIN = 43,
  EXPRESSION_TYPE_AGGREGATE_MAX = 44,
  EXPRESSION_TYPE_AGGREGATE_AVG = 45,
  EXPRESSION_TYPE_AGGREGATE_APPROX_COUNT_DISTINCT = 46,
  EXPRESSION_TYPE_AGGREGATE_VALS_TO_HYPERLOGLOG = 47,
  EXPRESSION_TYPE_AGGREGATE_HYPERLOGLOGS_TO_CARD = 48,

  // -----------------------------
  // Functions
  // -----------------------------
  EXPRESSION_TYPE_FUNCTION = 100,

  // -----------------------------
  // Internals added for Elastic
  // -----------------------------
  EXPRESSION_TYPE_HASH_RANGE = 200,

  // -----------------------------
  // Internals added for Case When
  // -----------------------------
  EXPRESSION_TYPE_OPERATOR_CASE_WHEN = 300,
  EXPRESSION_TYPE_OPERATOR_ALTERNATIVE = 301,
  EXPRESSION_TYPE_OPERATOR_CASE_EXPR = 302,

  // -----------------------------
  // Internals added for NULLIF
  // -----------------------------
  EXPRESSION_TYPE_OPERATOR_NULLIF = 304,

  // -----------------------------
  // Internals added for COALESCE
  // -----------------------------
  EXPRESSION_TYPE_OPERATOR_COALESCE = 305,

  // -----------------------------
  // Subquery IN/EXISTS
  // -----------------------------
  EXPRESSION_TYPE_ROW_SUBQUERY = 400,
  EXPRESSION_TYPE_SELECT_SUBQUERY = 401,

  // -----------------------------
  // String operators
  // -----------------------------
  EXPRESSION_TYPE_SUBSTR = 500,
  EXPRESSION_TYPE_ASCII = 501,
  EXPRESSION_TYPE_OCTET_LEN = 502,
  EXPRESSION_TYPE_CHAR = 503,
  EXPRESSION_TYPE_CHAR_LEN = 504,
  EXPRESSION_TYPE_SPACE = 505,
  EXPRESSION_TYPE_REPEAT = 506,
  EXPRESSION_TYPE_POSITION = 507,
  EXPRESSION_TYPE_LEFT = 508,
  EXPRESSION_TYPE_RIGHT = 509,
  EXPRESSION_TYPE_CONCAT = 510,
  EXPRESSION_TYPE_LTRIM = 511,
  EXPRESSION_TYPE_RTRIM = 512,
  EXPRESSION_TYPE_BTRIM = 513,
  EXPRESSION_TYPE_REPLACE = 514,
  EXPRESSION_TYPE_OVERLAY = 515,

  // -----------------------------
  // Date operators
  // -----------------------------
  EXPRESSION_TYPE_EXTRACT = 600,
  EXPRESSION_TYPE_DATE_TO_TIMESTAMP = 601,

  //===--------------------------------------------------------------------===//
  // Parser
  //===--------------------------------------------------------------------===//

  EXPRESSION_TYPE_STAR = 700,
  EXPRESSION_TYPE_PLACEHOLDER = 701,
  EXPRESSION_TYPE_COLUMN_REF = 702,
  EXPRESSION_TYPE_FUNCTION_REF = 703,

  //===--------------------------------------------------------------------===//
  // Misc
  //===--------------------------------------------------------------------===//
  EXPRESSION_TYPE_CAST = 900
};

//===--------------------------------------------------------------------===//
// Storage Backend Types
//===--------------------------------------------------------------------===//

enum ConcurrencyType {
  CONCURRENCY_TYPE_OPTIMISTIC = 0, // optimistic
  CONCURRENCY_TYPE_PESSIMISTIC = 1, // pessimistic
  CONCURRENCY_TYPE_SPECULATIVE_READ = 2, // optimistic + speculative read
  CONCURRENCY_TYPE_EAGER_WRITE = 3, // pessimistic + eager write
  CONCURRENCY_TYPE_TO = 4,   // timestamp ordering
  CONCURRENCY_TYPE_SSI = 5   // serializable snapshot isolation
};

enum IsolationLevelType {
  ISOLATION_LEVEL_TYPE_FULL = 0,            // full serializability
  ISOLATION_LEVEL_TYPE_SNAPSHOT = 1,        // snapshot isolation
  ISOLATION_LEVEL_TYPE_REPEATABLE_READ = 2  // repeatable read
};

enum BackendType {
  BACKEND_TYPE_INVALID = 0,  // invalid backend type

  BACKEND_TYPE_MM = 1,   // on volatile memory
  BACKEND_TYPE_FILE = 2  // on mmap file
};

//===--------------------------------------------------------------------===//
// Index Types
//===--------------------------------------------------------------------===//

enum IndexType {
  INDEX_TYPE_INVALID = 0,  // invalid index type

  INDEX_TYPE_BTREE = 1,   // btree
  INDEX_TYPE_BWTREE = 2,  // bwtree
  INDEX_TYPE_HASH = 3     // hash
};

enum IndexConstraintType {
  INDEX_CONSTRAINT_TYPE_INVALID = 0,  // invalid index constraint type

  INDEX_CONSTRAINT_TYPE_DEFAULT =
      1,  // default type - not used to enforce constraints
  INDEX_CONSTRAINT_TYPE_PRIMARY_KEY =
      2,                            // used to enforce primary key constraint
  INDEX_CONSTRAINT_TYPE_UNIQUE = 3  // used for unique constraint
};

//===--------------------------------------------------------------------===//
// Plan Node Types
//===--------------------------------------------------------------------===//

enum PlanNodeType {
  PLAN_NODE_TYPE_INVALID = 0,  // invalid plan node type

  // Scan Nodes
  PLAN_NODE_TYPE_ABSTRACT_SCAN = 10,
  PLAN_NODE_TYPE_SEQSCAN = 11,
  PLAN_NODE_TYPE_INDEXSCAN = 12,

  // Join Nodes
  PLAN_NODE_TYPE_NESTLOOP = 20,
  PLAN_NODE_TYPE_NESTLOOPINDEX = 21,
  PLAN_NODE_TYPE_MERGEJOIN = 22,
  PLAN_NODE_TYPE_HASHJOIN = 23,

  // Mutator Nodes
  PLAN_NODE_TYPE_UPDATE = 30,
  PLAN_NODE_TYPE_INSERT = 31,
  PLAN_NODE_TYPE_DELETE = 32,

  // Communication Nodes
  PLAN_NODE_TYPE_SEND = 40,
  PLAN_NODE_TYPE_RECEIVE = 41,
  PLAN_NODE_TYPE_PRINT = 42,

  // Algebra Nodes
  PLAN_NODE_TYPE_AGGREGATE = 50,
  PLAN_NODE_TYPE_HASHAGGREGATE = 51,  // TODO: Remove it.
  PLAN_NODE_TYPE_UNION = 52,
  PLAN_NODE_TYPE_ORDERBY = 53,
  PLAN_NODE_TYPE_PROJECTION = 54,
  PLAN_NODE_TYPE_MATERIALIZE = 55,
  PLAN_NODE_TYPE_LIMIT = 56,
  PLAN_NODE_TYPE_DISTINCT = 57,
  PLAN_NODE_TYPE_SETOP = 58,   // set operation
  PLAN_NODE_TYPE_APPEND = 59,  // append

  PLAN_NODE_TYPE_AGGREGATE_V2 = 61,
  PLAN_NODE_TYPE_HASH = 62,

  // Utility
  PLAN_NODE_TYPE_RESULT = 70,

  // Test
  PLAN_NODE_TYPE_MOCK = 80
};

//===--------------------------------------------------------------------===//
// Create Types
//===--------------------------------------------------------------------===//

enum CreateType {
  CREATE_TYPE_INVALID = 0,  // invalid create type

  CREATE_TYPE_DB = 1,         // db create type
  CREATE_TYPE_TABLE = 2,      // table create type
  CREATE_TYPE_INDEX = 3,      // index create type
  CREATE_TYPE_CONSTRAINT = 4  // constraint create type

};

//===--------------------------------------------------------------------===//
// Statement Types
//===--------------------------------------------------------------------===//

enum StatementType {
  STATEMENT_TYPE_INVALID = 0,  // invalid statement type

  STATEMENT_TYPE_SELECT = 1,        // select statement type
  STATEMENT_TYPE_INSERT = 3,        // insert statement type
  STATEMENT_TYPE_UPDATE = 4,        // update statement type
  STATEMENT_TYPE_DELETE = 5,        // delete statement type
  STATEMENT_TYPE_CREATE = 6,        // create statement type
  STATEMENT_TYPE_DROP = 7,          // drop statement type
  STATEMENT_TYPE_PREPARE = 8,       // prepare statement type
  STATEMENT_TYPE_EXECUTE = 9,       // execute statement type
  STATEMENT_TYPE_EXPORT = 10,       // export statement type
  STATEMENT_TYPE_RENAME = 11,       // rename statement type
  STATEMENT_TYPE_ALTER = 12,        // alter statement type
  STATEMENT_TYPE_TRANSACTION = 13,  // transaction statement type
};

//===--------------------------------------------------------------------===//
// Scan Direction Types
//===--------------------------------------------------------------------===//

enum ScanDirectionType {
  SCAN_DIRECTION_TYPE_INVALID = 0,  // invalid scan direction

  SCAN_DIRECTION_TYPE_FORWARD = 1,  // forward
  SCAN_DIRECTION_TYPE_BACKWARD = 2  // backward
};

//===--------------------------------------------------------------------===//
// Join Types
//===--------------------------------------------------------------------===//

enum PelotonJoinType {
  JOIN_TYPE_INVALID = 0,  // invalid join type

  JOIN_TYPE_LEFT = 1,   // left
  JOIN_TYPE_RIGHT = 2,  // right
  JOIN_TYPE_INNER = 3,  // inner
  JOIN_TYPE_OUTER = 4,  // outer
  JOIN_TYPE_SEMI = 5    // IN+Subquery is SEMI
};

//===--------------------------------------------------------------------===//
// Aggregate Types
//===--------------------------------------------------------------------===//
enum PelotonAggType {
  AGGREGATE_TYPE_INVALID = 0,
  AGGREGATE_TYPE_SORTED = 1,
  AGGREGATE_TYPE_HASH = 2,
  AGGREGATE_TYPE_PLAIN = 3  // no group-by
};

// ------------------------------------------------------------------
// Expression Quantifier Types
// ------------------------------------------------------------------
enum QuantifierType {
  QUANTIFIER_TYPE_NONE = 0,
  QUANTIFIER_TYPE_ANY = 1,
  QUANTIFIER_TYPE_ALL = 2,
};

//===--------------------------------------------------------------------===//
// Table Reference Types
//===--------------------------------------------------------------------===//

enum TableReferenceType {
  TABLE_REFERENCE_TYPE_INVALID = 0,  // invalid table reference type

  TABLE_REFERENCE_TYPE_NAME = 1,          // table name
  TABLE_REFERENCE_TYPE_SELECT = 2,        // output of select
  TABLE_REFERENCE_TYPE_JOIN = 3,          // output of join
  TABLE_REFERENCE_TYPE_CROSS_PRODUCT = 4  // out of cartesian product
};

//===--------------------------------------------------------------------===//
// Insert Types
//===--------------------------------------------------------------------===//

enum InsertType {
  INSERT_TYPE_INVALID = 0,  // invalid insert type

  INSERT_TYPE_VALUES = 1,  // values
  INSERT_TYPE_SELECT = 2   // select
};

//===--------------------------------------------------------------------===//
// Payload Types
//===--------------------------------------------------------------------===//

enum PayloadType {
  PAYLOAD_TYPE_INVALID = 0,  // invalid message type

  PAYLOAD_TYPE_CLIENT_REQUEST = 1,   // request
  PAYLOAD_TYPE_CLIENT_RESPONSE = 2,  // response
  PAYLOAD_TYPE_STOP = 3              // stop loop
};

//===--------------------------------------------------------------------===//
// Task Priority Types
//===--------------------------------------------------------------------===//

enum TaskPriorityType {
  TASK_PRIORTY_TYPE_INVALID = 0,  // invalid priority

  TASK_PRIORTY_TYPE_LOW = 10,
  TASK_PRIORTY_TYPE_NORMAL = 11,
  TASK_PRIORTY_TYPE_HIGH = 12
};

//===--------------------------------------------------------------------===//
// Result Types
//===--------------------------------------------------------------------===//

enum Result {
  RESULT_INVALID = 0,  // invalid result type

  RESULT_SUCCESS = 1,
  RESULT_FAILURE = 2,
  RESULT_ABORTED = 3,  // aborted
  RESULT_NOOP = 4,     // no op
  RESULT_UNKNOWN = 5
};

//===--------------------------------------------------------------------===//
// Constraint Types
//===--------------------------------------------------------------------===//

enum PostgresConstraintType {
  POSTGRES_CONSTRAINT_NULL, /* not standard SQL, but a lot of people * expect it
                               */

  POSTGRES_CONSTRAINT_NOTNULL,
  POSTGRES_CONSTRAINT_DEFAULT,
  POSTGRES_CONSTRAINT_CHECK,
  POSTGRES_CONSTRAINT_PRIMARY,
  POSTGRES_CONSTRAINT_UNIQUE,
  POSTGRES_CONSTRAINT_EXCLUSION,
  POSTGRES_CONSTRAINT_FOREIGN,
  POSTGRES_CONSTRAINT_ATTR_DEFERRABLE, /* attributes for previous constraint
                                          node */
  POSTGRES_CONSTRAINT_ATTR_NOT_DEFERRABLE,
  POSTGRES_CONSTRAINT_ATTR_DEFERRED,
  POSTGRES_CONSTRAINT_ATTR_IMMEDIATE
};

enum ConstraintType {
  CONSTRAINT_TYPE_INVALID = 0,  // invalid

  CONSTRAINT_TYPE_NULL = 1,      // notnull
  CONSTRAINT_TYPE_NOTNULL = 2,   // notnull
  CONSTRAINT_TYPE_DEFAULT = 3,   // default
  CONSTRAINT_TYPE_CHECK = 4,     // check
  CONSTRAINT_TYPE_PRIMARY = 5,   // primary key
  CONSTRAINT_TYPE_UNIQUE = 6,    // unique
  CONSTRAINT_TYPE_FOREIGN = 7,   // foreign key
  CONSTRAINT_TYPE_EXCLUSION = 8  // foreign key
};

//===--------------------------------------------------------------------===//
// Set Operation Types
//===--------------------------------------------------------------------===//
enum SetOpType {
  SETOP_TYPE_INVALID = 0,

  SETOP_TYPE_INTERSECT = 1,
  SETOP_TYPE_INTERSECT_ALL = 2,
  SETOP_TYPE_EXCEPT = 3,
  SETOP_TYPE_EXCEPT_ALL = 4
};

//===--------------------------------------------------------------------===//
// Log Types
//===--------------------------------------------------------------------===//

enum LoggingStatus {
  LOGGING_STATUS_TYPE_INVALID = 0,

  LOGGING_STATUS_TYPE_STANDBY = 1,
  LOGGING_STATUS_TYPE_RECOVERY = 2,
  LOGGING_STATUS_TYPE_LOGGING = 3,
  LOGGING_STATUS_TYPE_TERMINATE = 4,
  LOGGING_STATUS_TYPE_SLEEP = 5
};

enum LoggerType {
  LOGGER_TYPE_INVALID = 0,

  LOGGER_TYPE_FRONTEND = 1,
  LOGGER_TYPE_BACKEND = 2

};

enum LogRecordType {
  LOGRECORD_TYPE_INVALID = 0,

  // Transaction-related records
  LOGRECORD_TYPE_TRANSACTION_BEGIN = 1,
  LOGRECORD_TYPE_TRANSACTION_COMMIT = 2,
  LOGRECORD_TYPE_TRANSACTION_END = 3,
  LOGRECORD_TYPE_TRANSACTION_ABORT = 4,
  LOGRECORD_TYPE_TRANSACTION_DONE = 5,

  // Generic dml records
  LOGRECORD_TYPE_TUPLE_INSERT = 11,
  LOGRECORD_TYPE_TUPLE_DELETE = 12,
  LOGRECORD_TYPE_TUPLE_UPDATE = 13,

  // DML records for Write ahead logging
  LOGRECORD_TYPE_WAL_TUPLE_INSERT = 21,
  LOGRECORD_TYPE_WAL_TUPLE_DELETE = 22,
  LOGRECORD_TYPE_WAL_TUPLE_UPDATE = 23,

  // DML records for Write behind logging
  LOGRECORD_TYPE_WBL_TUPLE_INSERT = 31,
  LOGRECORD_TYPE_WBL_TUPLE_DELETE = 32,
  LOGRECORD_TYPE_WBL_TUPLE_UPDATE = 33
};

static const int INVALID_FILE_DESCRIPTOR = -1;

// ------------------------------------------------------------------
// Tuple serialization formats
// ------------------------------------------------------------------

enum TupleSerializationFormat {
  TUPLE_SERIALIZATION_NATIVE = 0,
  TUPLE_SERIALIZATION_DR = 1
};

// ------------------------------------------------------------------
// Endianess
// ------------------------------------------------------------------

enum Endianess { BYTE_ORDER_BIG_ENDIAN = 0, BYTE_ORDER_LITTLE_ENDIAN = 1 };

//===--------------------------------------------------------------------===//
// Type definitions.
//===--------------------------------------------------------------------===//

typedef uint64_t oid_t;

static const oid_t START_OID = 0;

static const oid_t INVALID_OID = std::numeric_limits<oid_t>::max();

static const oid_t MAX_OID = std::numeric_limits<oid_t>::max() - 1;

#define NULL_OID MAX_OID

// For transaction id

typedef uint64_t txn_id_t;

static const txn_id_t INVALID_TXN_ID = 0;

static const txn_id_t INITIAL_TXN_ID = 1;

static const txn_id_t START_TXN_ID = 2;

static const txn_id_t MAX_TXN_ID = std::numeric_limits<txn_id_t>::max();

// For commit id

typedef uint64_t cid_t;

static const cid_t INVALID_CID = 0;

static const cid_t START_CID = 1;

static const cid_t MAX_CID = std::numeric_limits<cid_t>::max();

//===--------------------------------------------------------------------===//
// ItemPointer
//===--------------------------------------------------------------------===//

// logical physical location
struct ItemPointer {
  // block
  oid_t block;

  // 0-based offset within block
  oid_t offset;

  ItemPointer() : block(INVALID_OID), offset(INVALID_OID) {}

  ItemPointer(oid_t block, oid_t offset) : block(block), offset(offset) {}

  bool IsNull() { return (block == INVALID_OID && offset == INVALID_OID); }
};

extern ItemPointer INVALID_ITEMPOINTER;

//===--------------------------------------------------------------------===//
// Utilities
//===--------------------------------------------------------------------===//

/// Works only for fixed-length types
std::size_t GetTypeSize(ValueType type);

bool IsNumeric(ValueType type);
bool IsIntegralType(ValueType type);

// for testing, obtain a random instance of the specified type
Value GetRandomValue(ValueType type);

int64_t GetMaxTypeValue(ValueType type);

bool HexDecodeToBinary(unsigned char *bufferdst, const char *hexString);

bool IsBasedOnWriteAheadLogging(LoggingType logging_type);

bool IsBasedOnWriteBehindLogging(LoggingType logging_type);

//===--------------------------------------------------------------------===//
// Transformers
//===--------------------------------------------------------------------===//

std::string BackendTypeToString(BackendType type);
BackendType StringToBackendType(std::string str);

std::string ValueTypeToString(ValueType type);
ValueType StringToValueType(std::string str);

std::string ExpressionTypeToString(ExpressionType type);
ExpressionType StringToExpressionType(std::string str);

std::string IndexTypeToString(IndexType type);
IndexType StringToIndexType(std::string str);

std::string PlanNodeTypeToString(PlanNodeType type);
PlanNodeType StringToPlanNodeType(std::string str);

std::string ConstraintTypeToString(ConstraintType type);
ConstraintType StringToConstraintType(std::string str);

std::string LoggingTypeToString(LoggingType type);
std::string LoggingStatusToString(LoggingStatus type);
std::string LoggerTypeToString(LoggerType type);
std::string LogRecordTypeToString(LogRecordType type);

ValueType PostgresValueTypeToPelotonValueType(
    PostgresValueType PostgresValType);
ConstraintType PostgresConstraintTypeToPelotonConstraintType(
    PostgresConstraintType PostgresConstrType);

//===--------------------------------------------------------------------===//
// Asserts
//===--------------------------------------------------------------------===//

#ifndef NDEBUG

#define AssertMsg(condition, message)                                    \
  do {                                                                   \
    if (!(condition)) {                                                  \
      std::cerr << "Assertion `" #condition "` failed in " << __FILE__   \
                << " line " << __LINE__ << ": " << message << std::endl; \
      std::exit(EXIT_FAILURE);                                           \
    }                                                                    \
  } while (false)

#else

#define AssertMsg(condition, message) \
  do {                                \
  } while (false)

#endif

}  // End peloton namespace
