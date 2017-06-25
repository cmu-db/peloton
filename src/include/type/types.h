//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// types.h
//
// Identification: src/include/type/types.h
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <bitset>
#include <climits>
#include <cstdint>
#include <functional>
#include <limits>
#include <string>
#include <unordered_map>
#include <unordered_set>
#include <vector>

#include "configuration/configuration.h"
#include "type/type.h"

namespace peloton {

// For all of the enums defined in this header, we will
// use this value to indicate that it is an invalid value
// I don't think it matters whether this is 0 or -1
#define INVALID_TYPE_ID 0

// forward declare
// class Value;

//===--------------------------------------------------------------------===//
// NULL-related Constants
//===--------------------------------------------------------------------===//

#define VALUE_COMPARE_LESSTHAN -1
#define VALUE_COMPARE_EQUAL 0
#define VALUE_COMPARE_GREATERTHAN 1
#define VALUE_COMPARE_INVALID -2
#define VALUE_COMPARE_NO_EQUAL \
  -3  // assigned when comparing array list and no element matching.

#define INVALID_RATIO -1

#define DEFAULT_DB_ID 12345
#define DEFAULT_DB_NAME "default_database"

extern int DEFAULT_TUPLES_PER_TILEGROUP;
extern int TEST_TUPLES_PER_TILEGROUP;

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
// Postgres Value Types
// This file defines all the types that we will support
// We do not allow for user-defined types, nor do we try to do anything dynamic.
//
// For more information, see 'pg_type.h' in Postgres
// https://github.com/postgres/postgres/blob/master/src/include/catalog/pg_type.h#L273
//===--------------------------------------------------------------------===//

enum class PostgresValueType {
  INVALID = INVALID_TYPE_ID,
  BOOLEAN = 16,
  TINYINT = 16, // BOOLEAN is an alias for TINYINT
  SMALLINT = 21,
  INTEGER = 23,
  VARBINARY = 17,
  BIGINT = 20,
  REAL = 700,
  DOUBLE = 701,
  TEXT = 25,
  BPCHAR = 1042,
  BPCHAR2 = 1014,
  VARCHAR = 1015,
  VARCHAR2 = 1043,
  DATE = 1082,
  TIMESTAMPS = 1114,
  TIMESTAMPS2 = 1184,
  TEXT_ARRAY = 1009,     // TEXTARRAYOID in postgres code
  INT2_ARRAY = 1005,     // INT2ARRAYOID in postgres code
  INT4_ARRAY = 1007,     // INT4ARRAYOID in postgres code
  OID_ARRAY = 1028,      // OIDARRAYOID in postgres code
  FLOADT4_ARRAY = 1021,  // FLOADT4ARRAYOID in postgres code
  DECIMAL = 1700
};

//===--------------------------------------------------------------------===//
// Predicate Expression Operation Types
//===--------------------------------------------------------------------===//

enum class ExpressionType {
  INVALID = INVALID_TYPE_ID,

  // -----------------------------
  // Arithmetic Operators
  // Implicit Numeric Casting: Trying to implement SQL-92.
  // Implicit Character Casting: Trying to implement SQL-92, but not easy...
  // Anyway, use explicit OPERATOR_CAST if you could.
  // -----------------------------

  // left + right (both must be number. implicitly casted)
  OPERATOR_PLUS = 1,
  // left - right (both must be number. implicitly casted)
  OPERATOR_MINUS = 2,
  // left * right (both must be number. implicitly casted)
  OPERATOR_MULTIPLY = 3,
  // left / right (both must be number. implicitly casted)
  OPERATOR_DIVIDE = 4,
  // left || right (both must be char/varchar)
  OPERATOR_CONCAT = 5,
  // left % right (both must be integer)
  OPERATOR_MOD = 6,
  // explicitly cast left as right (right is integer in ValueType enum)
  OPERATOR_CAST = 7,
  // logical not operator
  OPERATOR_NOT = 8,
  // is null test.
  OPERATOR_IS_NULL = 9,
  // exists test.
  OPERATOR_EXISTS = 18,
  OPERATOR_UNARY_MINUS = 60,

  // -----------------------------
  // Comparison Operators
  // -----------------------------
  // equal operator between left and right
  COMPARE_EQUAL = 10,
  // inequal operator between left and right
  COMPARE_NOTEQUAL = 11,
  // less than operator between left and right
  COMPARE_LESSTHAN = 12,
  // greater than operator between left and right
  COMPARE_GREATERTHAN = 13,
  // less than equal operator between left and right
  COMPARE_LESSTHANOREQUALTO = 14,
  // greater than equal operator between left and right
  COMPARE_GREATERTHANOREQUALTO = 15,
  // LIKE operator (left LIKE right). Both children must be string.
  COMPARE_LIKE = 16,
  // NOT LIKE operator (left NOT LIKE right). Both children must be string.
  COMPARE_NOTLIKE = 17,
  // IN operator [left IN (right1, right2, ...)]
  COMPARE_IN = 19,
  // IS DISTINCT FROM operator
  COMPARE_DISTINCT_FROM = 20,

  // -----------------------------
  // Conjunction Operators
  // -----------------------------
  CONJUNCTION_AND = 30,
  CONJUNCTION_OR = 31,

  // -----------------------------
  // Values
  // -----------------------------
  VALUE_CONSTANT = 40,
  VALUE_PARAMETER = 41,
  VALUE_TUPLE = 42,
  VALUE_TUPLE_ADDRESS = 43,
  VALUE_NULL = 44,
  VALUE_VECTOR = 45,
  VALUE_SCALAR = 46,

  // -----------------------------
  // Aggregates
  // -----------------------------
  AGGREGATE_COUNT = 50,
  AGGREGATE_COUNT_STAR = 51,
  AGGREGATE_SUM = 52,
  AGGREGATE_MIN = 53,
  AGGREGATE_MAX = 54,
  AGGREGATE_AVG = 55,

  // -----------------------------
  // Functions
  // -----------------------------
  FUNCTION = 100,

  // -----------------------------
  // Internals added for Elastic
  // -----------------------------
  HASH_RANGE = 200,

  // -----------------------------
  // Internals added for Case When
  // -----------------------------
  OPERATOR_CASE_EXPR = 302,

  // -----------------------------
  // Internals added for NULLIF
  // -----------------------------
  OPERATOR_NULLIF = 304,

  // -----------------------------
  // Internals added for COALESCE
  // -----------------------------
  OPERATOR_COALESCE = 305,

  // -----------------------------
  // Subquery IN/EXISTS
  // -----------------------------
  ROW_SUBQUERY = 400,
  SELECT_SUBQUERY = 401,

  // -----------------------------
  // String operators
  // -----------------------------
  SUBSTR = 500,
  ASCII = 501,
  OCTET_LEN = 502,
  CHAR = 503,
  CHAR_LEN = 504,
  SPACE = 505,
  REPEAT = 506,
  POSITION = 507,
  LEFT = 508,
  RIGHT = 509,
  CONCAT = 510,
  LTRIM = 511,
  RTRIM = 512,
  BTRIM = 513,
  REPLACE = 514,
  OVERLAY = 515,

  // -----------------------------
  // Date operators
  // -----------------------------
  EXTRACT = 600,
  DATE_TO_TIMESTAMP = 601,

  //===--------------------------------------------------------------------===//
  // Parser
  //===--------------------------------------------------------------------===//
  STAR = 700,
  PLACEHOLDER = 701,
  COLUMN_REF = 702,
  FUNCTION_REF = 703,
  TABLE_REF = 704,

  //===--------------------------------------------------------------------===//
  // Misc
  //===--------------------------------------------------------------------===//
  CAST = 900
};

// When short_str is true, return a short version of ExpressionType string
// For example, + instead of Operator_Plus. It's used to generate the
// expression name
std::string ExpressionTypeToString(ExpressionType type, bool short_str = false);
ExpressionType StringToExpressionType(const std::string &str);
std::ostream &operator<<(std::ostream &os, const ExpressionType &type);
ExpressionType ParserExpressionNameToExpressionType(const std::string &str);

// Note that we have some duplicate DatePartTypes with the 's' suffix
// They have to have the same value in order for it to work.
enum class DatePartType {
  INVALID = INVALID_TYPE_ID,
  CENTURY = 1,
  DAY = 2,
  DAYS = 2,
  DECADE = 3,
  DECADES = 3,
  DOW = 4,
  DOY = 5,
  HOUR = 7,
  HOURS = 7,
  MICROSECOND = 10,
  MICROSECONDS = 10,
  MILLENNIUM = 11,
  MILLISECOND = 12,
  MILLISECONDS = 12,
  MINUTE = 13,
  MINUTES = 13,
  MONTH = 14,
  MONTHS = 14,
  QUARTER = 15,
  QUARTERS = 15,
  SECOND = 16,
  SECONDS = 16,
  WEEK = 20,
  WEEKS = 20,
  YEAR = 21,
  YEARS = 21,
};
std::string DatePartTypeToString(DatePartType type);
DatePartType StringToDatePartType(const std::string &str);
std::ostream &operator<<(std::ostream &os, const DatePartType &type);

// PAVLO: 2017-01-18
// I removed these DatePartTypes because I don't think that
// it's something we can easily support right now. Things get
// weird with timezones
//   EPOCH = 6,
//   ISODOW = 8,
//   ISOYEAR = 9,
//   TIMEZONE = 17,
//   TIMEZONE_HOUR = 18,
//   TIMEZONE_HOURS = 18,
//   TIMEZONE_MINUTE = 19,
//   TIMEZONE_MINUTES = 19,

//===--------------------------------------------------------------------===//
// Network Message Types
//===--------------------------------------------------------------------===//

enum class NetworkMessageType : unsigned char {
  // Important: The character '0' is treated as a null message
  // That means we cannot have an invalid type
  NULL_COMMAND = '0',

  // Responses
  PARSE_COMPLETE = '1',
  BIND_COMPLETE = '2',
  CLOSE_COMPLETE = '3',
  COMMAND_COMPLETE = 'C',
  PARAMETER_STATUS = 'S',
  AUTHENTICATION_REQUEST = 'R',
  ERROR_RESPONSE = 'E',
  EMPTY_QUERY_RESPONSE = 'I',
  NO_DATA_RESPONSE = 'n',
  READY_FOR_QUERY = 'Z',
  ROW_DESCRIPTION = 'T',
  DATA_ROW = 'D',
  // Errors
  HUMAN_READABLE_ERROR = 'M',
  SQLSTATE_CODE_ERROR = 'C',
  // Commands
  EXECUTE_COMMAND = 'E',
  SYNC_COMMAND = 'S',
  TERMINATE_COMMAND = 'X',
  DESCRIBE_COMMAND = 'D',
  BIND_COMMAND = 'B',
  PARSE_COMMAND = 'P',
  SIMPLE_QUERY_COMMAND = 'Q',
  CLOSE_COMMAND = 'C',
  // SSL willingness
  SSL_YES = 'S',
  SSL_NO = 'N',
};

enum class NetworkTransactionStateType : unsigned char {
  INVALID = static_cast<unsigned char>(INVALID_TYPE_ID),
  IDLE = 'I',
  BLOCK = 'T',
  FAIL = 'E',
};

enum SqlStateErrorCode {
  SERIALIZATION_ERROR = '1',
};

//===--------------------------------------------------------------------===//
// Concurrency Control Types
//===--------------------------------------------------------------------===//

enum class ProtocolType {
  INVALID = INVALID_TYPE_ID,
  TIMESTAMP_ORDERING = 1  // timestamp ordering
};
std::string ProtocolTypeToString(ProtocolType type);
ProtocolType StringToProtocolType(const std::string &str);
std::ostream &operator<<(std::ostream &os, const ProtocolType &type);

//===--------------------------------------------------------------------===//
// Epoch Types
//===--------------------------------------------------------------------===//

enum class EpochType {
  INVALID = INVALID_TYPE_ID,
  DECENTRALIZED_EPOCH = 1  // decentralized epoch manager
};
std::string EpochTypeToString(EpochType type);
EpochType StringToEpochType(const std::string &str);
std::ostream &operator<<(std::ostream &os, const EpochType &type);


enum class TimestampType {
  INVALID = INVALID_TYPE_ID,
  SNAPSHOT_READ = 1,
  READ = 2,
  COMMIT = 3
};
std::string TimestampTypeToString(TimestampType type);
TimestampType StringToTimestampType(const std::string &str);
std::ostream &operator<<(std::ostream &os, const TimestampType &type);

//===--------------------------------------------------------------------===//
// Visibility Types
//===--------------------------------------------------------------------===//

enum class VisibilityType {
  INVALID = INVALID_TYPE_ID,
  INVISIBLE = 1,
  DELETED = 2,
  OK = 3
};
std::string VisibilityTypeToString(VisibilityType type);
VisibilityType StringToVisibilityType(const std::string &str);
std::ostream &operator<<(std::ostream &os, const VisibilityType &type);


enum class VisibilityIdType {
  INVALID = INVALID_TYPE_ID,
  READ_ID = 1,
  COMMIT_ID = 2
};
std::string VisibilityIdTypeToString(VisibilityIdType type);
VisibilityIdType StringToVisibilityIdType(const std::string &str);
std::ostream &operator<<(std::ostream &os, const VisibilityIdType &type);

//===--------------------------------------------------------------------===//
// Isolation Levels
//===--------------------------------------------------------------------===//

enum class IsolationLevelType {
  INVALID = INVALID_TYPE_ID,
  SERIALIZABLE = 1,     // serializable
  SNAPSHOT = 2,         // snapshot isolation
  REPEATABLE_READS = 3, // repeatable reads
  READ_COMMITTED = 4,   // read committed
  READ_ONLY = 5         // read only
};
std::string IsolationLevelTypeToString(IsolationLevelType type);
IsolationLevelType StringToIsolationLevelType(const std::string &str);
std::ostream &operator<<(std::ostream &os, const IsolationLevelType &type);

//===--------------------------------------------------------------------===//
// Conflict Avoidance types
//===--------------------------------------------------------------------===//

enum class ConflictAvoidanceType {
  INVALID = INVALID_TYPE_ID,
  WAIT = 1,     // wait-based
  ABORT = 2,    // abort-based
};
std::string ConflictAvoidanceTypeToString(ConflictAvoidanceType type);
ConflictAvoidanceType StringToConflictAvoidanceType(const std::string &str);
std::ostream &operator<<(std::ostream &os, const ConflictAvoidanceType &type);

//===--------------------------------------------------------------------===//
// Garbage Collection Types
//===--------------------------------------------------------------------===//

enum class GarbageCollectionType {
  INVALID = INVALID_TYPE_ID,
  OFF = 1,  // turn off GC
  ON = 2    // turn on GC
};
std::string GarbageCollectionTypeToString(GarbageCollectionType type);
GarbageCollectionType StringToGarbageCollectionType(const std::string &str);
std::ostream &operator<<(std::ostream &os, const GarbageCollectionType &type);

//===--------------------------------------------------------------------===//
// Backend Types
//===--------------------------------------------------------------------===//

enum class BackendType {
  INVALID = INVALID_TYPE_ID,  // invalid backend type
  MM = 1,                     // on volatile memory
  NVM = 2,                    // on non-volatile memory
  SSD = 3,                    // on ssd
  HDD = 4                     // on hdd
};
std::string BackendTypeToString(BackendType type);
BackendType StringToBackendType(const std::string &str);
std::ostream &operator<<(std::ostream &os, const BackendType &type);

//===--------------------------------------------------------------------===//
// Index Types
//===--------------------------------------------------------------------===//

enum class IndexType {
  INVALID = INVALID_TYPE_ID,  // invalid index type
  BWTREE = 1,                 // bwtree
  HASH = 2,                   // hash
  SKIPLIST = 3                // skiplist
};
std::string IndexTypeToString(IndexType type);
IndexType StringToIndexType(const std::string &str);
std::ostream &operator<<(std::ostream &os, const IndexType &type);

enum class IndexConstraintType {
  // invalid index constraint type
  INVALID = INVALID_TYPE_ID,
  // default type - not used to enforce constraints
  DEFAULT = 1,
  // used to enforce primary key constraint
  PRIMARY_KEY = 2,
  // used for unique constraint
  UNIQUE = 3
};
std::string IndexConstraintTypeToString(IndexConstraintType type);
IndexConstraintType StringToIndexConstraintType(const std::string &str);
std::ostream &operator<<(std::ostream &os, const IndexConstraintType &type);

//===--------------------------------------------------------------------===//
// Hybrid Scan Types
//===--------------------------------------------------------------------===//

enum class HybridScanType {
  INVALID = INVALID_TYPE_ID,
  SEQUENTIAL = 1,
  INDEX = 2,
  HYBRID = 3
};
std::string HybridScanTypeToString(HybridScanType type);
HybridScanType StringToHybridScanType(const std::string &str);
std::ostream &operator<<(std::ostream &os, const HybridScanType &type);

//===--------------------------------------------------------------------===//
// Parse Node Types
//===--------------------------------------------------------------------===//

enum class ParseNodeType {
  INVALID = INVALID_TYPE_ID,  // invalid parse node type

  // Scan Nodes
  SCAN = 10,

  // DDL Nodes
  CREATE = 20,
  DROP = 21,

  // Mutator Nodes
  UPDATE = 30,
  INSERT = 31,
  DELETE = 32,

  // Prepared Nodes
  PREPARE = 40,
  EXECUTE = 41,

  // Select Nodes
  SELECT = 50,
  JOIN_EXPR = 51,  // a join tree
  TABLE = 52,      // a single table

  // Test
  MOCK = 80
};
std::string ParseNodeTypeToString(ParseNodeType type);
ParseNodeType StringToParseNodeType(const std::string &str);
std::ostream &operator<<(std::ostream &os, const ParseNodeType &type);

//===--------------------------------------------------------------------===//
// Plan Node Types
//===--------------------------------------------------------------------===//

enum class PlanNodeType {
  INVALID = INVALID_TYPE_ID,  // invalid plan node type

  // Scan Nodes
  ABSTRACT_SCAN = 10,
  SEQSCAN = 11,
  INDEXSCAN = 12,

  // Join Nodes
  NESTLOOP = 20,
  NESTLOOPINDEX = 21,
  MERGEJOIN = 22,
  HASHJOIN = 23,

  // Mutator Nodes
  UPDATE = 30,
  INSERT = 31,
  DELETE = 32,

  // DDL Nodes
  DROP = 33,
  CREATE = 34,
  POPULATE_INDEX = 35,

  // Communication Nodes
  SEND = 40,
  RECEIVE = 41,
  PRINT = 42,

  // Algebra Nodes
  AGGREGATE = 50,
  UNION = 52,
  ORDERBY = 53,
  PROJECTION = 54,
  MATERIALIZE = 55,
  LIMIT = 56,
  DISTINCT = 57,
  SETOP = 58,   // set operation
  APPEND = 59,  // append
  AGGREGATE_V2 = 61,
  HASH = 62,

  // Utility
  RESULT = 70,
  COPY = 71,

  // Test
  MOCK = 80
};
std::string PlanNodeTypeToString(PlanNodeType type);
PlanNodeType StringToPlanNodeType(const std::string &str);
std::ostream &operator<<(std::ostream &os, const PlanNodeType &type);

//===--------------------------------------------------------------------===//
// Create Types
//===--------------------------------------------------------------------===//

enum class CreateType {
  INVALID = INVALID_TYPE_ID,  // invalid create type
  DB = 1,                     // db create type
  TABLE = 2,                  // table create type
  INDEX = 3,                  // index create type
  CONSTRAINT = 4              // constraint create type
};

//===--------------------------------------------------------------------===//
// Statement Types
//===--------------------------------------------------------------------===//

enum class StatementType {
  INVALID = INVALID_TYPE_ID,  // invalid statement type
  SELECT = 1,                 // select statement type
  INSERT = 3,                 // insert statement type
  UPDATE = 4,                 // update statement type
  DELETE = 5,                 // delete statement type
  CREATE = 6,                 // create statement type
  DROP = 7,                   // drop statement type
  PREPARE = 8,                // prepare statement type
  EXECUTE = 9,                // execute statement type
  RENAME = 11,                // rename statement type
  ALTER = 12,                 // alter statement type
  TRANSACTION = 13,           // transaction statement type,
  COPY = 14                   // copy type
};
std::string StatementTypeToString(StatementType type);
StatementType StringToStatementType(const std::string &str);
std::ostream &operator<<(std::ostream &os, const StatementType &type);

//===--------------------------------------------------------------------===//
// Scan Direction Types
//===--------------------------------------------------------------------===//

enum class ScanDirectionType {
  INVALID = INVALID_TYPE_ID,  // invalid scan direction
  FORWARD = 1,                // forward
  BACKWARD = 2                // backward
};

//===--------------------------------------------------------------------===//
// Join Types
//===--------------------------------------------------------------------===//

enum class JoinType {
  INVALID = INVALID_TYPE_ID,  // invalid join type
  LEFT = 1,                   // left
  RIGHT = 2,                  // right
  INNER = 3,                  // inner
  OUTER = 4,                  // outer
  SEMI = 5                    // IN+Subquery is SEMI
};
std::string JoinTypeToString(JoinType type);
JoinType StringToJoinType(const std::string &str);
std::ostream &operator<<(std::ostream &os, const JoinType &type);

//===--------------------------------------------------------------------===//
// Aggregate Types
//===--------------------------------------------------------------------===//

enum class AggregateType {
  INVALID = INVALID_TYPE_ID,
  SORTED = 1,
  HASH = 2,
  PLAIN = 3  // no group-by
};
std::string AggregateTypeToString(AggregateType type);
AggregateType StringToAggregateType(const std::string &str);
std::ostream &operator<<(std::ostream &os, const AggregateType &type);

// ------------------------------------------------------------------
// Expression Quantifier Types
// ------------------------------------------------------------------
enum class QuantifierType {
  NONE = 0,
  ANY = 1,
  ALL = 2,
};
std::string QuantifierTypeToString(QuantifierType type);
QuantifierType StringToQuantifierType(const std::string &str);
std::ostream &operator<<(std::ostream &os, const QuantifierType &type);

//===--------------------------------------------------------------------===//
// Table Reference Types
//===--------------------------------------------------------------------===//

enum class TableReferenceType {
  INVALID = INVALID_TYPE_ID,  // invalid table reference type
  NAME = 1,                   // table name
  SELECT = 2,                 // output of select
  JOIN = 3,                   // output of join
  CROSS_PRODUCT = 4           // out of cartesian product
};
std::string TableReferenceTypeToString(TableReferenceType type);
TableReferenceType StringToTableReferenceType(const std::string &str);
std::ostream &operator<<(std::ostream &os, const TableReferenceType &type);

//===--------------------------------------------------------------------===//
// Insert Types
//===--------------------------------------------------------------------===//

enum class InsertType {
  INVALID = INVALID_TYPE_ID,  // invalid insert type
  VALUES = 1,                 // values
  SELECT = 2                  // select
};
std::string InsertTypeToString(InsertType type);
InsertType StringToInsertType(const std::string &str);
std::ostream &operator<<(std::ostream &os, const InsertType &type);

//===--------------------------------------------------------------------===//
// Copy Types
//===--------------------------------------------------------------------===//

enum class CopyType {
  IMPORT_CSV,     // Import csv data to database
  IMPORT_TSV,     // Import tsv data to database
  EXPORT_CSV,     // Export data to csv file
  EXPORT_STDOUT,  // Export data to std out
  EXPORT_OTHER,   // Export data to other file format
};

//===--------------------------------------------------------------------===//
// Payload Types
//===--------------------------------------------------------------------===//

enum class PayloadType {
  INVALID = INVALID_TYPE_ID,  // invalid message type
  CLIENT_REQUEST = 1,         // request
  CLIENT_RESPONSE = 2,        // response
  STOP = 3                    // stop loop
};
std::string PayloadTypeToString(PayloadType type);
PayloadType StringToPayloadType(const std::string &str);
std::ostream &operator<<(std::ostream &os, const PayloadType &type);

//===--------------------------------------------------------------------===//
// Task Priority Types
//===--------------------------------------------------------------------===//

enum class TaskPriorityType {
  INVALID = INVALID_TYPE_ID,  // invalid priority
  LOW = 10,
  NORMAL = 11,
  HIGH = 12
};
std::string TaskPriorityTypeToString(TaskPriorityType type);
TaskPriorityType StringToTaskPriorityType(const std::string &str);
std::ostream &operator<<(std::ostream &os, const TaskPriorityType &type);

//===--------------------------------------------------------------------===//
// Result Types
//===--------------------------------------------------------------------===//

enum class ResultType {
  INVALID = INVALID_TYPE_ID,  // invalid result type
  SUCCESS = 1,
  FAILURE = 2,
  ABORTED = 3,  // aborted
  NOOP = 4,     // no op
  UNKNOWN = 5
};
std::string ResultTypeToString(ResultType type);
ResultType StringToResultType(const std::string &str);
std::ostream &operator<<(std::ostream &os, const ResultType &type);

//===--------------------------------------------------------------------===//
// Constraint Types
//===--------------------------------------------------------------------===//

enum class PostgresConstraintType {
  NOT_NULL, /* not standard SQL, but a lot of people * expect it */
  NOTNULL,
  DEFAULT,
  CHECK,
  PRIMARY,
  UNIQUE,
  EXCLUSION,
  FOREIGN,
  ATTR_DEFERRABLE, /* attributes for previous constraint node */
  ATTR_NOT_DEFERRABLE,
  ATTR_DEFERRED,
  ATTR_IMMEDIATE
};

enum class ConstraintType {
  INVALID = INVALID_TYPE_ID,  // invalid
  NOT_NULL = 1,               // notnull
  NOTNULL = 2,                // notnull
  DEFAULT = 3,                // default
  CHECK = 4,                  // check
  PRIMARY = 5,                // primary key
  UNIQUE = 6,                 // unique
  FOREIGN = 7,                // foreign key
  EXCLUSION = 8               // foreign key
};
std::string ConstraintTypeToString(ConstraintType type);
ConstraintType StringToConstraintType(const std::string &str);
std::ostream &operator<<(std::ostream &os, const ConstraintType &type);

//===--------------------------------------------------------------------===//
// Set Operation Types
//===--------------------------------------------------------------------===//
enum class SetOpType {
  INVALID = INVALID_TYPE_ID,
  INTERSECT = 1,
  INTERSECT_ALL = 2,
  EXCEPT = 3,
  EXCEPT_ALL = 4
};
std::string SetOpTypeToString(SetOpType type);
SetOpType StringToSetOpType(const std::string &str);
std::ostream &operator<<(std::ostream &os, const SetOpType &type);

//===--------------------------------------------------------------------===//
// Logging + Recovery Types
//===--------------------------------------------------------------------===//

enum class LoggingType {
  INVALID = INVALID_TYPE_ID,
  OFF = 1,  // turn off GC
  ON = 2    // turn on GC
};
std::string LoggingTypeToString(LoggingType type);
LoggingType StringToLoggingType(const std::string &str);
std::ostream &operator<<(std::ostream &os, const LoggingType &type);


enum class LogRecordType {
  INVALID = INVALID_TYPE_ID,

  // Transaction-related records
  TRANSACTION_BEGIN = 1,
  TRANSACTION_COMMIT = 2,

  // Generic dml records
  TUPLE_INSERT = 11,
  TUPLE_DELETE = 12,
  TUPLE_UPDATE = 13,

  // Epoch related records
  EPOCH_BEGIN = 21,
  EPOCH_END = 22,
};
std::string LogRecordTypeToString(LogRecordType type);
LogRecordType StringToLogRecordType(const std::string &str);
std::ostream &operator<<(std::ostream &os, const LogRecordType &type);


enum class CheckpointingType {
  INVALID = INVALID_TYPE_ID,
  OFF = 1,  // turn off GC
  ON = 2    // turn on GC
};
std::string CheckpointingTypeToString(CheckpointingType type);
CheckpointingType StringToCheckpointingType(const std::string &str);
std::ostream &operator<<(std::ostream &os, const CheckpointingType &type);



/* Possible values for peloton_tilegroup_layout GUC */
typedef enum LayoutType {
  LAYOUT_TYPE_INVALID = INVALID_TYPE_ID,
  LAYOUT_TYPE_ROW = 1,    /* Pure row layout */
  LAYOUT_TYPE_COLUMN = 2, /* Pure column layout */
  LAYOUT_TYPE_HYBRID = 3  /* Hybrid layout */
} LayoutType;

//===--------------------------------------------------------------------===//
// Statistics Types
//===--------------------------------------------------------------------===//

// Statistics Collection Type
// Disable or enable
enum StatsType {
  // Disable statistics collection
  STATS_TYPE_INVALID = INVALID_TYPE_ID,
  // Enable statistics collection
  STATS_TYPE_ENABLE = 1,
};

enum MetricType {
  // Metric type is invalid
  INVALID_METRIC = INVALID_TYPE_ID,
  // Metric to count a number
  COUNTER_METRIC = 1,
  // Access information, e.g., # tuples read, inserted, updated, deleted
  ACCESS_METRIC = 2,
  // Life time of a object
  LIFETIME_METRIC = 3,
  // Statistics for a specific database
  DATABASE_METRIC = 4,
  // Statistics for a specific table
  TABLE_METRIC = 5,
  // Statistics for a specific index
  INDEX_METRIC = 6,
  // Latency of transactions
  LATENCY_METRIC = 7,
  // Timestamp, e.g., creation time of a table/index
  TEMPORAL_METRIC = 8,
  // Statistics for a specific table
  QUERY_METRIC = 9,
  // Statistics for CPU
  PROCESSOR_METRIC = 10,
};

static const int INVALID_FILE_DESCRIPTOR = -1;

// ------------------------------------------------------------------
// Tuple serialization formats
// ------------------------------------------------------------------

enum class TupleSerializationFormat { NATIVE = 0, DR = 1 };

// ------------------------------------------------------------------
// Entity types
// ------------------------------------------------------------------

enum class EntityType {
  INVALID = INVALID_TYPE_ID,
  TABLE = 1,
  SCHEMA = 2,
  INDEX = 3,
  VIEW = 4,
  PREPARED_STATEMENT = 5,
};
std::string EntityTypeToString(EntityType type);
EntityType StringToEntityType(const std::string &str);
std::ostream &operator<<(std::ostream &os, const EntityType &type);

// ------------------------------------------------------------------
// Endianess
// ------------------------------------------------------------------

enum Endianess { BYTE_ORDER_BIG_ENDIAN = 0, BYTE_ORDER_LITTLE_ENDIAN = 1 };

//===--------------------------------------------------------------------===//
// Type definitions.
//===--------------------------------------------------------------------===//

typedef size_t hash_t;

typedef uint32_t oid_t;

static const oid_t START_OID = 0;

static const oid_t INVALID_OID = std::numeric_limits<oid_t>::max();

static const oid_t MAX_OID = std::numeric_limits<oid_t>::max() - 1;

#define NULL_OID MAX_OID

// For transaction id

typedef uint64_t txn_id_t;

static const txn_id_t INVALID_TXN_ID = 0;

static const txn_id_t INITIAL_TXN_ID = 1;

static const txn_id_t MAX_TXN_ID = std::numeric_limits<txn_id_t>::max();

// For commit id

typedef uint64_t cid_t;

static const cid_t INVALID_CID = 0;

static const cid_t MAX_CID = std::numeric_limits<cid_t>::max();

// For epoch id

typedef uint64_t eid_t;

static const cid_t INVALID_EID = 0;

static const cid_t MAX_EID = std::numeric_limits<eid_t>::max();

// For epoch
static const size_t EPOCH_LENGTH = 40;

// For threads
extern size_t CONNECTION_THREAD_COUNT;
extern size_t LOGGING_THREAD_COUNT;
extern size_t GC_THREAD_COUNT;
extern size_t EPOCH_THREAD_COUNT;
extern size_t MAX_CONCURRENCY;

//===--------------------------------------------------------------------===//
// TupleMetadata
//===--------------------------------------------------------------------===//
struct TupleMetadata {
  oid_t table_id = 0;
  oid_t tile_group_id = 0;
  oid_t tuple_slot_id = 0;
  cid_t tuple_end_cid = 0;
};

//===--------------------------------------------------------------------===//
// Column Bitmap
//===--------------------------------------------------------------------===//
static const size_t max_col_count = 128;
typedef std::bitset<max_col_count> ColBitmap;

//===--------------------------------------------------------------------===//
// read-write set
//===--------------------------------------------------------------------===//

enum class RWType {
  INVALID,
  READ,
  READ_OWN,  // select for update
  UPDATE,
  INSERT,
  DELETE,
  INS_DEL,  // delete after insert.
};
std::string RWTypeToString(RWType type);
RWType StringToRWType(const std::string &str);
std::ostream &operator<<(std::ostream &os, const RWType &type);

enum class GCSetType { COMMITTED, ABORTED };

// block -> offset -> type
typedef std::unordered_map<oid_t, std::unordered_map<oid_t, RWType>>
    ReadWriteSet;

// block -> offset -> is_index_deletion
typedef std::unordered_map<oid_t, std::unordered_map<oid_t, bool>> GCSet;

//===--------------------------------------------------------------------===//
// File Handle
//===--------------------------------------------------------------------===//
struct FileHandle {
  // FILE pointer
  FILE *file = nullptr;

  // File descriptor
  int fd;

  // Size of the file
  size_t size;

  FileHandle() : file(nullptr), fd(INVALID_FILE_DESCRIPTOR), size(0) {}

  FileHandle(FILE *file, int fd, size_t size)
      : file(file), fd(fd), size(size) {}
};
extern FileHandle INVALID_FILE_HANDLE;

//===--------------------------------------------------------------------===//
// Utilities
//===--------------------------------------------------------------------===//

bool HexDecodeToBinary(unsigned char *bufferdst, const char *hexString);

//===--------------------------------------------------------------------===//
// Transformers
//===--------------------------------------------------------------------===//

std::string TypeIdToString(type::Type::TypeId type);
type::Type::TypeId StringToTypeId(const std::string &str);

type::Type::TypeId PostgresValueTypeToPelotonValueType(PostgresValueType type);
ConstraintType PostgresConstraintTypeToPelotonConstraintType(
    PostgresConstraintType type);

std::string SqlStateErrorCodeToString(SqlStateErrorCode code);

namespace expression {
class AbstractExpression;
}

/**
 * @brief Generic specification of a projection target:
 *        < DEST_column_id , expression >
 */

namespace planner {
struct DerivedAttribute;
}

typedef std::pair<oid_t, const planner::DerivedAttribute> Target;

typedef std::vector<Target> TargetList;

/**
 * @brief Generic specification of a direct map:
 *        < NEW_col_id , <tuple_index (left or right tuple), OLD_col_id>    >
 */
typedef std::pair<oid_t, std::pair<oid_t, oid_t>> DirectMap;

typedef std::vector<DirectMap> DirectMapList;

//===--------------------------------------------------------------------===//
// Optimizer
//===--------------------------------------------------------------------===//
enum class PropertyType {
  PREDICATE,
  COLUMNS,
  DISTINCT,
  SORT,
  LIMIT,
};

namespace expression {
class AbstractExpression;
class ExprHasher;
class ExprEqualCmp;
}

// Augment abstract expression with a table alias set
struct MultiTableExpression {
  MultiTableExpression(
      expression::AbstractExpression* i_expr,
      std::unordered_set<std::string>& i_set)
      : expr(i_expr), table_alias_set(i_set) {}
  MultiTableExpression(const MultiTableExpression& mt_expr)
      : expr(mt_expr.expr), table_alias_set(mt_expr.table_alias_set) {}
  expression::AbstractExpression* expr;
  std::unordered_set<std::string> table_alias_set;
};

typedef std::vector<expression::AbstractExpression*> SingleTablePredicates;
typedef std::vector<MultiTableExpression> MultiTablePredicates;

// Mapping of Expression -> Column Offset created by operator
typedef std::unordered_map<std::shared_ptr<expression::AbstractExpression>,
                           unsigned, expression::ExprHasher,
                           expression::ExprEqualCmp> ExprMap;
// Used in optimizer to speed up expression comparsion
typedef std::unordered_set<std::shared_ptr<expression::AbstractExpression>,
                           expression::ExprHasher,
                           expression::ExprEqualCmp> ExprSet;

std::string PropertyTypeToString(PropertyType type);

//===--------------------------------------------------------------------===//
// Wire protocol typedefs
//===--------------------------------------------------------------------===//
#define SOCKET_BUFFER_SIZE 8192

/* byte type */
typedef unsigned char uchar;

/* type for buffer of bytes */
typedef std::vector<uchar> ByteBuf;

}  // End peloton namespace
