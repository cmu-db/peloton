//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// types_test.cpp
//
// Identification: test/common/types_test.cpp
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <set>
#include <string>

#include "common/harness.h"
#include "type/types.h"
#include "type/value_factory.h"

namespace peloton {
namespace test {

//===--------------------------------------------------------------------===//
// Types Test
//===--------------------------------------------------------------------===//

class TypesTests : public PelotonTest {};

TEST_F(TypesTests, DatePartTypeTest) {
  std::vector<DatePartType> list = {EXPRESSION_DATE_PART_INVALID,
                                    EXPRESSION_DATE_PART_CENTURY,
                                    EXPRESSION_DATE_PART_DAY,
                                    EXPRESSION_DATE_PART_DAYS,
                                    EXPRESSION_DATE_PART_DECADE,
                                    EXPRESSION_DATE_PART_DECADES,
                                    EXPRESSION_DATE_PART_DOW,
                                    EXPRESSION_DATE_PART_DOY,
                                    EXPRESSION_DATE_PART_EPOCH,
                                    EXPRESSION_DATE_PART_HOUR,
                                    EXPRESSION_DATE_PART_HOURS,
                                    EXPRESSION_DATE_PART_ISODOW,
                                    EXPRESSION_DATE_PART_ISOYEAR,
                                    EXPRESSION_DATE_PART_MICROSECOND,
                                    EXPRESSION_DATE_PART_MICROSECONDS,
                                    EXPRESSION_DATE_PART_MILLENNIUM,
                                    EXPRESSION_DATE_PART_MILLISECOND,
                                    EXPRESSION_DATE_PART_MILLISECONDS,
                                    EXPRESSION_DATE_PART_MINUTE,
                                    EXPRESSION_DATE_PART_MINUTES,
                                    EXPRESSION_DATE_PART_MONTH,
                                    EXPRESSION_DATE_PART_MONTHS,
                                    EXPRESSION_DATE_PART_QUARTER,
                                    EXPRESSION_DATE_PART_QUARTERS,
                                    EXPRESSION_DATE_PART_SECOND,
                                    EXPRESSION_DATE_PART_SECONDS,
                                    EXPRESSION_DATE_PART_TIMEZONE,
                                    EXPRESSION_DATE_PART_TIMEZONE_HOUR,
                                    EXPRESSION_DATE_PART_TIMEZONE_HOURS,
                                    EXPRESSION_DATE_PART_TIMEZONE_MINUTE,
                                    EXPRESSION_DATE_PART_TIMEZONE_MINUTES,
                                    EXPRESSION_DATE_PART_WEEK,
                                    EXPRESSION_DATE_PART_WEEKS,
                                    EXPRESSION_DATE_PART_YEAR,
                                    EXPRESSION_DATE_PART_YEARS};

  // Make sure that ToString and FromString work
  std::set<std::string> all_strings;
  for (auto val : list) {
    std::string str = peloton::DatePartTypeToString(val);
    EXPECT_TRUE(str.size() > 0);
    all_strings.insert(str);

    auto newVal = peloton::StringToDatePartType(str);
    EXPECT_EQ(val, newVal);
  }
  EXPECT_FALSE(all_strings.empty());

  // Then make sure that we can't cast garbage
  std::string invalid("MattPerronWroteTheseMethods");
  EXPECT_THROW(peloton::StringToDatePartType(invalid), peloton::Exception);
  EXPECT_THROW(peloton::DatePartTypeToString(static_cast<DatePartType>(-99999)),
               peloton::Exception);

  // Extra: Make sure that the duplicate DatePartTypes with the 's' suffix map
  // to the same value. For example, 'SECOND' should be equivalent to 'SECONDS'
  for (auto str : all_strings) {
    std::string plural = str + "S";
    if (all_strings.find(plural) == all_strings.end()) continue;

    auto expected = peloton::StringToDatePartType(str);
    auto result = peloton::StringToDatePartType(plural);
    EXPECT_EQ(expected, result);
  }  // FOR
}

TEST_F(TypesTests, BackendTypeTest) {
  std::vector<BackendType> list = {BACKEND_TYPE_INVALID, BACKEND_TYPE_MM,
                                   BACKEND_TYPE_NVM, BACKEND_TYPE_SSD,
                                   BACKEND_TYPE_HDD};

  // Make sure that ToString and FromString work
  for (auto val : list) {
    std::string str = peloton::BackendTypeToString(val);
    EXPECT_TRUE(str.size() > 0);

    auto newVal = peloton::StringToBackendType(str);
    EXPECT_EQ(val, newVal);
  }

  // Then make sure that we can't cast garbage
  std::string invalid("WU TANG");
  EXPECT_THROW(peloton::StringToBackendType(invalid), peloton::Exception);
  EXPECT_THROW(peloton::BackendTypeToString(static_cast<BackendType>(-99999)),
               peloton::Exception);
}

TEST_F(TypesTests, TypeIdTest) {
  std::vector<type::Type::TypeId> list = {
      type::Type::INVALID,   type::Type::PARAMETER_OFFSET,
      type::Type::BOOLEAN,   type::Type::TINYINT,
      type::Type::SMALLINT,  type::Type::INTEGER,
      type::Type::BIGINT,    type::Type::DECIMAL,
      type::Type::TIMESTAMP, type::Type::DATE,
      type::Type::VARCHAR,   type::Type::VARBINARY,
      type::Type::ARRAY,     type::Type::UDT};

  // Make sure that ToString and FromString work
  for (auto val : list) {
    std::string str = peloton::TypeIdToString(val);
    EXPECT_TRUE(str.size() > 0);

    auto newVal = peloton::StringToTypeId(str);
    EXPECT_EQ(val, newVal);
  }

  // Then make sure that we can't cast garbage
  std::string invalid("JoyIsDangerous");
  EXPECT_THROW(peloton::StringToTypeId(invalid), peloton::Exception);
  EXPECT_THROW(peloton::TypeIdToString(static_cast<type::Type::TypeId>(-99999)),
               peloton::Exception);
}

TEST_F(TypesTests, StatementTypeTest) {
  std::vector<StatementType> list = {
      STATEMENT_TYPE_INVALID, STATEMENT_TYPE_SELECT,
      STATEMENT_TYPE_INSERT,  STATEMENT_TYPE_UPDATE,
      STATEMENT_TYPE_DELETE,  STATEMENT_TYPE_CREATE,
      STATEMENT_TYPE_DROP,    STATEMENT_TYPE_PREPARE,
      STATEMENT_TYPE_EXECUTE, STATEMENT_TYPE_RENAME,
      STATEMENT_TYPE_ALTER,   STATEMENT_TYPE_TRANSACTION,
      STATEMENT_TYPE_COPY};

  // Make sure that ToString and FromString work
  for (auto val : list) {
    std::string str = peloton::StatementTypeToString(val);
    EXPECT_TRUE(str.size() > 0);

    auto newVal = peloton::StringToStatementType(str);
    EXPECT_EQ(val, newVal);
  }

  // Then make sure that we can't cast garbage
  std::string invalid("PrashanthTrillAsFuck");
  EXPECT_THROW(peloton::StringToStatementType(invalid), peloton::Exception);
  EXPECT_THROW(
      peloton::StatementTypeToString(static_cast<StatementType>(-99999)),
      peloton::Exception);
}

TEST_F(TypesTests, ExpressionTypeTest) {
  std::vector<ExpressionType> list = {
      ExpressionType::INVALID,
      ExpressionType::OPERATOR_PLUS,
      ExpressionType::OPERATOR_MINUS,
      ExpressionType::OPERATOR_MULTIPLY,
      ExpressionType::OPERATOR_DIVIDE,
      ExpressionType::OPERATOR_CONCAT,
      ExpressionType::OPERATOR_MOD,
      ExpressionType::OPERATOR_CAST,
      ExpressionType::OPERATOR_NOT,
      ExpressionType::OPERATOR_IS_NULL,
      ExpressionType::OPERATOR_EXISTS,
      ExpressionType::OPERATOR_UNARY_MINUS,
      ExpressionType::COMPARE_EQUAL,
      ExpressionType::COMPARE_NOTEQUAL,
      ExpressionType::COMPARE_LESSTHAN,
      ExpressionType::COMPARE_GREATERTHAN,
      ExpressionType::COMPARE_LESSTHANOREQUALTO,
      ExpressionType::COMPARE_GREATERTHANOREQUALTO,
      ExpressionType::COMPARE_LIKE,
      ExpressionType::COMPARE_NOTLIKE,
      ExpressionType::COMPARE_IN,
      ExpressionType::CONJUNCTION_AND,
      ExpressionType::CONJUNCTION_OR,
      ExpressionType::VALUE_CONSTANT,
      ExpressionType::VALUE_PARAMETER,
      ExpressionType::VALUE_TUPLE,
      ExpressionType::VALUE_TUPLE_ADDRESS,
      ExpressionType::VALUE_NULL,
      ExpressionType::VALUE_VECTOR,
      ExpressionType::VALUE_SCALAR,
      ExpressionType::AGGREGATE_COUNT,
      ExpressionType::AGGREGATE_COUNT_STAR,
      ExpressionType::AGGREGATE_SUM,
      ExpressionType::AGGREGATE_MIN,
      ExpressionType::AGGREGATE_MAX,
      ExpressionType::AGGREGATE_AVG,
      ExpressionType::FUNCTION,
      ExpressionType::HASH_RANGE,
      ExpressionType::OPERATOR_CASE_EXPR,
      ExpressionType::OPERATOR_NULLIF,
      ExpressionType::OPERATOR_COALESCE,
      ExpressionType::ROW_SUBQUERY,
      ExpressionType::SELECT_SUBQUERY,
      ExpressionType::SUBSTR,
      ExpressionType::ASCII,
      ExpressionType::OCTET_LEN,
      ExpressionType::CHAR,
      ExpressionType::CHAR_LEN,
      ExpressionType::SPACE,
      ExpressionType::REPEAT,
      ExpressionType::POSITION,
      ExpressionType::LEFT,
      ExpressionType::RIGHT,
      ExpressionType::CONCAT,
      ExpressionType::LTRIM,
      ExpressionType::RTRIM,
      ExpressionType::BTRIM,
      ExpressionType::REPLACE,
      ExpressionType::OVERLAY,
      ExpressionType::EXTRACT,
      ExpressionType::DATE_TO_TIMESTAMP,
      ExpressionType::STAR,
      ExpressionType::PLACEHOLDER,
      ExpressionType::COLUMN_REF,
      ExpressionType::FUNCTION_REF,
      ExpressionType::CAST};

  // Make sure that ToString and FromString work
  for (auto val : list) {
    std::string str = peloton::ExpressionTypeToString(val);
    EXPECT_TRUE(str.size() > 0);

    auto newVal = peloton::StringToExpressionType(str);
    EXPECT_EQ(val, newVal);
  }

  // Then make sure that we can't cast garbage
  std::string invalid("LinLovesEverybody");
  EXPECT_THROW(peloton::StringToExpressionType(invalid), peloton::Exception);
  EXPECT_THROW(
      peloton::ExpressionTypeToString(static_cast<ExpressionType>(-99999)),
      peloton::Exception);
}

TEST_F(TypesTests, IndexTypeTest) {
  std::vector<IndexType> list = {INDEX_TYPE_INVALID, INDEX_TYPE_BWTREE,
                                 INDEX_TYPE_HASH};

  // Make sure that ToString and FromString work
  for (auto val : list) {
    std::string str = peloton::IndexTypeToString(val);
    EXPECT_TRUE(str.size() > 0);

    auto newVal = peloton::StringToIndexType(str);
    EXPECT_EQ(val, newVal);
  }

  // Then make sure that we can't cast garbage
  std::string invalid("DanaSlaysMofos");
  EXPECT_THROW(peloton::StringToIndexType(invalid), peloton::Exception);
  EXPECT_THROW(peloton::IndexTypeToString(static_cast<IndexType>(-99999)),
               peloton::Exception);
}

TEST_F(TypesTests, IndexConstraintTypeTest) {
  std::vector<IndexConstraintType> list = {
      INDEX_CONSTRAINT_TYPE_INVALID, INDEX_CONSTRAINT_TYPE_DEFAULT,
      INDEX_CONSTRAINT_TYPE_PRIMARY_KEY, INDEX_CONSTRAINT_TYPE_UNIQUE};

  // Make sure that ToString and FromString work
  for (auto val : list) {
    std::string str = peloton::IndexConstraintTypeToString(val);
    EXPECT_TRUE(str.size() > 0);

    auto newVal = peloton::StringToIndexConstraintType(str);
    EXPECT_EQ(val, newVal);
  }

  // Then make sure that we can't cast garbage
  std::string invalid("XXXXX");
  EXPECT_THROW(peloton::StringToIndexConstraintType(invalid),
               peloton::Exception);
  EXPECT_THROW(peloton::IndexConstraintTypeToString(
                   static_cast<IndexConstraintType>(-99999)),
               peloton::Exception);
}

TEST_F(TypesTests, PlanNodeTypeTest) {
  std::vector<PlanNodeType> list = {
      PLAN_NODE_TYPE_INVALID,     PLAN_NODE_TYPE_ABSTRACT_SCAN,
      PLAN_NODE_TYPE_SEQSCAN,     PLAN_NODE_TYPE_INDEXSCAN,
      PLAN_NODE_TYPE_NESTLOOP,    PLAN_NODE_TYPE_NESTLOOPINDEX,
      PLAN_NODE_TYPE_MERGEJOIN,   PLAN_NODE_TYPE_HASHJOIN,
      PLAN_NODE_TYPE_UPDATE,      PLAN_NODE_TYPE_INSERT,
      PLAN_NODE_TYPE_DELETE,      PLAN_NODE_TYPE_DROP,
      PLAN_NODE_TYPE_CREATE,      PLAN_NODE_TYPE_SEND,
      PLAN_NODE_TYPE_RECEIVE,     PLAN_NODE_TYPE_PRINT,
      PLAN_NODE_TYPE_AGGREGATE,   PLAN_NODE_TYPE_UNION,
      PLAN_NODE_TYPE_ORDERBY,     PLAN_NODE_TYPE_PROJECTION,
      PLAN_NODE_TYPE_MATERIALIZE, PLAN_NODE_TYPE_LIMIT,
      PLAN_NODE_TYPE_DISTINCT,    PLAN_NODE_TYPE_SETOP,
      PLAN_NODE_TYPE_APPEND,      PLAN_NODE_TYPE_AGGREGATE_V2,
      PLAN_NODE_TYPE_HASH,        PLAN_NODE_TYPE_RESULT,
      PLAN_NODE_TYPE_COPY,        PLAN_NODE_TYPE_MOCK};

  // Make sure that ToString and FromString work
  for (auto val : list) {
    std::string str = peloton::PlanNodeTypeToString(val);
    EXPECT_TRUE(str.size() > 0);

    auto newVal = peloton::StringToPlanNodeType(str);
    EXPECT_EQ(val, newVal);
  }

  // Then make sure that we can't cast garbage
  std::string invalid("AndySmellsBad");
  EXPECT_THROW(peloton::StringToPlanNodeType(invalid), peloton::Exception);
  EXPECT_THROW(peloton::PlanNodeTypeToString(static_cast<PlanNodeType>(-99999)),
               peloton::Exception);
}

TEST_F(TypesTests, ParseNodeTypeTest) {
  std::vector<ParseNodeType> list = {
      PARSE_NODE_TYPE_INVALID,   PARSE_NODE_TYPE_SCAN,
      PARSE_NODE_TYPE_CREATE,    PARSE_NODE_TYPE_DROP,
      PARSE_NODE_TYPE_UPDATE,    PARSE_NODE_TYPE_INSERT,
      PARSE_NODE_TYPE_DELETE,    PARSE_NODE_TYPE_PREPARE,
      PARSE_NODE_TYPE_EXECUTE,   PARSE_NODE_TYPE_SELECT,
      PARSE_NODE_TYPE_JOIN_EXPR, PARSE_NODE_TYPE_TABLE,
      PARSE_NODE_TYPE_MOCK};

  // Make sure that ToString and FromString work
  for (auto val : list) {
    std::string str = peloton::ParseNodeTypeToString(val);
    EXPECT_TRUE(str.size() > 0);

    auto newVal = peloton::StringToParseNodeType(str);
    EXPECT_EQ(val, newVal);
  }

  // Then make sure that we can't cast garbage
  std::string invalid("TerrierHasFleas");
  EXPECT_THROW(peloton::StringToParseNodeType(invalid), peloton::Exception);
  EXPECT_THROW(
      peloton::ParseNodeTypeToString(static_cast<ParseNodeType>(-99999)),
      peloton::Exception);
}

TEST_F(TypesTests, ResultTypeTest) {
  std::vector<ResultType> list = {
      ResultType::INVALID, ResultType::SUCCESS,
      ResultType::FAILURE, ResultType::ABORTED,
      ResultType::NOOP,    ResultType::UNKNOWN};

  // Make sure that ToString and FromString work
  for (auto val : list) {
    std::string str = peloton::ResultTypeToString(val);
    EXPECT_TRUE(str.size() > 0);

    auto newVal = peloton::StringToResultType(str);
    EXPECT_EQ(val, newVal);
  }

  // Then make sure that we can't cast garbage
  std::string invalid("Blah blah blah!!!");
  EXPECT_THROW(peloton::StringToResultType(invalid), peloton::Exception);
  EXPECT_THROW(peloton::ResultTypeToString(static_cast<ResultType>(-99999)),
               peloton::Exception);
}

TEST_F(TypesTests, ConstraintTypeTest) {
  std::vector<ConstraintType> list = {
      CONSTRAINT_TYPE_INVALID,  CONSTRAINT_TYPE_NULL,
      CONSTRAINT_TYPE_NOTNULL,  CONSTRAINT_TYPE_DEFAULT,
      CONSTRAINT_TYPE_CHECK,    CONSTRAINT_TYPE_PRIMARY,
      CONSTRAINT_TYPE_UNIQUE,   CONSTRAINT_TYPE_FOREIGN,
      CONSTRAINT_TYPE_EXCLUSION};

  // Make sure that ToString and FromString work
  for (auto val : list) {
    std::string str = peloton::ConstraintTypeToString(val);
    EXPECT_TRUE(str.size() > 0);

    auto newVal = peloton::StringToConstraintType(str);
    EXPECT_EQ(val, newVal);
  }

  // Then make sure that we can't cast garbage
  std::string invalid("ZiqiGottTheFlu");
  EXPECT_THROW(peloton::StringToConstraintType(invalid), peloton::Exception);
  EXPECT_THROW(
      peloton::ConstraintTypeToString(static_cast<ConstraintType>(-99999)),
      peloton::Exception);
}

TEST_F(TypesTests, LoggingTypeTest) {
  std::vector<LoggingType> list = {
      LOGGING_TYPE_INVALID, LOGGING_TYPE_NVM_WBL, LOGGING_TYPE_SSD_WBL,
      LOGGING_TYPE_HDD_WBL, LOGGING_TYPE_NVM_WAL, LOGGING_TYPE_SSD_WAL,
      LOGGING_TYPE_HDD_WAL,
  };

  // Make sure that ToString and FromString work
  for (auto val : list) {
    std::string str = peloton::LoggingTypeToString(val);
    EXPECT_TRUE(str.size() > 0);

    auto newVal = peloton::StringToLoggingType(str);
    EXPECT_EQ(val, newVal);
  }

  // Then make sure that we can't cast garbage
  std::string invalid("WU TANG");
  EXPECT_THROW(peloton::StringToLoggingType(invalid), peloton::Exception);
  EXPECT_THROW(peloton::LoggingTypeToString(static_cast<LoggingType>(-99999)),
               peloton::Exception);
}

TEST_F(TypesTests, LoggingStatusTypeTest) {
  std::vector<LoggingStatusType> list = {
      LOGGING_STATUS_TYPE_INVALID,   LOGGING_STATUS_TYPE_STANDBY,
      LOGGING_STATUS_TYPE_RECOVERY,  LOGGING_STATUS_TYPE_LOGGING,
      LOGGING_STATUS_TYPE_TERMINATE, LOGGING_STATUS_TYPE_SLEEP};

  // Make sure that ToString and FromString work
  for (auto val : list) {
    std::string str = peloton::LoggingStatusTypeToString(val);
    EXPECT_TRUE(str.size() > 0);

    auto newVal = peloton::StringToLoggingStatusType(str);
    EXPECT_EQ(val, newVal);
  }

  // Then make sure that we can't cast garbage
  std::string invalid("WU TANG");
  EXPECT_THROW(peloton::StringToLoggingStatusType(invalid), peloton::Exception);
  EXPECT_THROW(peloton::LoggingStatusTypeToString(
                   static_cast<LoggingStatusType>(-99999)),
               peloton::Exception);
}

TEST_F(TypesTests, LoggerTypeTest) {
  std::vector<LoggerType> list = {LOGGER_TYPE_INVALID, LOGGER_TYPE_FRONTEND,
                                  LOGGER_TYPE_BACKEND};

  // Make sure that ToString and FromString work
  for (auto val : list) {
    std::string str = peloton::LoggerTypeToString(val);
    EXPECT_TRUE(str.size() > 0);

    auto newVal = peloton::StringToLoggerType(str);
    EXPECT_EQ(val, newVal);
  }

  // Then make sure that we can't cast garbage
  std::string invalid("WU TANG");
  EXPECT_THROW(peloton::StringToLoggerType(invalid), peloton::Exception);
  EXPECT_THROW(peloton::LoggerTypeToString(static_cast<LoggerType>(-99999)),
               peloton::Exception);
}

TEST_F(TypesTests, LogRecordTypeTest) {
  std::vector<LogRecordType> list = {
      LOGRECORD_TYPE_INVALID,
      LOGRECORD_TYPE_TRANSACTION_BEGIN,
      LOGRECORD_TYPE_TRANSACTION_COMMIT,
      LOGRECORD_TYPE_TRANSACTION_END,
      LOGRECORD_TYPE_TRANSACTION_ABORT,
      LOGRECORD_TYPE_TRANSACTION_DONE,
      LOGRECORD_TYPE_TUPLE_INSERT,
      LOGRECORD_TYPE_TUPLE_DELETE,
      LOGRECORD_TYPE_TUPLE_UPDATE,
      LOGRECORD_TYPE_WAL_TUPLE_INSERT,
      LOGRECORD_TYPE_WAL_TUPLE_DELETE,
      LOGRECORD_TYPE_WAL_TUPLE_UPDATE,
      LOGRECORD_TYPE_WBL_TUPLE_INSERT,
      LOGRECORD_TYPE_WBL_TUPLE_DELETE,
      LOGRECORD_TYPE_WBL_TUPLE_UPDATE,
      LOGRECORD_TYPE_ITERATION_DELIMITER,
  };

  // Make sure that ToString and FromString work
  for (auto val : list) {
    std::string str = peloton::LogRecordTypeToString(val);
    EXPECT_TRUE(str.size() > 0);

    auto newVal = peloton::StringToLogRecordType(str);
    EXPECT_EQ(val, newVal);
  }

  // Then make sure that we can't cast garbage
  std::string invalid("WU TANG");
  EXPECT_THROW(peloton::StringToLogRecordType(invalid), peloton::Exception);
  EXPECT_THROW(
      peloton::LogRecordTypeToString(static_cast<LogRecordType>(-99999)),
      peloton::Exception);
}

}  // End test namespace
}  // End peloton namespace
