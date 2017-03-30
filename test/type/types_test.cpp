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
  std::vector<DatePartType> list = {
      DatePartType::INVALID,      DatePartType::CENTURY,
      DatePartType::DAY,          DatePartType::DAYS,
      DatePartType::DECADE,       DatePartType::DECADES,
      DatePartType::DOW,          DatePartType::DOY,
      DatePartType::HOUR,         DatePartType::HOURS,
      DatePartType::MICROSECOND,  DatePartType::MICROSECONDS,
      DatePartType::MILLENNIUM,   DatePartType::MILLISECOND,
      DatePartType::MILLISECONDS, DatePartType::MINUTE,
      DatePartType::MINUTES,      DatePartType::MONTH,
      DatePartType::MONTHS,       DatePartType::QUARTER,
      DatePartType::QUARTERS,     DatePartType::SECOND,
      DatePartType::SECONDS,      DatePartType::WEEK,
      DatePartType::WEEKS,        DatePartType::YEAR,
      DatePartType::YEARS};

  // Make sure that ToString and FromString work
  std::set<std::string> all_strings;
  for (auto val : list) {
    std::string str = peloton::DatePartTypeToString(val);
    EXPECT_TRUE(str.size() > 0);
    all_strings.insert(str);

    auto newVal = peloton::StringToDatePartType(str);
    EXPECT_EQ(val, newVal);

    std::ostringstream os;
    os << val;
    EXPECT_EQ(str, os.str());
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
  std::vector<BackendType> list = {BackendType::INVALID, BackendType::MM,
                                   BackendType::NVM, BackendType::SSD,
                                   BackendType::HDD};

  // Make sure that ToString and FromString work
  for (auto val : list) {
    std::string str = peloton::BackendTypeToString(val);
    EXPECT_TRUE(str.size() > 0);

    auto newVal = peloton::StringToBackendType(str);
    EXPECT_EQ(val, newVal);

    std::ostringstream os;
    os << val;
    EXPECT_EQ(str, os.str());
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
      StatementType::INVALID, StatementType::SELECT,
      StatementType::INSERT,  StatementType::UPDATE,
      StatementType::DELETE,  StatementType::CREATE,
      StatementType::DROP,    StatementType::PREPARE,
      StatementType::EXECUTE, StatementType::RENAME,
      StatementType::ALTER,   StatementType::TRANSACTION,
      StatementType::COPY};

  // Make sure that ToString and FromString work
  for (auto val : list) {
    std::string str = peloton::StatementTypeToString(val);
    EXPECT_TRUE(str.size() > 0);

    auto newVal = peloton::StringToStatementType(str);
    EXPECT_EQ(val, newVal);

    std::ostringstream os;
    os << val;
    EXPECT_EQ(str, os.str());
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

    std::ostringstream os;
    os << val;
    EXPECT_EQ(str, os.str());
  }

  // Then make sure that we can't cast garbage
  std::string invalid("LinLovesEverybody");
  EXPECT_THROW(peloton::StringToExpressionType(invalid), peloton::Exception);
  EXPECT_THROW(
      peloton::ExpressionTypeToString(static_cast<ExpressionType>(-99999)),
      peloton::Exception);
}

TEST_F(TypesTests, IndexTypeTest) {
  std::vector<IndexType> list = {IndexType::INVALID, IndexType::BWTREE,
                                 IndexType::HASH, IndexType::SKIPLIST};

  // Make sure that ToString and FromString work
  for (auto val : list) {
    std::string str = peloton::IndexTypeToString(val);
    EXPECT_TRUE(str.size() > 0);

    auto newVal = peloton::StringToIndexType(str);
    EXPECT_EQ(val, newVal);

    std::ostringstream os;
    os << val;
    EXPECT_EQ(str, os.str());
  }

  // Then make sure that we can't cast garbage
  std::string invalid("DanaSlaysMofos");
  EXPECT_THROW(peloton::StringToIndexType(invalid), peloton::Exception);
  EXPECT_THROW(peloton::IndexTypeToString(static_cast<IndexType>(-99999)),
               peloton::Exception);
}

TEST_F(TypesTests, IndexConstraintTypeTest) {
  std::vector<IndexConstraintType> list = {
      IndexConstraintType::INVALID, IndexConstraintType::DEFAULT,
      IndexConstraintType::PRIMARY_KEY, IndexConstraintType::UNIQUE};

  // Make sure that ToString and FromString work
  for (auto val : list) {
    std::string str = peloton::IndexConstraintTypeToString(val);
    EXPECT_TRUE(str.size() > 0);

    auto newVal = peloton::StringToIndexConstraintType(str);
    EXPECT_EQ(val, newVal);

    std::ostringstream os;
    os << val;
    EXPECT_EQ(str, os.str());
  }

  // Then make sure that we can't cast garbage
  std::string invalid("XXXXX");
  EXPECT_THROW(peloton::StringToIndexConstraintType(invalid),
               peloton::Exception);
  EXPECT_THROW(peloton::IndexConstraintTypeToString(
                   static_cast<IndexConstraintType>(-99999)),
               peloton::Exception);
}

TEST_F(TypesTests, HybridScanTypeTest) {
  std::vector<HybridScanType> list = {
      HybridScanType::INVALID, HybridScanType::SEQUENTIAL,
      HybridScanType::INDEX, HybridScanType::HYBRID};

  // Make sure that ToString and FromString work
  for (auto val : list) {
    std::string str = peloton::HybridScanTypeToString(val);
    EXPECT_TRUE(str.size() > 0);

    auto newVal = peloton::StringToHybridScanType(str);
    EXPECT_EQ(val, newVal);

    std::ostringstream os;
    os << val;
    EXPECT_EQ(str, os.str());
  }

  // Then make sure that we can't cast garbage
  std::string invalid("XXXXX");
  EXPECT_THROW(peloton::StringToHybridScanType(invalid), peloton::Exception);
  EXPECT_THROW(
      peloton::HybridScanTypeToString(static_cast<HybridScanType>(-99999)),
      peloton::Exception);
}

TEST_F(TypesTests, JoinTypeTest) {
  std::vector<JoinType> list = {JoinType::INVALID, JoinType::LEFT,
                                JoinType::RIGHT,   JoinType::INNER,
                                JoinType::OUTER,   JoinType::SEMI};

  // Make sure that ToString and FromString work
  for (auto val : list) {
    std::string str = peloton::JoinTypeToString(val);
    EXPECT_TRUE(str.size() > 0);

    auto newVal = peloton::StringToJoinType(str);
    EXPECT_EQ(val, newVal);

    std::ostringstream os;
    os << val;
    EXPECT_EQ(str, os.str());
  }

  // Then make sure that we can't cast garbage
  std::string invalid("XXXXX");
  EXPECT_THROW(peloton::StringToJoinType(invalid), peloton::Exception);
  EXPECT_THROW(peloton::JoinTypeToString(static_cast<JoinType>(-99999)),
               peloton::Exception);
}

TEST_F(TypesTests, PlanNodeTypeTest) {
  std::vector<PlanNodeType> list = {
      PlanNodeType::INVALID,     PlanNodeType::ABSTRACT_SCAN,
      PlanNodeType::SEQSCAN,     PlanNodeType::INDEXSCAN,
      PlanNodeType::NESTLOOP,    PlanNodeType::NESTLOOPINDEX,
      PlanNodeType::MERGEJOIN,   PlanNodeType::HASHJOIN,
      PlanNodeType::UPDATE,      PlanNodeType::INSERT,
      PlanNodeType::DELETE,      PlanNodeType::DROP,
      PlanNodeType::CREATE,      PlanNodeType::SEND,
      PlanNodeType::RECEIVE,     PlanNodeType::PRINT,
      PlanNodeType::AGGREGATE,   PlanNodeType::UNION,
      PlanNodeType::ORDERBY,     PlanNodeType::PROJECTION,
      PlanNodeType::MATERIALIZE, PlanNodeType::LIMIT,
      PlanNodeType::DISTINCT,    PlanNodeType::SETOP,
      PlanNodeType::APPEND,      PlanNodeType::AGGREGATE_V2,
      PlanNodeType::HASH,        PlanNodeType::RESULT,
      PlanNodeType::COPY,        PlanNodeType::MOCK};

  // Make sure that ToString and FromString work
  for (auto val : list) {
    std::string str = peloton::PlanNodeTypeToString(val);
    EXPECT_TRUE(str.size() > 0);

    auto newVal = peloton::StringToPlanNodeType(str);
    EXPECT_EQ(val, newVal);

    std::ostringstream os;
    os << val;
    EXPECT_EQ(str, os.str());
  }

  // Then make sure that we can't cast garbage
  std::string invalid("AndySmellsBad");
  EXPECT_THROW(peloton::StringToPlanNodeType(invalid), peloton::Exception);
  EXPECT_THROW(peloton::PlanNodeTypeToString(static_cast<PlanNodeType>(-99999)),
               peloton::Exception);
}

TEST_F(TypesTests, ParseNodeTypeTest) {
  std::vector<ParseNodeType> list = {
      ParseNodeType::INVALID, ParseNodeType::SCAN,      ParseNodeType::CREATE,
      ParseNodeType::DROP,    ParseNodeType::UPDATE,    ParseNodeType::INSERT,
      ParseNodeType::DELETE,  ParseNodeType::PREPARE,   ParseNodeType::EXECUTE,
      ParseNodeType::SELECT,  ParseNodeType::JOIN_EXPR, ParseNodeType::TABLE,
      ParseNodeType::MOCK};

  // Make sure that ToString and FromString work
  for (auto val : list) {
    std::string str = peloton::ParseNodeTypeToString(val);
    EXPECT_TRUE(str.size() > 0);

    auto newVal = peloton::StringToParseNodeType(str);
    EXPECT_EQ(val, newVal);

    std::ostringstream os;
    os << val;
    EXPECT_EQ(str, os.str());
  }

  // Then make sure that we can't cast garbage
  std::string invalid("TerrierHasFleas");
  EXPECT_THROW(peloton::StringToParseNodeType(invalid), peloton::Exception);
  EXPECT_THROW(
      peloton::ParseNodeTypeToString(static_cast<ParseNodeType>(-99999)),
      peloton::Exception);
}

TEST_F(TypesTests, ResultTypeTest) {
  std::vector<ResultType> list = {ResultType::INVALID, ResultType::SUCCESS,
                                  ResultType::FAILURE, ResultType::ABORTED,
                                  ResultType::NOOP,    ResultType::UNKNOWN};

  // Make sure that ToString and FromString work
  for (auto val : list) {
    std::string str = peloton::ResultTypeToString(val);
    EXPECT_TRUE(str.size() > 0);

    auto newVal = peloton::StringToResultType(str);
    EXPECT_EQ(val, newVal);

    std::ostringstream os;
    os << val;
    EXPECT_EQ(str, os.str());
  }

  // Then make sure that we can't cast garbage
  std::string invalid("Blah blah blah!!!");
  EXPECT_THROW(peloton::StringToResultType(invalid), peloton::Exception);
  EXPECT_THROW(peloton::ResultTypeToString(static_cast<ResultType>(-99999)),
               peloton::Exception);
}

TEST_F(TypesTests, ConstraintTypeTest) {
  std::vector<ConstraintType> list = {
      ConstraintType::INVALID,  ConstraintType::NOT_NULL,
      ConstraintType::NOTNULL,  ConstraintType::DEFAULT,
      ConstraintType::CHECK,    ConstraintType::PRIMARY,
      ConstraintType::UNIQUE,   ConstraintType::FOREIGN,
      ConstraintType::EXCLUSION};

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

// TEST_F(TypesTests, LoggingTypeTest) {
//   std::vector<LoggingType> list = {
//       LoggingType::INVALID, LoggingType::NVM_WBL, LoggingType::SSD_WBL,
//       LoggingType::HDD_WBL, LoggingType::NVM_WAL, LoggingType::SSD_WAL,
//       LoggingType::HDD_WAL,
//   };

//   // Make sure that ToString and FromString work
//   for (auto val : list) {
//     std::string str = peloton::LoggingTypeToString(val);
//     EXPECT_TRUE(str.size() > 0);

//     auto newVal = peloton::StringToLoggingType(str);
//     EXPECT_EQ(val, newVal);
//   }

//   // Then make sure that we can't cast garbage
//   std::string invalid("WU TANG");
//   EXPECT_THROW(peloton::StringToLoggingType(invalid), peloton::Exception);
//   EXPECT_THROW(peloton::LoggingTypeToString(static_cast<LoggingType>(-99999)),
//                peloton::Exception);
// }

// TEST_F(TypesTests, LoggingStatusTypeTest) {
//   std::vector<LoggingStatusType> list = {
//       LoggingStatusType::INVALID,   LoggingStatusType::STANDBY,
//       LoggingStatusType::RECOVERY,  LoggingStatusType::LOGGING,
//       LoggingStatusType::TERMINATE, LoggingStatusType::SLEEP};

//   // Make sure that ToString and FromString work
//   for (auto val : list) {
//     std::string str = peloton::LoggingStatusTypeToString(val);
//     EXPECT_TRUE(str.size() > 0);

//     auto newVal = peloton::StringToLoggingStatusType(str);
//     EXPECT_EQ(val, newVal);
//   }

//   // Then make sure that we can't cast garbage
//   std::string invalid("WU TANG");
//   EXPECT_THROW(peloton::StringToLoggingStatusType(invalid), peloton::Exception);
//   EXPECT_THROW(peloton::LoggingStatusTypeToString(
//                    static_cast<LoggingStatusType>(-99999)),
//                peloton::Exception);
// }

// TEST_F(TypesTests, LoggerTypeTest) {
//   std::vector<LoggerType> list = {LoggerType::INVALID, LoggerType::FRONTEND,
//                                   LoggerType::BACKEND};

//   // Make sure that ToString and FromString work
//   for (auto val : list) {
//     std::string str = peloton::LoggerTypeToString(val);
//     EXPECT_TRUE(str.size() > 0);

//     auto newVal = peloton::StringToLoggerType(str);
//     EXPECT_EQ(val, newVal);
//   }

//   // Then make sure that we can't cast garbage
//   std::string invalid("WU TANG");
//   EXPECT_THROW(peloton::StringToLoggerType(invalid), peloton::Exception);
//   EXPECT_THROW(peloton::LoggerTypeToString(static_cast<LoggerType>(-99999)),
//                peloton::Exception);
// }

// TEST_F(TypesTests, LogRecordTypeTest) {
//   std::vector<LogRecordType> list = {
//       LOGRECORD_TYPE_INVALID,
//       LOGRECORD_TYPE_TRANSACTION_BEGIN,
//       LOGRECORD_TYPE_TRANSACTION_COMMIT,
//       LOGRECORD_TYPE_TRANSACTION_END,
//       LOGRECORD_TYPE_TRANSACTION_ABORT,
//       LOGRECORD_TYPE_TRANSACTION_DONE,
//       LOGRECORD_TYPE_TUPLE_INSERT,
//       LOGRECORD_TYPE_TUPLE_DELETE,
//       LOGRECORD_TYPE_TUPLE_UPDATE,
//       LOGRECORD_TYPE_WAL_TUPLE_INSERT,
//       LOGRECORD_TYPE_WAL_TUPLE_DELETE,
//       LOGRECORD_TYPE_WAL_TUPLE_UPDATE,
//       LOGRECORD_TYPE_WBL_TUPLE_INSERT,
//       LOGRECORD_TYPE_WBL_TUPLE_DELETE,
//       LOGRECORD_TYPE_WBL_TUPLE_UPDATE,
//       LOGRECORD_TYPE_ITERATION_DELIMITER,
//   };

//   // Make sure that ToString and FromString work
//   for (auto val : list) {
//     std::string str = peloton::LogRecordTypeToString(val);
//     EXPECT_TRUE(str.size() > 0);

//     auto newVal = peloton::StringToLogRecordType(str);
//     EXPECT_EQ(val, newVal);
//   }

//   // Then make sure that we can't cast garbage
//   std::string invalid("WU TANG");
//   EXPECT_THROW(peloton::StringToLogRecordType(invalid), peloton::Exception);
//   EXPECT_THROW(
//       peloton::LogRecordTypeToString(static_cast<LogRecordType>(-99999)),
//       peloton::Exception);
// }

}  // End test namespace
}  // End peloton namespace
