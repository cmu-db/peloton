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
#include "common/internal_types.h"
#include "type/value_factory.h"

namespace peloton {
namespace test {

//===--------------------------------------------------------------------===//
// Internal Types Test
//===--------------------------------------------------------------------===//

class InternalTypesTests : public PelotonTest {};

TEST_F(InternalTypesTests, DatePartTypeTest) {
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

TEST_F(InternalTypesTests, BackendTypeTest) {
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

TEST_F(InternalTypesTests, TypeIdTest) {
  std::vector<type::TypeId> list = {
      type::TypeId::INVALID, type::TypeId::PARAMETER_OFFSET,
      type::TypeId::BOOLEAN, type::TypeId::TINYINT, type::TypeId::SMALLINT,
      type::TypeId::INTEGER, type::TypeId::BIGINT, type::TypeId::DECIMAL,
      type::TypeId::TIMESTAMP, type::TypeId::DATE, type::TypeId::VARCHAR,
      type::TypeId::VARBINARY, type::TypeId::ARRAY, type::TypeId::UDT};

  // Make sure that ToString and FromString work
  for (auto val : list) {
    std::string str = peloton::TypeIdToString(val);
    EXPECT_TRUE(str.size() > 0);

    auto newVal = peloton::StringToTypeId(str);
    EXPECT_EQ(val, newVal);

    // TODO: Need to change TypeId to enum class
    // std::ostringstream os;
    // os << val;
    // EXPECT_EQ(str, os.str());
  }

  // Then make sure that we can't cast garbage
  std::string invalid("JoyIsDangerous");
  EXPECT_THROW(peloton::StringToTypeId(invalid), peloton::Exception);
  EXPECT_THROW(peloton::TypeIdToString(static_cast<type::TypeId>(-99999)),
               peloton::Exception);
}

TEST_F(InternalTypesTests, StatementTypeTest) {
  std::vector<StatementType> list = {
      StatementType::INVALID, StatementType::SELECT,
      StatementType::INSERT,  StatementType::UPDATE,
      StatementType::DELETE,  StatementType::CREATE,
      StatementType::DROP,    StatementType::PREPARE,
      StatementType::EXECUTE, StatementType::RENAME,
      StatementType::ALTER,   StatementType::TRANSACTION,
      StatementType::COPY,    StatementType::ANALYZE};

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

TEST_F(InternalTypesTests, ExpressionTypeTest) {
  std::vector<ExpressionType> list = {
      ExpressionType::INVALID, ExpressionType::OPERATOR_PLUS,
      ExpressionType::OPERATOR_MINUS, ExpressionType::OPERATOR_MULTIPLY,
      ExpressionType::OPERATOR_DIVIDE, ExpressionType::OPERATOR_CONCAT,
      ExpressionType::OPERATOR_MOD, ExpressionType::OPERATOR_CAST,
      ExpressionType::OPERATOR_NOT, ExpressionType::OPERATOR_IS_NULL,
      ExpressionType::OPERATOR_EXISTS, ExpressionType::OPERATOR_UNARY_MINUS,
      ExpressionType::COMPARE_EQUAL, ExpressionType::COMPARE_NOTEQUAL,
      ExpressionType::COMPARE_LESSTHAN, ExpressionType::COMPARE_GREATERTHAN,
      ExpressionType::COMPARE_LESSTHANOREQUALTO,
      ExpressionType::COMPARE_GREATERTHANOREQUALTO,
      ExpressionType::COMPARE_LIKE, ExpressionType::COMPARE_NOTLIKE,
      ExpressionType::COMPARE_IN, ExpressionType::COMPARE_DISTINCT_FROM,
      ExpressionType::CONJUNCTION_AND, ExpressionType::CONJUNCTION_OR,
      ExpressionType::VALUE_CONSTANT, ExpressionType::VALUE_PARAMETER,
      ExpressionType::VALUE_TUPLE, ExpressionType::VALUE_TUPLE_ADDRESS,
      ExpressionType::VALUE_NULL, ExpressionType::VALUE_VECTOR,
      ExpressionType::VALUE_SCALAR, ExpressionType::AGGREGATE_COUNT,
      ExpressionType::AGGREGATE_COUNT_STAR, ExpressionType::AGGREGATE_SUM,
      ExpressionType::AGGREGATE_MIN, ExpressionType::AGGREGATE_MAX,
      ExpressionType::AGGREGATE_AVG, ExpressionType::FUNCTION,
      ExpressionType::HASH_RANGE, ExpressionType::OPERATOR_CASE_EXPR,
      ExpressionType::OPERATOR_NULLIF, ExpressionType::OPERATOR_COALESCE,
      ExpressionType::ROW_SUBQUERY, ExpressionType::SELECT_SUBQUERY,
      ExpressionType::STAR, ExpressionType::PLACEHOLDER,
      ExpressionType::COLUMN_REF, ExpressionType::FUNCTION_REF,
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

TEST_F(InternalTypesTests, IndexTypeTest) {
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

TEST_F(InternalTypesTests, IndexConstraintTypeTest) {
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

TEST_F(InternalTypesTests, HybridScanTypeTest) {
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

TEST_F(InternalTypesTests, JoinTypeTest) {
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

TEST_F(InternalTypesTests, PlanNodeTypeTest) {
  std::vector<PlanNodeType> list = {
      PlanNodeType::INVALID, PlanNodeType::SEQSCAN, PlanNodeType::INDEXSCAN,
      PlanNodeType::NESTLOOP, PlanNodeType::NESTLOOPINDEX,
      PlanNodeType::MERGEJOIN, PlanNodeType::HASHJOIN, PlanNodeType::UPDATE,
      PlanNodeType::INSERT, PlanNodeType::DELETE, PlanNodeType::DROP,
      PlanNodeType::CREATE, PlanNodeType::SEND, PlanNodeType::RECEIVE,
      PlanNodeType::PRINT, PlanNodeType::AGGREGATE, PlanNodeType::UNION,
      PlanNodeType::ORDERBY, PlanNodeType::PROJECTION,
      PlanNodeType::MATERIALIZE, PlanNodeType::LIMIT, PlanNodeType::DISTINCT,
      PlanNodeType::SETOP, PlanNodeType::APPEND, PlanNodeType::AGGREGATE_V2,
      PlanNodeType::HASH, PlanNodeType::RESULT, PlanNodeType::COPY,
      PlanNodeType::MOCK};

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

TEST_F(InternalTypesTests, ParseNodeTypeTest) {
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

TEST_F(InternalTypesTests, ResultTypeTest) {
  std::vector<ResultType> list = {ResultType::INVALID, ResultType::SUCCESS,
                                  ResultType::FAILURE, ResultType::ABORTED,
                                  ResultType::NOOP,    ResultType::UNKNOWN,
                                  ResultType::QUEUING};

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

TEST_F(InternalTypesTests, ConstraintTypeTest) {
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

    std::ostringstream os;
    os << val;
    EXPECT_EQ(str, os.str());
  }

  // Then make sure that we can't cast garbage
  std::string invalid("ZiqiGottTheFlu");
  EXPECT_THROW(peloton::StringToConstraintType(invalid), peloton::Exception);
  EXPECT_THROW(
      peloton::ConstraintTypeToString(static_cast<ConstraintType>(-99999)),
      peloton::Exception);
}

TEST_F(InternalTypesTests, LoggingTypeTest) {
  std::vector<LoggingType> list = {
      LoggingType::INVALID, LoggingType::OFF, LoggingType::ON
  };

  // Make sure that ToString and FromString work
  for (auto val : list) {
    std::string str = peloton::LoggingTypeToString(val);
    EXPECT_TRUE(str.size() > 0);

    auto newVal = peloton::StringToLoggingType(str);
    EXPECT_EQ(val, newVal);

    std::ostringstream os;
    os << val;
    EXPECT_EQ(str, os.str());
  }

  // Then make sure that we can't cast garbage
  std::string invalid("WU TANG");
  EXPECT_THROW(peloton::StringToLoggingType(invalid), peloton::Exception);
  EXPECT_THROW(peloton::LoggingTypeToString(static_cast<LoggingType>(-99999)),
               peloton::Exception);
}

TEST_F(InternalTypesTests, CheckpointingTypeTest) {
  std::vector<CheckpointingType> list = {
      CheckpointingType::INVALID, CheckpointingType::OFF, CheckpointingType::ON
  };

  // Make sure that ToString and FromString work
  for (auto val : list) {
    std::string str = peloton::CheckpointingTypeToString(val);
    EXPECT_TRUE(str.size() > 0);

    auto newVal = peloton::StringToCheckpointingType(str);
    EXPECT_EQ(val, newVal);

    std::ostringstream os;
    os << val;
    EXPECT_EQ(str, os.str());
  }

  // Then make sure that we can't cast garbage
  std::string invalid("WU TANG");
  EXPECT_THROW(peloton::StringToCheckpointingType(invalid), peloton::Exception);
  EXPECT_THROW(peloton::CheckpointingTypeToString(
                   static_cast<CheckpointingType>(-99999)),
               peloton::Exception);
}

TEST_F(InternalTypesTests, GarbageCollectionTypeTest) {
  std::vector<GarbageCollectionType> list = {
      GarbageCollectionType::INVALID, GarbageCollectionType::OFF, GarbageCollectionType::ON
  };

  // Make sure that ToString and FromString work
  for (auto val : list) {
    std::string str = peloton::GarbageCollectionTypeToString(val);
    EXPECT_TRUE(str.size() > 0);

    auto newVal = peloton::StringToGarbageCollectionType(str);
    EXPECT_EQ(val, newVal);

    std::ostringstream os;
    os << val;
    EXPECT_EQ(str, os.str());
  }

  // Then make sure that we can't cast garbage
  std::string invalid("WU TANG");
  EXPECT_THROW(peloton::StringToGarbageCollectionType(invalid),
               peloton::Exception);
  EXPECT_THROW(peloton::GarbageCollectionTypeToString(
                   static_cast<GarbageCollectionType>(-99999)),
               peloton::Exception);
}

TEST_F(InternalTypesTests, ProtocolTypeTest) {
  std::vector<ProtocolType> list = {
      ProtocolType::INVALID, 
      ProtocolType::TIMESTAMP_ORDERING
  };

  // Make sure that ToString and FromString work
  for (auto val : list) {
    std::string str = peloton::ProtocolTypeToString(val);
    EXPECT_TRUE(str.size() > 0);

    auto newVal = peloton::StringToProtocolType(str);
    EXPECT_EQ(val, newVal);

    std::ostringstream os;
    os << val;
    EXPECT_EQ(str, os.str());
  }

  // Then make sure that we can't cast garbage
  std::string invalid("WU TANG");
  EXPECT_THROW(peloton::StringToProtocolType(invalid), peloton::Exception);
  EXPECT_THROW(peloton::ProtocolTypeToString(static_cast<ProtocolType>(-99999)),
               peloton::Exception);
}

TEST_F(InternalTypesTests, EpochTypeTest) {
  std::vector<EpochType> list = {
      EpochType::INVALID, 
      EpochType::DECENTRALIZED_EPOCH
  };

  // Make sure that ToString and FromString work
  for (auto val : list) {
    std::string str = peloton::EpochTypeToString(val);
    EXPECT_TRUE(str.size() > 0);

    auto newVal = peloton::StringToEpochType(str);
    EXPECT_EQ(val, newVal);

    std::ostringstream os;
    os << val;
    EXPECT_EQ(str, os.str());
  }

  // Then make sure that we can't cast garbage
  std::string invalid("WU TANG");
  EXPECT_THROW(peloton::StringToEpochType(invalid), peloton::Exception);
  EXPECT_THROW(peloton::EpochTypeToString(static_cast<EpochType>(-99999)),
               peloton::Exception);
}

TEST_F(InternalTypesTests, TimestampTypeTest) {
  std::vector<TimestampType> list = {
      TimestampType::INVALID, TimestampType::SNAPSHOT_READ, TimestampType::READ,
      TimestampType::COMMIT};

  // Make sure that ToString and FromString work
  for (auto val : list) {
    std::string str = peloton::TimestampTypeToString(val);
    EXPECT_TRUE(str.size() > 0);

    auto newVal = peloton::StringToTimestampType(str);
    EXPECT_EQ(val, newVal);

    std::ostringstream os;
    os << val;
    EXPECT_EQ(str, os.str());
  }

  // Then make sure that we can't cast garbage
  std::string invalid("WU TANG");
  EXPECT_THROW(peloton::StringToTimestampType(invalid), peloton::Exception);
  EXPECT_THROW(
      peloton::TimestampTypeToString(static_cast<TimestampType>(-99999)),
      peloton::Exception);
}

TEST_F(InternalTypesTests, VisibilityTypeTest) {
  std::vector<VisibilityType> list = {
      VisibilityType::INVALID, VisibilityType::INVISIBLE,
      VisibilityType::DELETED, VisibilityType::OK};

  // Make sure that ToString and FromString work
  for (auto val : list) {
    std::string str = peloton::VisibilityTypeToString(val);
    EXPECT_TRUE(str.size() > 0);

    auto newVal = peloton::StringToVisibilityType(str);
    EXPECT_EQ(val, newVal);

    std::ostringstream os;
    os << val;
    EXPECT_EQ(str, os.str());
  }

  // Then make sure that we can't cast garbage
  std::string invalid("WU TANG");
  EXPECT_THROW(peloton::StringToVisibilityType(invalid), peloton::Exception);
  EXPECT_THROW(
      peloton::VisibilityTypeToString(static_cast<VisibilityType>(-99999)),
      peloton::Exception);
}

TEST_F(InternalTypesTests, VisibilityIdTypeTest) {
  std::vector<VisibilityIdType> list = {
      VisibilityIdType::INVALID, 
      VisibilityIdType::READ_ID, 
      VisibilityIdType::COMMIT_ID
  };

  // Make sure that ToString and FromString work
  for (auto val : list) {
    std::string str = peloton::VisibilityIdTypeToString(val);
    EXPECT_TRUE(str.size() > 0);

    auto newVal = peloton::StringToVisibilityIdType(str);
    EXPECT_EQ(val, newVal);

    std::ostringstream os;
    os << val;
    EXPECT_EQ(str, os.str());
  }

  // Then make sure that we can't cast garbage
  std::string invalid("WU TANG");
  EXPECT_THROW(peloton::StringToVisibilityIdType(invalid), peloton::Exception);
  EXPECT_THROW(
      peloton::VisibilityIdTypeToString(static_cast<VisibilityIdType>(-99999)),
      peloton::Exception);
}

TEST_F(InternalTypesTests, IsolationLevelTypeTest) {
  std::vector<IsolationLevelType> list = {
      IsolationLevelType::INVALID,        IsolationLevelType::SERIALIZABLE,
      IsolationLevelType::SNAPSHOT,       IsolationLevelType::REPEATABLE_READS,
      IsolationLevelType::READ_COMMITTED, IsolationLevelType::READ_ONLY};

  // Make sure that ToString and FromString work
  for (auto val : list) {
    std::string str = peloton::IsolationLevelTypeToString(val);
    EXPECT_TRUE(str.size() > 0);

    auto newVal = peloton::StringToIsolationLevelType(str);
    EXPECT_EQ(val, newVal);

    std::ostringstream os;
    os << val;
    EXPECT_EQ(str, os.str());
  }

  // Then make sure that we can't cast garbage
  std::string invalid("WU TANG");
  EXPECT_THROW(peloton::StringToIsolationLevelType(invalid),
               peloton::Exception);
  EXPECT_THROW(peloton::IsolationLevelTypeToString(
                   static_cast<IsolationLevelType>(-99999)),
               peloton::Exception);
}

TEST_F(InternalTypesTests, ConflictAvoidanceTypeTest) {
  std::vector<ConflictAvoidanceType> list = {
      ConflictAvoidanceType::INVALID, ConflictAvoidanceType::WAIT,
      ConflictAvoidanceType::ABORT,
  };

  // Make sure that ToString and FromString work
  for (auto val : list) {
    std::string str = peloton::ConflictAvoidanceTypeToString(val);
    EXPECT_TRUE(str.size() > 0);

    auto newVal = peloton::StringToConflictAvoidanceType(str);
    EXPECT_EQ(val, newVal);

    std::ostringstream os;
    os << val;
    EXPECT_EQ(str, os.str());
  }

  // Then make sure that we can't cast garbage
  std::string invalid("WU TANG");
  EXPECT_THROW(peloton::StringToConflictAvoidanceType(invalid),
               peloton::Exception);
  EXPECT_THROW(peloton::ConflictAvoidanceTypeToString(
                   static_cast<ConflictAvoidanceType>(-99999)),
               peloton::Exception);
}

TEST_F(InternalTypesTests, RWTypeTest) {
  std::vector<RWType> list = {
      RWType::INVALID, RWType::READ,   RWType::READ_OWN, RWType::UPDATE,
      RWType::INSERT,  RWType::DELETE, RWType::INS_DEL,
  };

  // Make sure that ToString and FromString work
  for (auto val : list) {
    std::string str = peloton::RWTypeToString(val);
    EXPECT_TRUE(str.size() > 0);

    auto newVal = peloton::StringToRWType(str);
    EXPECT_EQ(val, newVal);

    std::ostringstream os;
    os << val;
    EXPECT_EQ(str, os.str());
  }

  // Then make sure that we can't cast garbage
  std::string invalid("WU TANG");
  EXPECT_THROW(peloton::StringToRWType(invalid), peloton::Exception);
  EXPECT_THROW(peloton::RWTypeToString(static_cast<RWType>(-99999)),
               peloton::Exception);
}

TEST_F(InternalTypesTests, CreateTypeTest) {
  std::vector<CreateType> list = {
      CreateType::INVALID, CreateType::DB,         CreateType::TABLE,
      CreateType::INDEX,   CreateType::CONSTRAINT, CreateType::TRIGGER,
  };

  // Make sure that ToString and FromString work
  for (auto val : list) {
    std::string str = peloton::CreateTypeToString(val);
    EXPECT_TRUE(str.size() > 0);

    auto newVal = peloton::StringToCreateType(str);
    EXPECT_EQ(val, newVal);

    std::ostringstream os;
    os << val;
    EXPECT_EQ(str, os.str());
  }

  // Then make sure that we can't cast garbage
  std::string invalid("WU TANG");
  EXPECT_THROW(peloton::StringToCreateType(invalid), peloton::Exception);
  EXPECT_THROW(peloton::CreateTypeToString(static_cast<CreateType>(-99999)),
               peloton::Exception);
}

TEST_F(InternalTypesTests, DropTypeTest) {
  std::vector<DropType> list = {
      DropType::INVALID, DropType::DB,         DropType::TABLE,
      DropType::INDEX,   DropType::CONSTRAINT, DropType::TRIGGER,
  };

  // Make sure that ToString and FromString work
  for (auto val : list) {
    std::string str = peloton::DropTypeToString(val);
    EXPECT_TRUE(str.size() > 0);

    auto newVal = peloton::StringToDropType(str);
    EXPECT_EQ(val, newVal);

    std::ostringstream os;
    os << val;
    EXPECT_EQ(str, os.str());
  }

  // Then make sure that we can't cast garbage
  std::string invalid("WU TANG");
  EXPECT_THROW(peloton::StringToDropType(invalid), peloton::Exception);
  EXPECT_THROW(peloton::DropTypeToString(static_cast<DropType>(-99999)),
               peloton::Exception);
}

TEST_F(InternalTypesTests, AggregateTypeTest) {
  std::vector<AggregateType> list = {
      AggregateType::INVALID, AggregateType::SORTED, AggregateType::HASH,
      AggregateType::PLAIN,
  };

  // Make sure that ToString and FromString work
  for (auto val : list) {
    std::string str = peloton::AggregateTypeToString(val);
    EXPECT_TRUE(str.size() > 0);

    auto newVal = peloton::StringToAggregateType(str);
    EXPECT_EQ(val, newVal);

    std::ostringstream os;
    os << val;
    EXPECT_EQ(str, os.str());
  }

  // Then make sure that we can't cast garbage
  std::string invalid("WU TANG");
  EXPECT_THROW(peloton::StringToAggregateType(invalid), peloton::Exception);
  EXPECT_THROW(
      peloton::AggregateTypeToString(static_cast<AggregateType>(-99999)),
      peloton::Exception);
}

TEST_F(InternalTypesTests, QuantifierTypeTest) {
  std::vector<QuantifierType> list = {
      QuantifierType::NONE, QuantifierType::ANY, QuantifierType::ALL,
  };

  // Make sure that ToString and FromString work
  for (auto val : list) {
    std::string str = peloton::QuantifierTypeToString(val);
    EXPECT_TRUE(str.size() > 0);

    auto newVal = peloton::StringToQuantifierType(str);
    EXPECT_EQ(val, newVal);

    std::ostringstream os;
    os << val;
    EXPECT_EQ(str, os.str());
  }

  // Then make sure that we can't cast garbage
  std::string invalid("WU TANG");
  EXPECT_THROW(peloton::StringToQuantifierType(invalid), peloton::Exception);
  EXPECT_THROW(
      peloton::QuantifierTypeToString(static_cast<QuantifierType>(-99999)),
      peloton::Exception);
}

TEST_F(InternalTypesTests, TableReferenceTypeTest) {
  std::vector<TableReferenceType> list = {
      TableReferenceType::INVALID,       TableReferenceType::NAME,
      TableReferenceType::SELECT,        TableReferenceType::JOIN,
      TableReferenceType::CROSS_PRODUCT,
  };

  // Make sure that ToString and FromString work
  for (auto val : list) {
    std::string str = peloton::TableReferenceTypeToString(val);
    EXPECT_TRUE(str.size() > 0);

    auto newVal = peloton::StringToTableReferenceType(str);
    EXPECT_EQ(val, newVal);

    std::ostringstream os;
    os << val;
    EXPECT_EQ(str, os.str());
  }

  // Then make sure that we can't cast garbage
  std::string invalid("WU TANG");
  EXPECT_THROW(peloton::StringToTableReferenceType(invalid),
               peloton::Exception);
  EXPECT_THROW(peloton::TableReferenceTypeToString(
                   static_cast<TableReferenceType>(-99999)),
               peloton::Exception);
}

TEST_F(InternalTypesTests, InsertTypeTest) {
  std::vector<InsertType> list = {
      InsertType::INVALID, InsertType::VALUES, InsertType::SELECT,
  };

  // Make sure that ToString and FromString work
  for (auto val : list) {
    std::string str = peloton::InsertTypeToString(val);
    EXPECT_TRUE(str.size() > 0);

    auto newVal = peloton::StringToInsertType(str);
    EXPECT_EQ(val, newVal);

    std::ostringstream os;
    os << val;
    EXPECT_EQ(str, os.str());
  }

  // Then make sure that we can't cast garbage
  std::string invalid("WU TANG");
  EXPECT_THROW(peloton::StringToInsertType(invalid), peloton::Exception);
  EXPECT_THROW(peloton::InsertTypeToString(static_cast<InsertType>(-99999)),
               peloton::Exception);
}

TEST_F(InternalTypesTests, CopyTypeTest) {
  std::vector<CopyType> list = {
      CopyType::INVALID,    CopyType::IMPORT_CSV,    CopyType::IMPORT_TSV,
      CopyType::EXPORT_CSV, CopyType::EXPORT_STDOUT, CopyType::EXPORT_OTHER,
  };

  // Make sure that ToString and FromString work
  for (auto val : list) {
    std::string str = peloton::CopyTypeToString(val);
    EXPECT_TRUE(str.size() > 0);

    auto newVal = peloton::StringToCopyType(str);
    EXPECT_EQ(val, newVal);

    std::ostringstream os;
    os << val;
    EXPECT_EQ(str, os.str());
  }

  // Then make sure that we can't cast garbage
  std::string invalid("WU TANG");
  EXPECT_THROW(peloton::StringToCopyType(invalid), peloton::Exception);
  EXPECT_THROW(peloton::CopyTypeToString(static_cast<CopyType>(-99999)),
               peloton::Exception);
}

TEST_F(InternalTypesTests, PayloadTypeTest) {
  std::vector<PayloadType> list = {
      PayloadType::INVALID, PayloadType::CLIENT_REQUEST,
      PayloadType::CLIENT_RESPONSE, PayloadType::STOP,
  };

  // Make sure that ToString and FromString work
  for (auto val : list) {
    std::string str = peloton::PayloadTypeToString(val);
    EXPECT_TRUE(str.size() > 0);

    auto newVal = peloton::StringToPayloadType(str);
    EXPECT_EQ(val, newVal);

    std::ostringstream os;
    os << val;
    EXPECT_EQ(str, os.str());
  }

  // Then make sure that we can't cast garbage
  std::string invalid("Squirrels All Around");
  EXPECT_THROW(peloton::StringToPayloadType(invalid), peloton::Exception);
  EXPECT_THROW(peloton::PayloadTypeToString(static_cast<PayloadType>(-99999)),
               peloton::Exception);
}

TEST_F(InternalTypesTests, TaskPriorityTypeTest) {
  std::vector<TaskPriorityType> list = {
      TaskPriorityType::INVALID, TaskPriorityType::LOW,
      TaskPriorityType::NORMAL, TaskPriorityType::HIGH,
  };

  // Make sure that ToString and FromString work
  for (auto val : list) {
    std::string str = peloton::TaskPriorityTypeToString(val);
    EXPECT_TRUE(str.size() > 0);

    auto newVal = peloton::StringToTaskPriorityType(str);
    EXPECT_EQ(val, newVal);

    std::ostringstream os;
    os << val;
    EXPECT_EQ(str, os.str());
  }

  // Then make sure that we can't cast garbage
  std::string invalid("WU TANG");
  EXPECT_THROW(peloton::StringToTaskPriorityType(invalid), peloton::Exception);
  EXPECT_THROW(
      peloton::TaskPriorityTypeToString(static_cast<TaskPriorityType>(-99999)),
      peloton::Exception);
}

TEST_F(InternalTypesTests, SetOpTypeTest) {
  std::vector<SetOpType> list = {
      SetOpType::INVALID, SetOpType::INTERSECT,  SetOpType::INTERSECT_ALL,
      SetOpType::EXCEPT,  SetOpType::EXCEPT_ALL,
  };

  // Make sure that ToString and FromString work
  for (auto val : list) {
    std::string str = peloton::SetOpTypeToString(val);
    EXPECT_TRUE(str.size() > 0);

    auto newVal = peloton::StringToSetOpType(str);
    EXPECT_EQ(val, newVal);

    std::ostringstream os;
    os << val;
    EXPECT_EQ(str, os.str());
  }

  // Then make sure that we can't cast garbage
  std::string invalid("WU TANG");
  EXPECT_THROW(peloton::StringToSetOpType(invalid), peloton::Exception);
  EXPECT_THROW(peloton::SetOpTypeToString(static_cast<SetOpType>(-99999)),
               peloton::Exception);
}

TEST_F(InternalTypesTests, LogRecordTypeTest) {
  std::vector<LogRecordType> list = {
      LogRecordType::INVALID, LogRecordType::TRANSACTION_BEGIN,
      LogRecordType::TRANSACTION_COMMIT, LogRecordType::TUPLE_INSERT,
      LogRecordType::TUPLE_DELETE, LogRecordType::TUPLE_UPDATE,
      LogRecordType::EPOCH_BEGIN, LogRecordType::EPOCH_END,
  };

  // Make sure that ToString and FromString work
  for (auto val : list) {
    std::string str = peloton::LogRecordTypeToString(val);
    EXPECT_TRUE(str.size() > 0);

    auto newVal = peloton::StringToLogRecordType(str);
    EXPECT_EQ(val, newVal);

    std::ostringstream os;
    os << val;
    EXPECT_EQ(str, os.str());
  }

  // Then make sure that we can't cast garbage
  std::string invalid("WU TANG");
  EXPECT_THROW(peloton::StringToLogRecordType(invalid), peloton::Exception);
  EXPECT_THROW(
      peloton::LogRecordTypeToString(static_cast<LogRecordType>(-99999)),
      peloton::Exception);
}

TEST_F(InternalTypesTests, PropertyTypeTest) {
  std::vector<PropertyType> list = {
      PropertyType::INVALID, PropertyType::COLUMNS, PropertyType::DISTINCT,
      PropertyType::SORT,    PropertyType::LIMIT,
  };

  // Make sure that ToString and FromString work
  for (auto val : list) {
    std::string str = peloton::PropertyTypeToString(val);
    EXPECT_TRUE(str.size() > 0);

    auto newVal = peloton::StringToPropertyType(str);
    EXPECT_EQ(val, newVal);

    std::ostringstream os;
    os << val;
    EXPECT_EQ(str, os.str());
  }

  // Then make sure that we can't cast garbage
  std::string invalid("WU TANG");
  EXPECT_THROW(peloton::StringToPropertyType(invalid), peloton::Exception);
  EXPECT_THROW(peloton::PropertyTypeToString(static_cast<PropertyType>(-99999)),
               peloton::Exception);
}

TEST_F(InternalTypesTests, EntityTypeTest) {
  std::vector<EntityType> list = {
      EntityType::INVALID, EntityType::TABLE, EntityType::SCHEMA,
      EntityType::INDEX, EntityType::VIEW, EntityType::PREPARED_STATEMENT,
  };

  // Make sure that ToString and FromString work
  for (auto val : list) {
    std::string str = peloton::EntityTypeToString(val);
    EXPECT_TRUE(str.size() > 0);

    auto newVal = peloton::StringToEntityType(str);
    EXPECT_EQ(val, newVal);

    std::ostringstream os;
    os << val;
    EXPECT_EQ(str, os.str());
  }

  // Then make sure that we can't cast garbage
  std::string invalid("The terrier is passed out right now");
  EXPECT_THROW(peloton::StringToEntityType(invalid), peloton::Exception);
  EXPECT_THROW(peloton::EntityTypeToString(static_cast<EntityType>(-99999)),
               peloton::Exception);
}

TEST_F(InternalTypesTests, GCVersionTypeTest) {
  std::vector<GCVersionType> list = {
      GCVersionType::INVALID,       GCVersionType::COMMIT_UPDATE,
      GCVersionType::COMMIT_DELETE, GCVersionType::COMMIT_INS_DEL,
      GCVersionType::ABORT_UPDATE,  GCVersionType::ABORT_DELETE,
      GCVersionType::ABORT_INSERT,  GCVersionType::ABORT_INS_DEL,
  };

  // Make sure that ToString and FromString work
  for (auto val : list) {
    std::string str = peloton::GCVersionTypeToString(val);
    EXPECT_TRUE(str.size() > 0);

    auto newVal = peloton::StringToGCVersionType(str);
    EXPECT_EQ(val, newVal);

    std::ostringstream os;
    os << val;
    EXPECT_EQ(str, os.str());
  }

  // Then make sure that we can't cast garbage
  std::string invalid("WU TANG");
  EXPECT_THROW(peloton::StringToGCVersionType(invalid), peloton::Exception);
  EXPECT_THROW(
      peloton::GCVersionTypeToString(static_cast<GCVersionType>(-99999)),
      peloton::Exception);
}

TEST_F(InternalTypesTests, PostgresValueTypeTest) {
  // Note that we are not testing BOOLEAN here because it is an alias
  // for TINYINT. So we won't get back the correct string representation.
  std::vector<PostgresValueType> list = {
      PostgresValueType::INVALID,
      PostgresValueType::TINYINT,
      PostgresValueType::SMALLINT,
      PostgresValueType::INTEGER,
      PostgresValueType::VARBINARY,
      PostgresValueType::BIGINT,
      PostgresValueType::REAL,
      PostgresValueType::DOUBLE,
      PostgresValueType::TEXT,
      PostgresValueType::BPCHAR,
      PostgresValueType::BPCHAR2,
      PostgresValueType::VARCHAR,
      PostgresValueType::VARCHAR2,
      PostgresValueType::DATE,
      PostgresValueType::TIMESTAMPS,
      PostgresValueType::TIMESTAMPS2,
      PostgresValueType::TEXT_ARRAY,
      PostgresValueType::INT2_ARRAY,
      PostgresValueType::INT4_ARRAY,
      PostgresValueType::OID_ARRAY,
      PostgresValueType::FLOADT4_ARRAY,
      PostgresValueType::DECIMAL,
  };

  // Make sure that ToString and FromString work
  for (auto val : list) {
    std::string str = peloton::PostgresValueTypeToString(val);
    EXPECT_TRUE(str.size() > 0);

    auto newVal = peloton::StringToPostgresValueType(str);
    EXPECT_EQ(val, newVal);

    std::ostringstream os;
    os << val;
    EXPECT_EQ(str, os.str());
  }

  // Then make sure that we can't cast garbage
  std::string invalid("Never Trust The Terrier");
  EXPECT_THROW(peloton::StringToPostgresValueType(invalid), peloton::Exception);
  EXPECT_THROW(peloton::PostgresValueTypeToString(static_cast<PostgresValueType>(-99999)),
               peloton::Exception);
}

}  // End test namespace
}  // End peloton namespace
