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

#include "common/types.h"
#include "common/harness.h"
#include "common/value_factory.h"

namespace peloton {
namespace test {

//===--------------------------------------------------------------------===//
// Types Test
//===--------------------------------------------------------------------===//

class TypesTests : public PelotonTest {};

TEST_F(TypesTests, TypeIdTest) {
  std::vector<common::Type::TypeId> list = {
      common::Type::INVALID,   common::Type::PARAMETER_OFFSET,
      common::Type::BOOLEAN,   common::Type::TINYINT,
      common::Type::SMALLINT,  common::Type::INTEGER,
      common::Type::BIGINT,    common::Type::DECIMAL,
      common::Type::TIMESTAMP, common::Type::DATE,
      common::Type::VARCHAR,   common::Type::VARBINARY,
      common::Type::ARRAY,     common::Type::UDT};

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
}

TEST_F(TypesTests, IndexTypeTest) {
  std::vector<IndexType> list = {INDEX_TYPE_INVALID, INDEX_TYPE_BTREE,
                                 INDEX_TYPE_BWTREE, INDEX_TYPE_HASH};

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
}

TEST_F(TypesTests, ExpressionTypeTest) {
  std::vector<ExpressionType> list = {
      EXPRESSION_TYPE_INVALID,
      EXPRESSION_TYPE_OPERATOR_PLUS,
      EXPRESSION_TYPE_OPERATOR_MINUS,
      EXPRESSION_TYPE_OPERATOR_MULTIPLY,
      EXPRESSION_TYPE_OPERATOR_DIVIDE,
      EXPRESSION_TYPE_OPERATOR_CONCAT,
      EXPRESSION_TYPE_OPERATOR_MOD,
      EXPRESSION_TYPE_OPERATOR_CAST,
      EXPRESSION_TYPE_OPERATOR_NOT,
      EXPRESSION_TYPE_OPERATOR_IS_NULL,
      EXPRESSION_TYPE_OPERATOR_EXISTS,
      EXPRESSION_TYPE_OPERATOR_UNARY_MINUS,
      EXPRESSION_TYPE_COMPARE_EQUAL,
      EXPRESSION_TYPE_COMPARE_NOTEQUAL,
      EXPRESSION_TYPE_COMPARE_LESSTHAN,
      EXPRESSION_TYPE_COMPARE_GREATERTHAN,
      EXPRESSION_TYPE_COMPARE_LESSTHANOREQUALTO,
      EXPRESSION_TYPE_COMPARE_GREATERTHANOREQUALTO,
      EXPRESSION_TYPE_COMPARE_LIKE,
      EXPRESSION_TYPE_COMPARE_NOTLIKE,
      EXPRESSION_TYPE_COMPARE_IN,
      EXPRESSION_TYPE_CONJUNCTION_AND,
      EXPRESSION_TYPE_CONJUNCTION_OR,
      EXPRESSION_TYPE_VALUE_CONSTANT,
      EXPRESSION_TYPE_VALUE_PARAMETER,
      EXPRESSION_TYPE_VALUE_TUPLE,
      EXPRESSION_TYPE_VALUE_TUPLE_ADDRESS,
      EXPRESSION_TYPE_VALUE_NULL,
      EXPRESSION_TYPE_VALUE_VECTOR,
      EXPRESSION_TYPE_VALUE_SCALAR,
      EXPRESSION_TYPE_AGGREGATE_COUNT,
      EXPRESSION_TYPE_AGGREGATE_COUNT_STAR,
      EXPRESSION_TYPE_AGGREGATE_SUM,
      EXPRESSION_TYPE_AGGREGATE_MIN,
      EXPRESSION_TYPE_AGGREGATE_MAX,
      EXPRESSION_TYPE_AGGREGATE_AVG,
      EXPRESSION_TYPE_FUNCTION,
      EXPRESSION_TYPE_HASH_RANGE,
      EXPRESSION_TYPE_OPERATOR_CASE_EXPR,
      EXPRESSION_TYPE_OPERATOR_NULLIF,
      EXPRESSION_TYPE_OPERATOR_COALESCE,
      EXPRESSION_TYPE_ROW_SUBQUERY,
      EXPRESSION_TYPE_SELECT_SUBQUERY,
      EXPRESSION_TYPE_SUBSTR,
      EXPRESSION_TYPE_ASCII,
      EXPRESSION_TYPE_OCTET_LEN,
      EXPRESSION_TYPE_CHAR,
      EXPRESSION_TYPE_CHAR_LEN,
      EXPRESSION_TYPE_SPACE,
      EXPRESSION_TYPE_REPEAT,
      EXPRESSION_TYPE_POSITION,
      EXPRESSION_TYPE_LEFT,
      EXPRESSION_TYPE_RIGHT,
      EXPRESSION_TYPE_CONCAT,
      EXPRESSION_TYPE_LTRIM,
      EXPRESSION_TYPE_RTRIM,
      EXPRESSION_TYPE_BTRIM,
      EXPRESSION_TYPE_REPLACE,
      EXPRESSION_TYPE_OVERLAY,
      EXPRESSION_TYPE_EXTRACT,
      EXPRESSION_TYPE_DATE_TO_TIMESTAMP,
      EXPRESSION_TYPE_STAR,
      EXPRESSION_TYPE_PLACEHOLDER,
      EXPRESSION_TYPE_COLUMN_REF,
      EXPRESSION_TYPE_FUNCTION_REF,
      EXPRESSION_TYPE_CAST};

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
}

}  // End test namespace
}  // End peloton namespace
