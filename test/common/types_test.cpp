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
  std::string invalid("JoyArulrajIsDangerous");
  EXPECT_THROW(peloton::StringToTypeId(invalid), peloton::Exception);
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
  std::string invalid("LinMaLovesEverybody");
  EXPECT_THROW(peloton::StringToExpressionType(invalid), peloton::Exception);
}

}  // End test namespace
}  // End peloton namespace
