//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// boolean_value_test.cpp
//
// Identification: test/common/boolean_value_test.cpp
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "common/boolean_type.h"
#include "common/harness.h"
#include "common/value_factory.h"

namespace peloton {
namespace test {

//===--------------------------------------------------------------------===//
// Boolean Value Test
//===--------------------------------------------------------------------===//

class BooleanValueTests : public PelotonTest {};

TEST_F(BooleanValueTests, BasicTest) {
  auto valTrue = common::ValueFactory::GetBooleanValue(true);
  auto valFalse = common::ValueFactory::GetBooleanValue(false);
  auto valNull =
      common::ValueFactory::GetNullValueByType(common::Type::BOOLEAN);

  EXPECT_TRUE(valTrue.IsTrue());
  EXPECT_FALSE(valTrue.IsFalse());
  EXPECT_FALSE(valTrue.IsNull());

  EXPECT_FALSE(valFalse.IsTrue());
  EXPECT_TRUE(valFalse.IsFalse());
  EXPECT_FALSE(valFalse.IsNull());

  EXPECT_FALSE(valNull.IsTrue());
  EXPECT_FALSE(valNull.IsFalse());
  EXPECT_TRUE(valNull.IsNull());
}

TEST_F(BooleanValueTests, ComparisonTest) {
  std::vector<ExpressionType> compares = {
      EXPRESSION_TYPE_COMPARE_EQUAL,
      EXPRESSION_TYPE_COMPARE_NOTEQUAL,
      EXPRESSION_TYPE_COMPARE_LESSTHAN,
      EXPRESSION_TYPE_COMPARE_LESSTHANOREQUALTO,
      EXPRESSION_TYPE_COMPARE_GREATERTHAN,
      EXPRESSION_TYPE_COMPARE_GREATERTHANOREQUALTO};

  bool values[] = {true, false};

  for (int i = 0; i < 2; i++) {
    for (int j = 0; j < 2; j++) {
      for (auto etype : compares) {
        bool expected;
        common::Value result;
        auto val0 = common::ValueFactory::GetBooleanValue(values[i]);
        auto val1 = common::ValueFactory::GetBooleanValue(values[j]);

        switch (etype) {
          case EXPRESSION_TYPE_COMPARE_EQUAL:
            expected = values[i] == values[j];
            result = val0.CompareEquals(val1);
            break;
          case EXPRESSION_TYPE_COMPARE_NOTEQUAL:
            expected = values[i] != values[j];
            result = val0.CompareNotEquals(val1);
            break;
          case EXPRESSION_TYPE_COMPARE_LESSTHAN:
            expected = values[i] < values[j];
            result = val0.CompareLessThan(val1);
            break;
          case EXPRESSION_TYPE_COMPARE_LESSTHANOREQUALTO:
            expected = values[i] <= values[j];
            result = val0.CompareLessThanEquals(val1);
            break;
          case EXPRESSION_TYPE_COMPARE_GREATERTHAN:
            expected = values[i] > values[j];
            result = val0.CompareGreaterThan(val1);
            break;
          case EXPRESSION_TYPE_COMPARE_GREATERTHANOREQUALTO:
            expected = values[i] >= values[j];
            result = val0.CompareGreaterThanEquals(val1);
            break;
          default:
            throw Exception("Unexpected comparison");
        }  // SWITCH
           //        printf("%s %s %s => %d | %d\n", val0.ToString().c_str(),
           //               ExpressionTypeToString(etype).c_str(),
           //               val1.ToString().c_str(),
           //               expected, result.IsTrue());

        EXPECT_EQ(expected, result.IsTrue());
        EXPECT_FALSE(result.IsNull());
      }
    }
  }
}

TEST_F(BooleanValueTests, NullTest) {
  auto valTrue = common::ValueFactory::GetBooleanValue(true);
  auto valFalse = common::ValueFactory::GetBooleanValue(false);
  auto valNull =
      common::ValueFactory::GetNullValueByType(common::Type::BOOLEAN);

  common::Value result;

  // TRUE == NULL
  result = valTrue.CompareEquals(valNull);
  EXPECT_FALSE(result.IsTrue());
  EXPECT_FALSE(result.IsFalse());
  EXPECT_TRUE(result.IsNull());

  // FALSE == NULL
  result = valTrue.CompareEquals(valNull);
  EXPECT_FALSE(result.IsTrue());
  EXPECT_FALSE(result.IsFalse());
  EXPECT_TRUE(result.IsNull());

  // FALSE == NULL
  result = valTrue.CompareEquals(valNull);
  EXPECT_FALSE(result.IsTrue());
  EXPECT_FALSE(result.IsFalse());
  EXPECT_TRUE(result.IsNull());

  // TRUE != NULL
  result = valTrue.CompareNotEquals(valNull);
  EXPECT_FALSE(result.IsTrue());
  EXPECT_FALSE(result.IsFalse());
  EXPECT_TRUE(result.IsNull());

  // FALSE != NULL
  result = valTrue.CompareNotEquals(valNull);
  EXPECT_FALSE(result.IsTrue());
  EXPECT_FALSE(result.IsFalse());
  EXPECT_TRUE(result.IsNull());
}

TEST_F(BooleanValueTests, CastTest) {
  common::Value result;

  auto valTrue0 = common::ValueFactory::GetVarcharValue("TrUe");
  result = common::ValueFactory::CastAsBoolean(valTrue0);
  EXPECT_TRUE(result.IsTrue());

  auto valTrue1 = common::ValueFactory::GetVarcharValue("1");
  result = common::ValueFactory::CastAsBoolean(valTrue1);
  EXPECT_TRUE(result.IsTrue());

  auto valFalse0 = common::ValueFactory::GetVarcharValue("FaLsE");
  result = common::ValueFactory::CastAsBoolean(valFalse0);
  EXPECT_TRUE(result.IsFalse());

  auto valFalse1 = common::ValueFactory::GetVarcharValue("0");
  result = common::ValueFactory::CastAsBoolean(valFalse1);
  EXPECT_TRUE(result.IsFalse());

  auto valBustedLike = common::ValueFactory::GetVarcharValue("YourMom");
  EXPECT_THROW(common::ValueFactory::CastAsBoolean(valBustedLike),
               peloton::Exception);
}

}  // End test namespace
}  // End peloton namespace
