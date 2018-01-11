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

#include "common/harness.h"
#include "type/boolean_type.h"
#include "type/value_factory.h"

namespace peloton {
namespace test {

//===--------------------------------------------------------------------===//
// Boolean Value Test
//===--------------------------------------------------------------------===//

class BooleanValueTests : public PelotonTest {};

TEST_F(BooleanValueTests, BasicTest) {
  auto valTrue = type::ValueFactory::GetBooleanValue(true);
  auto valFalse = type::ValueFactory::GetBooleanValue(false);
  auto valNull = type::ValueFactory::GetNullValueByType(type::TypeId::BOOLEAN);

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
      ExpressionType::COMPARE_EQUAL,
      ExpressionType::COMPARE_NOTEQUAL,
      ExpressionType::COMPARE_LESSTHAN,
      ExpressionType::COMPARE_LESSTHANOREQUALTO,
      ExpressionType::COMPARE_GREATERTHAN,
      ExpressionType::COMPARE_GREATERTHANOREQUALTO};

  int values[] = {true, false, type::PELOTON_BOOLEAN_NULL};

  CmpBool result;
  type::Value val0;
  type::Value val1;

  for (int i = 0; i < 2; i++) {
    for (int j = 0; j < 2; j++) {
      bool expected_null = false;

      // VALUE #0
      if (values[i] == type::PELOTON_BOOLEAN_NULL) {
        val0 = type::ValueFactory::GetNullValueByType(type::TypeId::BOOLEAN);
        expected_null = true;
      } else {
        val0 =
            type::ValueFactory::GetBooleanValue(static_cast<bool>(values[i]));
      }

      // VALUE #1
      if (values[j] == type::PELOTON_BOOLEAN_NULL) {
        val1 = type::ValueFactory::GetNullValueByType(type::TypeId::BOOLEAN);
        expected_null = true;
      } else {
        val1 =
            type::ValueFactory::GetBooleanValue(static_cast<bool>(values[j]));
      }

      for (auto etype : compares) {
        bool expected;

        switch (etype) {
          case ExpressionType::COMPARE_EQUAL:
            expected = values[i] == values[j];
            result = val0.CompareEquals(val1);
            break;
          case ExpressionType::COMPARE_NOTEQUAL:
            expected = values[i] != values[j];
            result = val0.CompareNotEquals(val1);
            break;
          case ExpressionType::COMPARE_LESSTHAN:
            expected = values[i] < values[j];
            result = val0.CompareLessThan(val1);
            break;
          case ExpressionType::COMPARE_LESSTHANOREQUALTO:
            expected = values[i] <= values[j];
            result = val0.CompareLessThanEquals(val1);
            break;
          case ExpressionType::COMPARE_GREATERTHAN:
            expected = values[i] > values[j];
            result = val0.CompareGreaterThan(val1);
            break;
          case ExpressionType::COMPARE_GREATERTHANOREQUALTO:
            expected = values[i] >= values[j];
            result = val0.CompareGreaterThanEquals(val1);
            break;
          default:
            throw Exception("Unexpected comparison");
        }  // SWITCH
        LOG_TRACE("%s %s %s => %d | %d\n", val0.ToString().c_str(),
                  ExpressionTypeToString(etype).c_str(),
                  val1.ToString().c_str(),
                  static_cast<int>(expected),
                  static_cast<int>(result));

        if (expected_null) expected = false;

        EXPECT_EQ(expected, result == CmpBool::TRUE);
        EXPECT_EQ(!expected, result == CmpBool::FALSE);
        EXPECT_EQ(expected_null, result == CmpBool::NULL_);
      }
    }
  }
}

TEST_F(BooleanValueTests, ToStringTest) {
  type::Value val;
  type::Value result;
  std::string str;
  type::Value valStr;

  val = type::ValueFactory::GetBooleanValue(true);
  str = val.ToString();
  valStr = type::ValueFactory::GetVarcharValue(str);
  result = type::ValueFactory::CastAsBoolean(valStr);
  EXPECT_TRUE(result.IsTrue());

  val = type::ValueFactory::GetBooleanValue(false);
  str = val.ToString();
  valStr = type::ValueFactory::GetVarcharValue(str);
  result = type::ValueFactory::CastAsBoolean(valStr);
  EXPECT_TRUE(result.IsFalse());
}

TEST_F(BooleanValueTests, HashTest) {
  int values[] = {true, false, type::PELOTON_BOOLEAN_NULL};

  CmpBool result;
  type::Value val0;
  type::Value val1;

  for (int i = 0; i < 2; i++) {
    if (values[i] == type::PELOTON_BOOLEAN_NULL) {
      val0 = type::ValueFactory::GetNullValueByType(type::TypeId::BOOLEAN);
    } else {
      val0 = type::ValueFactory::GetBooleanValue(static_cast<bool>(values[i]));
    }
    for (int j = 0; j < 2; j++) {
      if (values[j] == type::PELOTON_BOOLEAN_NULL) {
        val1 = type::ValueFactory::GetNullValueByType(type::TypeId::BOOLEAN);
      } else {
        val1 =
            type::ValueFactory::GetBooleanValue(static_cast<bool>(values[j]));
      }

      result = val0.CompareEquals(val1);
      auto hash0 = val0.Hash();
      auto hash1 = val1.Hash();

      if (result == CmpBool::TRUE) {
        EXPECT_EQ(hash0, hash1);
      } else {
        EXPECT_NE(hash0, hash1);
      }
    }
  }
}

TEST_F(BooleanValueTests, CastTest) {
  type::Value result;

  auto valTrue0 = type::ValueFactory::GetVarcharValue("TrUe");
  result = type::ValueFactory::CastAsBoolean(valTrue0);
  EXPECT_TRUE(result.IsTrue());
  result = valTrue0.CastAs(type::TypeId::BOOLEAN);
  EXPECT_TRUE(result.IsTrue());

  auto valTrue1 = type::ValueFactory::GetVarcharValue("1");
  result = type::ValueFactory::CastAsBoolean(valTrue1);
  EXPECT_TRUE(result.IsTrue());

  auto valTrue2 = type::ValueFactory::GetVarcharValue("t");
  result = type::ValueFactory::CastAsBoolean(valTrue2);
  EXPECT_TRUE(result.IsTrue());

  auto valFalse0 = type::ValueFactory::GetVarcharValue("FaLsE");
  result = type::ValueFactory::CastAsBoolean(valFalse0);
  EXPECT_TRUE(result.IsFalse());
  result = valFalse0.CastAs(type::TypeId::BOOLEAN);
  EXPECT_TRUE(result.IsFalse());

  auto valFalse1 = type::ValueFactory::GetVarcharValue("0");
  result = type::ValueFactory::CastAsBoolean(valFalse1);
  EXPECT_TRUE(result.IsFalse());

  auto valFalse2 = type::ValueFactory::GetVarcharValue("f");
  result = type::ValueFactory::CastAsBoolean(valFalse2);
  EXPECT_TRUE(result.IsFalse());

  auto valBustedLike = type::ValueFactory::GetVarcharValue("YourMom");
  EXPECT_THROW(type::ValueFactory::CastAsBoolean(valBustedLike),
               peloton::Exception);
}

}  // namespace test
}  // namespace peloton
