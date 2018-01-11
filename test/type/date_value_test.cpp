//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// date_value_test.cpp
//
// Identification: test/common/date_value_test.cpp
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "common/harness.h"
#include "type/date_type.h"
#include "type/value_factory.h"

namespace peloton {
namespace test {

//===--------------------------------------------------------------------===//
// Date Value Test
//===--------------------------------------------------------------------===//

class DateValueTests : public PelotonTest {};

TEST_F(DateValueTests, ComparisonTest) {
  std::vector<ExpressionType> compares = {
      ExpressionType::COMPARE_EQUAL,
      ExpressionType::COMPARE_NOTEQUAL,
      ExpressionType::COMPARE_LESSTHAN,
      ExpressionType::COMPARE_LESSTHANOREQUALTO,
      ExpressionType::COMPARE_GREATERTHAN,
      ExpressionType::COMPARE_GREATERTHANOREQUALTO};

  int32_t values[] = {1000000000, 2000000000, type::PELOTON_DATE_NULL};

  CmpBool result;
  type::Value val0;
  type::Value val1;

  for (int i = 0; i < 3; i++) {
    for (int j = 0; j < 3; j++) {
      bool expected_null = false;

      // VALUE #0
      if (values[i] == type::PELOTON_DATE_NULL) {
        val0 = type::ValueFactory::GetNullValueByType(type::TypeId::DATE);
        expected_null = true;
      } else {
        val0 = type::ValueFactory::GetDateValue(
            static_cast<int32_t>(values[i]));
      }

      // VALUE #1
      if (values[j] == type::PELOTON_DATE_NULL) {
        val1 = type::ValueFactory::GetNullValueByType(type::TypeId::DATE);
        expected_null = true;
      } else {
        val1 = type::ValueFactory::GetDateValue(
            static_cast<int32_t>(values[j]));
      }
      bool temp = expected_null;

      for (auto etype : compares) {
        bool expected;
        expected_null = temp;
        switch (etype) {
          case ExpressionType::COMPARE_EQUAL:
            expected = values[i] == values[j];
            result = val0.CompareEquals(val1);
            break;
          case ExpressionType::COMPARE_NOTEQUAL:
            expected = values[i] != values[j];
            result = val0.CompareNotEquals(val1);
            if (!val1.IsNull() && expected_null) {
              expected_null = false;
            }
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

        if (expected_null) {
          EXPECT_EQ(expected_null, result == CmpBool::NULL_);
        } else {
          EXPECT_EQ(expected, result == CmpBool::TRUE);
          EXPECT_EQ(!expected, result == CmpBool::FALSE);
        }
      }
    }
  }
}

TEST_F(DateValueTests, NullToStringTest) {
  auto val_null = type::ValueFactory::GetNullValueByType(type::TypeId::DATE);

  EXPECT_EQ(val_null.ToString(), "date_null");
}

TEST_F(DateValueTests, HashTest) {
  int32_t values[] = {1000000000, 2000000000, type::PELOTON_DATE_NULL};

  type::Value result;
  type::Value val0;
  type::Value val1;

  for (int i = 0; i < 2; i++) {
    if (values[i] == type::PELOTON_DATE_NULL) {
      val0 = type::ValueFactory::GetNullValueByType(type::TypeId::DATE);
    } else {
      val0 = type::ValueFactory::GetDateValue(
          static_cast<int32_t>(values[i]));
    }
    for (int j = 0; j < 2; j++) {
      if (values[j] == type::PELOTON_DATE_NULL) {
        val1 = type::ValueFactory::GetNullValueByType(type::TypeId::DATE);
      } else {
        val1 = type::ValueFactory::GetDateValue(
            static_cast<int32_t>(values[j]));
      }

      result = type::ValueFactory::GetBooleanValue(val0.CompareEquals(val1));
      auto hash0 = val0.Hash();
      auto hash1 = val1.Hash();

      if (result.IsTrue()) {
        EXPECT_EQ(hash0, hash1);
      } else {
        EXPECT_NE(hash0, hash1);
      }
    }
  }
}

TEST_F(DateValueTests, CopyTest) {
  type::Value val0 =
      type::ValueFactory::GetDateValue(static_cast<int32_t>(1000000));
  type::Value val1 = val0.Copy();
  EXPECT_EQ(CmpBool::TRUE, val0.CompareEquals(val1));
}

TEST_F(DateValueTests, CastTest) {
  type::Value result;

  auto str_null = type::ValueFactory::GetNullValueByType(type::TypeId::VARCHAR);
  auto val_null = type::ValueFactory::GetNullValueByType(type::TypeId::DATE);

  result = val_null.CastAs(type::TypeId::DATE);
  EXPECT_TRUE(result.IsNull());
  EXPECT_EQ(CmpBool::NULL_, result.CompareEquals(val_null));
  EXPECT_EQ(result.GetTypeId(), val_null.GetTypeId());

  result = val_null.CastAs(type::TypeId::VARCHAR);
  EXPECT_TRUE(result.IsNull());
  EXPECT_EQ(CmpBool::NULL_, result.CompareEquals(str_null));
  EXPECT_EQ(result.GetTypeId(), str_null.GetTypeId());

  EXPECT_THROW(val_null.CastAs(type::TypeId::BOOLEAN), peloton::Exception);

  auto val_valid =
      type::ValueFactory::GetDateValue(static_cast<int32_t>(1481746648));
  result = val_valid.CastAs(type::TypeId::VARCHAR);
  EXPECT_FALSE(result.IsNull());
}

}  // namespace test
}  // namespace peloton
