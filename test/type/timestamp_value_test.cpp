//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// timestamp_value_test.cpp
//
// Identification: test/common/timestamp_value_test.cpp
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "common/harness.h"
#include "type/timestamp_type.h"
#include "type/value_factory.h"

namespace peloton {
namespace test {

//===--------------------------------------------------------------------===//
// Timestamp Value Test
//===--------------------------------------------------------------------===//

class TimestampValueTests : public PelotonTest {};

TEST_F(TimestampValueTests, ComparisonTest) {
  std::vector<ExpressionType> compares = {
      ExpressionType::COMPARE_EQUAL,
      ExpressionType::COMPARE_NOTEQUAL,
      ExpressionType::COMPARE_LESSTHAN,
      ExpressionType::COMPARE_LESSTHANOREQUALTO,
      ExpressionType::COMPARE_GREATERTHAN,
      ExpressionType::COMPARE_GREATERTHANOREQUALTO};

  uint64_t values[] = {1000000000, 2000000000, type::PELOTON_TIMESTAMP_NULL};

  CmpBool result;
  type::Value val0;
  type::Value val1;

  for (int i = 0; i < 3; i++) {
    for (int j = 0; j < 3; j++) {
      bool expected_null = false;

      // VALUE #0
      if (values[i] == type::PELOTON_TIMESTAMP_NULL) {
        val0 = type::ValueFactory::GetNullValueByType(type::TypeId::TIMESTAMP);
        expected_null = true;
      } else {
        val0 = type::ValueFactory::GetTimestampValue(
            static_cast<uint64_t>(values[i]));
      }

      // VALUE #1
      if (values[j] == type::PELOTON_TIMESTAMP_NULL) {
        val1 = type::ValueFactory::GetNullValueByType(type::TypeId::TIMESTAMP);
        expected_null = true;
      } else {
        val1 = type::ValueFactory::GetTimestampValue(
            static_cast<uint64_t>(values[j]));
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
                  val1.ToString().c_str(), expected, static_cast<int>(result));

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

TEST_F(TimestampValueTests, NullToStringTest) {
  auto valNull = type::ValueFactory::GetNullValueByType(type::TypeId::TIMESTAMP);

  EXPECT_EQ(valNull.ToString(), "timestamp_null");
}

TEST_F(TimestampValueTests, HashTest) {
  uint64_t values[] = {1000000000, 2000000000, type::PELOTON_TIMESTAMP_NULL};

  type::Value result;
  type::Value val0;
  type::Value val1;

  for (int i = 0; i < 2; i++) {
    if (values[i] == type::PELOTON_TIMESTAMP_NULL) {
      val0 = type::ValueFactory::GetNullValueByType(type::TypeId::TIMESTAMP);
    } else {
      val0 = type::ValueFactory::GetTimestampValue(
          static_cast<uint64_t>(values[i]));
    }
    for (int j = 0; j < 2; j++) {
      if (values[j] == type::PELOTON_TIMESTAMP_NULL) {
        val1 = type::ValueFactory::GetNullValueByType(type::TypeId::TIMESTAMP);
      } else {
        val1 = type::ValueFactory::GetTimestampValue(
            static_cast<uint64_t>(values[j]));
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

TEST_F(TimestampValueTests, CopyTest) {
  type::Value val0 =
      type::ValueFactory::GetTimestampValue(static_cast<uint64_t>(1000000));
  type::Value val1 = val0.Copy();
  EXPECT_EQ(CmpBool::TRUE, val0.CompareEquals(val1));
}

TEST_F(TimestampValueTests, CastTest) {
  type::Value result;

  auto strNull = type::ValueFactory::GetNullValueByType(type::TypeId::VARCHAR);
  auto valNull = type::ValueFactory::GetNullValueByType(type::TypeId::TIMESTAMP);

  result = valNull.CastAs(type::TypeId::TIMESTAMP);
  EXPECT_TRUE(result.IsNull());
  EXPECT_EQ(CmpBool::NULL_, result.CompareEquals(valNull));
  EXPECT_EQ(result.GetTypeId(), valNull.GetTypeId());

  result = valNull.CastAs(type::TypeId::VARCHAR);
  EXPECT_TRUE(result.IsNull());
  EXPECT_EQ(CmpBool::NULL_, result.CompareEquals(strNull));
  EXPECT_EQ(result.GetTypeId(), strNull.GetTypeId());

  EXPECT_THROW(valNull.CastAs(type::TypeId::BOOLEAN), peloton::Exception);

  auto valValid =
      type::ValueFactory::GetTimestampValue(static_cast<uint64_t>(1481746648));
  result = valValid.CastAs(type::TypeId::VARCHAR);
  EXPECT_FALSE(result.IsNull());
}

}  // namespace test
}  // namespace peloton
