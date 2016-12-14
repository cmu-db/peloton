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

#include "common/timestamp_type.h"
#include "common/harness.h"
#include "common/value_factory.h"

namespace peloton {
namespace test {

//===--------------------------------------------------------------------===//
// Timestamp Value Test
//===--------------------------------------------------------------------===//

class TimestampValueTests : public PelotonTest {};

TEST_F(TimestampValueTests, ComparisonTest) {
  std::vector<ExpressionType> compares = {
      EXPRESSION_TYPE_COMPARE_EQUAL,
      EXPRESSION_TYPE_COMPARE_NOTEQUAL,
      EXPRESSION_TYPE_COMPARE_LESSTHAN,
      EXPRESSION_TYPE_COMPARE_LESSTHANOREQUALTO,
      EXPRESSION_TYPE_COMPARE_GREATERTHAN,
      EXPRESSION_TYPE_COMPARE_GREATERTHANOREQUALTO};

  uint64_t values[] = {1000000000, 2000000000, common::PELOTON_TIMESTAMP_NULL};

  common::Value result;
  common::Value val0;
  common::Value val1;

  for (int i = 0; i < 3; i++) {
    for (int j = 0; j < 3; j++) {
      bool expected_null = false;

      // VALUE #0
      if (values[i] == common::PELOTON_TIMESTAMP_NULL) {
        val0 = common::ValueFactory::GetNullValueByType(common::Type::TIMESTAMP);
        expected_null = true;
      } else {
        val0 =
            common::ValueFactory::GetTimestampValue(static_cast<uint64_t>(values[i]));
      }

      // VALUE #1
      if (values[j] == common::PELOTON_TIMESTAMP_NULL) {
        val1 = common::ValueFactory::GetNullValueByType(common::Type::TIMESTAMP);
        expected_null = true;
      } else {
        val1 =
            common::ValueFactory::GetTimestampValue(static_cast<uint64_t>(values[j]));
      }
      bool temp = expected_null;

      for (auto etype : compares) {
        bool expected;
        expected_null = temp;
        switch (etype) {
          case EXPRESSION_TYPE_COMPARE_EQUAL:
            expected = values[i] == values[j];
            result = val0.CompareEquals(val1);
            break;
          case EXPRESSION_TYPE_COMPARE_NOTEQUAL:
            expected = values[i] != values[j];
            result = val0.CompareNotEquals(val1);
            if (!val1.IsNull() && expected_null) {
              expected_null = false;
            }
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
        LOG_TRACE("%s %s %s => %d | %d\n", val0.ToString().c_str(),
                  ExpressionTypeToString(etype).c_str(),
                  val1.ToString().c_str(), expected, result.IsTrue());

        if (expected_null) {
          EXPECT_EQ(expected_null, result.IsNull());
        }
        else {
          EXPECT_EQ(expected, result.IsTrue());
          EXPECT_EQ(!expected, result.IsFalse());
        }
      }
    }
  }
}

TEST_F(TimestampValueTests, NullToStringTest) {
  auto valNull =
      common::ValueFactory::GetNullValueByType(common::Type::TIMESTAMP);

  EXPECT_EQ(valNull.ToString(), "timestamp_null");
}

TEST_F(TimestampValueTests, HashTest) {
  uint64_t values[] = {1000000000, 2000000000, common::PELOTON_TIMESTAMP_NULL};

  common::Value result;
  common::Value val0;
  common::Value val1;

  for (int i = 0; i < 2; i++) {
    if (values[i] == common::PELOTON_TIMESTAMP_NULL) {
      val0 = common::ValueFactory::GetNullValueByType(common::Type::TIMESTAMP);
    } else {
      val0 =
        common::ValueFactory::GetTimestampValue(static_cast<uint64_t>(values[i]));
    }
    for (int j = 0; j < 2; j++) {
      if (values[j] == common::PELOTON_TIMESTAMP_NULL) {
        val1 = common::ValueFactory::GetNullValueByType(common::Type::TIMESTAMP);
      } else {
        val1 =
          common::ValueFactory::GetTimestampValue(static_cast<uint64_t>(values[j]));
      }

      result = val0.CompareEquals(val1);
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
  common::Value result;
  common::Value val0 =
    common::ValueFactory::GetTimestampValue(static_cast<uint64_t>(1000000));
  common::Value val1 = val0.Copy();
  EXPECT_EQ(val0.CompareEquals(val1).IsTrue(), true);
}

TEST_F(TimestampValueTests, CastTest) {
  common::Value result;

  auto strNull =
    common::ValueFactory::GetNullValueByType(common::Type::VARCHAR);
  auto valNull =
    common::ValueFactory::GetNullValueByType(common::Type::TIMESTAMP);

  result = valNull.CastAs(common::Type::TIMESTAMP);
  EXPECT_TRUE(result.IsNull());
  EXPECT_EQ(result.CompareEquals(valNull).IsNull(), true);
  EXPECT_EQ(result.GetTypeId(), valNull.GetTypeId());

  result = valNull.CastAs(common::Type::VARCHAR);
  EXPECT_TRUE(result.IsNull());
  EXPECT_EQ(result.CompareEquals(strNull).IsNull(), true);
  EXPECT_EQ(result.GetTypeId(), strNull.GetTypeId());

  EXPECT_THROW(valNull.CastAs(common::Type::BOOLEAN),
               peloton::Exception);

  auto valValid =
    common::ValueFactory::GetTimestampValue(static_cast<uint64_t>(1481746648));
  result = valValid.CastAs(common::Type::VARCHAR);
  EXPECT_FALSE(result.IsNull());
}


}  // End test namespace
}  // End peloton namespace
