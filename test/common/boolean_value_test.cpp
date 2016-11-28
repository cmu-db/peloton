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
  auto valTrue = common::ValueFactory::GetBooleanValue(true);
  auto valFalse = common::ValueFactory::GetBooleanValue(false);

  common::Value result;

  // TRUE == TRUE
  result = valTrue.CompareEquals(valTrue);
  EXPECT_TRUE(result.IsTrue());
  EXPECT_FALSE(result.IsFalse());
  EXPECT_FALSE(result.IsNull());

  // TRUE == FALSE
  result = valTrue.CompareEquals(valFalse);
  EXPECT_FALSE(result.IsTrue());
  EXPECT_TRUE(result.IsFalse());
  EXPECT_FALSE(result.IsNull());

  // TRUE != TRUE
  result = valTrue.CompareNotEquals(valTrue);
  EXPECT_FALSE(result.IsTrue());
  EXPECT_TRUE(result.IsFalse());
  EXPECT_FALSE(result.IsNull());

  // TRUE != FALSE
  result = valTrue.CompareNotEquals(valFalse);
  EXPECT_TRUE(result.IsTrue());
  EXPECT_FALSE(result.IsFalse());
  EXPECT_FALSE(result.IsNull());
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
