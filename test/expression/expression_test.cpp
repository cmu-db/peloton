//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// expression_test.cpp
//
// Identification: test/expression/expression_test.cpp
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <memory>
#include <set>
#include <string>
#include <vector>

#include "common/harness.h"

#include "expression/expression_util.h"
#include "expression/function_expression.h"
#include "type/value.h"
#include "type/value_factory.h"

using ::testing::NotNull;
using ::testing::Return;

namespace peloton {

namespace test {

typedef std::unique_ptr<expression::AbstractExpression> ExpPtr;

class ExpressionTests : public PelotonTest {};

// A simple test to make sure function expressions are filled in correctly
TEST_F(ExpressionTests, FunctionExpressionTest) {
  // these will be gc'd by substr
  auto str = expression::ExpressionUtil::ConstantValueFactory(
      type::ValueFactory::GetVarcharValue("test123"));
  auto from = expression::ExpressionUtil::ConstantValueFactory(
      type::ValueFactory::GetIntegerValue(2));
  auto to = expression::ExpressionUtil::ConstantValueFactory(
      type::ValueFactory::GetIntegerValue(3));
  // these need unique ptrs to clean them
  auto substr =
      ExpPtr(new expression::FunctionExpression("substr", {str, from, to}));
  auto not_found = ExpPtr(new expression::FunctionExpression("", {}));
  // throw an exception when we cannot find a function
  EXPECT_THROW(
      expression::ExpressionUtil::TransformExpression(nullptr, not_found.get()),
      peloton::Exception);
  expression::ExpressionUtil::TransformExpression(nullptr, substr.get());
  // do a lookup (we pass null schema because there are no tuple value
  // expressions
  EXPECT_TRUE(substr->Evaluate(nullptr, nullptr, nullptr)
                  .CompareEquals(type::ValueFactory::GetVarcharValue("est")) ==
              type::CMP_TRUE);
}

TEST_F(ExpressionTests, ExtractDateTests) {
  // PAVLO: 2017-01-18
  // This will test whether we can invoke the EXTRACT function
  // correctly. This is different than DateFunctionsTests
  // because it goes through the in our expression system.
  //  This should be uncommented once we get a full implementation

  //  std::string date = "2017-01-01 12:13:14.999999+00";
  //
  //  // <PART> <EXPECTED>
  //  // You can generate the expected value in postgres using this SQL:
  //  // SELECT EXTRACT(MILLISECONDS
  //  //                FROM CAST('2017-01-01 12:13:14.999999+00' AS
  //  TIMESTAMP));
  //  std::vector<std::pair<DatePartType, double>> data = {
  //      std::make_pair(DatePartType::CENTURY, 21),
  //      std::make_pair(DatePartType::DECADE, 201),
  //      std::make_pair(DatePartType::DOW, 0),
  //      std::make_pair(DatePartType::DOY, 1),
  //      std::make_pair(DatePartType::YEAR, 2017),
  //      std::make_pair(DatePartType::MONTH, 1),
  //      std::make_pair(DatePartType::DAY, 2),
  //      std::make_pair(DatePartType::HOUR, 12),
  //      std::make_pair(DatePartType::MINUTE, 13),
  //
  //      // Note that we can support these DatePartTypes with and without
  //      // a trailing 's' at the end.
  //      std::make_pair(DatePartType::SECOND, 14),
  //      std::make_pair(DatePartType::SECONDS, 14),
  //      std::make_pair(DatePartType::MILLISECOND, 14999.999),
  //      std::make_pair(DatePartType::MILLISECONDS, 14999.999),
  //  };
  //
  //  for (auto x : data) {
  //    // these will be cleaned up by extract_expr
  //    auto part = expression::ExpressionUtil::ConstantValueFactory(
  //        type::ValueFactory::GetIntegerValue(static_cast<int>(x.first)));
  //    auto timestamp = expression::ExpressionUtil::ConstantValueFactory(
  //        type::ValueFactory::CastAsTimestamp(
  //            type::ValueFactory::GetVarcharValue(date)));
  //
  //    // these need unique ptrs to clean them
  //    auto extract_expr = ExpPtr(
  //        new expression::FunctionExpression("extract", {part, timestamp}));
  //
  //    expression::ExpressionUtil::TransformExpression(nullptr,
  //                                                    extract_expr.get());
  //
  //    // Perform evaluation and check the result matches
  //    // NOTE: We pass null schema because there are no tuple value
  //    expressions
  //    type::Value expected = type::ValueFactory::GetDecimalValue(x.second);
  //    type::Value result = extract_expr->Evaluate(nullptr, nullptr, nullptr);
  //    EXPECT_FALSE(result.IsNull());
  //    EXPECT_EQ(type::CmpBool::CMP_TRUE, expected.CompareEquals(result));
  //  }
}

}  // namespace test
}  // namespace peloton
