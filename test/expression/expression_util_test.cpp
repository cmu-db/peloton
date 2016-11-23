//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// expression_test.cpp
//
// Identification: test/expression/expressionutil_test.cpp
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <memory>
#include <set>
#include <string>
#include <vector>

#include "common/harness.h"
#include "common/logger.h"

#include "common/value.h"
#include "common/value_factory.h"
#include "expression/abstract_expression.h"
#include "expression/expression_util.h"

using ::testing::NotNull;
using ::testing::Return;

namespace peloton {

namespace test {

class ExpressionUtilTest : public PelotonTest {};

expression::AbstractExpression* createExpTree() {
  auto exp1 = expression::ExpressionUtil::ConstantValueFactory(
      common::ValueFactory::GetIntegerValue(1));
  auto exp2 = expression::ExpressionUtil::ConstantValueFactory(
      common::ValueFactory::GetIntegerValue(1));
  auto root = expression::ExpressionUtil::ComparisonFactory(
      EXPRESSION_TYPE_COMPARE_EQUAL, exp1, exp2);

  return (root);
}

// Make sure that we can traverse a tree
TEST_F(ExpressionUtilTest, GetInfoTest) {
  auto root = createExpTree();

  std::string info = expression::ExpressionUtil::GetInfo(root);

  LOG_INFO("%s", info.c_str());

  EXPECT_TRUE(info.size() > 0);

  delete root;
}

}  // namespace test
}  // namespace peloton
