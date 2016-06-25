//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// rule_test.cpp
//
// Identification: test/optimizer/rule_test.cpp
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//


#include "common/harness.h"

#define private public

#include "optimizer/optimizer.h"
#include "optimizer/rule.h"
#include "optimizer/rule_impls.h"
#include "optimizer/op_expression.h"
#include "optimizer/operators.h"

namespace peloton {
namespace test {

//===--------------------------------------------------------------------===//
// Binding Tests
//===--------------------------------------------------------------------===//

using namespace optimizer;

class RuleTests : public PelotonTest {};

TEST_F(RuleTests, SimpleRuleApplyTest) {
  // Build op plan node to match rule
  auto left_get = std::make_shared<OpExpression>(LogicalGet::make(0, {}));
  auto right_get = std::make_shared<OpExpression>(LogicalGet::make(0, {}));
  auto pred =
    std::make_shared<OpExpression>(ExprConstant::make(Value::GetTrue()));
  auto join = std::make_shared<OpExpression>(LogicalInnerJoin::make());
  join->PushChild(left_get);
  join->PushChild(right_get);
  join->PushChild(pred);

  // Setup rule
  InnerJoinCommutativity rule;

  EXPECT_TRUE(rule.Check(join));

  std::vector<std::shared_ptr<OpExpression>> outputs;
  rule.Transform(join, outputs);
  EXPECT_EQ(outputs.size(), 1);
}

} /* namespace test */
} /* namespace peloton */
