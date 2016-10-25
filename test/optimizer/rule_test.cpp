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

#include "catalog/catalog.h"
#include "common/logger.h"
#include "common/statement.h"
#include "executor/create_executor.h"
#include "executor/delete_executor.h"
#include "executor/insert_executor.h"
#include "executor/plan_executor.h"
#include "executor/update_executor.h"
#include "optimizer/simple_optimizer.h"
#include "parser/parser.h"
#include "planner/create_plan.h"
#include "planner/delete_plan.h"
#include "planner/insert_plan.h"
#include "planner/update_plan.h"

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
  auto val = common::ValueFactory::GetBooleanValue(1);
  auto pred =
    std::make_shared<OpExpression>(ExprConstant::make(val));
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
