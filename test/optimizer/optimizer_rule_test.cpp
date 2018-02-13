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

#include "optimizer/operator_expression.h"
#include "optimizer/operators.h"
#include "optimizer/optimizer.h"
#include "optimizer/rule.h"
#include "optimizer/rule_impls.h"

#include "catalog/catalog.h"
#include "common/logger.h"
#include "common/statement.h"
#include "executor/create_executor.h"
#include "executor/delete_executor.h"
#include "executor/insert_executor.h"
#include "executor/plan_executor.h"
#include "executor/update_executor.h"
#include "planner/create_plan.h"
#include "planner/delete_plan.h"
#include "planner/insert_plan.h"
#include "planner/update_plan.h"
#include "type/value_factory.h"

namespace peloton {
namespace test {

//===--------------------------------------------------------------------===//
// Binding Tests
//===--------------------------------------------------------------------===//

using namespace optimizer;

class OptimizerRuleTests : public PelotonTest {};

TEST_F(OptimizerRuleTests, SimpleCommutativeRuleTest) {
  // Build op plan node to match rule
  auto left_get = std::make_shared<OperatorExpression>(LogicalGet::make());
  auto right_get = std::make_shared<OperatorExpression>(LogicalGet::make());
  auto join = std::make_shared<OperatorExpression>(LogicalInnerJoin::make());
  join->PushChild(left_get);
  join->PushChild(right_get);

  // Setup rule
  InnerJoinCommutativity rule;

  EXPECT_TRUE(rule.Check(join, nullptr));

  std::vector<std::shared_ptr<OperatorExpression>> outputs;
  rule.Transform(join, outputs, nullptr);
  EXPECT_EQ(outputs.size(), 1);

  auto output_join = outputs[0];

  EXPECT_EQ(output_join->Children()[0], right_get);
  EXPECT_EQ(output_join->Children()[1], left_get);

}

TEST_F(OptimizerRuleTests, SimpleAssociativeRuleTest) {
  // Build op plan node to match rule
  // (left JOIN middle) JOIN right
  auto left_get = std::make_shared<OperatorExpression>(LogicalGet::make());
  auto middle_get = std::make_shared<OperatorExpression>(LogicalGet::make());
  auto right_get = std::make_shared<OperatorExpression>(LogicalGet::make());
  auto child_join = std::make_shared<OperatorExpression>(LogicalInnerJoin::make());
  child_join->PushChild(left_get);
  child_join->PushChild(middle_get);

  auto parent_join = std::make_shared<OperatorExpression>(LogicalInnerJoin::make());
  parent_join->PushChild(child_join);
  parent_join->PushChild(right_get);


  // Setup rule
  InnerJoinAssociativity rule;

  EXPECT_TRUE(rule.Check(parent_join, nullptr));

  std::vector<std::shared_ptr<OperatorExpression>> outputs;
  rule.Transform(parent_join, outputs, nullptr);
  EXPECT_EQ(outputs.size(), 1);

  auto output_join = outputs[0];

  EXPECT_EQ(output_join->Children()[0], left_get);
  EXPECT_EQ(output_join->Children()[1]->Children()[0], middle_get);
  EXPECT_EQ(output_join->Children()[1]->Children()[1], right_get);


}

}  // namespace test
}  // namespace peloton
