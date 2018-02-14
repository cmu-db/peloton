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

#include <include/concurrency/transaction_manager_factory.h>
#include <include/parser/postgresparser.h>
#include "common/harness.h"

#define private public

#include "binder/bind_node_visitor.h"
#include "catalog/catalog.h"
#include "common/logger.h"
#include "common/statement.h"
#include "executor/create_executor.h"
#include "executor/delete_executor.h"
#include "executor/insert_executor.h"
#include "executor/plan_executor.h"
#include "executor/update_executor.h"
#include "optimizer/operator_expression.h"
#include "optimizer/operators.h"
#include "optimizer/optimizer.h"
#include "optimizer/rule.h"
#include "optimizer/rule_impls.h"
#include "planner/create_plan.h"
#include "planner/delete_plan.h"
#include "planner/insert_plan.h"
#include "planner/update_plan.h"
#include "sql/testing_sql_util.h"
#include "type/value_factory.h"

namespace peloton {
namespace test {

//===--------------------------------------------------------------------===//
// Binding Tests
//===--------------------------------------------------------------------===//

using namespace optimizer;

class OptimizerRuleTests : public PelotonTest {
 protected:
  GroupExpression *GetSingleGroupExpression(Memo &memo, GroupExpression *expr,
                                            int child_group_idx) {
    auto group = memo.GetGroupByID(expr->GetChildGroupId(child_group_idx));
    EXPECT_EQ(1, group->logical_expressions_.size());
    return group->logical_expressions_[0].get();
  }
};

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

TEST_F(OptimizerRuleTests, AssociativeRuleTest) {
  auto &txn_manager = concurrency::TransactionManagerFactory::GetInstance();
  auto txn = txn_manager.BeginTransaction();
  catalog::Catalog::GetInstance()->CreateDatabase(DEFAULT_DB_NAME, txn);
  txn_manager.CommitTransaction(txn);

  TestingSQLUtil::ExecuteSQLQuery(
      "CREATE TABLE test1(a INT PRIMARY KEY, b INT, c INT);");
  TestingSQLUtil::ExecuteSQLQuery(
      "CREATE TABLE test2(a INT PRIMARY KEY, b INT, c INT);");
  TestingSQLUtil::ExecuteSQLQuery(
      "CREATE TABLE test3(a INT PRIMARY KEY, b INT, c INT);");

  auto &peloton_parser = parser::PostgresParser::GetInstance();
  auto stmt =
      peloton_parser.BuildParseTree("SELECT * FROM test1, test2, test3");
  auto parse_tree = stmt->GetStatements().at(0).get();
  auto predicates = std::vector<expression::AbstractExpression *>();

  optimizer::Optimizer optimizer;

  // Push Associativity rule and execute tasks
  optimizer.metadata_.rule_set.transformation_rules_.clear();
  optimizer.metadata_.rule_set.transformation_rules_.emplace_back(
      new InnerJoinAssociativity());

  txn = txn_manager.BeginTransaction();

  auto bind_node_visitor =
      std::make_shared<binder::BindNodeVisitor>(txn, DEFAULT_DB_NAME);
  bind_node_visitor->BindNameToNode(parse_tree);

  std::shared_ptr<GroupExpression> gexpr =
      optimizer.InsertQueryTree(parse_tree, txn);
  std::vector<GroupID> child_groups = {gexpr->GetGroupID()};

  auto &memo = optimizer.metadata_.memo;
  std::shared_ptr<GroupExpression> head_gexpr =
      std::make_shared<GroupExpression>(Operator(), child_groups);

  // Check plan is of structure (left JOIN middle) JOIN right
  // Check Parent join
  auto group_expr = GetSingleGroupExpression(memo, head_gexpr.get(), 0);
  EXPECT_EQ(OpType::InnerJoin, group_expr->Op().GetType());
  auto join_op = group_expr->Op().As<LogicalInnerJoin>();
  EXPECT_EQ(0, join_op->join_predicates.size());

  // Check left join
  auto l_group_expr = GetSingleGroupExpression(memo, group_expr, 0);
  EXPECT_EQ(OpType::InnerJoin, l_group_expr->Op().GetType());
  auto left = GetSingleGroupExpression(memo, l_group_expr, 0);
  auto middle = GetSingleGroupExpression(memo, l_group_expr, 1);
  EXPECT_EQ(OpType::Get, left->Op().GetType());
  EXPECT_EQ(OpType::Get, middle->Op().GetType());

  // Check right Get
  auto right = GetSingleGroupExpression(memo, group_expr, 1);
  EXPECT_EQ(OpType::Get, right->Op().GetType());

  std::shared_ptr<OptimizeContext> root_context =
      std::make_shared<OptimizeContext>(&(optimizer.metadata_), nullptr);

  auto task_stack =
      std::unique_ptr<OptimizerTaskStack>(new OptimizerTaskStack());
  optimizer.metadata_.SetTaskPool(task_stack.get());
  task_stack->Push(
      new ApplyRule(group_expr, new InnerJoinAssociativity, root_context));

  while (!task_stack->Empty()) {
    auto task = task_stack->Pop();
    task->execute();
  }

  LOG_DEBUG("Executed all tasks");

  // Check plan is now: left JOIN (middle JOIN right)
  // Check Parent join
  EXPECT_EQ(OpType::InnerJoin, group_expr->Op().GetType());
  join_op = group_expr->Op().As<LogicalInnerJoin>();
  EXPECT_EQ(0, join_op->join_predicates.size());
  EXPECT_EQ(2, group_expr->GetChildrenGroupsSize());
  LOG_DEBUG("Parent join: OK");

  // Check left Get
  // TODO: Not sure why left is at index 1, but the (middle JOIN right) is at
  // index 0
  left = GetSingleGroupExpression(memo, group_expr, 1);
  EXPECT_EQ(OpType::Get, left->Op().GetType());
  LOG_DEBUG("Left Leaf: OK");

  // Check (right JOIN right)
  auto r_group_expr = GetSingleGroupExpression(memo, group_expr, 0);
  EXPECT_EQ(OpType::InnerJoin, r_group_expr->Op().GetType());
  middle = GetSingleGroupExpression(memo, r_group_expr, 0);
  right = GetSingleGroupExpression(memo, r_group_expr, 1);
  EXPECT_EQ(OpType::Get, middle->Op().GetType());
  EXPECT_EQ(OpType::Get, right->Op().GetType());
  LOG_DEBUG("Right join: OK");

  txn_manager.CommitTransaction(txn);
}

}  // namespace test
}  // namespace peloton
