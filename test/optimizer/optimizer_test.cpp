//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// optimizer_test.cpp
//
// Identification: test/optimizer/optimizer_test.cpp
//
// Copyright (c) 2015-2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "common/harness.h"

#include "catalog/catalog.h"
#include "common/logger.h"
#include "common/statement.h"
#include "concurrency/transaction_manager_factory.h"
#include "executor/create_executor.h"
#include "executor/insert_executor.h"
#include "executor/plan_executor.h"
#include "optimizer/optimizer.h"
#include "parser/mock_sql_statement.h"
#include "parser/postgresparser.h"
#include "planner/create_plan.h"
#include "planner/delete_plan.h"
#include "planner/insert_plan.h"
#include "planner/update_plan.h"
#include "sql/testing_sql_util.h"
#include "planner/seq_scan_plan.h"
#include "planner/abstract_join_plan.h"
#include "planner/hash_join_plan.h"
#include "binder/bind_node_visitor.h"
#include "traffic_cop/traffic_cop.h"
#include "expression/tuple_value_expression.h"
#include "optimizer/mock_task.h"
#include "optimizer/operators.h"
#include "optimizer/rule_impls.h"
#include "traffic_cop/traffic_cop.h"

namespace peloton {
namespace test {

//===--------------------------------------------------------------------===//
// Binding Tests
//===--------------------------------------------------------------------===//

using namespace optimizer;

class OptimizerTests : public PelotonTest {
 protected:
  GroupExpression *GetSingleGroupExpression(Memo &memo, GroupExpression *expr,
                                            int child_group_idx) {
    auto group = memo.GetGroupByID(expr->GetChildGroupId(child_group_idx));
    EXPECT_EQ(1, group->GetLogicalExpressions().size());
    return group->GetLogicalExpressions()[0].get();
  }

  virtual void TearDown() override {
    // TODO don't assume that all tests will need a test database
    // Destroy test database
    auto &txn_manager = concurrency::TransactionManagerFactory::GetInstance();
    auto txn = txn_manager.BeginTransaction();
    catalog::Catalog::GetInstance()->DropDatabaseWithName(DEFAULT_DB_NAME, txn);
    txn_manager.CommitTransaction(txn);

    // Call parent virtual function
    PelotonTest::TearDown();
  }
};

// Test whether update stament will use index scan plan
// TODO: Split the tests into separate test cases.
TEST_F(OptimizerTests, HashJoinTest) {
  LOG_INFO("Bootstrapping...");
  auto &txn_manager = concurrency::TransactionManagerFactory::GetInstance();
  auto txn = txn_manager.BeginTransaction();
  catalog::Catalog::GetInstance()->CreateDatabase(DEFAULT_DB_NAME, txn);
  txn_manager.CommitTransaction(txn);
  LOG_INFO("Bootstrapping completed!");

  optimizer::Optimizer optimizer;
  auto &traffic_cop = tcop::TrafficCop::GetInstance();
  traffic_cop.SetTaskCallback(TestingSQLUtil::UtilTestTaskCallback,
                              &TestingSQLUtil::counter_);

  // Create a table first
  txn = txn_manager.BeginTransaction();
  traffic_cop.SetTcopTxnState(txn);
  LOG_INFO("Creating table");
  LOG_INFO("Query: CREATE TABLE table_a(aid INT PRIMARY KEY,value INT);");
  std::unique_ptr<Statement> statement;
  statement.reset(new Statement(
      "CREATE", "CREATE TABLE table_a(aid INT PRIMARY KEY,value INT);"));

  auto &peloton_parser = parser::PostgresParser::GetInstance();

  auto create_stmt = peloton_parser.BuildParseTree(
      "CREATE TABLE table_a(aid INT PRIMARY KEY,value INT);");
  auto bind_node_visitor = binder::BindNodeVisitor(txn, DEFAULT_DB_NAME);
  bind_node_visitor.BindNameToNode(create_stmt->GetStatement(0));

  statement->SetPlanTree(optimizer.BuildPelotonPlanTree(create_stmt, txn));

  std::vector<type::Value> params;
  std::vector<ResultValue> result;
  std::vector<int> result_format;
  result_format = std::vector<int>(statement->GetTupleDescriptor().size(), 0);
  TestingSQLUtil::counter_.store(1);
  executor::ExecutionResult status = traffic_cop.ExecuteHelper(
      statement->GetPlanTree(), params, result, result_format);
  if (traffic_cop.GetQueuing()) {
    TestingSQLUtil::ContinueAfterComplete();
    traffic_cop.ExecuteStatementPlanGetResult();
    status = traffic_cop.p_status_;
    traffic_cop.SetQueuing(false);
  }
  LOG_INFO("Statement executed. Result: %s",
           ResultTypeToString(status.m_result).c_str());
  LOG_INFO("Table Created");
  traffic_cop.CommitQueryHelper();

  txn = txn_manager.BeginTransaction();
  EXPECT_EQ(catalog::Catalog::GetInstance()
                ->GetDatabaseWithName(DEFAULT_DB_NAME, txn)
                ->GetTableCount(),
            1);

  traffic_cop.SetTcopTxnState(txn);
  LOG_INFO("Creating table");
  LOG_INFO("Query: CREATE TABLE table_b(bid INT PRIMARY KEY,value INT);");
  statement.reset(new Statement(
      "CREATE", "CREATE TABLE table_b(bid INT PRIMARY KEY,value INT);"));

  create_stmt = peloton_parser.BuildParseTree(
      "CREATE TABLE table_b(bid INT PRIMARY KEY,value INT);");

  bind_node_visitor = binder::BindNodeVisitor(txn, DEFAULT_DB_NAME);
  bind_node_visitor.BindNameToNode(create_stmt->GetStatement(0));

  statement->SetPlanTree(optimizer.BuildPelotonPlanTree(create_stmt, txn));

  result_format = std::vector<int>(statement->GetTupleDescriptor().size(), 0);
  TestingSQLUtil::counter_.store(1);
  status = traffic_cop.ExecuteHelper(statement->GetPlanTree(), params, result,
                                     result_format);
  if (traffic_cop.GetQueuing()) {
    TestingSQLUtil::ContinueAfterComplete();
    traffic_cop.ExecuteStatementPlanGetResult();
    status = traffic_cop.p_status_;
    traffic_cop.SetQueuing(false);
  }
  LOG_INFO("Statement executed. Result: %s",
           ResultTypeToString(status.m_result).c_str());
  LOG_INFO("Table Created");
  traffic_cop.CommitQueryHelper();

  txn = txn_manager.BeginTransaction();
  EXPECT_EQ(catalog::Catalog::GetInstance()
                ->GetDatabaseWithName(DEFAULT_DB_NAME, txn)
                ->GetTableCount(),
            2);

  // Inserting a tuple to table_a
  traffic_cop.SetTcopTxnState(txn);
  LOG_INFO("Inserting a tuple...");
  LOG_INFO("Query: INSERT INTO table_a(aid, value) VALUES (1,1);");
  statement.reset(new Statement(
      "INSERT", "INSERT INTO table_a(aid, value) VALUES (1, 1);"));

  auto insert_stmt = peloton_parser.BuildParseTree(
      "INSERT INTO table_a(aid, value) VALUES (1, 1);");

  bind_node_visitor = binder::BindNodeVisitor(txn, DEFAULT_DB_NAME);
  bind_node_visitor.BindNameToNode(insert_stmt->GetStatement(0));

  statement->SetPlanTree(optimizer.BuildPelotonPlanTree(insert_stmt, txn));

  result_format = std::vector<int>(statement->GetTupleDescriptor().size(), 0);
  TestingSQLUtil::counter_.store(1);
  status = traffic_cop.ExecuteHelper(statement->GetPlanTree(), params, result,
                                     result_format);
  if (traffic_cop.GetQueuing()) {
    TestingSQLUtil::ContinueAfterComplete();
    traffic_cop.ExecuteStatementPlanGetResult();
    status = traffic_cop.p_status_;
    traffic_cop.SetQueuing(false);
  }
  LOG_INFO("Statement executed. Result: %s",
           ResultTypeToString(status.m_result).c_str());
  LOG_INFO("Tuple inserted to table_a!");
  traffic_cop.CommitQueryHelper();

  // Inserting a tuple to table_b
  txn = txn_manager.BeginTransaction();
  traffic_cop.SetTcopTxnState(txn);
  LOG_INFO("Inserting a tuple...");
  LOG_INFO("Query: INSERT INTO table_b(bid, value) VALUES (1,2);");
  statement.reset(new Statement(
      "INSERT", "INSERT INTO table_b(bid, value) VALUES (1, 2);"));

  insert_stmt = peloton_parser.BuildParseTree(
      "INSERT INTO table_b(bid, value) VALUES (1, 2);");
  bind_node_visitor = binder::BindNodeVisitor(txn, DEFAULT_DB_NAME);
  bind_node_visitor.BindNameToNode(insert_stmt->GetStatement(0));

  statement->SetPlanTree(optimizer.BuildPelotonPlanTree(insert_stmt, txn));

  result_format = std::vector<int>(statement->GetTupleDescriptor().size(), 0);
  TestingSQLUtil::counter_.store(1);
  status = traffic_cop.ExecuteHelper(statement->GetPlanTree(), params, result,
                                     result_format);
  if (traffic_cop.GetQueuing()) {
    TestingSQLUtil::ContinueAfterComplete();
    traffic_cop.ExecuteStatementPlanGetResult();
    status = traffic_cop.p_status_;
    traffic_cop.SetQueuing(false);
  }
  LOG_INFO("Statement executed. Result: %s",
           ResultTypeToString(status.m_result).c_str());
  LOG_INFO("Tuple inserted to table_b!");
  traffic_cop.CommitQueryHelper();

  txn = txn_manager.BeginTransaction();
  traffic_cop.SetTcopTxnState(txn);
  LOG_INFO("Join ...");
  LOG_INFO("Query: SELECT * FROM table_a INNER JOIN table_b ON aid = bid;");
  statement.reset(new Statement(
      "SELECT", "SELECT * FROM table_a INNER JOIN table_b ON aid = bid;"));

  auto select_stmt = peloton_parser.BuildParseTree(
      "SELECT * FROM table_a INNER JOIN table_b ON aid = bid;");
  bind_node_visitor = binder::BindNodeVisitor(txn, DEFAULT_DB_NAME);
  bind_node_visitor.BindNameToNode(select_stmt->GetStatement(0));

  statement->SetPlanTree(optimizer.BuildPelotonPlanTree(select_stmt, txn));

  result_format = std::vector<int>(4, 0);
  TestingSQLUtil::counter_.store(1);
  status = traffic_cop.ExecuteHelper(statement->GetPlanTree(), params, result,
                                     result_format);
  if (traffic_cop.GetQueuing()) {
    TestingSQLUtil::ContinueAfterComplete();
    traffic_cop.ExecuteStatementPlanGetResult();
    status = traffic_cop.p_status_;
    traffic_cop.SetQueuing(false);
  }
  LOG_INFO("Statement executed. Result: %s",
           ResultTypeToString(status.m_result).c_str());
  LOG_INFO("Join completed!");
  traffic_cop.CommitQueryHelper();

  LOG_INFO("After Join...");
}

TEST_F(OptimizerTests, PredicatePushDownTest) {
  auto &txn_manager = concurrency::TransactionManagerFactory::GetInstance();
  auto txn = txn_manager.BeginTransaction();
  catalog::Catalog::GetInstance()->CreateDatabase(DEFAULT_DB_NAME, txn);
  txn_manager.CommitTransaction(txn);

  TestingSQLUtil::ExecuteSQLQuery(
      "CREATE TABLE test(a INT PRIMARY KEY, b INT, c INT);");
  TestingSQLUtil::ExecuteSQLQuery(
      "CREATE TABLE test1(a INT PRIMARY KEY, b INT, c INT);");

  auto &peloton_parser = parser::PostgresParser::GetInstance();
  auto stmt = peloton_parser.BuildParseTree(
      "SELECT * FROM test, test1 WHERE test.a = test1.a AND test1.b = 22");

  optimizer::Optimizer optimizer;
  txn = txn_manager.BeginTransaction();

  auto bind_node_visitor = binder::BindNodeVisitor(txn, DEFAULT_DB_NAME);
  bind_node_visitor.BindNameToNode(stmt->GetStatement(0));

  auto plan = optimizer.BuildPelotonPlanTree(stmt, txn);
  txn_manager.CommitTransaction(txn);

  auto &child_plan = plan->GetChildren();
  EXPECT_EQ(2, child_plan.size());

  auto l_plan = dynamic_cast<planner::SeqScanPlan *>(child_plan[0].get());
  auto r_plan = dynamic_cast<planner::SeqScanPlan *>(
      child_plan[1]->GetChildren()[0].get());
  planner::SeqScanPlan *test_plan = l_plan;
  planner::SeqScanPlan *test1_plan = r_plan;

  if (l_plan->GetTable()->GetName() == "test1") {
    test_plan = r_plan;
    test1_plan = l_plan;
  }

  auto test_predicate = test_plan->GetPredicate();
  EXPECT_EQ(nullptr, test_predicate);
  auto test1_predicate = test1_plan->GetPredicate();
  EXPECT_EQ(ExpressionType::COMPARE_EQUAL,
            test1_predicate->GetExpressionType());
  auto tv = dynamic_cast<expression::TupleValueExpression *>(
      test1_predicate->GetModifiableChild(0));
  EXPECT_TRUE(tv != nullptr);
  EXPECT_EQ("test1", tv->GetTableName());
  EXPECT_EQ("b", tv->GetColumnName());
  auto constant = dynamic_cast<expression::ConstantValueExpression *>(
      test1_predicate->GetModifiableChild(1));
  EXPECT_TRUE(constant != nullptr);
  EXPECT_EQ(22, constant->GetValue().GetAs<int>());
}

TEST_F(OptimizerTests, PushFilterThroughJoinTest) {
  auto &txn_manager = concurrency::TransactionManagerFactory::GetInstance();
  auto txn = txn_manager.BeginTransaction();
  catalog::Catalog::GetInstance()->CreateDatabase(DEFAULT_DB_NAME, txn);
  txn_manager.CommitTransaction(txn);

  TestingSQLUtil::ExecuteSQLQuery(
      "CREATE TABLE test(a INT PRIMARY KEY, b INT, c INT);");
  TestingSQLUtil::ExecuteSQLQuery(
      "CREATE TABLE test1(a INT PRIMARY KEY, b INT, c INT);");

  auto &peloton_parser = parser::PostgresParser::GetInstance();
  auto stmt = peloton_parser.BuildParseTree(
      "SELECT * FROM test, test1 WHERE test.a = test1.a AND test1.b = 22");
  auto parse_tree = stmt->GetStatement(0);
  auto predicates = std::vector<expression::AbstractExpression *>();
  optimizer::util::SplitPredicates(
      reinterpret_cast<parser::SelectStatement *>(parse_tree)
          ->where_clause.get(),
      predicates);

  optimizer::Optimizer optimizer;
  // Only include PushFilterThroughJoin rewrite rule
  optimizer.GetMetadata().rule_set.GetRewriteRulesMap().clear();
  optimizer.GetMetadata().rule_set.AddRewriteRule(
      RewriteRuleSetName::PREDICATE_PUSH_DOWN, new PushFilterThroughJoin());
  txn = txn_manager.BeginTransaction();

  auto bind_node_visitor = binder::BindNodeVisitor(txn, DEFAULT_DB_NAME);
  bind_node_visitor.BindNameToNode(parse_tree);

  std::shared_ptr<GroupExpression> gexpr =
      optimizer.TestInsertQueryTree(parse_tree, txn);
  std::vector<GroupID> child_groups = {gexpr->GetGroupID()};

  std::shared_ptr<GroupExpression> head_gexpr =
      std::make_shared<GroupExpression>(Operator(), child_groups);

  std::shared_ptr<OptimizeContext> root_context =
      std::make_shared<OptimizeContext>(&(optimizer.GetMetadata()), nullptr);
  auto task_stack =
      std::unique_ptr<OptimizerTaskStack>(new OptimizerTaskStack());
  optimizer.GetMetadata().SetTaskPool(task_stack.get());
  task_stack->Push(new TopDownRewrite(gexpr->GetGroupID(), root_context,
                                      RewriteRuleSetName::PREDICATE_PUSH_DOWN));

  while (!task_stack->Empty()) {
    auto task = task_stack->Pop();
    task->execute();
  }

  auto &memo = optimizer.GetMetadata().memo;

  // Check join in the root
  auto group_expr = GetSingleGroupExpression(memo, head_gexpr.get(), 0);
  EXPECT_EQ(OpType::InnerJoin, group_expr->Op().GetType());
  auto join_op = group_expr->Op().As<LogicalInnerJoin>();
  EXPECT_EQ(1, join_op->join_predicates.size());
  EXPECT_TRUE(join_op->join_predicates[0].expr->ExactlyEquals(*predicates[0]));

  // Check left get
  auto l_group_expr = GetSingleGroupExpression(memo, group_expr, 0);
  EXPECT_EQ(OpType::Get, l_group_expr->Op().GetType());
  auto get_op = l_group_expr->Op().As<LogicalGet>();
  EXPECT_TRUE(get_op->predicates.empty());

  // Check right filter
  auto r_group_expr = GetSingleGroupExpression(memo, group_expr, 1);
  EXPECT_EQ(OpType::LogicalFilter, r_group_expr->Op().GetType());
  auto filter_op = r_group_expr->Op().As<LogicalFilter>();
  EXPECT_EQ(1, filter_op->predicates.size());
  EXPECT_TRUE(filter_op->predicates[0].expr->ExactlyEquals(*predicates[1]));

  // Check get below filter
  group_expr = GetSingleGroupExpression(memo, r_group_expr, 0);
  EXPECT_EQ(OpType::Get, l_group_expr->Op().GetType());
  get_op = group_expr->Op().As<LogicalGet>();
  EXPECT_TRUE(get_op->predicates.empty());

  txn_manager.CommitTransaction(txn);
}

TEST_F(OptimizerTests, PredicatePushDownRewriteTest) {
  auto &txn_manager = concurrency::TransactionManagerFactory::GetInstance();
  auto txn = txn_manager.BeginTransaction();
  catalog::Catalog::GetInstance()->CreateDatabase(DEFAULT_DB_NAME, txn);
  txn_manager.CommitTransaction(txn);

  TestingSQLUtil::ExecuteSQLQuery(
      "CREATE TABLE test(a INT PRIMARY KEY, b INT, c INT);");
  TestingSQLUtil::ExecuteSQLQuery(
      "CREATE TABLE test1(a INT PRIMARY KEY, b INT, c INT);");

  auto &peloton_parser = parser::PostgresParser::GetInstance();
  auto stmt = peloton_parser.BuildParseTree(
      "SELECT * FROM test, test1 WHERE test.a = test1.a AND test1.b = 22");
  auto parse_tree = stmt->GetStatement(0);
  auto predicates = std::vector<expression::AbstractExpression *>();
  optimizer::util::SplitPredicates(
      reinterpret_cast<parser::SelectStatement *>(parse_tree)
          ->where_clause.get(),
      predicates);

  optimizer::Optimizer optimizer;
  // Only include PushFilterThroughJoin rewrite rule
  optimizer.GetMetadata().rule_set.GetPredicatePushDownRules().clear();
  optimizer.GetMetadata().rule_set.GetPredicatePushDownRules().emplace_back(
      new PushFilterThroughJoin());
  optimizer.GetMetadata().rule_set.GetPredicatePushDownRules().emplace_back(
      new CombineConsecutiveFilter());
  optimizer.GetMetadata().rule_set.GetPredicatePushDownRules().emplace_back(
      new EmbedFilterIntoGet());

  txn = txn_manager.BeginTransaction();

  auto bind_node_visitor = binder::BindNodeVisitor(txn, DEFAULT_DB_NAME);
  bind_node_visitor.BindNameToNode(parse_tree);

  std::shared_ptr<GroupExpression> gexpr =
      optimizer.TestInsertQueryTree(parse_tree, txn);
  std::vector<GroupID> child_groups = {gexpr->GetGroupID()};

  std::shared_ptr<GroupExpression> head_gexpr =
      std::make_shared<GroupExpression>(Operator(), child_groups);

  std::shared_ptr<OptimizeContext> root_context =
      std::make_shared<OptimizeContext>(&(optimizer.GetMetadata()), nullptr);
  auto task_stack =
      std::unique_ptr<OptimizerTaskStack>(new OptimizerTaskStack());
  optimizer.GetMetadata().SetTaskPool(task_stack.get());
  task_stack->Push(new TopDownRewrite(gexpr->GetGroupID(), root_context,
                                      RewriteRuleSetName::PREDICATE_PUSH_DOWN));

  while (!task_stack->Empty()) {
    auto task = task_stack->Pop();
    task->execute();
  }

  auto &memo = optimizer.GetMetadata().memo;

  // Check join in the root
  auto group_expr = GetSingleGroupExpression(memo, head_gexpr.get(), 0);
  EXPECT_EQ(OpType::InnerJoin, group_expr->Op().GetType());
  auto join_op = group_expr->Op().As<LogicalInnerJoin>();
  EXPECT_EQ(1, join_op->join_predicates.size());
  EXPECT_TRUE(join_op->join_predicates[0].expr->ExactlyEquals(*predicates[0]));

  // Check left get
  auto l_group_expr = GetSingleGroupExpression(memo, group_expr, 0);
  EXPECT_EQ(OpType::Get, l_group_expr->Op().GetType());
  auto get_op = l_group_expr->Op().As<LogicalGet>();
  EXPECT_TRUE(get_op->predicates.empty());

  // Check right filter
  auto r_group_expr = GetSingleGroupExpression(memo, group_expr, 1);
  EXPECT_EQ(OpType::Get, r_group_expr->Op().GetType());
  get_op = r_group_expr->Op().As<LogicalGet>();
  EXPECT_EQ(1, get_op->predicates.size());
  EXPECT_TRUE(get_op->predicates[0].expr->ExactlyEquals(*predicates[1]));

  txn_manager.CommitTransaction(txn);
}

TEST_F(OptimizerTests, ExecuteTaskStackTest) {
  // Currently need database for test teardown
  auto &txn_manager = concurrency::TransactionManagerFactory::GetInstance();
  auto txn = txn_manager.BeginTransaction();
  catalog::Catalog::GetInstance()->CreateDatabase(DEFAULT_DB_NAME, txn);
  txn_manager.CommitTransaction(txn);

  const auto num_tasks = 2;
  optimizer::Optimizer optimizer;
  const int root_group_id = 0;
  const auto root_group =
      new Group(root_group_id, std::unordered_set<std::string>());
  optimizer.GetMetadata().memo.Groups().emplace_back(root_group);

  auto required_prop = std::make_shared<PropertySet>(PropertySet());
  auto root_context =
      std::make_shared<OptimizeContext>(&(optimizer.GetMetadata()), required_prop);
  auto task_stack =
      std::unique_ptr<OptimizerTaskStack>(new OptimizerTaskStack());
  auto &timer = optimizer.GetMetadata().timer;

  // Insert tasks into task stack
  for (unsigned int i = 0; i < num_tasks; i++) {
    auto task = new optimizer::test::MockTask();
    EXPECT_CALL(*task, execute()).Times(1);
    task_stack->Push(task);
  }
  optimizer.GetMetadata().SetTaskPool(task_stack.get());

  const auto start_time = timer.GetDuration();
  optimizer.TestExecuteTaskStack(*task_stack, root_group_id, root_context);
  ASSERT_EQ(timer.GetInvocations(), num_tasks);
  ASSERT_GT(timer.GetDuration(), start_time);
}

}  // namespace test
}  // namespace peloton
