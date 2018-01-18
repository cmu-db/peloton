//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// delete_test.cpp
//
// Identification: test/executor/delete_test.cpp
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <cstdio>

#include "executor/testing_executor_util.h"
#include "sql/testing_sql_util.h"
#include "common/harness.h"

#include "catalog/catalog.h"
#include "catalog/schema.h"
#include "common/logger.h"
#include "common/statement.h"
#include "concurrency/transaction_context.h"
#include "concurrency/transaction_manager_factory.h"
#include "executor/abstract_executor.h"
#include "executor/create_executor.h"
#include "executor/delete_executor.h"
#include "executor/executor_context.h"
#include "executor/insert_executor.h"
#include "executor/logical_tile.h"
#include "executor/logical_tile_factory.h"
#include "executor/plan_executor.h"
#include "executor/seq_scan_executor.h"
#include "executor/update_executor.h"
#include "expression/abstract_expression.h"
#include "expression/expression_util.h"
#include "optimizer/optimizer.h"
#include "parser/postgresparser.h"
#include "planner/create_plan.h"
#include "planner/delete_plan.h"
#include "planner/insert_plan.h"
#include "planner/plan_util.h"
#include "planner/seq_scan_plan.h"
#include "planner/update_plan.h"
#include "storage/data_table.h"
#include "storage/tile_group_factory.h"
#include "traffic_cop/traffic_cop.h"
#include "common/internal_types.h"
#include "type/value.h"
#include "type/value_factory.h"

#include "common/harness.h"
#include "executor/mock_executor.h"

namespace peloton {
namespace test {

//===--------------------------------------------------------------------===//
// Update Tests
//===--------------------------------------------------------------------===//

class UpdateTests : public PelotonTest {};

namespace {

storage::DataTable *CreateTable() {
  const int tuple_count = TESTS_TUPLES_PER_TILEGROUP;
  std::unique_ptr<storage::DataTable> table(TestingExecutorUtil::CreateTable());

  // Schema for first tile group. Vertical partition is 2, 2.
  std::vector<catalog::Schema> schemas1(
      {catalog::Schema({TestingExecutorUtil::GetColumnInfo(0),
                        TestingExecutorUtil::GetColumnInfo(1)}),
       catalog::Schema({TestingExecutorUtil::GetColumnInfo(2),
                        TestingExecutorUtil::GetColumnInfo(3)})});

  // Schema for second tile group. Vertical partition is 1, 3.
  std::vector<catalog::Schema> schemas2(
      {catalog::Schema({TestingExecutorUtil::GetColumnInfo(0)}),
       catalog::Schema({TestingExecutorUtil::GetColumnInfo(1),
                        TestingExecutorUtil::GetColumnInfo(2),
                        TestingExecutorUtil::GetColumnInfo(3)})});

  TestingHarness::GetInstance().GetNextTileGroupId();

  std::map<oid_t, std::pair<oid_t, oid_t>> column_map1;
  column_map1[0] = std::make_pair(0, 0);
  column_map1[1] = std::make_pair(0, 1);
  column_map1[2] = std::make_pair(1, 0);
  column_map1[3] = std::make_pair(1, 1);

  std::map<oid_t, std::pair<oid_t, oid_t>> column_map2;
  column_map2[0] = std::make_pair(0, 0);
  column_map2[1] = std::make_pair(1, 0);
  column_map2[2] = std::make_pair(1, 1);
  column_map2[3] = std::make_pair(1, 2);

  // Create tile groups.
  table->AddTileGroup(std::shared_ptr<storage::TileGroup>(
      storage::TileGroupFactory::GetTileGroup(
          INVALID_OID, INVALID_OID,
          TestingHarness::GetInstance().GetNextTileGroupId(), table.get(),
          schemas1, column_map1, tuple_count)));

  table->AddTileGroup(std::shared_ptr<storage::TileGroup>(
      storage::TileGroupFactory::GetTileGroup(
          INVALID_OID, INVALID_OID,
          TestingHarness::GetInstance().GetNextTileGroupId(), table.get(),
          schemas2, column_map2, tuple_count)));

  TestingExecutorUtil::PopulateTiles(table->GetTileGroup(0), tuple_count);
  TestingExecutorUtil::PopulateTiles(table->GetTileGroup(1), tuple_count);
  TestingExecutorUtil::PopulateTiles(table->GetTileGroup(2), tuple_count);

  return table.release();
}

TEST_F(UpdateTests, MultiColumnUpdates) {
  // Create table.
  std::unique_ptr<storage::DataTable> table(CreateTable());
  //  storage::DataTable* table = CreateTable();
  LOG_INFO("%s", table->GetInfo().c_str());

  // Do a select to get the original values
  //  std::unique_ptr<Statement> statement;
  //  auto& peloton_parser = parser::PostgresParser::GetInstance();
  //  auto select_stmt =
  //      peloton_parser.BuildParseTree("SELECT * FROM test_table LIMIT 1;");
  //  statement->SetPlanTree(
  //      optimizer::SimpleOptimizer::BuildPelotonPlanTree(select_stmt));
  //  std::vector<type::Value> params;
  //  std::vector<ResultType> result;
  //  executor::PlanExecutor::PrintPlan(statement->GetPlanTree().get(), "Plan");
  //
  //  std::vector<int> result_format;
  //  auto tuple_descriptor =
  //      traffic_cop::TrafficCop::GetInstance().GenerateTupleDescriptor(
  //          select_stmt->GetStatement(0));
  //  result_format = std::move(std::vector<int>(tuple_descriptor.size(), 0));
  //  UNUSED_ATTRIBUTE executor::ExecutionResult status =
  //      executor::PlanExecutor::ExecutePlan(statement->GetPlanTree().get(),
  //      params,
  //                                        result, result_format);
  //  LOG_INFO("Statement executed. Result: %s",
  //  ResultTypeToString(status.m_result).c_str());
}

TEST_F(UpdateTests, UpdatingOld) {
  LOG_INFO("Bootstrapping...");
  auto catalog = catalog::Catalog::GetInstance();
  auto &txn_manager = concurrency::TransactionManagerFactory::GetInstance();
  auto txn = txn_manager.BeginTransaction();
  catalog->CreateDatabase(DEFAULT_DB_NAME, txn);
  LOG_INFO("Bootstrapping completed!");

  std::unique_ptr<optimizer::AbstractOptimizer> optimizer(
      new optimizer::Optimizer);
  auto &traffic_cop = tcop::TrafficCop::GetInstance();
  traffic_cop.SetTaskCallback(TestingSQLUtil::UtilTestTaskCallback,
                              &TestingSQLUtil::counter_);
  // Create a table first
  LOG_INFO("Creating a table...");
  auto id_column = catalog::Column(
      type::TypeId::INTEGER, type::Type::GetTypeSize(type::TypeId::INTEGER),
      "dept_id", true);
  catalog::Constraint constraint(ConstraintType::PRIMARY, "con_primary");
  id_column.AddConstraint(constraint);
  auto manager_id_column = catalog::Column(
      type::TypeId::INTEGER, type::Type::GetTypeSize(type::TypeId::INTEGER),
      "manager_id", true);
  auto name_column =
      catalog::Column(type::TypeId::VARCHAR, 32, "dept_name", false);

  std::unique_ptr<catalog::Schema> table_schema(
      new catalog::Schema({id_column, manager_id_column, name_column}));
  std::unique_ptr<executor::ExecutorContext> context(
      new executor::ExecutorContext(txn));
  planner::CreatePlan node("department_table", DEFAULT_DB_NAME,
                           std::move(table_schema), CreateType::TABLE);
  executor::CreateExecutor create_executor(&node, context.get());
  create_executor.Init();
  create_executor.Execute();
  EXPECT_EQ(catalog->GetDatabaseWithName(DEFAULT_DB_NAME, txn)->GetTableCount(),
            1);

  LOG_INFO("Table created!");

  storage::DataTable *table =
      catalog->GetTableWithName(DEFAULT_DB_NAME, "department_table", txn);
  txn_manager.CommitTransaction(txn);

  // Inserting a tuple end-to-end
  txn = txn_manager.BeginTransaction();
  traffic_cop.SetTcopTxnState(txn);

  LOG_INFO("Inserting a tuple...");
  LOG_INFO(
      "Query: INSERT INTO department_table(dept_id,manager_id,dept_name) "
      "VALUES (1,12,'hello_1');");
  std::unique_ptr<Statement> statement;
  statement.reset(new Statement("INSERT",
                                "INSERT INTO "
                                "department_table(dept_id,manager_id,dept_name)"
                                " VALUES (1,12,'hello_1');"));
  auto &peloton_parser = parser::PostgresParser::GetInstance();
  LOG_INFO("Building parse tree...");
  auto insert_stmt = peloton_parser.BuildParseTree(
      "INSERT INTO department_table(dept_id,manager_id,dept_name) VALUES "
      "(1,12,'hello_1');");
  LOG_INFO("Building parse tree completed!");
  LOG_INFO("Building plan tree...");

  statement->SetPlanTree(
      optimizer->BuildPelotonPlanTree(insert_stmt, DEFAULT_DB_NAME, txn));
  LOG_INFO("Building plan tree completed!");
  std::vector<type::Value> params;
  std::vector<ResultValue> result;
  LOG_INFO("Executing plan...\n%s",
           planner::PlanUtil::GetInfo(statement->GetPlanTree().get()).c_str());

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
  LOG_INFO("Tuple inserted!");
  traffic_cop.CommitQueryHelper();

  LOG_INFO("%s", table->GetInfo().c_str());

  // Now Updating end-to-end
  txn = txn_manager.BeginTransaction();
  traffic_cop.SetTcopTxnState(txn);

  LOG_INFO("Updating a tuple...");
  LOG_INFO(
      "Query: UPDATE department_table SET dept_name = 'CS' WHERE dept_id = 1");
  statement.reset(new Statement(
      "UPDATE",
      "UPDATE department_table SET dept_name = 'CS' WHERE dept_id = 1"));
  LOG_INFO("Building parse tree...");
  auto update_stmt = peloton_parser.BuildParseTree(
      "UPDATE department_table SET dept_name = 'CS' WHERE dept_id = 1");
  LOG_INFO("Building parse tree completed!");
  LOG_INFO("Building plan tree...");

  statement->SetPlanTree(
      optimizer->BuildPelotonPlanTree(update_stmt, DEFAULT_DB_NAME, txn));
  LOG_INFO("Building plan tree completed!");
  LOG_INFO("Executing plan...\n%s",
           planner::PlanUtil::GetInfo(statement->GetPlanTree().get()).c_str());
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
  LOG_INFO("Tuple Updated!");
  traffic_cop.CommitQueryHelper();

  LOG_INFO("%s", table->GetInfo().c_str());

  txn = txn_manager.BeginTransaction();
  traffic_cop.SetTcopTxnState(txn);

  LOG_INFO("Updating another tuple...");
  LOG_INFO(
      "Query: UPDATE department_table SET manager_id = manager_id + 1 WHERE "
      "dept_id = 1");
  statement.reset(new Statement("UPDATE",
                                "UPDATE department_table SET manager_id = "
                                "manager_id + 1 WHERE dept_id = 1"));
  LOG_INFO("Building parse tree...");
  update_stmt = peloton_parser.BuildParseTree(
      "UPDATE department_table SET manager_id = manager_id + 1 WHERE dept_id = "
      "1");
  LOG_INFO("Building parse tree completed!");
  LOG_INFO("Building plan tree...");

  statement->SetPlanTree(
      optimizer->BuildPelotonPlanTree(update_stmt, DEFAULT_DB_NAME, txn));
  LOG_INFO("Building plan tree completed!");
  LOG_INFO("Executing plan...\n%s",
           planner::PlanUtil::GetInfo(statement->GetPlanTree().get()).c_str());
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
  LOG_INFO("Tuple Updated!");
  traffic_cop.CommitQueryHelper();

  LOG_INFO("%s", table->GetInfo().c_str());

  txn = txn_manager.BeginTransaction();
  traffic_cop.SetTcopTxnState(txn);
  LOG_INFO("Updating primary key...");
  LOG_INFO("Query: UPDATE department_table SET dept_id = 2 WHERE dept_id = 1");
  statement.reset(new Statement(
      "UPDATE", "UPDATE department_table SET dept_id = 2 WHERE dept_id = 1"));
  LOG_INFO("Building parse tree...");
  update_stmt = peloton_parser.BuildParseTree(
      "UPDATE department_table SET dept_id = 2 WHERE dept_id = 1");
  LOG_INFO("Building parse tree completed!");
  LOG_INFO("Building plan tree...");
  statement->SetPlanTree(
      optimizer->BuildPelotonPlanTree(update_stmt, DEFAULT_DB_NAME, txn));
  LOG_INFO("Building plan tree completed!");
  LOG_INFO("Executing plan...\n%s",
           planner::PlanUtil::GetInfo(statement->GetPlanTree().get()).c_str());
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
  LOG_INFO("Tuple Updated!");
  traffic_cop.CommitQueryHelper();

  LOG_INFO("%s", table->GetInfo().c_str());

  // Deleting now
  txn = txn_manager.BeginTransaction();
  traffic_cop.SetTcopTxnState(txn);

  LOG_INFO("Deleting a tuple...");
  LOG_INFO("Query: DELETE FROM department_table WHERE dept_name = 'CS'");
  statement.reset(new Statement(
      "DELETE", "DELETE FROM department_table WHERE dept_name = 'CS'"));
  LOG_INFO("Building parse tree...");
  auto delete_stmt = peloton_parser.BuildParseTree(
      "DELETE FROM department_table WHERE dept_name = 'CS'");
  LOG_INFO("Building parse tree completed!");
  LOG_INFO("Building plan tree...");

  statement->SetPlanTree(
      optimizer->BuildPelotonPlanTree(delete_stmt, DEFAULT_DB_NAME, txn));
  LOG_INFO("Building plan tree completed!");
  LOG_INFO("Executing plan...\n%s",
           planner::PlanUtil::GetInfo(statement->GetPlanTree().get()).c_str());
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
  LOG_INFO("Tuple deleted!");
  traffic_cop.CommitQueryHelper();

  // free the database just created
  txn = txn_manager.BeginTransaction();
  catalog->DropDatabaseWithName(DEFAULT_DB_NAME, txn);
  txn_manager.CommitTransaction(txn);
}
}  // namespace?
}  // namespace test
}  // namespace peloton
