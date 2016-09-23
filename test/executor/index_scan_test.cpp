//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// index_scan_test.cpp
//
// Identification: test/executor/index_scan_test.cpp
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <memory>

#include "catalog/catalog.h"
#include "common/harness.h"
#include "common/logger.h"
#include "common/statement.h"
#include "common/types.h"
#include "common/value_factory.h"
#include "concurrency/transaction_manager_factory.h"
#include "executor/create_executor.h"
#include "executor/delete_executor.h"
#include "executor/executor_context.h"
#include "executor/index_scan_executor.h"
#include "executor/insert_executor.h"
#include "executor/logical_tile.h"
#include "executor/logical_tile_factory.h"
#include "executor/plan_executor.h"
#include "optimizer/simple_optimizer.h"
#include "parser/parser.h"
#include "planner/create_plan.h"
#include "planner/delete_plan.h"
#include "planner/index_scan_plan.h"
#include "planner/insert_plan.h"
#include "storage/data_table.h"

#include "executor/executor_tests_util.h"

using ::testing::NotNull;
using ::testing::Return;

namespace peloton {
namespace test {

class IndexScanTests : public PelotonTest {};

// Index scan of table with index predicate.
TEST_F(IndexScanTests, IndexPredicateTest) {
  // First, generate the table with index
  std::unique_ptr<storage::DataTable> data_table(
      ExecutorTestsUtil::CreateAndPopulateTable());

  // Column ids to be added to logical tile after scan.
  std::vector<oid_t> column_ids({0, 1, 3});

  //===--------------------------------------------------------------------===//
  // ATTR 0 <= 110
  //===--------------------------------------------------------------------===//

  auto index = data_table->GetIndex(0);
  std::vector<oid_t> key_column_ids;
  std::vector<ExpressionType> expr_types;
  std::vector<common::Value *> values;
  std::vector<expression::AbstractExpression *> runtime_keys;

  key_column_ids.push_back(0);
  expr_types.push_back(
      ExpressionType::EXPRESSION_TYPE_COMPARE_LESSTHANOREQUALTO);
  values.push_back(common::ValueFactory::GetIntegerValue(110).Copy());

  // Create index scan desc

  planner::IndexScanPlan::IndexScanDesc index_scan_desc(
      index, key_column_ids, expr_types, values, runtime_keys);

  expression::AbstractExpression *predicate = nullptr;

  // Create plan node.
  planner::IndexScanPlan node(data_table.get(), predicate, column_ids,
                              index_scan_desc);

  auto &txn_manager = concurrency::TransactionManagerFactory::GetInstance();
  auto txn = txn_manager.BeginTransaction();
  std::unique_ptr<executor::ExecutorContext> context(
      new executor::ExecutorContext(txn));

  // Run the executor
  executor::IndexScanExecutor executor(&node, context.get());
  int expected_num_tiles = 3;

  EXPECT_TRUE(executor.Init());

  std::vector<std::unique_ptr<executor::LogicalTile>> result_tiles;

  for (int i = 0; i < expected_num_tiles; i++) {
    EXPECT_TRUE(executor.Execute());
    std::unique_ptr<executor::LogicalTile> result_tile(executor.GetOutput());
    EXPECT_THAT(result_tile, NotNull());
    result_tiles.emplace_back(result_tile.release());
  }

  EXPECT_FALSE(executor.Execute());
  EXPECT_EQ(result_tiles.size(), expected_num_tiles);
  EXPECT_EQ(result_tiles[0].get()->GetTupleCount(), 5);
  EXPECT_EQ(result_tiles[1].get()->GetTupleCount(), 5);
  EXPECT_EQ(result_tiles[2].get()->GetTupleCount(), 2);

  txn_manager.CommitTransaction(txn);
}

TEST_F(IndexScanTests, MultiColumnPredicateTest) {
  // First, generate the table with index
  std::unique_ptr<storage::DataTable> data_table(
      ExecutorTestsUtil::CreateAndPopulateTable());

  // Column ids to be added to logical tile after scan.
  std::vector<oid_t> column_ids({0, 1, 3});

  //===--------------------------------------------------------------------===//
  // ATTR 1 > 50 & ATTR 0 < 70
  //===--------------------------------------------------------------------===//

  auto index = data_table->GetIndex(1);
  std::vector<oid_t> key_column_ids;
  std::vector<ExpressionType> expr_types;
  std::vector<common::Value *> values;
  std::vector<expression::AbstractExpression *> runtime_keys;

  key_column_ids.push_back(1);
  key_column_ids.push_back(0);
  expr_types.push_back(ExpressionType::EXPRESSION_TYPE_COMPARE_GREATERTHAN);
  expr_types.push_back(ExpressionType::EXPRESSION_TYPE_COMPARE_LESSTHAN);
  values.push_back(common::ValueFactory::GetIntegerValue(50).Copy());
  values.push_back(common::ValueFactory::GetIntegerValue(70).Copy());

  // Create index scan desc

  planner::IndexScanPlan::IndexScanDesc index_scan_desc(
      index, key_column_ids, expr_types, values, runtime_keys);

  expression::AbstractExpression *predicate = nullptr;

  // Create plan node.
  planner::IndexScanPlan node(data_table.get(), predicate, column_ids,
                              index_scan_desc);

  auto &txn_manager = concurrency::TransactionManagerFactory::GetInstance();
  auto txn = txn_manager.BeginTransaction();
  std::unique_ptr<executor::ExecutorContext> context(
      new executor::ExecutorContext(txn));

  // Run the executor
  executor::IndexScanExecutor executor(&node, context.get());
  int expected_num_tiles = 1;

  EXPECT_TRUE(executor.Init());

  std::vector<std::unique_ptr<executor::LogicalTile>> result_tiles;

  for (int i = 0; i < expected_num_tiles; i++) {
    EXPECT_TRUE(executor.Execute());
    std::unique_ptr<executor::LogicalTile> result_tile(executor.GetOutput());
    EXPECT_THAT(result_tile, NotNull());
    result_tiles.emplace_back(result_tile.release());
  }

  EXPECT_FALSE(executor.Execute());
  EXPECT_EQ(result_tiles.size(), expected_num_tiles);
  EXPECT_EQ(result_tiles[0].get()->GetTupleCount(), 2);

  txn_manager.CommitTransaction(txn);
}

void ShowTable(std::string database_name, std::string table_name) {
  auto table = catalog::Catalog::GetInstance()->GetTableWithName(database_name,
                                                                 table_name);
  std::unique_ptr<Statement> statement;
  auto &peloton_parser = parser::Parser::GetInstance();
  bridge::peloton_status status;
  std::vector<common::Value *> params;
  std::vector<ResultType> result;
  statement.reset(new Statement("SELECT", "SELECT * FROM " + table->GetName()));
  auto select_stmt =
      peloton_parser.BuildParseTree("SELECT * FROM " + table->GetName());
  statement->SetPlanTree(
      optimizer::SimpleOptimizer::BuildPelotonPlanTree(select_stmt));
  bridge::PlanExecutor::PrintPlan(statement->GetPlanTree().get(), "Plan");
  std::vector<int> result_format;
  result_format =
      std::move(std::vector<int>(statement->GetTupleDescriptor().size(), 0));
  status = bridge::PlanExecutor::ExecutePlan(statement->GetPlanTree().get(),
                                             params, result, result_format);
}

void ExecuteSQLQuery(const std::string statement_name,
                     const std::string query_string) {
  LOG_INFO("Query: %s", query_string.c_str());
  static std::unique_ptr<Statement> statement;
  statement.reset(new Statement(statement_name, query_string));
  auto &peloton_parser = parser::Parser::GetInstance();
  LOG_INFO("Building parse tree...");
  auto insert_stmt = peloton_parser.BuildParseTree(query_string);
  LOG_INFO("Building parse tree completed!");
  LOG_INFO("Building plan tree...");
  statement->SetPlanTree(
      optimizer::SimpleOptimizer::BuildPelotonPlanTree(insert_stmt));
  LOG_INFO("Building plan tree completed!");
  std::vector<common::Value *> params;
  std::vector<ResultType> result;
  bridge::PlanExecutor::PrintPlan(statement->GetPlanTree().get(), "Plan");
  LOG_INFO("Executing plan...");
  std::vector<int> result_format;
  result_format =
      std::move(std::vector<int>(statement->GetTupleDescriptor().size(), 0));
  UNUSED_ATTRIBUTE bridge::peloton_status status =
      bridge::PlanExecutor::ExecutePlan(statement->GetPlanTree().get(), params,
                                        result, result_format);
  LOG_INFO("Statement executed. Result: %d", status.m_result);
  ShowTable(DEFAULT_DB_NAME, "department_table");
}

TEST_F(IndexScanTests, SQLTest) {
  LOG_INFO("Bootstrapping...");
  catalog::Catalog::GetInstance()->CreateDatabase(DEFAULT_DB_NAME, nullptr);
  LOG_INFO("Bootstrapping completed!");

  // Create a table first
  LOG_INFO("Creating a table...");
  auto id_column = catalog::Column(
      common::Type::INTEGER, common::Type::GetTypeSize(common::Type::INTEGER),
      "dept_id", true);
  // Set dept_id as the primary key (only that we can have a primary key index)
  catalog::Constraint constraint(CONSTRAINT_TYPE_PRIMARY, "con_primary");
  id_column.AddConstraint(constraint);
  auto name_column =
      catalog::Column(common::Type::VARCHAR, 32, "dept_name", false);

  std::unique_ptr<catalog::Schema> table_schema(
      new catalog::Schema({id_column, name_column}));
  auto &txn_manager = concurrency::TransactionManagerFactory::GetInstance();
  auto txn = txn_manager.BeginTransaction();
  std::unique_ptr<executor::ExecutorContext> context(
      new executor::ExecutorContext(txn));
  planner::CreatePlan node("department_table", DEFAULT_DB_NAME,
                           std::move(table_schema),
                           CreateType::CREATE_TYPE_TABLE);
  executor::CreateExecutor create_executor(&node, context.get());
  create_executor.Init();
  create_executor.Execute();
  txn_manager.CommitTransaction(txn);
  EXPECT_EQ(catalog::Catalog::GetInstance()
                ->GetDatabaseWithName(DEFAULT_DB_NAME)
                ->GetTableCount(),
            1);
  LOG_INFO("Table created!");

  // Inserting a tuple end-to-end
  LOG_INFO("Inserting a tuple...");
  ExecuteSQLQuery(
      "INSERT",
      "INSERT INTO department_table(dept_id,dept_name) VALUES (1,'hello_1');");
  LOG_INFO("Tuple inserted!");

  LOG_INFO("Inserting a tuple...");
  ExecuteSQLQuery(
      "INSERT",
      "INSERT INTO department_table(dept_id,dept_name) VALUES (2, 'hello_2');");
  LOG_INFO("Tuple inserted!");

  LOG_INFO("Inserting a tuple...");
  ExecuteSQLQuery(
      "INSERT",
      "INSERT INTO department_table(dept_id,dept_name) VALUES (3,'hello_2');");
  LOG_INFO("Tuple inserted!");

  LOG_INFO("Select a tuple...");
  ExecuteSQLQuery("SELECT STAR",
                  "SELECT * FROM department_table WHERE dept_id = 1;");
  LOG_INFO("Tuple selected");

  LOG_INFO("Select a column...");
  ExecuteSQLQuery("SELECT COLUMN",
                  "SELECT dept_name FROM department_table WHERE dept_id = 2;");
  LOG_INFO("Column selected");

  LOG_INFO("Select COUNT(*)...");
  ExecuteSQLQuery("SELECT AGGREGATE",
                  "SELECT COUNT(*) FROM department_table WHERE dept_id < 3;");
  LOG_INFO("Aggregation selected");

  // free the database just created
  txn = txn_manager.BeginTransaction();
  catalog::Catalog::GetInstance()->DropDatabaseWithName(DEFAULT_DB_NAME, txn);
  txn_manager.CommitTransaction(txn);
}

}  // namespace test
}  // namespace peloton
