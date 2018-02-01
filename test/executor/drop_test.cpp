//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// drop_test.cpp
//
// Identification: test/executor/drop_test.cpp
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <cstdio>

#include "gtest/gtest.h"

#include "catalog/catalog.h"
#include "catalog/database_catalog.h"
#include "common/harness.h"
#include "concurrency/transaction_manager_factory.h"
#include "executor/create_executor.h"
#include "executor/drop_executor.h"
#include "parser/postgresparser.h"
#include "planner/drop_plan.h"
#include "planner/plan_util.h"

namespace peloton {
namespace test {

#define TEST_DB_NAME "test_db"

//===--------------------------------------------------------------------===//
// Catalog Tests
//===--------------------------------------------------------------------===//

class DropTests : public PelotonTest {};

TEST_F(DropTests, DroppingDatabase) {
  auto catalog = catalog::Catalog::GetInstance();
  catalog->Bootstrap();

  auto &txn_manager = concurrency::TransactionManagerFactory::GetInstance();
  auto txn = txn_manager.BeginTransaction();

  catalog->CreateDatabase(TEST_DB_NAME, txn);
  txn_manager.CommitTransaction(txn);

  txn = txn_manager.BeginTransaction();
  EXPECT_TRUE(catalog->GetDatabaseObject(TEST_DB_NAME, txn).get() != NULL);
  txn_manager.CommitTransaction(txn);

  parser::DropStatement drop_statement(
      parser::DropStatement::EntityType::kDatabase);

  drop_statement.TryBindDatabaseName(TEST_DB_NAME);

  planner::DropPlan drop_plan(&drop_statement);

  // Execute drop database
  txn = txn_manager.BeginTransaction();
  std::unique_ptr<executor::ExecutorContext> context(
      new executor::ExecutorContext(txn));
  executor::DropExecutor DropDBExecutor(&drop_plan, context.get());
  DropDBExecutor.Init();
  DropDBExecutor.Execute();
  txn_manager.CommitTransaction(txn);

  // The database should be deleted now
  txn = txn_manager.BeginTransaction();
  EXPECT_ANY_THROW(
    catalog->GetDatabaseObject(TEST_DB_NAME, txn);
  );
  txn_manager.CommitTransaction(txn);

}

TEST_F(DropTests, DroppingTable) {
  auto catalog = catalog::Catalog::GetInstance();
  catalog->Bootstrap();

  auto &txn_manager = concurrency::TransactionManagerFactory::GetInstance();
  auto txn = txn_manager.BeginTransaction();
  // Insert a table first
  auto id_column = catalog::Column(
      type::TypeId::INTEGER, type::Type::GetTypeSize(type::TypeId::INTEGER),
      "dept_id", true);
  auto name_column =
      catalog::Column(type::TypeId::VARCHAR, 32, "dept_name", false);

  std::unique_ptr<catalog::Schema> table_schema(
      new catalog::Schema({id_column, name_column}));
  std::unique_ptr<catalog::Schema> table_schema2(
      new catalog::Schema({id_column, name_column}));

  catalog->CreateDatabase(TEST_DB_NAME, txn);
  txn_manager.CommitTransaction(txn);

  txn = txn_manager.BeginTransaction();
  catalog->CreateTable(TEST_DB_NAME, "department_table", std::move(table_schema),
                       txn);
  txn_manager.CommitTransaction(txn);

  txn = txn_manager.BeginTransaction();
  catalog->CreateTable(TEST_DB_NAME, "department_table_2",
                       std::move(table_schema2), txn);
  txn_manager.CommitTransaction(txn);

  txn = txn_manager.BeginTransaction();
  EXPECT_EQ(
      (int)catalog->GetDatabaseObject(TEST_DB_NAME, txn)->GetTableObjects().size(),
      2);

  // Now dropping the table using the executor
  catalog->DropTable(TEST_DB_NAME, "department_table", txn);
  EXPECT_EQ(
      (int)catalog->GetDatabaseObject(TEST_DB_NAME, txn)->GetTableObjects().size(),
      1);

  // free the database just created
  catalog->DropDatabaseWithName(TEST_DB_NAME, txn);
  txn_manager.CommitTransaction(txn);
}

TEST_F(DropTests, DroppingTrigger) {
  auto catalog = catalog::Catalog::GetInstance();
  catalog->Bootstrap();

  auto &txn_manager = concurrency::TransactionManagerFactory::GetInstance();
  auto txn = txn_manager.BeginTransaction();

  // Create a table first
  auto id_column = catalog::Column(
      type::TypeId::INTEGER, type::Type::GetTypeSize(type::TypeId::INTEGER),
      "dept_id", true);
  auto name_column =
      catalog::Column(type::TypeId::VARCHAR, 32, "dept_name", false);

  std::unique_ptr<catalog::Schema> table_schema(
      new catalog::Schema({id_column, name_column}));

  catalog->CreateDatabase(TEST_DB_NAME, txn);
  txn_manager.CommitTransaction(txn);

  txn = txn_manager.BeginTransaction();
  catalog->CreateTable(TEST_DB_NAME, "department_table", std::move(table_schema),
                       txn);
  txn_manager.CommitTransaction(txn);

  // Create a trigger
  auto parser = parser::PostgresParser::GetInstance();
  std::string query =
      "CREATE TRIGGER update_dept_name "
      "BEFORE UPDATE OF dept_name ON department_table "
      "EXECUTE PROCEDURE log_update_dept_name();";
  std::unique_ptr<parser::SQLStatementList> stmt_list(
      parser.BuildParseTree(query).release());
  EXPECT_TRUE(stmt_list->is_valid);
  EXPECT_EQ(StatementType::CREATE, stmt_list->GetStatement(0)->GetType());
  auto create_trigger_stmt =
      static_cast<parser::CreateStatement *>(stmt_list->GetStatement(0));

  create_trigger_stmt->TryBindDatabaseName(TEST_DB_NAME);

  // Create plans
  planner::CreatePlan plan(create_trigger_stmt);
  // Execute the create trigger
  txn = txn_manager.BeginTransaction();
  std::unique_ptr<executor::ExecutorContext> context(
      new executor::ExecutorContext(txn));
  executor::CreateExecutor createTriggerExecutor(&plan, context.get());
  createTriggerExecutor.Init();
  createTriggerExecutor.Execute();

  // Check the effect of creation
  storage::DataTable *target_table =
      catalog::Catalog::GetInstance()->GetTableWithName(
          TEST_DB_NAME, "department_table", txn);
  txn_manager.CommitTransaction(txn);
  EXPECT_EQ(1, target_table->GetTriggerNumber());
  trigger::Trigger *new_trigger = target_table->GetTriggerByIndex(0);
  EXPECT_EQ(new_trigger->GetTriggerName(), "update_dept_name");

  LOG_INFO("Create trigger finishes. Now drop it.");

  // Drop statement and drop plan
  parser::DropStatement drop_statement(
      parser::DropStatement::EntityType::kTrigger, "department_table",
      "update_dept_name");

  drop_statement.TryBindDatabaseName(TEST_DB_NAME);

  planner::DropPlan drop_plan(&drop_statement);

  // Execute the create trigger
  txn = txn_manager.BeginTransaction();
  std::unique_ptr<executor::ExecutorContext> context2(
      new executor::ExecutorContext(txn));
  executor::DropExecutor drop_executor(&drop_plan, context2.get());
  drop_executor.Init();
  drop_executor.Execute();
  txn_manager.CommitTransaction(txn);

  // Check the effect of drop
  // Most major check in this test case
  EXPECT_EQ(0, target_table->GetTriggerNumber());

  // Now dropping the table using the executer
  txn = txn_manager.BeginTransaction();
  catalog->DropTable(TEST_DB_NAME, "department_table", txn);
  EXPECT_EQ(0, (int)catalog::Catalog::GetInstance()
                   ->GetDatabaseObject(TEST_DB_NAME, txn)
                   ->GetTableObjects()
                   .size());
  txn_manager.CommitTransaction(txn);

  // free the database just created
  txn = txn_manager.BeginTransaction();
  catalog->DropDatabaseWithName(TEST_DB_NAME, txn);
  txn_manager.CommitTransaction(txn);
}

}  // namespace test
}  // namespace peloton
