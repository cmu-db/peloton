//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// create_test.cpp
//
// Identification: test/executor/create_test.cpp
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <cstdio>

#include "catalog/catalog.h"
#include "common/harness.h"
#include "concurrency/transaction_manager_factory.h"
#include "executor/create_executor.h"
#include "executor/executor_context.h"
#include "planner/create_plan.h"
#include "commands/trigger.h"
#include "storage/data_table.h"
#include "expression/abstract_expression.h"
#include "expression/tuple_value_expression.h"

#include "gtest/gtest.h"

namespace peloton {
namespace test {

//===--------------------------------------------------------------------===//
// Catalog Tests
//===--------------------------------------------------------------------===//

class CreateTests : public PelotonTest {};

TEST_F(CreateTests, CreatingTable) {
  // Bootstrap
  auto &txn_manager = concurrency::TransactionManagerFactory::GetInstance();
  auto txn = txn_manager.BeginTransaction();
  catalog::Catalog::GetInstance()->CreateDatabase(DEFAULT_DB_NAME, txn);

  // Insert a table first
  auto id_column = catalog::Column(type::TypeId::INTEGER,
                                   type::Type::GetTypeSize(type::TypeId::INTEGER),
                                   "dept_id", true);
  auto name_column =
      catalog::Column(type::TypeId::VARCHAR, 32, "dept_name", false);

  // Schema
  std::unique_ptr<catalog::Schema> table_schema(
      new catalog::Schema({id_column, name_column}));

  std::unique_ptr<executor::ExecutorContext> context(
      new executor::ExecutorContext(txn));

  // Create plans
  planner::CreatePlan node("department_table", DEFAULT_DB_NAME,
                           std::move(table_schema), CreateType::TABLE);

  // Create executer
  executor::CreateExecutor executor(&node, context.get());

  executor.Init();
  executor.Execute();

  txn_manager.CommitTransaction(txn);
  EXPECT_EQ(catalog::Catalog::GetInstance()
                ->GetDatabaseWithName(DEFAULT_DB_NAME)
                ->GetTableCount(),
            1);

  // free the database just created
  txn = txn_manager.BeginTransaction();
  catalog::Catalog::GetInstance()->DropDatabaseWithName(DEFAULT_DB_NAME, txn);
  txn_manager.CommitTransaction(txn);
}

TEST_F(CreateTests, CreatingTrigger) {
  // Bootstrap
  auto &txn_manager = concurrency::TransactionManagerFactory::GetInstance();
  auto txn = txn_manager.BeginTransaction();
  // catalog::Catalog::GetInstance()->CreateDatabase(DEFAULT_DB_NAME, txn);
  auto catalog = catalog::Catalog::GetInstance();
  catalog->Bootstrap();
  catalog->CreateDatabase(DEFAULT_DB_NAME, txn);

  // Insert a table first
  auto id_column = catalog::Column(type::Type::INTEGER,
                                   type::Type::GetTypeSize(type::Type::INTEGER),
                                   "balance", true);
  auto name_column =
      catalog::Column(type::Type::VARCHAR, 32, "dept_name", false);

  // Schema
  std::unique_ptr<catalog::Schema> table_schema(
      new catalog::Schema({id_column, name_column}));

  std::unique_ptr<executor::ExecutorContext> context(
      new executor::ExecutorContext(txn));

  // Create plans
  planner::CreatePlan node("accounts", DEFAULT_DB_NAME, std::move(table_schema),
                           CreateType::TABLE);

  // Create executer
  executor::CreateExecutor executor(&node, context.get());

  executor.Init();
  executor.Execute();

  txn_manager.CommitTransaction(txn);
  EXPECT_EQ(catalog::Catalog::GetInstance()
                ->GetDatabaseWithName(DEFAULT_DB_NAME)
                ->GetTableCount(),
            1);

  // Create statement
  auto parser = parser::PostgresParser::GetInstance();
  std::string query =
      "CREATE TRIGGER check_update "
      "BEFORE UPDATE OF balance ON accounts "
      "FOR EACH ROW "
      "WHEN (OLD.balance <> NEW.balance) "
      "EXECUTE PROCEDURE check_account_update();";
  auto stmt_list = parser.BuildParseTree(query).release();
  EXPECT_TRUE(stmt_list->is_valid);
  EXPECT_EQ(StatementType::CREATE, stmt_list->GetStatement(0)->GetType());
  auto create_trigger_stmt =
      static_cast<parser::CreateStatement *>(stmt_list->GetStatement(0));

  // Create plans
  planner::CreatePlan plan(create_trigger_stmt);

  // plan type
  EXPECT_EQ(CreateType::TRIGGER, plan.GetCreateType());
  // trigger name
  EXPECT_EQ("check_update", plan.GetTriggerName());
  // table name
  EXPECT_EQ("accounts", plan.GetTableName());
  // funcname
  std::vector<std::string> func_name = plan.GetTriggerFuncName();
  EXPECT_EQ(1, func_name.size());
  EXPECT_EQ("check_account_update", func_name[0]);
  // args
  EXPECT_EQ(0, plan.GetTriggerArgs().size());
  // columns
  std::vector<std::string> columns = plan.GetTriggerColumns();
  EXPECT_EQ(1, columns.size());
  EXPECT_EQ("balance", columns[0]);
  // when
  auto when = plan.GetTriggerWhen();
  EXPECT_NE(nullptr, when);
  EXPECT_EQ(ExpressionType::COMPARE_NOTEQUAL, when->GetExpressionType());
  EXPECT_EQ(2, when->GetChildrenSize());
  auto left = when->GetChild(0);
  auto right = when->GetChild(1);
  EXPECT_EQ(ExpressionType::VALUE_TUPLE, left->GetExpressionType());
  EXPECT_EQ("old", static_cast<const expression::TupleValueExpression *>(left)
                       ->GetTableName());
  EXPECT_EQ("balance", static_cast<const expression::TupleValueExpression *>(
                           left)->GetColumnName());
  EXPECT_EQ(ExpressionType::VALUE_TUPLE, right->GetExpressionType());
  EXPECT_EQ("new", static_cast<const expression::TupleValueExpression *>(right)
                       ->GetTableName());
  EXPECT_EQ("balance", static_cast<const expression::TupleValueExpression *>(
                           right)->GetColumnName());
  // type (level, timing, event)
  auto trigger_type = plan.GetTriggerType();
  // level
  EXPECT_TRUE(TRIGGER_FOR_ROW(trigger_type));
  // timing
  EXPECT_TRUE(TRIGGER_FOR_BEFORE(trigger_type));
  EXPECT_FALSE(TRIGGER_FOR_AFTER(trigger_type));
  EXPECT_FALSE(TRIGGER_FOR_INSTEAD(trigger_type));
  // event
  EXPECT_TRUE(TRIGGER_FOR_UPDATE(trigger_type));
  EXPECT_FALSE(TRIGGER_FOR_INSERT(trigger_type));
  EXPECT_FALSE(TRIGGER_FOR_DELETE(trigger_type));
  EXPECT_FALSE(TRIGGER_FOR_TRUNCATE(trigger_type));

  // Execute the create trigger
  txn = txn_manager.BeginTransaction();
  std::unique_ptr<executor::ExecutorContext> context2(
    new executor::ExecutorContext(txn));
  executor::CreateExecutor createTriggerExecutor(&plan, context2.get());
  createTriggerExecutor.Init();
  createTriggerExecutor.Execute();
  txn_manager.CommitTransaction(txn);

  // Check the effect of creation
  storage::DataTable *target_table =
      catalog::Catalog::GetInstance()->GetTableWithName(DEFAULT_DB_NAME,
                                                        "accounts");
  EXPECT_EQ(1, target_table->GetTriggerNumber());
  commands::Trigger *new_trigger = target_table->GetTriggerByIndex(0);
  EXPECT_EQ(new_trigger->GetTriggerName(), "check_update");

  commands::TriggerList *new_trigger_list = target_table->GetTriggerList();
  EXPECT_EQ(1, new_trigger_list->GetTriggerListSize());
  EXPECT_TRUE(new_trigger_list->HasTriggerType(commands::EnumTriggerType::BEFORE_UPDATE_ROW));

  // free the database just created
  txn = txn_manager.BeginTransaction();
  catalog::Catalog::GetInstance()->DropDatabaseWithName(DEFAULT_DB_NAME, txn);
  txn_manager.CommitTransaction(txn);

  if (when) {
    delete when;
  }

  if (stmt_list) {
    delete stmt_list;
  }

  // TODO: test for creating a trigger without "when"
}


TEST_F(CreateTests, CreatingTriggerInCatalog) {
  // Bootstrap
  auto &txn_manager = concurrency::TransactionManagerFactory::GetInstance();
  auto txn = txn_manager.BeginTransaction();
  auto catalog = catalog::Catalog::GetInstance();
  catalog->Bootstrap();
  catalog->CreateDatabase(DEFAULT_DB_NAME, txn);

  // Insert a table first
  auto id_column = catalog::Column(type::Type::INTEGER,
                                   type::Type::GetTypeSize(type::Type::INTEGER),
                                   "balance", true);
  auto name_column =
      catalog::Column(type::Type::VARCHAR, 32, "dept_name", false);

  // Schema
  std::unique_ptr<catalog::Schema> table_schema(
      new catalog::Schema({id_column, name_column}));

  std::unique_ptr<executor::ExecutorContext> context(
      new executor::ExecutorContext(txn));

  // Create plans
  planner::CreatePlan node("accounts", DEFAULT_DB_NAME, std::move(table_schema),
                           CreateType::TABLE);

  // Create executer
  executor::CreateExecutor executor(&node, context.get());

  executor.Init();
  executor.Execute();

  txn_manager.CommitTransaction(txn);
  EXPECT_EQ(catalog::Catalog::GetInstance()
                ->GetDatabaseWithName(DEFAULT_DB_NAME)
                ->GetTableCount(),
            1);

  // Create statement
  auto parser = parser::PostgresParser::GetInstance();
  std::string query =
      "CREATE TRIGGER check_update "
      "BEFORE UPDATE OF balance ON accounts "
      "FOR EACH ROW "
      "WHEN (OLD.balance <> NEW.balance) "
      "EXECUTE PROCEDURE check_account_update();";
  auto stmt_list = parser.BuildParseTree(query).release();
  EXPECT_TRUE(stmt_list->is_valid);
  EXPECT_EQ(StatementType::CREATE, stmt_list->GetStatement(0)->GetType());
  auto create_trigger_stmt =
      static_cast<parser::CreateStatement *>(stmt_list->GetStatement(0));

  // Create plans
  planner::CreatePlan plan(create_trigger_stmt);

  // plan type
  EXPECT_EQ(CreateType::TRIGGER, plan.GetCreateType());
  // trigger name
  EXPECT_EQ("check_update", plan.GetTriggerName());
  // table name
  EXPECT_EQ("accounts", plan.GetTableName());
  // funcname
  std::vector<std::string> func_name = plan.GetTriggerFuncName();
  EXPECT_EQ(1, func_name.size());
  EXPECT_EQ("check_account_update", func_name[0]);
  // args
  EXPECT_EQ(0, plan.GetTriggerArgs().size());
  // columns
  std::vector<std::string> columns = plan.GetTriggerColumns();
  EXPECT_EQ(1, columns.size());
  EXPECT_EQ("balance", columns[0]);
  // when
  auto when = plan.GetTriggerWhen();
  EXPECT_NE(nullptr, when);
  EXPECT_EQ(ExpressionType::COMPARE_NOTEQUAL, when->GetExpressionType());
  EXPECT_EQ(2, when->GetChildrenSize());
  auto left = when->GetChild(0);
  auto right = when->GetChild(1);
  EXPECT_EQ(ExpressionType::VALUE_TUPLE, left->GetExpressionType());
  EXPECT_EQ("old", static_cast<const expression::TupleValueExpression *>(left)
                       ->GetTableName());
  EXPECT_EQ("balance", static_cast<const expression::TupleValueExpression *>(
                           left)->GetColumnName());
  EXPECT_EQ(ExpressionType::VALUE_TUPLE, right->GetExpressionType());
  EXPECT_EQ("new", static_cast<const expression::TupleValueExpression *>(right)
                       ->GetTableName());
  EXPECT_EQ("balance", static_cast<const expression::TupleValueExpression *>(
                           right)->GetColumnName());
  // type (level, timing, event)
  auto trigger_type = plan.GetTriggerType();
  // level
  EXPECT_TRUE(TRIGGER_FOR_ROW(trigger_type));
  // timing
  EXPECT_TRUE(TRIGGER_FOR_BEFORE(trigger_type));
  EXPECT_FALSE(TRIGGER_FOR_AFTER(trigger_type));
  EXPECT_FALSE(TRIGGER_FOR_INSTEAD(trigger_type));
  // event
  EXPECT_TRUE(TRIGGER_FOR_UPDATE(trigger_type));
  EXPECT_FALSE(TRIGGER_FOR_INSERT(trigger_type));
  EXPECT_FALSE(TRIGGER_FOR_DELETE(trigger_type));
  EXPECT_FALSE(TRIGGER_FOR_TRUNCATE(trigger_type));

  // Execute the create trigger
  txn = txn_manager.BeginTransaction();
  std::unique_ptr<executor::ExecutorContext> context2(
    new executor::ExecutorContext(txn));
  executor::CreateExecutor createTriggerExecutor(&plan, context2.get());
  createTriggerExecutor.Init();
  createTriggerExecutor.Execute();
  // txn_manager.CommitTransaction(txn);


  // check whether the trigger catalog table contains this new trigger
  oid_t database_oid = catalog::DatabaseCatalog::GetInstance()->GetDatabaseOid(DEFAULT_DB_NAME, txn);
  oid_t table_oid = catalog::TableCatalog::GetInstance()->GetTableOid("accounts", database_oid, txn);
  auto trigger_list = catalog::TriggerCatalog::GetInstance()->GetTriggersByType(database_oid, table_oid, 
                              (TRIGGER_TYPE_ROW|TRIGGER_TYPE_BEFORE|TRIGGER_TYPE_UPDATE), txn);

  txn_manager.CommitTransaction(txn);

  EXPECT_EQ(1, trigger_list->GetTriggerListSize());
  EXPECT_TRUE(trigger_list->HasTriggerType(commands::EnumTriggerType::BEFORE_UPDATE_ROW));

  // free the database just created
  txn = txn_manager.BeginTransaction();
  catalog::Catalog::GetInstance()->DropDatabaseWithName(DEFAULT_DB_NAME, txn);
  txn_manager.CommitTransaction(txn);

  if (when) {
    delete when;
  }

  if (stmt_list) {
    delete stmt_list;
  }
}

}  // End test namespace
}  // End peloton namespace
