//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// trigger_test.cpp
//
// Identification: test/trigger/trigger_test.cpp
//
// Copyright (c) 2015-17, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "trigger/trigger.h"
#include "catalog/catalog.h"
#include "common/harness.h"
#include "concurrency/transaction_manager_factory.h"
#include "executor/executors.h"
#include "parser/pg_trigger.h"
#include "parser/postgresparser.h"
#include "planner/create_plan.h"
#include "planner/insert_plan.h"
#include "storage/abstract_table.h"

namespace peloton {
namespace test {

class TriggerTests : public PelotonTest {
 protected:
  std::string table_name = "accounts";
  std::string col_1 = "dept_id";
  std::string col_2 = "dept_name";

  // helper for c_str copy
  static char *cstrdup(const char *c_str) {
    char *new_str = new char[strlen(c_str) + 1];
    strcpy(new_str, c_str);
    return new_str;
  }

  void CreateTableHelper() {
    auto &txn_manager = concurrency::TransactionManagerFactory::GetInstance();
    auto txn = txn_manager.BeginTransaction();
    catalog::Catalog::GetInstance()->CreateDatabase(DEFAULT_DB_NAME, txn);

    // Insert a table first
    auto id_column = catalog::Column(
        type::TypeId::INTEGER, type::Type::GetTypeSize(type::TypeId::INTEGER),
        col_1, true);
    auto name_column = catalog::Column(type::TypeId::VARCHAR, 32, col_2, false);

    // Schema
    std::unique_ptr<catalog::Schema> table_schema(
        new catalog::Schema({id_column, name_column}));

    std::unique_ptr<executor::ExecutorContext> context(
        new executor::ExecutorContext(txn));

    // Create plans
    planner::CreatePlan node(table_name, DEFUALT_SCHEMA_NAME, DEFAULT_DB_NAME,
                             std::move(table_schema), CreateType::TABLE);

    // Create executer
    executor::CreateExecutor executor(&node, context.get());

    executor.Init();
    executor.Execute();

    txn_manager.CommitTransaction(txn);
  }

  void InsertTupleHelper(int number, std::string text) {
    auto &txn_manager = concurrency::TransactionManagerFactory::GetInstance();
    auto txn = txn_manager.BeginTransaction();

    auto table = catalog::Catalog::GetInstance()->GetTableWithName(
        DEFAULT_DB_NAME, DEFUALT_SCHEMA_NAME, std::string(table_name), txn);

    std::unique_ptr<executor::ExecutorContext> context(
        new executor::ExecutorContext(txn));

    std::unique_ptr<parser::InsertStatement> insert_node(
        new parser::InsertStatement(InsertType::VALUES));

    auto table_ref = new parser::TableRef(TableReferenceType::NAME);
    parser::TableInfo *table_info = new parser::TableInfo();
    table_info->table_name = table_name;
    table_ref->table_info_.reset(table_info);
    insert_node->table_ref_.reset(table_ref);

    insert_node->columns.push_back(col_1);
    insert_node->columns.push_back(col_2);

    insert_node->insert_values.push_back(
        std::vector<std::unique_ptr<expression::AbstractExpression>>());
    auto &values = insert_node->insert_values.at(0);

    values.push_back(std::unique_ptr<expression::AbstractExpression>(
        new expression::ConstantValueExpression(
            type::ValueFactory::GetIntegerValue(number))));

    values.push_back(std::unique_ptr<expression::AbstractExpression>(
        new expression::ConstantValueExpression(
            type::ValueFactory::GetVarcharValue(text))));

    insert_node->select.reset(new parser::SelectStatement());

    planner::InsertPlan node(table, &insert_node->columns,
                             &insert_node->insert_values);
    executor::InsertExecutor executor(&node, context.get());

    EXPECT_TRUE(executor.Init());
    EXPECT_TRUE(executor.Execute());
    EXPECT_EQ(1, table->GetTupleCount());

    txn_manager.CommitTransaction(txn);
  }

  void CreateTriggerHelper(std::string query, int trigger_number,
                           std::string trigger_name) {
    // Bootstrap
    auto &txn_manager = concurrency::TransactionManagerFactory::GetInstance();
    auto parser = parser::PostgresParser::GetInstance();
    // NOTE: Catalog::GetInstance()->Bootstrap() has been called in previous
    // tests you can only call it once!

    std::unique_ptr<parser::SQLStatementList> stmt_list(
        parser.BuildParseTree(query).release());
    EXPECT_TRUE(stmt_list->is_valid);
    EXPECT_EQ(StatementType::CREATE, stmt_list->GetStatement(0)->GetType());
    auto create_trigger_stmt =
        static_cast<parser::CreateStatement *>(stmt_list->GetStatement(0));

    create_trigger_stmt->TryBindDatabaseName(DEFAULT_DB_NAME);
    // Create plans
    planner::CreatePlan plan(create_trigger_stmt);

    // plan type
    EXPECT_EQ(CreateType::TRIGGER, plan.GetCreateType());

    // Execute the create trigger
    auto txn = txn_manager.BeginTransaction();
    std::unique_ptr<executor::ExecutorContext> context2(
        new executor::ExecutorContext(txn));
    executor::CreateExecutor createTriggerExecutor(&plan, context2.get());
    createTriggerExecutor.Init();
    createTriggerExecutor.Execute();

    // Check the effect of creation
    storage::DataTable *target_table =
        catalog::Catalog::GetInstance()->GetTableWithName(
            DEFAULT_DB_NAME, DEFUALT_SCHEMA_NAME, table_name, txn);
    txn_manager.CommitTransaction(txn);
    EXPECT_EQ(trigger_number, target_table->GetTriggerNumber());
    trigger::Trigger *new_trigger = target_table->GetTriggerByIndex(0);
    EXPECT_EQ(trigger_name, new_trigger->GetTriggerName());
  }
};

TEST_F(TriggerTests, BasicTest) {
  // Create statement
  auto parser = parser::PostgresParser::GetInstance();

  std::string query1 =
      "CREATE TRIGGER check_update "
      "BEFORE UPDATE OF balance ON accounts "
      "FOR EACH ROW "
      "WHEN (OLD.balance <> NEW.balance) "
      "EXECUTE PROCEDURE check_account_update();";
  std::unique_ptr<parser::SQLStatementList> stmt_list1(
      parser.BuildParseTree(query1).release());
  EXPECT_TRUE(stmt_list1->is_valid);
  EXPECT_EQ(StatementType::CREATE, stmt_list1->GetStatement(0)->GetType());
  auto create_trigger_stmt1 =
      static_cast<parser::CreateStatement *>(stmt_list1->GetStatement(0));

  create_trigger_stmt1->TryBindDatabaseName(DEFAULT_DB_NAME);
  // Create plans
  const planner::CreatePlan plan1(create_trigger_stmt1);

  trigger::Trigger trigger1(plan1);
  EXPECT_EQ("check_update", trigger1.GetTriggerName());
  int16_t trigger_type1 = trigger1.GetTriggerType();
  EXPECT_TRUE(TRIGGER_FOR_ROW(trigger_type1));
  EXPECT_TRUE(TRIGGER_FOR_BEFORE(trigger_type1));
  EXPECT_TRUE(TRIGGER_FOR_UPDATE(trigger_type1));
  EXPECT_FALSE(TRIGGER_FOR_DELETE(trigger_type1));

  std::string query2 =
      "CREATE TRIGGER check_update_and_delete "
      "BEFORE UPDATE OF balance OR DELETE ON accounts "
      "FOR EACH ROW "
      "WHEN (OLD.balance <> NEW.balance) "
      "EXECUTE PROCEDURE check_account_update();";
  std::unique_ptr<parser::SQLStatementList> stmt_list2(
      parser.BuildParseTree(query2).release());
  EXPECT_TRUE(stmt_list2->is_valid);
  auto create_trigger_stmt2 =
      static_cast<parser::CreateStatement *>(stmt_list2->GetStatement(0));

  create_trigger_stmt2->TryBindDatabaseName(DEFAULT_DB_NAME);
  const planner::CreatePlan plan2(create_trigger_stmt2);
  trigger::Trigger trigger2(plan2);
  EXPECT_EQ("check_update_and_delete", trigger2.GetTriggerName());
  int16_t trigger_type2 = trigger2.GetTriggerType();
  EXPECT_TRUE(TRIGGER_FOR_ROW(trigger_type2));
  EXPECT_TRUE(TRIGGER_FOR_BEFORE(trigger_type2));
  EXPECT_TRUE(TRIGGER_FOR_UPDATE(trigger_type2));
  EXPECT_TRUE(TRIGGER_FOR_DELETE(trigger_type2));

  trigger::TriggerList trigger_list;
  trigger_list.AddTrigger(trigger1);
  EXPECT_EQ(1, trigger_list.GetTriggerListSize());
  EXPECT_TRUE(trigger_list.HasTriggerType(TriggerType::BEFORE_UPDATE_ROW));
  EXPECT_FALSE(trigger_list.HasTriggerType(TriggerType::BEFORE_DELETE_ROW));
  EXPECT_FALSE(trigger_list.HasTriggerType(TriggerType::BEFORE_INSERT_ROW));

  trigger_list.AddTrigger(trigger2);
  EXPECT_EQ(2, trigger_list.GetTriggerListSize());
  EXPECT_TRUE(trigger_list.HasTriggerType(TriggerType::BEFORE_UPDATE_ROW));
  EXPECT_TRUE(trigger_list.HasTriggerType(TriggerType::BEFORE_DELETE_ROW));
  EXPECT_FALSE(trigger_list.HasTriggerType(TriggerType::BEFORE_INSERT_ROW));
}

// Test trigger type: before & after, each row, insert
TEST_F(TriggerTests, BeforeAndAfterRowInsertTriggers) {
  // Bootstrap
  auto &txn_manager = concurrency::TransactionManagerFactory::GetInstance();
  auto parser = parser::PostgresParser::GetInstance();
  // NOTE: Catalog::GetInstance()->Bootstrap() has been called in previous tests
  // you can only call it once!

  // Create table
  CreateTableHelper();

  // Create statement (before row insert)
  std::string query =
      "CREATE TRIGGER b_r_insert_trigger "
      "BEFORE INSERT ON accounts "
      "FOR EACH ROW WHEN (NEW.dept_id = 2333) "
      "EXECUTE PROCEDURE b_r_insert_trigger_func();";
  std::unique_ptr<parser::SQLStatementList> stmt_list(
      parser.BuildParseTree(query).release());
  EXPECT_TRUE(stmt_list->is_valid);
  EXPECT_EQ(StatementType::CREATE, stmt_list->GetStatement(0)->GetType());
  auto create_trigger_stmt =
      static_cast<parser::CreateStatement *>(stmt_list->GetStatement(0));
  create_trigger_stmt->TryBindDatabaseName(DEFAULT_DB_NAME);
  // Create plans
  planner::CreatePlan plan(create_trigger_stmt);

  // plan type
  EXPECT_EQ(CreateType::TRIGGER, plan.GetCreateType());

  // type (level, timing, event)
  auto trigger_type = plan.GetTriggerType();
  // level
  EXPECT_TRUE(TRIGGER_FOR_ROW(trigger_type));
  // timing
  EXPECT_TRUE(TRIGGER_FOR_BEFORE(trigger_type));
  EXPECT_FALSE(TRIGGER_FOR_AFTER(trigger_type));
  EXPECT_FALSE(TRIGGER_FOR_INSTEAD(trigger_type));
  // event
  EXPECT_FALSE(TRIGGER_FOR_UPDATE(trigger_type));
  EXPECT_TRUE(TRIGGER_FOR_INSERT(trigger_type));
  EXPECT_FALSE(TRIGGER_FOR_DELETE(trigger_type));
  EXPECT_FALSE(TRIGGER_FOR_TRUNCATE(trigger_type));

  // Execute the create trigger
  auto txn = txn_manager.BeginTransaction();
  std::unique_ptr<executor::ExecutorContext> context2(
      new executor::ExecutorContext(txn));
  executor::CreateExecutor createTriggerExecutor(&plan, context2.get());
  createTriggerExecutor.Init();
  createTriggerExecutor.Execute();

  // Check the effect of creation
  storage::DataTable *target_table =
      catalog::Catalog::GetInstance()->GetTableWithName(
          DEFAULT_DB_NAME, DEFUALT_SCHEMA_NAME, "accounts", txn);
  txn_manager.CommitTransaction(txn);
  EXPECT_EQ(1, target_table->GetTriggerNumber());
  trigger::Trigger *new_trigger = target_table->GetTriggerByIndex(0);
  EXPECT_EQ(new_trigger->GetTriggerName(), "b_r_insert_trigger");

  trigger::TriggerList *new_trigger_list = target_table->GetTriggerList();
  EXPECT_EQ(1, new_trigger_list->GetTriggerListSize());
  EXPECT_TRUE(new_trigger_list->HasTriggerType(TriggerType::BEFORE_INSERT_ROW));

  // create another trigger in simple way (after row insert)
  CreateTriggerHelper(
      "CREATE TRIGGER a_r_insert_trigger "
      "After INSERT ON accounts "
      "FOR EACH ROW "
      "EXECUTE PROCEDURE a_r_insert_trigger_func();",
      2, "a_r_insert_trigger");

  InsertTupleHelper(2333, "LTI");

  // TODO: the effect of this operation should be verified automatically.
  // The UDF should be called after this operation happens.
  // Auto check can be implemented after UDF is implemented.

  // free the database just created
  txn = txn_manager.BeginTransaction();
  catalog::Catalog::GetInstance()->DropDatabaseWithName(DEFAULT_DB_NAME, txn);
  txn_manager.CommitTransaction(txn);
}

// Test trigger type: after, statement, insert
TEST_F(TriggerTests, AfterStatmentInsertTriggers) {
  // Bootstrap
  auto &txn_manager = concurrency::TransactionManagerFactory::GetInstance();
  auto parser = parser::PostgresParser::GetInstance();
  // NOTE: Catalog::GetInstance()->Bootstrap() has been called in previous tests
  // you can only call it once!

  // Create table
  CreateTableHelper();

  // Create statement

  std::string query =
      "CREATE TRIGGER a_s_insert_trigger "
      "AFTER INSERT ON accounts "
      "FOR EACH STATEMENT "
      "EXECUTE PROCEDURE a_s_insert_trigger_func();";
  std::unique_ptr<parser::SQLStatementList> stmt_list(
      parser.BuildParseTree(query).release());
  EXPECT_TRUE(stmt_list->is_valid);
  EXPECT_EQ(StatementType::CREATE, stmt_list->GetStatement(0)->GetType());
  auto create_trigger_stmt =
      static_cast<parser::CreateStatement *>(stmt_list->GetStatement(0));

  create_trigger_stmt->TryBindDatabaseName(DEFAULT_DB_NAME);
  // Create plans
  planner::CreatePlan plan(create_trigger_stmt);

  // plan type
  EXPECT_EQ(CreateType::TRIGGER, plan.GetCreateType());

  // type (level, timing, event)
  auto trigger_type = plan.GetTriggerType();
  // level
  EXPECT_FALSE(TRIGGER_FOR_ROW(trigger_type));
  // timing
  EXPECT_FALSE(TRIGGER_FOR_BEFORE(trigger_type));
  EXPECT_TRUE(TRIGGER_FOR_AFTER(trigger_type));
  EXPECT_FALSE(TRIGGER_FOR_INSTEAD(trigger_type));
  // event
  EXPECT_FALSE(TRIGGER_FOR_UPDATE(trigger_type));
  EXPECT_TRUE(TRIGGER_FOR_INSERT(trigger_type));
  EXPECT_FALSE(TRIGGER_FOR_DELETE(trigger_type));
  EXPECT_FALSE(TRIGGER_FOR_TRUNCATE(trigger_type));

  // Execute the create trigger
  auto txn = txn_manager.BeginTransaction();
  std::unique_ptr<executor::ExecutorContext> context2(
      new executor::ExecutorContext(txn));
  executor::CreateExecutor createTriggerExecutor(&plan, context2.get());
  createTriggerExecutor.Init();
  createTriggerExecutor.Execute();

  // Check the effect of creation
  storage::DataTable *target_table =
      catalog::Catalog::GetInstance()->GetTableWithName(
          DEFAULT_DB_NAME, DEFUALT_SCHEMA_NAME, "accounts", txn);
  txn_manager.CommitTransaction(txn);
  EXPECT_EQ(1, target_table->GetTriggerNumber());
  trigger::Trigger *new_trigger = target_table->GetTriggerByIndex(0);
  EXPECT_EQ(new_trigger->GetTriggerName(), "a_s_insert_trigger");

  trigger::TriggerList *new_trigger_list = target_table->GetTriggerList();
  EXPECT_EQ(1, new_trigger_list->GetTriggerListSize());
  EXPECT_TRUE(
      new_trigger_list->HasTriggerType(TriggerType::AFTER_INSERT_STATEMENT));

  InsertTupleHelper(2333, "LTI");

  // TODO: the effect of this operation should be verified automatically.
  // The UDF should be called after this operation happens.
  // Auto check can be implemented after UDF is implemented.

  // free the database just created
  txn = txn_manager.BeginTransaction();
  catalog::Catalog::GetInstance()->DropDatabaseWithName(DEFAULT_DB_NAME, txn);
  txn_manager.CommitTransaction(txn);
}

// Test other types of trigger in a relatively simple way. Because the workflow
// is similar, and it is costly to manage redundant test cases.
TEST_F(TriggerTests, OtherTypesTriggers) {
  // Bootstrap
  auto &txn_manager = concurrency::TransactionManagerFactory::GetInstance();
  auto parser = parser::PostgresParser::GetInstance();
  // NOTE: Catalog::GetInstance()->Bootstrap() has been called in previous tests
  // you can only call it once!

  // Create table
  CreateTableHelper();

  // Create statement

  // BRUpdateTriggers
  CreateTriggerHelper(
      "CREATE TRIGGER b_r_update_trigger "
      "BEFORE UPDATE ON accounts "
      "FOR EACH ROW "
      "EXECUTE PROCEDURE b_r_update_trigger_func();",
      1, "b_r_update_trigger");
  // ARUpdateTriggers
  CreateTriggerHelper(
      "CREATE TRIGGER a_r_update_trigger "
      "AFTER UPDATE ON accounts "
      "FOR EACH ROW "
      "EXECUTE PROCEDURE a_r_update_trigger();",
      2, "a_r_update_trigger");
  // BRDeleteTriggers
  CreateTriggerHelper(
      "CREATE TRIGGER b_r_delete_trigger "
      "BEFORE DELETE ON accounts "
      "FOR EACH ROW "
      "EXECUTE PROCEDURE b_r_delete_trigger();",
      3, "b_r_delete_trigger");
  // ARDeleteTriggers
  CreateTriggerHelper(
      "CREATE TRIGGER a_r_delete_trigger "
      "AFTER DELETE ON accounts "
      "FOR EACH ROW "
      "EXECUTE PROCEDURE a_r_delete_trigger();",
      4, "a_r_delete_trigger");
  // BSInsertTriggers
  CreateTriggerHelper(
      "CREATE TRIGGER b_s_insert_trigger "
      "BEFORE INSERT ON accounts "
      "FOR EACH STATEMENT "
      "EXECUTE PROCEDURE b_s_insert_trigger();",
      5, "b_s_insert_trigger");
  // BSUpdateTriggers
  CreateTriggerHelper(
      "CREATE TRIGGER b_s_update_trigger "
      "BEFORE UPDATE ON accounts "
      "FOR EACH STATEMENT "
      "EXECUTE PROCEDURE b_s_update_trigger();",
      6, "b_s_update_trigger");
  // ASUpdateTriggers
  CreateTriggerHelper(
      "CREATE TRIGGER a_s_update_trigger "
      "AFTER UPDATE ON accounts "
      "FOR EACH STATEMENT "
      "EXECUTE PROCEDURE a_s_update_trigger();",
      7, "a_s_update_trigger");
  // BSDeleteTriggers
  CreateTriggerHelper(
      "CREATE TRIGGER b_s_delete_trigger "
      "BEFORE DELETE ON accounts "
      "FOR EACH STATEMENT "
      "EXECUTE PROCEDURE b_s_delete_trigger();",
      8, "b_s_delete_trigger");
  // ASDeleteTriggers
  CreateTriggerHelper(
      "CREATE TRIGGER a_s_delete_trigger "
      "AFTER DELETE ON accounts "
      "FOR EACH STATEMENT "
      "EXECUTE PROCEDURE a_s_delete_trigger();",
      9, "a_s_delete_trigger");

  auto txn = txn_manager.BeginTransaction();
  storage::DataTable *target_table =
      catalog::Catalog::GetInstance()->GetTableWithName(
          DEFAULT_DB_NAME, DEFUALT_SCHEMA_NAME, table_name, txn);
  txn_manager.CommitTransaction(txn);

  trigger::TriggerList *new_trigger_list = target_table->GetTriggerList();
  EXPECT_EQ(9, new_trigger_list->GetTriggerListSize());
  EXPECT_TRUE(new_trigger_list->HasTriggerType(TriggerType::BEFORE_UPDATE_ROW));
  EXPECT_TRUE(new_trigger_list->HasTriggerType(TriggerType::AFTER_UPDATE_ROW));
  EXPECT_TRUE(new_trigger_list->HasTriggerType(TriggerType::BEFORE_DELETE_ROW));
  EXPECT_TRUE(new_trigger_list->HasTriggerType(TriggerType::AFTER_DELETE_ROW));
  EXPECT_TRUE(
      new_trigger_list->HasTriggerType(TriggerType::BEFORE_INSERT_STATEMENT));
  EXPECT_TRUE(
      new_trigger_list->HasTriggerType(TriggerType::BEFORE_UPDATE_STATEMENT));
  EXPECT_TRUE(
      new_trigger_list->HasTriggerType(TriggerType::AFTER_UPDATE_STATEMENT));
  EXPECT_TRUE(
      new_trigger_list->HasTriggerType(TriggerType::BEFORE_DELETE_STATEMENT));
  EXPECT_TRUE(
      new_trigger_list->HasTriggerType(TriggerType::AFTER_DELETE_STATEMENT));

  // Invoke triggers directly
  new_trigger_list->ExecTriggers(TriggerType::BEFORE_UPDATE_ROW);
  new_trigger_list->ExecTriggers(TriggerType::BEFORE_INSERT_STATEMENT);
  new_trigger_list->ExecTriggers(TriggerType::BEFORE_UPDATE_STATEMENT);
  new_trigger_list->ExecTriggers(TriggerType::AFTER_UPDATE_STATEMENT);
  new_trigger_list->ExecTriggers(TriggerType::BEFORE_DELETE_STATEMENT);
  new_trigger_list->ExecTriggers(TriggerType::AFTER_DELETE_STATEMENT);

  // TODO: the effect of this operation should be verified automatically.
  // The UDF should be called after this operation happens.
  // Auto check can be implemented after UDF is implemented.

  // free the database just created
  txn = txn_manager.BeginTransaction();
  catalog::Catalog::GetInstance()->DropDatabaseWithName(DEFAULT_DB_NAME, txn);
  txn_manager.CommitTransaction(txn);
}
}  // namespace test
}  // namespace peloton
