//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// trigger_test.cpp
//
// Identification: test/commands/trigger_test.cpp
//
// Copyright (c) 2015-17, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//


#include "catalog/catalog.h"
#include "common/harness.h"
#include "commands/trigger.h"
#include "executor/create_executor.h"
#include "parser/pg_trigger.h"
#include "parser/postgresparser.h"
#include "planner/create_plan.h"

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
    auto id_column = catalog::Column(type::Type::INTEGER,
                                     type::Type::GetTypeSize(type::Type::INTEGER),
                                     col_1, true);
    auto name_column =
      catalog::Column(type::Type::VARCHAR, 32, col_2, false);

    // Schema
    std::unique_ptr<catalog::Schema> table_schema(
      new catalog::Schema({id_column, name_column}));

    std::unique_ptr<executor::ExecutorContext> context(
      new executor::ExecutorContext(txn));

    // Create plans
    planner::CreatePlan node(table_name, DEFAULT_DB_NAME, std::move(table_schema),
                             CreateType::TABLE);

    // Create executer
    executor::CreateExecutor executor(&node, context.get());

    executor.Init();
    executor.Execute();

    txn_manager.CommitTransaction(txn);
  }

  void InsertTupleHelper(int number, std::string text) {
    auto &txn_manager = concurrency::TransactionManagerFactory::GetInstance();
    auto txn = txn_manager.BeginTransaction();

    char *table_name_c = cstrdup(table_name.c_str());
    char *col_1_c = cstrdup(col_1.c_str());
    char *col_2_c = cstrdup(col_2.c_str());


    auto table = catalog::Catalog::GetInstance()->GetTableWithName(
      DEFAULT_DB_NAME, std::string(table_name));

    std::unique_ptr<executor::ExecutorContext> context(
      new executor::ExecutorContext(txn));

    std::unique_ptr<parser::InsertStatement> insert_node(
      new parser::InsertStatement(InsertType::VALUES));

    auto table_ref = new parser::TableRef(TableReferenceType::NAME);
    parser::TableInfo *table_info = new parser::TableInfo();
    table_info->table_name = table_name_c;
    table_ref->table_info_ = table_info;
    insert_node->table_ref_ = table_ref;

    insert_node->columns = new std::vector<char *>;
    insert_node->columns->push_back(col_1_c);
    insert_node->columns->push_back(col_2_c);

    insert_node->insert_values =
      new std::vector<std::vector<expression::AbstractExpression *> *>;
    auto values_ptr = new std::vector<expression::AbstractExpression *>;
    insert_node->insert_values->push_back(values_ptr);

    values_ptr->push_back(new expression::ConstantValueExpression(
      type::ValueFactory::GetIntegerValue(number)));

    values_ptr->push_back(new expression::ConstantValueExpression(
      type::ValueFactory::GetVarcharValue(text)));

    insert_node->select = new parser::SelectStatement();

    planner::InsertPlan node(table, insert_node->columns,
                             insert_node->insert_values);
    executor::InsertExecutor executor(&node, context.get());

    EXPECT_TRUE(executor.Init());
    EXPECT_TRUE(executor.Execute());
    EXPECT_EQ(1, table->GetTupleCount());

    txn_manager.CommitTransaction(txn);
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
  std::unique_ptr<parser::SQLStatementList> stmt_list1(parser.BuildParseTree(query1).release());
  EXPECT_TRUE(stmt_list1->is_valid);
  EXPECT_EQ(StatementType::CREATE, stmt_list1->GetStatement(0)->GetType());
  auto create_trigger_stmt1 =
      static_cast<parser::CreateStatement *>(stmt_list1->GetStatement(0));

  // Create plans
  const planner::CreatePlan plan1(create_trigger_stmt1);

  commands::Trigger trigger1(plan1);
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
  std::unique_ptr<parser::SQLStatementList> stmt_list2(parser.BuildParseTree(query2).release());
  EXPECT_TRUE(stmt_list2->is_valid);
  auto create_trigger_stmt2 =
      static_cast<parser::CreateStatement *>(stmt_list2->GetStatement(0));
  const planner::CreatePlan plan2(create_trigger_stmt2);
  commands::Trigger trigger2(plan2);
  EXPECT_EQ("check_update_and_delete", trigger2.GetTriggerName());
  int16_t trigger_type2 = trigger2.GetTriggerType();
  EXPECT_TRUE(TRIGGER_FOR_ROW(trigger_type2));
  EXPECT_TRUE(TRIGGER_FOR_BEFORE(trigger_type2));
  EXPECT_TRUE(TRIGGER_FOR_UPDATE(trigger_type2));
  EXPECT_TRUE(TRIGGER_FOR_DELETE(trigger_type2));

  commands::TriggerList trigger_list;
  trigger_list.AddTrigger(trigger1);
  EXPECT_EQ(1, trigger_list.GetTriggerListSize());
  EXPECT_TRUE(trigger_list.HasTriggerType(
      commands::EnumTriggerType::BEFORE_UPDATE_ROW));
  EXPECT_FALSE(trigger_list.HasTriggerType(
      commands::EnumTriggerType::BEFORE_DELETE_ROW));
  EXPECT_FALSE(trigger_list.HasTriggerType(
      commands::EnumTriggerType::BEFORE_INSERT_ROW));

  trigger_list.AddTrigger(trigger2);
  EXPECT_EQ(2, trigger_list.GetTriggerListSize());
  EXPECT_TRUE(trigger_list.HasTriggerType(
      commands::EnumTriggerType::BEFORE_UPDATE_ROW));
  EXPECT_TRUE(trigger_list.HasTriggerType(
      commands::EnumTriggerType::BEFORE_DELETE_ROW));
  EXPECT_FALSE(trigger_list.HasTriggerType(
      commands::EnumTriggerType::BEFORE_INSERT_ROW));

}

// Test trigger type: before, each row, insert
TEST_F(TriggerTests, BRInsertTriggers) {
  // Bootstrap
  auto &txn_manager = concurrency::TransactionManagerFactory::GetInstance();
  auto parser = parser::PostgresParser::GetInstance();
  catalog::Catalog::GetInstance()->Bootstrap();

  // Create table
  CreateTableHelper();

  // Create statement

  std::string query =
    "CREATE TRIGGER b_r_insert_trigger "
      "BEFORE UPDATE OF dept_id ON accounts "
      "FOR EACH ROW "
      "EXECUTE PROCEDURE b_r_insert_trigger_func();";
  std::unique_ptr<parser::SQLStatementList> stmt_list(parser.BuildParseTree(query).release());
  EXPECT_TRUE(stmt_list->is_valid);
  EXPECT_EQ(StatementType::CREATE, stmt_list->GetStatement(0)->GetType());
  auto create_trigger_stmt =
    static_cast<parser::CreateStatement *>(stmt_list->GetStatement(0));

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
  EXPECT_TRUE(TRIGGER_FOR_UPDATE(trigger_type));
  EXPECT_FALSE(TRIGGER_FOR_INSERT(trigger_type));
  EXPECT_FALSE(TRIGGER_FOR_DELETE(trigger_type));
  EXPECT_FALSE(TRIGGER_FOR_TRUNCATE(trigger_type));

  // Execute the create trigger
  auto txn = txn_manager.BeginTransaction();
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
  EXPECT_EQ(new_trigger->GetTriggerName(), "b_r_insert_trigger");

  commands::TriggerList *new_trigger_list = target_table->GetTriggerList();
  EXPECT_EQ(1, new_trigger_list->GetTriggerListSize());
  EXPECT_TRUE(new_trigger_list->HasTriggerType(commands::EnumTriggerType::BEFORE_UPDATE_ROW));


  InsertTupleHelper(2333, "LTI");

  // TODO: the effect of this operation should be verified automatically.
  // The UDF should be called after this operation happens.
  // Auto check can be implemented after UDF is implemented.

  // free the database just created
  txn = txn_manager.BeginTransaction();
  catalog::Catalog::GetInstance()->DropDatabaseWithName(DEFAULT_DB_NAME, txn);
  txn_manager.CommitTransaction(txn);

}
}
}
