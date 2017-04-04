//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// constraints_test.cpp
//
// Identification: test/catalog/constraints_test.cpp
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
#define VALUE_TESTS

#include <include/tcop/tcop.h>
#include <include/planner/create_plan.h>
#include <include/executor/executors.h>
#include "gtest/gtest.h"

#include "concurrency/testing_transaction_util.h"
#include "common/harness.h"

#include "catalog/schema.h"
#include "catalog/foreign_key.h"
#include "type/value.h"
#include "concurrency/transaction.h"
#include "concurrency/transaction_manager_factory.h"
#include "executor/abstract_executor.h"
#include "storage/tile_group_factory.h"
#include "storage/tuple.h"
#include "storage/table_factory.h"
#include "storage/database.h"
#include "index/index_factory.h"
#include "catalog/constraints_tests_util.h"

#include "common/harness.h"
#include "common/macros.h"
#include "common/logger.h"
#include "parser/parser.h"
#include "optimizer/simple_optimizer.h"
#include "executor/logical_tile_factory.h"
#include "executor/plan_executor.h"
#include "executor/seq_scan_executor.h"
#include "executor/update_executor.h"
#include "planner/plan_util.h"

#define NOTNULL_TEST
#define MULTI_NOTNULL_TEST
#define CHECK_TEST
#define DEFAULT_TEST
//#define PRIMARY_UNIQUEKEY_TEST
//#define FOREIGHN_KEY_TEST
#define UNIQUE_TEST
#define MULTI_UNIQUE_TEST

namespace peloton {
namespace test {

//===--------------------------------------------------------------------===//
// Constraints Tests
//===--------------------------------------------------------------------===//

class ConstraintsTests : public PelotonTest {};

// FIXME: see the explanation rpc_client_test and rpc_server_test
#ifdef NOTNULL_TEST
TEST_F(ConstraintsTests, NOTNULLTest) {
  // First, generate the table with index
  // this table has 15 rows:
  //  int(primary)  int   double  var(22) (unique)
  //  0             1     2       "3"
  //  10            11    12      "13"
  //  20            21    22      "23"
  //  .....
  //  140           141   142     "143"
  // ConstraintsTestsUtil::CreateAndPopulateTable();

  // Bootstrap
  ConstraintsTestsUtil::CreateAndPopulateTable();

  std::unique_ptr<storage::DataTable> data_table(
      ConstraintsTestsUtil::CreateAndPopulateTable());

  auto &txn_manager = concurrency::TransactionManagerFactory::GetInstance();

  // begin this transaction
  auto txn = txn_manager.BeginTransaction();

  // Test1: insert a tuple with column 1 = null
  bool hasException = false;
/*
  try {
    ConstraintsTestsUtil::ExecuteInsert(
        txn, data_table.get(),
        type::ValueFactory::GetNullValueByType(type::Type::INTEGER),
        type::ValueFactory::GetIntegerValue(
            ConstraintsTestsUtil::PopulatedValue(15, 1)),
        type::ValueFactory::GetIntegerValue(
            ConstraintsTestsUtil::PopulatedValue(15, 2)),
        type::ValueFactory::GetVarcharValue(
            std::to_string(ConstraintsTestsUtil::PopulatedValue(15, 3))));

  } catch (ConstraintException e) {
    hasException = true;
  }
  EXPECT_TRUE(hasException);
*/

  // Test2: insert a legal tuple
  hasException = false;
  try {
    ConstraintsTestsUtil::ExecuteInsert(
        txn, data_table.get(), type::ValueFactory::GetIntegerValue(
                                   ConstraintsTestsUtil::PopulatedValue(15, 0)),
        type::ValueFactory::GetIntegerValue(
            ConstraintsTestsUtil::PopulatedValue(15, 1)),
        type::ValueFactory::GetIntegerValue(
            ConstraintsTestsUtil::PopulatedValue(15, 2)),
        type::ValueFactory::GetVarcharValue(
            std::to_string(ConstraintsTestsUtil::PopulatedValue(15, 3))));

  } catch (ConstraintException e) {
    hasException = true;
  }
  EXPECT_FALSE(hasException);

  // commit this transaction
  txn_manager.CommitTransaction(txn);
  delete data_table.release();
}
#endif

#ifdef MULTI_NOTNULL_TEST
TEST_F(ConstraintsTests, MULTINOTNULLTest) {
  auto column1 = catalog::Column(type::Type::INTEGER,
                                 type::Type::GetTypeSize(type::Type::INTEGER),
                                 "A", false, 0);
  auto column2 = catalog::Column(type::Type::VARCHAR, 25, "B", false, 1);
  // std::cout << "xxxxxx column idx " << column1.GetOffset() << std::endl;
  // std::cout << "xxxxxx column idx " << column2.GetOffset() << std::endl;

  std::vector<oid_t> cols;
  cols.push_back(0);
  cols.push_back(1);

  std::vector<catalog::Column> columns;
  columns.push_back(column1);
  columns.push_back(column2);

  auto mc = catalog::MultiConstraint(ConstraintType::NOTNULL, "c1", cols);
  std::cout << "**** MULTI CONSTRAINTS ****" << mc.GetInfo() << std::endl;
  catalog::Schema *table_schema = new catalog::Schema(columns);
  table_schema->AddMultiConstraints(mc);
  std::string table_name("TEST_TABLE");
  bool own_schema = true;
  bool adapt_table = false;
  storage::DataTable *table = storage::TableFactory::GetDataTable(
      INVALID_OID, INVALID_OID, table_schema, table_name,
      TESTS_TUPLES_PER_TILEGROUP, own_schema, adapt_table);
  std::unique_ptr<storage::DataTable> data_table(table);

  auto &txn_manager = concurrency::TransactionManagerFactory::GetInstance();

  // begin this transaction
  auto txn = txn_manager.BeginTransaction();

  // two columns are both NULL
  bool hasException = false;
  try {
    std::vector<type::Value> ccs;
    ccs.push_back(type::ValueFactory::GetNullValueByType(type::Type::INTEGER));
    ccs.push_back(type::ValueFactory::GetNullValueByType(type::Type::INTEGER));

    ConstraintsTestsUtil::ExecuteMultiInsert(txn, data_table.get(), ccs);
  } catch (ConstraintException e) {
    hasException = true;
  }
  EXPECT_TRUE(hasException);

  // one column is NULL
  hasException = false;
  try {
    std::vector<type::Value> ccs;
    ccs.push_back(type::ValueFactory::GetNullValueByType(type::Type::INTEGER));
    ccs.push_back(type::ValueFactory::GetIntegerValue(10));
    ConstraintsTestsUtil::ExecuteMultiInsert(txn, data_table.get(), ccs);
  } catch (ConstraintException e) {
    hasException = true;
  }
  EXPECT_TRUE(hasException);

  // two columns are not NULL
  hasException = false;
  try {
    std::vector<type::Value> ccs;
    ccs.push_back(type::ValueFactory::GetIntegerValue(10));
    ccs.push_back(type::ValueFactory::GetIntegerValue(10));

    ConstraintsTestsUtil::ExecuteMultiInsert(txn, data_table.get(), ccs);
  } catch (ConstraintException e) {
    hasException = true;
  }
  EXPECT_FALSE(hasException);

  // Test2: insert not a valid column violate the constraint
  hasException = false;
  try {
    ConstraintsTestsUtil::ExecuteOneInsert(
        txn, data_table.get(),
        type::ValueFactory::GetIntegerValue(
            ConstraintsTestsUtil::PopulatedValue(-1, 1)));
  } catch (ConstraintException e) {
    hasException = true;
  }
  EXPECT_TRUE(hasException);

  // commit this transaction
  txn_manager.CommitTransaction(txn);
  delete data_table.release();
}
#endif

#ifdef DEFAULT_TEST
TEST_F(ConstraintsTests, DEFAULTTEST) {
  // Create the database
  auto catalog = catalog::Catalog::GetInstance();
  catalog->CreateDatabase(DEFAULT_DB_NAME, nullptr);

  optimizer::SimpleOptimizer optimizer;
  auto& traffic_cop = tcop::TrafficCop::GetInstance();

  auto &txn_manager = concurrency::TransactionManagerFactory::GetInstance();

  // Create the table
  auto txn = txn_manager.BeginTransaction();
  LOG_INFO("================================================");
  LOG_INFO("========Starting to create the test table=======");

  // Three columns
  auto primary_col = catalog::Column(type::Type::INTEGER,
                                     type::Type::GetTypeSize(
                                       type::Type::INTEGER), "id", true);
  catalog::Constraint primary_constraint(ConstraintType::PRIMARY, "primary");
  primary_col.AddConstraint(primary_constraint);

  auto col1 = catalog::Column(type::Type::INTEGER,
                              type::Type::GetTypeSize(
                                type::Type::INTEGER), "col1", true);

  auto col2 = catalog::Column(type::Type::INTEGER,
                              type::Type::GetTypeSize(
                                type::Type::INTEGER), "col2", true);

  std::unique_ptr<catalog::Schema> table_schema(
    new catalog::Schema({primary_col, col1, col2}));

  std::unique_ptr<executor::ExecutorContext> context(
    new executor::ExecutorContext(txn));

  planner::CreatePlan node("test_table", DEFAULT_DB_NAME,
                           std::move(table_schema), CreateType::TABLE);
  executor::CreateExecutor create_executor(&node, context.get());

  create_executor.Init();
  create_executor.Execute();

  create_executor.Init();
  create_executor.Execute();

  txn_manager.CommitTransaction(txn);

  LOG_INFO("==============Test table created !==============");
  LOG_INFO("================================================");

  storage::DataTable* table =
    catalog->GetTableWithName(DEFAULT_DB_NAME, "test_table");
  LOG_INFO("%s", table->GetInfo().c_str());

  // Insert some records
  txn = txn_manager.BeginTransaction();
  LOG_INFO("================================================");
  LOG_INFO("============Starting to insert records==========");

  std::unique_ptr<Statement> statement;
  statement.reset(new Statement("INSERT",
                                "INSERT INTO "
                                  "test_table(id, col1, "
                                  "col2)" " VALUES (1, 10, 100);"));
  auto& peloton_parser = parser::Parser::GetInstance();
  auto insert_stmt = peloton_parser.BuildParseTree(
    "INSERT INTO test_table(id, col1, col2) VALUES (1, 10, 100);");

  statement->SetPlanTree(optimizer.BuildPelotonPlanTree(insert_stmt));
  std::vector<type::Value> params;
  std::vector<StatementResult> result;
  LOG_INFO("Executing plan...\n%s",
           planner::PlanUtil::GetInfo(statement->GetPlanTree().get()).c_str());

  std::vector<int> result_format;
  result_format =
    std::move(std::vector<int>(statement->GetTupleDescriptor().size(), 0));
  bridge::peloton_status status = traffic_cop.ExecuteStatementPlan(
    statement->GetPlanTree().get(), params, result, result_format);
  LOG_INFO("Statement executed. Result: %s",
           ResultTypeToString(status.m_result).c_str());
  LOG_INFO("Tuple inserted!");

  txn_manager.CommitTransaction(txn);

  // Do a select query to check the results
  txn = txn_manager.BeginTransaction();
  // Do a select to get the original values
  std::unique_ptr<Statement> statement1;
  statement1.reset(new Statement("SELECT", "SELECT * FROM test_table LIMIT 1;"));
  auto select_stmt =
    peloton_parser.BuildParseTree("SELECT * FROM test_table LIMIT 1;");

  statement1->SetPlanTree(optimizer.BuildPelotonPlanTree(select_stmt));
  std::vector<type::Value> params1;
  std::vector<StatementResult> result1;
  // bridge::PlanExecutor::PrintPlan(statement->GetPlanTree().get(), "Plan");
  std::vector<int> result_format1;
  auto tuple_descriptor = traffic_cop.GenerateTupleDescriptor(
    select_stmt->GetStatement(0));
  result_format1 = std::move(std::vector<int>(tuple_descriptor.size(), 0));
  UNUSED_ATTRIBUTE bridge::peloton_status status1 =
    traffic_cop.ExecuteStatementPlan(statement1->GetPlanTree().get(), params1,
                                      result1, result_format1);
  LOG_INFO("Statement executed. Result: %s",
           ResultTypeToString(status1.m_result).c_str());
  txn_manager.CommitTransaction(txn);

  // Check the results
  // Todo: having trouble checking the tuple values
  for (StatementResult r : result1) {

    if (!r.first.empty()) {
      LOG_INFO("Data 1 : %s", reinterpret_cast<char*>(r.first.data()));
    }
    if (!r.second.empty()) {
      LOG_INFO("Data 2 : %s", reinterpret_cast<char*>(r.second.data()));
    }
  }

  // Delete the database
  txn = txn_manager.BeginTransaction();
  catalog->DropDatabaseWithName(DEFAULT_DB_NAME, txn);
  txn_manager.CommitTransaction(txn);

}
#endif

#ifdef CHECK_TEST
TEST_F(ConstraintsTests, CHECKTest) {
  // First, generate the table with index
  // this table has 15 rows:
  //  int(primary)  int   double  var(22) (unique)
  //  0             1     2       "3"
  //  10            11    12      "13"
  //  20            21    22      "23"
  //  .....
  //  140           141   142     "143"

  /*
  std::string cmd = "select A > 0 FROM TEST_TABLE;";
  parser::SQLStatementList* stmt_list =
  parser::Parser::ParseSQLString(cmd.c_str());
  EXPECT_TRUE(stmt_list->is_valid);
  if (stmt_list->is_valid == false) {
    std::cout << "Message: " << stmt_list->parser_msg << ",line: " <<
  stmt_list->error_line << " ,col:" << stmt_list->error_col << std::endl;
  }
  std::cout << stmt_list->GetStatements().size() << " " <<
  stmt_list->GetStatements().at(0)->GetType()<< std::endl;
  std::cout << "xxxxxxxxx" << stmt_list->GetInfo().c_str() << std::endl;
  delete stmt_list;
  */

  auto column1 = catalog::Column(type::Type::INTEGER, 25, "A", false, 0);
  auto constraints = catalog::Constraint(ConstraintType::CHECK, "check1");
  type::Value tmp_value = type::Value(type::Type::INTEGER, 0);
  constraints.AddCheck(ExpressionType::COMPARE_GREATERTHAN, tmp_value);
  column1.AddConstraint(constraints);
  std::cout << "****" << constraints.GetInfo() << std::endl;
  catalog::Schema *table_schema = new catalog::Schema({column1});
  std::string table_name("TEST_TABLE");
  bool own_schema = true;
  bool adapt_table = false;
  storage::DataTable *table = storage::TableFactory::GetDataTable(
      INVALID_OID, INVALID_OID, table_schema, table_name,
      TESTS_TUPLES_PER_TILEGROUP, own_schema, adapt_table);
  std::unique_ptr<storage::DataTable> data_table(table);

  auto &txn_manager = concurrency::TransactionManagerFactory::GetInstance();

  // begin this transaction
  auto txn = txn_manager.BeginTransaction();
  // Test1: insert a tuple with column  meet the constraint requirment
  bool hasException = false;
  try {
    ConstraintsTestsUtil::ExecuteOneInsert(
        txn, data_table.get(), type::ValueFactory::GetIntegerValue(10));
  } catch (ConstraintException e) {
    hasException = true;
  }
  EXPECT_FALSE(hasException);

  // Test2: insert not a valid column violate the constraint
  hasException = false;
  try {
    ConstraintsTestsUtil::ExecuteOneInsert(
        txn, data_table.get(), type::ValueFactory::GetIntegerValue(-1));
  } catch (ConstraintException e) {
    hasException = true;
  }
  EXPECT_TRUE(hasException);

  // commit this transaction
  txn_manager.CommitTransaction(txn);
  delete data_table.release();
}
#endif

TEST_F(ConstraintsTests, DEFAULTTEST) {
  std::unique_ptr<storage::DataTable> data_table(
    ConstraintsTestsUtil::CreateAndPopulateTable());

  auto schema = data_table->GetSchema();

  catalog::Constraint constraint(ConstraintType::DEFAULT, "Default Constraint");
  auto v = type::ValueFactory::GetIntegerValue(ConstraintsTestsUtil::PopulatedValue(15, 1));
  constraint.addDefaultValue(v);
  schema->AddConstraint(1, constraint);

  auto &txn_manager = concurrency::TransactionManagerFactory::GetInstance();

  // begin this transaction
  auto txn = txn_manager.BeginTransaction();

  ConstraintsTestsUtil::ExecuteInsert(
    txn, data_table.get(),
    type::ValueFactory::GetIntegerValue(
      ConstraintsTestsUtil::PopulatedValue(15, 0)),
    type::ValueFactory::GetNullValueByType(type::Type::INTEGER),
    type::ValueFactory::GetIntegerValue(
      ConstraintsTestsUtil::PopulatedValue(15, 2)),
    type::ValueFactory::GetVarcharValue(
      std::to_string(ConstraintsTestsUtil::PopulatedValue(15, 3))));



  txn_manager.CommitTransaction(txn);
  delete data_table.release();
}

#ifdef UNIQUE_TEST
TEST_F(ConstraintsTests, UNIQUETest) {
  auto &txn_manager = concurrency::TransactionManagerFactory::GetInstance();
  auto catalog = catalog::Catalog::GetInstance();
  auto txn = txn_manager.BeginTransaction();
  catalog->CreateDatabase(DEFAULT_DB_NAME, nullptr);
  auto column1 = catalog::Column(type::Type::INTEGER, 25, "A", false, 0);
  auto column2 = catalog::Column(type::Type::INTEGER, 25, "B", false, 1);

  auto constraints = catalog::Constraint(ConstraintType::UNIQUE, "unique1");
  column1.AddConstraint(constraints);
  std::cout << "****" << constraints.GetInfo() << std::endl;
  std::unique_ptr<catalog::Schema> table_schema(
      new catalog::Schema({column1, column2}));
  std::string table_name("TEST_TABLE");
  catalog::Catalog::GetInstance()->CreateTable(DEFAULT_DB_NAME, table_name,
                                               std::move(table_schema), txn);
  txn_manager.CommitTransaction(txn);
  storage::Database *database = catalog->GetDatabaseWithName(DEFAULT_DB_NAME);
  storage::DataTable *table = database->GetTableWithName(table_name);

  // table->AddUNIQUEIndex();
  std::unique_ptr<storage::DataTable> data_table(table);

  txn = txn_manager.BeginTransaction();
  // begin this transaction
  // Test1: insert a tuple with column  meet the unique requirment
  bool hasException = false;
  try {
    // bool result = true;
    // result =
    ConstraintsTestsUtil::ExecuteOneInsert(
        txn, data_table.get(), type::ValueFactory::GetIntegerValue(10));
    // if (result == false) hasException = true;
  } catch (ConstraintException e) {
    hasException = true;
  }
  EXPECT_FALSE(hasException);

  // Test2: insert not a valid column violate the constraint
  hasException = false;
  try {
    // bool result = true;
    // result =
    ConstraintsTestsUtil::ExecuteOneInsert(
        txn, data_table.get(), type::ValueFactory::GetIntegerValue(10));
    // if (result == false) hasException = true;
  } catch (ConstraintException e) {
    hasException = true;
  }
  EXPECT_TRUE(hasException);

  hasException = false;
  try {
    // bool result = true;
    // result =
    ConstraintsTestsUtil::ExecuteOneInsert(
        txn, data_table.get(), type::ValueFactory::GetIntegerValue(20));
    // if (result == false) hasException = true;
  } catch (ConstraintException e) {
    hasException = true;
  }
  EXPECT_FALSE(hasException);

  // commit this transaction
  txn_manager.CommitTransaction(txn);
  delete data_table.release();
  delete table_schema.release();
}
#endif

#ifdef MULTI_UNIQUE_TEST
TEST_F(ConstraintsTests, MULTIUNIQUETest) {
  auto &txn_manager = concurrency::TransactionManagerFactory::GetInstance();
  auto catalog = catalog::Catalog::GetInstance();
  auto txn = txn_manager.BeginTransaction();
  std::string db_name = "db1";
  catalog->CreateDatabase(db_name, nullptr);
  auto column1 = catalog::Column(type::Type::INTEGER, 25, "A", false, 0);
  auto column2 = catalog::Column(type::Type::INTEGER, 25, "B", false, 1);
  auto column3 = catalog::Column(type::Type::INTEGER, 25, "C", false, 2);
  std::vector<oid_t> cols;
  cols.push_back(0);
  cols.push_back(1);
  std::vector<catalog::Column> columns;
  columns.push_back(column1);
  columns.push_back(column2);
  columns.push_back(column3);
  auto mc = catalog::MultiConstraint(ConstraintType::UNIQUE, "c1", cols);
  std::cout << "**** MULTI CONSTRAINTS ****" << mc.GetInfo() << std::endl;

  std::unique_ptr<catalog::Schema> table_schema(new catalog::Schema(columns));
  table_schema->AddMultiConstraints(mc);
  std::string table_name("TEST_TABLE_1");
  catalog->CreateTable(db_name, table_name, std::move(table_schema), txn);
  txn_manager.CommitTransaction(txn);
  storage::Database *database = catalog->GetDatabaseWithName(db_name);
  storage::DataTable *table = database->GetTableWithName(table_name);

  // table->AddUNIQUEIndex();
  std::unique_ptr<storage::DataTable> data_table(table);

  txn = txn_manager.BeginTransaction();
  // begin this transaction
  // Test1: insert a tuple with column  meet the unique requirment
  bool hasException = false;
  try {
    std::vector<type::Value> ccs;
    ccs.push_back(type::ValueFactory::GetIntegerValue(10));
    ccs.push_back(type::ValueFactory::GetIntegerValue(11));
    // bool result = true;
    // result =
    ConstraintsTestsUtil::ExecuteMultiInsert(txn, data_table.get(), ccs);
    // if (result == false) hasException = true;
  } catch (ConstraintException e) {
    hasException = true;
  }
  EXPECT_FALSE(hasException);

  // Test2: insert not a valid column violate the constraint
  hasException = false;
  try {
    std::vector<type::Value> ccs;
    ccs.push_back(type::ValueFactory::GetIntegerValue(10));
    ccs.push_back(type::ValueFactory::GetIntegerValue(11));
    // bool result = true;
    // result =
    ConstraintsTestsUtil::ExecuteMultiInsert(txn, data_table.get(), ccs);
    // if (result == false) hasException = true;
  } catch (ConstraintException e) {
    hasException = true;
  }
  EXPECT_TRUE(hasException);

  hasException = false;
  try {
    std::vector<type::Value> ccs;
    ccs.push_back(type::ValueFactory::GetIntegerValue(10));
    ccs.push_back(type::ValueFactory::GetIntegerValue(12));
    // bool result = true;
    // result =
    ConstraintsTestsUtil::ExecuteMultiInsert(txn, data_table.get(), ccs);
    // if (result == false) hasException = true;
  } catch (ConstraintException e) {
    hasException = true;
  }
  EXPECT_FALSE(hasException);

  // commit this transaction
  txn_manager.CommitTransaction(txn);
  delete data_table.release();
  delete table_schema.release();
}
#endif

/*
#ifdef PRIMARY_UNIQUEKEY_TEST
TEST_F(ConstraintsTests, CombinedPrimaryKeyTest) {
  // First, generate the table with index
  // this table has 10 rows:
  //  int(primary)  int(primary)
  //  0             0
  //  1             1
  //  2             2
  //  .....
  //  9             9

  auto &txn_manager = concurrency::TransactionManagerFactory::GetInstance();

  {
    std::unique_ptr<storage::DataTable> data_table(
        TransactionTestsUtil::CreateCombinedPrimaryKeyTable());
    // Test1: insert 2 tuple with duplicated primary key
    // txn1: insert (0, 1) -- success
    // txn0 commit
    // txn1: insert (1, 1) -- fail
    // txn1 commit
    TransactionScheduler scheduler(2, data_table.get(), &txn_manager);
    scheduler.Txn(0).Insert(0, 1);
    scheduler.Txn(0).Commit();
    scheduler.Txn(1).Insert(1, 1);
    scheduler.Txn(1).Commit();

    scheduler.Run();

    EXPECT_TRUE(ResultType::SUCCESS == scheduler.schedules[0].txn_result);
    EXPECT_TRUE(ResultType::ABORTED == scheduler.schedules[1].txn_result);
  }
}

TEST_F(ConstraintsTests, MultiTransactionUniqueConstraintsTest) {
  // First, generate the table with index
  // this table has 10 rows:
  //  int(primary)  int(unique)
  //  0             0
  //  1             1
  //  2             2
  //  .....
  //  9             9

  auto &txn_manager = concurrency::TransactionManagerFactory::GetInstance();

  {
    std::unique_ptr<storage::DataTable> data_table(
        TransactionTestsUtil::CreatePrimaryKeyUniqueKeyTable());
    // Test1: insert 2 tuple with duplicated primary key
    TransactionScheduler scheduler(2, data_table.get(), &txn_manager);
    scheduler.Txn(0).Insert(10, 10);
    scheduler.Txn(1).Insert(10, 11);
    scheduler.Txn(0).Commit();
    scheduler.Txn(1).Commit();

    scheduler.Run();

    EXPECT_TRUE((ResultType::SUCCESS == scheduler.schedules[0].txn_result &&
                 ResultType::ABORTED == scheduler.schedules[1].txn_result) ||
                (ResultType::SUCCESS == scheduler.schedules[1].txn_result &&
                 ResultType::ABORTED == scheduler.schedules[0].txn_result));
  }

  {
    std::unique_ptr<storage::DataTable> data_table(
        TransactionTestsUtil::CreatePrimaryKeyUniqueKeyTable());
    // Test2: update a tuple to be a illegal primary key
    // txn1: update (1, 1) -> (1,11) -- success
    // txn0: update (0, 0) -> (0,1) -- fail
    // txn1 commit
    // txn0 commit
    TransactionScheduler scheduler(2, data_table.get(), &txn_manager);
    scheduler.Txn(1).Update(1, 11);
    scheduler.Txn(0).Update(0, 1);
    scheduler.Txn(1).Commit();
    scheduler.Txn(0).Commit();

    scheduler.Run();

    EXPECT_TRUE(ResultType::ABORTED == scheduler.schedules[0].txn_result);
    EXPECT_TRUE(ResultType::SUCCESS == scheduler.schedules[1].txn_result);
  }

  {
    std::unique_ptr<storage::DataTable> data_table(
        TransactionTestsUtil::CreatePrimaryKeyUniqueKeyTable());
    // Test3: update a tuple to be a legal primary key
    // txn1: update (1, 1) -> (1,11) -- success
    // txn1 commit
    // txn0: update (0, 0) -> (0,1) -- success
    // txn0 commit
    TransactionScheduler scheduler(2, data_table.get(), &txn_manager);
    scheduler.Txn(1).Update(1, 11);
    scheduler.Txn(1).Commit();
    scheduler.Txn(0).Update(0, 1);
    scheduler.Txn(0).Commit();

    scheduler.Run();

    EXPECT_TRUE(ResultType::SUCCESS == scheduler.schedules[0].txn_result);
    EXPECT_TRUE(ResultType::SUCCESS == scheduler.schedules[1].txn_result);
  }
}
#endif

#ifdef FOREIGHN_KEY_TEST
TEST_F(ConstraintsTests, ForeignKeyInsertTest) {
  // First, initial 2 tables like following
  //     TABLE A -- src table          TABLE B -- sink table
  // int(primary, ref B)  int            int(primary)  int
  //    0                 0               0             0
  //    1                 0               1             0
  //    2                 0               2             0
  //                                      .....
  //                                      9             0

  // create new db
  auto &manager = catalog::Manager::GetInstance();
  oid_t current_db_oid = 2001;
  auto newdb = new storage::Database(current_db_oid);
  manager.AddDatabase(newdb);

  auto table_A =
      TransactionTestsUtil::CreateTable(3, "tableA", 0, 1000, 1000, true);
  // we wouldn't use table_B later here so we don't save the return value
  TransactionTestsUtil::CreateTable(10, "tableB", 0, 1001, 1001, true);

  // add the foreign key constraints for table_A
  std::unique_ptr<catalog::ForeignKey> foreign_key(new catalog::ForeignKey(
      1001, {"id"}, {0}, {"id"}, {0}, 'r', 'c', "THIS_IS_FOREIGN_CONSTRAINT"));
  table_A->AddForeignKey(foreign_key.get());

  auto &txn_manager = concurrency::TransactionManagerFactory::GetInstance();

  // Test1: insert 2 tuple, one of which doesn't follow foreign key constraint
  // txn0 insert (10,10) --> fail
  // txn1 insert (9,10) --> success
  // txn0 commit
  // txn1 commit
  {
    TransactionScheduler scheduler(2, table_A, &txn_manager);
    scheduler.Txn(0).Insert(10, 10);
    scheduler.Txn(1).Insert(9, 10);
    scheduler.Txn(0).Commit();
    scheduler.Txn(1).Commit();

    scheduler.Run();

    EXPECT_TRUE(ResultType::ABORTED == scheduler.schedules[0].txn_result);
    EXPECT_TRUE(ResultType::SUCCESS == scheduler.schedules[1].txn_result);
  }

  // this will also indirectly delete all tables in this database
  delete newdb;
}
#endif
*/
}  // End test namespace
}  // End peloton namespace
