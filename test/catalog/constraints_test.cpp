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
#include "optimizer/simple_optimizer.h"
#include "executor/logical_tile_factory.h"
#include "executor/plan_executor.h"
#include "executor/seq_scan_executor.h"
#include "executor/update_executor.h"
#include "planner/plan_util.h"
#include "parser/postgresparser.h"

#define NOTNULL_TEST
#define MULTI_NOTNULL_TEST
#define CHECK_TEST
#define DEFAULT_TEST
//#define PRIMARY_UNIQUEKEY_TEST
#define FOREIGHN_KEY_TEST
#define FOREIGHN_MULTI_KEY_TEST
#define UNIQUE_TEST
#define MULTI_UNIQUE_TEST

#define DEFAULT_VALUE 11111

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
  ConstraintsTestsUtil::CreateAndPopulateTable();
  std::unique_ptr<storage::DataTable> data_table(
     ConstraintsTestsUtil::CreateAndPopulateTable());

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
  LOG_DEBUG("**** %s", constraints.GetInfo().c_str());
  std::unique_ptr<catalog::Schema> table_schema(
      new catalog::Schema({column1, column2}));
  std::string table_name("TEST_TABLE");
  catalog::Catalog::GetInstance()->CreateTable(DEFAULT_DB_NAME, table_name,
                                               std::move(table_schema), txn);
  txn_manager.CommitTransaction(txn);
  storage::Database *database = catalog->GetDatabaseWithName(DEFAULT_DB_NAME);
  storage::DataTable *table = database->GetTableWithName(table_name);

  // table->AddUNIQUEIndex();

  txn = txn_manager.BeginTransaction();
  // begin this transaction
  // Test1: insert a tuple with column  meet the unique requirment
  bool hasException = false;
/*

  try {
    // bool result = true;
    // result =
    ConstraintsTestsUtil::ExecuteOneInsert(
        txn, table, type::ValueFactory::GetIntegerValue(10));
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
        txn, table, type::ValueFactory::GetIntegerValue(10));
    // if (result == false) hasException = true;
  } catch (ConstraintException e) {
    hasException = true;
  }
  EXPECT_TRUE(hasException);
*/

  hasException = false;
  try {
    ConstraintsTestsUtil::ExecuteOneInsert(
        txn, table, type::ValueFactory::GetIntegerValue(10));
  } catch (ConstraintException e) {
    hasException = true;
  }
  EXPECT_FALSE(hasException);

  // commit this transaction
  txn_manager.CommitTransaction(txn);
  txn = txn_manager.BeginTransaction();
  catalog::Catalog::GetInstance()->DropDatabaseWithName(db_name, txn);
  txn_manager.CommitTransaction(txn);
}
#endif

#ifdef MULTI_NOTNULL_TEST
TEST_F(ConstraintsTests, MULTINOTNULLTest) {
  auto column1 = catalog::Column(type::Type::INTEGER,
                                 type::Type::GetTypeSize(type::Type::INTEGER),
                                 "A", false, 0);
  auto column2 = catalog::Column(type::Type::VARCHAR, 25, "B", false, 1);

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
  catalog::Constraint defalut_constraint(ConstraintType::DEFAULT, "default");
  defalut_constraint.addDefaultValue(type::ValueFactory::GetIntegerValue(DEFAULT_VALUE));
  col2.AddConstraint(defalut_constraint);

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

  std::string q1 = "INSERT INTO test_table VALUES (1, 10, NULL);";

  std::unique_ptr<Statement> statement;
  statement.reset(new Statement("INSERT", q1));
  auto& peloton_parser = parser::PostgresParser::GetInstance();
  auto insert_stmt = peloton_parser.BuildParseTree(q1);

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
  std::string s1, s2, col3val;

  for (unsigned int i = 0; i < result1.size(); i++) {
    StatementResult r = result1[i];
    std::string ss1, ss2;

    for (unsigned char c : r.first) {
      ss1 += c;
    }

    for (unsigned char c : r.second) {
      ss2 += c;
    }

    s1 += ss1 + " ";
    s2 += ss2 + " ";

    if (i == result1.size() - 1) {
      col3val += ss2;
    }
  }

  LOG_INFO("SELECT result from 1 : %s", s1.c_str());
  LOG_INFO("SELECT result from 2 : %s", s2.c_str());

  EXPECT_EQ(DEFAULT_VALUE, std::stoi(col3val));

  // Delete the database
  txn = txn_manager.BeginTransaction();
  catalog->DropDatabaseWithName(DEFAULT_DB_NAME, txn);
  txn_manager.CommitTransaction(txn);

}
#endif

#ifdef FOREIGHN_KEY_TEST
TEST_F(ConstraintsTests, ForeignKeyTest) {
  // Create the database
  auto catalog = catalog::Catalog::GetInstance();
  catalog->CreateDatabase(DEFAULT_DB_NAME, nullptr);

  optimizer::SimpleOptimizer optimizer;
  auto& traffic_cop = tcop::TrafficCop::GetInstance();

  auto &txn_manager = concurrency::TransactionManagerFactory::GetInstance();

  // Create the referenced table
  auto txn = txn_manager.BeginTransaction();
  LOG_INFO("================================================");
  LOG_INFO("=====Starting to create the referenced table====");

  // Referenced Table
  // Three columns
  auto primary_col = catalog::Column(type::Type::INTEGER,
                                     type::Type::GetTypeSize(
                                       type::Type::INTEGER), "uid", true);
  catalog::Constraint primary_constraint(ConstraintType::PRIMARY, "primary");
  primary_col.AddConstraint(primary_constraint);

  auto col1 = catalog::Column(type::Type::INTEGER,
                              type::Type::GetTypeSize(
                                type::Type::INTEGER), "num", true);

  std::unique_ptr<catalog::Schema> table_schema(
    new catalog::Schema({primary_col, col1}));

  std::unique_ptr<executor::ExecutorContext> context(
    new executor::ExecutorContext(txn));

  planner::CreatePlan node("table2", DEFAULT_DB_NAME,
                           std::move(table_schema), CreateType::TABLE);
  executor::CreateExecutor create_executor(&node, context.get());

  create_executor.Init();
  create_executor.Execute();

  txn_manager.CommitTransaction(txn);

  LOG_INFO("==========Referenced table created !============");
  LOG_INFO("================================================");


  // Create the referencing table
  txn = txn_manager.BeginTransaction();
  LOG_INFO("================================================");
  LOG_INFO("=====Starting to create the referenced table====");

  // Three columns
  auto primary_col1 = catalog::Column(type::Type::INTEGER,
                                     type::Type::GetTypeSize(
                                       type::Type::INTEGER), "gid", true);
  catalog::Constraint primary_constraint1(ConstraintType::PRIMARY, "primary");
  primary_col1.AddConstraint(primary_constraint1);

  auto col2 = catalog::Column(type::Type::INTEGER,
                              type::Type::GetTypeSize(
                                type::Type::INTEGER), "uid", true);

  std::unique_ptr<catalog::Schema> table_schema1(
    new catalog::Schema({primary_col1, col2}));

  std::unique_ptr<executor::ExecutorContext> context1(
    new executor::ExecutorContext(txn));

  planner::CreatePlan node1("table1", DEFAULT_DB_NAME,
                           std::move(table_schema1), CreateType::TABLE);
  executor::CreateExecutor create_executor1(&node1, context1.get());

  create_executor1.Init();
  create_executor1.Execute();

  txn_manager.CommitTransaction(txn);

  LOG_INFO("==========Referencing table created !===========");
  LOG_INFO("================================================");

  storage::DataTable* table1 =
    catalog->GetTableWithName(DEFAULT_DB_NAME, "table1");
  storage::DataTable* table2 =
    catalog->GetTableWithName(DEFAULT_DB_NAME, "table2");

  LOG_INFO("Table 1 : %s", table1->GetInfo().c_str());
  LOG_INFO("Table 2 : %s", table2->GetInfo().c_str());

  // Add the foreign key constraint to table 1
  txn = txn_manager.BeginTransaction();

  catalog::ForeignKey fk(table2->GetOid(), {"uid"}, {0}, {"uid"}, {1}, '0', '0', "fuck");
  table1->AddForeignKey(&fk);

  txn_manager.CommitTransaction(txn);

  // Insert some records
  txn = txn_manager.BeginTransaction();
  LOG_INFO("================================================");
  LOG_INFO("============Starting to insert records==========");

  std::string q1 = "INSERT INTO table2 VALUES (1, 10);";

  std::unique_ptr<Statement> statement;
  statement.reset(new Statement("INSERT", q1));
  auto& peloton_parser = parser::PostgresParser::GetInstance();
  auto insert_stmt = peloton_parser.BuildParseTree(q1);

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

  // Insert some records
  txn = txn_manager.BeginTransaction();
  LOG_INFO("================================================");
  LOG_INFO("============Starting to insert records==========");

  std::string q2 = "INSERT INTO table1 VALUES (100, 3);";

  statement.reset(new Statement("INSERT", q2));
  auto insert_stmt2 = peloton_parser.BuildParseTree(q2);

  statement->SetPlanTree(optimizer.BuildPelotonPlanTree(insert_stmt2));
  std::vector<type::Value> params2;
  std::vector<StatementResult> result2;
  LOG_INFO("Executing plan...\n%s",
           planner::PlanUtil::GetInfo(statement->GetPlanTree().get()).c_str());

  std::vector<int> result_format2;
  result_format2 =
    std::move(std::vector<int>(statement->GetTupleDescriptor().size(), 0));
  bridge::peloton_status status2 = traffic_cop.ExecuteStatementPlan(
    statement->GetPlanTree().get(), params2, result2, result_format2);
  LOG_INFO("Statement executed. Result: %s",
           ResultTypeToString(status2.m_result).c_str());
  EXPECT_EQ("ABORTED", ResultTypeToString(status2.m_result));
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

  txn = txn_manager.BeginTransaction();
  // begin this transaction
  // Test1: insert a tuple with column  meet the unique requirment
  bool hasException = false;
  try {
    // bool result = true;
    // result =
    ConstraintsTestsUtil::ExecuteOneInsert(
        txn, table, type::ValueFactory::GetIntegerValue(10));
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
        txn, table, type::ValueFactory::GetIntegerValue(10));
    // if (result == false) hasException = true;
  } catch (ConstraintException e) {
    hasException = true;
  }
  EXPECT_TRUE(hasException);

  hasException = false;
  try {
    ConstraintsTestsUtil::ExecuteOneInsert(
        txn, table, type::ValueFactory::GetIntegerValue(20));
  } catch (ConstraintException e) {
    hasException = true;
  }
  EXPECT_FALSE(hasException);

  // commit this transaction
  txn_manager.CommitTransaction(txn);

  txn = txn_manager.BeginTransaction();
  catalog::Catalog::GetInstance()->DropDatabaseWithName(DEFAULT_DB_NAME, txn);
  txn_manager.CommitTransaction(txn);
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
    ConstraintsTestsUtil::ExecuteMultiInsert(txn, table, ccs);
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
    ConstraintsTestsUtil::ExecuteMultiInsert(txn, table, ccs);
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
    ConstraintsTestsUtil::ExecuteMultiInsert(txn, table, ccs);
  } catch (ConstraintException e) {
    hasException = true;
  }
  EXPECT_FALSE(hasException);

  // commit this transaction
  txn_manager.CommitTransaction(txn);
  txn = txn_manager.BeginTransaction();
  catalog::Catalog::GetInstance()->DropDatabaseWithName(db_name, txn);
  txn_manager.CommitTransaction(txn);
}
#endif

#ifdef FOREIGHN_KEY_TEST
TEST_F(ConstraintsTests, ForeignKeySingleInsertTest) {
  // First, initial 2 tables like following
  //     TABLE A -- src table          TABLE B -- sink table
  // a int(primary, ref B)  b int      b int(primary)  c int
  //    0                    0               0             0
  //    1                    0               1             0
  //    2                    0               2             0
  //                                      .....
  //                                         9             0

  // create new db
  auto &txn_manager = concurrency::TransactionManagerFactory::GetInstance();
  auto txn = txn_manager.BeginTransaction();
  auto catalog = catalog::Catalog::GetInstance();
  std::string db_name = "db2";
  std::string table_a_name = "tableA";
  std::string table_b_name = "tableB";
  catalog::Catalog::GetInstance()->CreateDatabase(db_name, nullptr);
  // txn_manager.CommitTransaction(txn);

  auto column1 = catalog::Column(type::Type::INTEGER, 25, "a", false, 0);
  auto column2 = catalog::Column(type::Type::INTEGER, 25, "b", false, 1);

  auto constraints = catalog::Constraint(ConstraintType::PRIMARY, "primary1");
  column1.AddConstraint(constraints);
  std::cout << "****" << constraints.GetInfo() << std::endl;
  std::unique_ptr<catalog::Schema> tableA_schema(
      new catalog::Schema({column1, column2}));

  catalog->CreateTable(db_name, table_a_name, std::move(tableA_schema), txn);
  txn_manager.CommitTransaction(txn);

  auto table_A = catalog->GetTableWithName(db_name, table_a_name);

  txn = txn_manager.BeginTransaction();
  auto column3 = catalog::Column(type::Type::INTEGER, 25, "b", false, 0);
  column3.AddConstraint(constraints);
  auto column4 = catalog::Column(type::Type::INTEGER, 25, "c", false, 1);
  std::unique_ptr<catalog::Schema> tableB_schema(
      new catalog::Schema({column3, column4}));

  catalog->CreateTable(db_name, table_b_name, std::move(tableB_schema), txn);

  auto table_a = catalog->GetTableWithName(db_name, table_a_name);
  auto table_b = catalog->GetTableWithName(db_name, table_b_name);
  txn_manager.CommitTransaction(txn);
  oid_t table_B_id = table_b->GetTableOid();
  catalog::ForeignKey *foreign_key =
      new catalog::ForeignKey(table_B_id, {"a", "b"}, {0, 1}, {"b", "c"},
                              {0, 1}, 'r', 'c', "foreign_constraint1");
  table_A->AddForeignKey(foreign_key);

  // Test1: insert a tuple with column  meet the constraint requirment

  txn = txn_manager.BeginTransaction();
  // begin this transaction
  // Test1: insert a tuple with column  meet the unique requirment
  bool hasException = false;
  try {
    std::vector<type::Value> ccs;
    ccs.push_back(type::ValueFactory::GetIntegerValue(1));
    ccs.push_back(type::ValueFactory::GetIntegerValue(2));
    ConstraintsTestsUtil::ExecuteMultiInsert(txn, table_b, ccs);
    ccs.clear();
    ccs.push_back(type::ValueFactory::GetIntegerValue(1));
    ccs.push_back(type::ValueFactory::GetIntegerValue(2));
    ConstraintsTestsUtil::ExecuteMultiInsert(txn, table_a, ccs);
  } catch (ConstraintException e) {
    hasException = true;
  }
  EXPECT_FALSE(hasException);

  hasException = true;
  try {
    std::vector<type::Value> ccs;
    ccs.push_back(type::ValueFactory::GetIntegerValue(3));
    ccs.push_back(type::ValueFactory::GetIntegerValue(4));
    ConstraintsTestsUtil::ExecuteMultiInsert(txn, table_b, ccs);
    ccs.clear();
    ccs.push_back(type::ValueFactory::GetIntegerValue(2));
    ccs.push_back(type::ValueFactory::GetIntegerValue(5));
    ConstraintsTestsUtil::ExecuteMultiInsert(txn, table_a, ccs);
  } catch (ConstraintException e) {
    hasException = true;
  }
  EXPECT_TRUE(hasException);

  // commit this transaction
  txn_manager.CommitTransaction(txn);
}

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

*/
}  // End test namespace
}  // End peloton namespace
