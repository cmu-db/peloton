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

#define NOTNULL_TEST
#define MULTI_NOTNULL_TEST
#define CHECK_TEST
//#define PRIMARY_UNIQUEKEY_TEST
#define FOREIGHN_KEY_TEST
#define FOREIGHN_MULTI_KEY_TEST
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

  // Bootstrap
  auto &txn_manager = concurrency::TransactionManagerFactory::GetInstance();
  auto catalog = catalog::Catalog::GetInstance();
  auto txn = txn_manager.BeginTransaction();
  auto db_name = "db";
  catalog->CreateDatabase(db_name, txn);
  auto column1 = catalog::Column(type::Type::INTEGER, 25, "A", false, 0);
  auto constraints = catalog::Constraint(ConstraintType::NOTNULL, "notnull1");
  column1.AddConstraint(constraints);
  std::cout << "****" << constraints.GetInfo() << std::endl;
  std::unique_ptr<catalog::Schema> table_schema(new catalog::Schema({column1}));
  std::string table_name("TEST_TABLE");
  catalog::Catalog::GetInstance()->CreateTable(db_name, table_name,
                                               std::move(table_schema), txn);
  txn_manager.CommitTransaction(txn);
  storage::Database *database = catalog->GetDatabaseWithName(db_name);
  storage::DataTable *table = database->GetTableWithName(table_name);

  // Test1: insert a tuple with column 1 = null
  txn = txn_manager.BeginTransaction();
  bool hasException = false;
  try {
    ConstraintsTestsUtil::ExecuteOneInsert(
        txn, table,
        type::ValueFactory::GetNullValueByType(type::Type::INTEGER));
  } catch (ConstraintException e) {
    hasException = true;
  }
  EXPECT_TRUE(hasException);

  // Test2: insert a legal tuple
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

  // commit this transaction
  txn_manager.CommitTransaction(txn);
  delete data_table.release();
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
  txn = txn_manager.BeginTransaction();
  catalog::Catalog::GetInstance()->DropDatabaseWithName(db_name, txn);
  txn_manager.CommitTransaction(txn);
  delete foreign_key;
}
#endif

#ifdef FOREIGHN_MULTI_KEY_TEST
TEST_F(ConstraintsTests, ForeignKeyMultiInsertTest) {
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
  catalog->CreateDatabase(db_name, txn);

  auto column1 = catalog::Column(type::Type::INTEGER, 25, "a", false, 0);
  auto column2 = catalog::Column(type::Type::INTEGER, 25, "b", false, 1);

  std::unique_ptr<catalog::Schema> tableA_schema(
      new catalog::Schema({column1, column2}));

  catalog->CreateTable(db_name, table_a_name, std::move(tableA_schema), txn);
  txn_manager.CommitTransaction(txn);
  auto table_A = catalog->GetTableWithName(db_name, table_a_name);

  txn = txn_manager.BeginTransaction();
  auto column3 = catalog::Column(type::Type::INTEGER, 25, "b", false, 0);
  auto column4 = catalog::Column(type::Type::INTEGER, 25, "c", false, 1);
  std::vector<oid_t> cols;
  cols.push_back(0);
  cols.push_back(1);
  auto mc =
      catalog::MultiConstraint(ConstraintType::PRIMARY, "multiprimary1", cols);
  std::cout << "**** MULTI CONSTRAINTS ****" << mc.GetInfo() << std::endl;

  catalog::Schema *table_schema = new catalog::Schema({column3, column4});
  table_schema->AddMultiConstraints(mc);
  std::unique_ptr<catalog::Schema> tableB_schema(table_schema);

  catalog->CreateTable(db_name, table_b_name, std::move(tableB_schema), txn);
  auto table_a = catalog->GetTableWithName(db_name, table_a_name);
  auto table_b = catalog->GetTableWithName(db_name, table_b_name);
  txn_manager.CommitTransaction(txn);
  oid_t table_B_id = table_b->GetTableOid();
  catalog::ForeignKey *foreign_key = new catalog::ForeignKey(
      table_B_id, {"B"}, {1}, {"B"}, {1}, 'r', 'c', "foreign_constraint1");
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
    ccs.push_back(type::ValueFactory::GetIntegerValue(2));
    ccs.push_back(type::ValueFactory::GetIntegerValue(1));
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
  txn = txn_manager.BeginTransaction();
  catalog::Catalog::GetInstance()->DropDatabaseWithName(db_name, txn);
  txn_manager.CommitTransaction(txn);
  delete foreign_key;
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

*/
}  // End test namespace
}  // End peloton namespace
