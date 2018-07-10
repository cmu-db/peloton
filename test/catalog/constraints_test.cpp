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

#include "gtest/gtest.h"
#include "sql/testing_sql_util.h"

#include "catalog/testing_constraints_util.h"

#include "catalog/catalog.h"
#include "common/internal_types.h"
#include "concurrency/testing_transaction_util.h"
#include "executor/executors.h"
#include "optimizer/optimizer.h"
#include "parser/postgresparser.h"
#include "planner/create_plan.h"
#include "planner/plan_util.h"

#define DEFAULT_VALUE 11111

#define CONSTRAINT_NOTNULL_TEST
#define CONSTRAINT_DEFAULT_TEST
// #define CONSTRAINT_CHECK_TEST
#define CONSTRAINT_UNIQUE_TEST
#define CONSTRAINT_FOREIGN_KEY_TEST

namespace peloton {
namespace test {

//===--------------------------------------------------------------------===//
// Constraints Tests
//===--------------------------------------------------------------------===//

class ConstraintsTests : public PelotonTest {};

#ifdef CONSTRAINT_NOTNULL_TEST
TEST_F(ConstraintsTests, NOTNULLTest) {
  // First, generate the table with index
  // this table has 15 rows:
  //  int(primary)  int   double  var(22) (unique)
  //  0             1     2       "3"
  //  10            11    12      "13"
  //  20            21    22      "23"
  //  .....
  //  140           141   142     "143"

  // Set all of the columns to be NOT NULL
  std::vector<oid_t> notnull_col_ids;
  for (int i = 0; i < CONSTRAINTS_NUM_COLS; i++) {
    notnull_col_ids.push_back(i);
  }
  std::unordered_map<oid_t, type::Value> default_values;
  storage::DataTable *data_table =
      TestingConstraintsUtil::CreateTable(notnull_col_ids, default_values);
  TestingConstraintsUtil::PopulateTable(data_table);

  // Bootstrap
  auto &txn_manager = concurrency::TransactionManagerFactory::GetInstance();

  std::vector<type::Value> values = {
      type::ValueFactory::GetIntegerValue(1),
      type::ValueFactory::GetIntegerValue(22),
      type::ValueFactory::GetDecimalValue(3.33),
      type::ValueFactory::GetVarcharValue("4444")};
  std::vector<type::Value> null_values = {
      type::ValueFactory::GetNullValueByType(type::TypeId::INTEGER),
      type::ValueFactory::GetNullValueByType(type::TypeId::INTEGER),
      type::ValueFactory::GetNullValueByType(type::TypeId::DECIMAL),
      type::ValueFactory::GetNullValueByType(type::TypeId::VARCHAR)};

  // Test1: Insert a tuple with column that satisfies the requirement
  auto txn = txn_manager.BeginTransaction();
  bool hasException = false;
  try {
    TestingConstraintsUtil::ExecuteMultiInsert(txn, data_table, values);
  } catch (ConstraintException e) {
    hasException = true;
  }
  EXPECT_FALSE(hasException);
  txn_manager.CommitTransaction(txn);

  // Test2: Set each of the columns to null one by one
  for (int i = 0; i < (int)values.size(); i++) {
    auto txn = txn_manager.BeginTransaction();
    hasException = false;
    std::vector<type::Value> new_values;
    for (int j = 0; j < (int)values.size(); j++) {
      new_values.push_back(i == j ? null_values[j] : values[j]);
    }
    try {
      TestingConstraintsUtil::ExecuteMultiInsert(txn, data_table, new_values);
    } catch (ConstraintException e) {
      hasException = true;
    }
    EXPECT_TRUE(hasException);
    txn_manager.CommitTransaction(txn);
  }  // FOR

  // free the database just created
  txn = txn_manager.BeginTransaction();
  catalog::Catalog::GetInstance()->DropDatabaseWithName(txn, DEFAULT_DB_NAME);
  txn_manager.CommitTransaction(txn);
}
#endif

#ifdef CONSTRAINT_DEFAULT_TEST
TEST_F(ConstraintsTests, DEFAULTTEST) {
  // Set default value within col_B
  std::vector<oid_t> notnull_col_ids;
  std::unordered_map<oid_t, type::Value> default_values;
  for (int i = 0; i < CONSTRAINTS_NUM_COLS; i++) {
    // COL_B
    if (i == 1) {
      default_values[i] = type::ValueFactory::GetIntegerValue(DEFAULT_VALUE);
    }
    // COL_A + COL_C + COL_D
    else {
      // do nothing
    }
  }
  storage::DataTable *data_table =
      TestingConstraintsUtil::CreateTable(notnull_col_ids, default_values);
  // Add primary key
  auto catalog = catalog::Catalog::GetInstance();
  auto &txn_manager = concurrency::TransactionManagerFactory::GetInstance();
  auto txn = txn_manager.BeginTransaction();
  for (oid_t i = 0; i < CONSTRAINTS_NUM_COLS; i++) {
    // COL_A
    if (i == 0) {
      catalog->AddPrimaryKeyConstraint(txn,
                                       data_table->GetDatabaseOid(),
                                       data_table->GetOid(),
                                       {i},
                                       "con_primary");
    }
    // COL_B + COL_C + COL_D
    else {
      // do nothing
    }
  }
  txn_manager.CommitTransaction(txn);

  // populate test data
  TestingConstraintsUtil::PopulateTable(data_table);

  // Bootstrap
  std::vector<ResultValue> result;
  std::vector<FieldInfo> tuple_descriptor;
  std::string error_message;
  int rows_affected;

  // Test1: Insert a tuple without the second column defined
  // It should get set with the default value
  std::string sql = StringUtil::Format(
      "INSERT INTO %s (col_a, col_c, col_d) "
      "VALUES (9999, 2.2, 'xxx');",
      CONSTRAINTS_TEST_TABLE);
  auto status = TestingSQLUtil::ExecuteSQLQuery(sql, result, tuple_descriptor,
                                                rows_affected, error_message);
  EXPECT_EQ(ResultType::SUCCESS, status);

  sql = StringUtil::Format("SELECT col_b FROM %s WHERE col_a = 9999",
                           CONSTRAINTS_TEST_TABLE);
  status = TestingSQLUtil::ExecuteSQLQuery(sql, result, tuple_descriptor,
                                           rows_affected, error_message);
  EXPECT_EQ(ResultType::SUCCESS, status);
  std::string resultStr = TestingSQLUtil::GetResultValueAsString(result, 0);
  EXPECT_EQ(std::to_string(DEFAULT_VALUE), resultStr);
  LOG_INFO("OUTPUT:%s", resultStr.c_str());
}
#endif

#ifdef CONSTRAINT_CHECK_TEST
TEST_F(ConstraintsTests, CHECKTest) {
  // First, generate the table with index
  // this table has 15 rows:
  //  int(primary)  int   double  var(22) (unique)
  //  0             1     2       "3"
  //  10            11    12      "13"
  //  20            21    22      "23"
  //  .....
  //  140           141   142     "143"
  auto catalog = catalog::Catalog::GetInstance();
  auto &txn_manager = concurrency::TransactionManagerFactory::GetInstance();
  auto txn = txn_manager.BeginTransaction();

  catalog->CreateDatabase(txn, DEFAULT_DB_NAME);
  auto column1 = catalog::Column(type::TypeId::INTEGER, 25, "A", false, 0);
  std::unique_ptr<catalog::Schema> table_schema(new catalog::Schema({column1}));

  std::string table_name("TEST_TABLE");
  auto result =
      catalog->CreateTable(txn,
                           DEFAULT_DB_NAME,
                           DEFAULT_SCHEMA_NAME,
                           std::move(table_schema),
                           table_name,
                           false);
  EXPECT_EQ(ResultType::SUCCESS, result);

  auto data_table = catalog->GetTableWithName(txn,
                                              DEFAULT_DB_NAME,
                                              DEFAULT_SCHEMA_NAME,
                                              table_name);
  EXPECT_NE(nullptr, data_table);

  // add check constraint
  type::Value tmp_value = type::ValueFactory::GetIntegerValue(0);
  catalog->AddCheckConstraint(txn,
                              data_table->GetDatabaseOid(),
                              data_table->GetOid(),
                              {0},
                              std::make_pair(ExpressionType::COMPARE_GREATERTHAN, tmp_value),
                              "con_check");
  txn_manager.CommitTransaction(txn);

  // begin this transaction
  txn = txn_manager.BeginTransaction();
  // Test1: insert a tuple with column  meet the constraint requirment
  bool hasException = false;
  try {
    TestingConstraintsUtil::ExecuteOneInsert(
        txn, data_table, type::ValueFactory::GetIntegerValue(10));
  } catch (ConstraintException e) {
    hasException = true;
  }
  EXPECT_FALSE(hasException);

  // Test2: insert not a valid column violate the constraint
  hasException = false;
  try {
    TestingConstraintsUtil::ExecuteOneInsert(
        txn, data_table, type::ValueFactory::GetIntegerValue(-1));
  } catch (ConstraintException e) {
    hasException = true;
  }
  EXPECT_TRUE(hasException);

  // commit this transaction
  txn_manager.CommitTransaction(txn);

  txn = txn_manager.BeginTransaction();
  auto result = catalog->DropDatabaseWithName(txn, DEFAULT_DB_NAME);
  EXPECT_EQ(ResultType::SUCCESS, result);
  txn_manager.CommitTransaction(txn);
}
#endif

#ifdef CONSTRAINT_UNIQUE_TEST
TEST_F(ConstraintsTests, UNIQUETest) {
  auto &txn_manager = concurrency::TransactionManagerFactory::GetInstance();
  auto catalog = catalog::Catalog::GetInstance();
  auto txn = txn_manager.BeginTransaction();
  std::string db_name = "db1";
  catalog->CreateDatabase(txn, db_name);
  auto column1 = catalog::Column(type::TypeId::INTEGER, 25, "A", false);
  auto column2 = catalog::Column(type::TypeId::INTEGER, 25, "B", false);

  std::unique_ptr<catalog::Schema> table_schema(
      new catalog::Schema({column1, column2}));
  std::string table_name("TEST_TABLE");
  catalog->CreateTable(txn,
                       db_name,
                       DEFAULT_SCHEMA_NAME,
                       std::move(table_schema),
                       table_name,
                       false);

  auto table = catalog->GetTableWithName(txn,
                                         db_name,
                                         DEFAULT_SCHEMA_NAME,
                                         table_name);
  catalog->AddUniqueConstraint(txn,
                               table->GetDatabaseOid(),
                               table->GetOid(),
                               {0},
                               "con_unique");
  txn_manager.CommitTransaction(txn);

  txn = txn_manager.BeginTransaction();
  // begin this transaction
  // Test1: insert a tuple with column  meet the unique requirement
  bool result = TestingConstraintsUtil::ExecuteOneInsert(
      txn, table, type::ValueFactory::GetIntegerValue(10));
  EXPECT_TRUE(result);

  // Test2: insert not a valid column violate the constraint
  result = TestingConstraintsUtil::ExecuteOneInsert(
        txn, table, type::ValueFactory::GetIntegerValue(10));
  EXPECT_FALSE(result);

  result = TestingConstraintsUtil::ExecuteOneInsert(
        txn, table, type::ValueFactory::GetIntegerValue(20));
  EXPECT_TRUE(result);

  // commit this transaction
  txn_manager.CommitTransaction(txn);

  txn = txn_manager.BeginTransaction();
  catalog->DropDatabaseWithName(txn, db_name);
  txn_manager.CommitTransaction(txn);
}

 TEST_F(ConstraintsTests, MULTIUNIQUETest) {
  auto &txn_manager = concurrency::TransactionManagerFactory::GetInstance();
  auto catalog = catalog::Catalog::GetInstance();
  auto txn = txn_manager.BeginTransaction();
  std::string db_name = "db1";
  catalog->CreateDatabase(txn, db_name);
  auto column1 = catalog::Column(type::TypeId::INTEGER, 25, "A", false);
  auto column2 = catalog::Column(type::TypeId::INTEGER, 25, "B", false);
  auto column3 = catalog::Column(type::TypeId::INTEGER, 25, "C", false);
  std::vector<oid_t> cols;
  cols.push_back(0);
  cols.push_back(1);
  std::vector<catalog::Column> columns;
  columns.push_back(column1);
  columns.push_back(column2);
  columns.push_back(column3);
  std::unique_ptr<catalog::Schema> table_schema(new catalog::Schema(columns));
  std::string table_name("TEST_TABLE_1");
  catalog->CreateTable(txn,
                       db_name,
                       DEFAULT_SCHEMA_NAME,
                       std::move(table_schema),
                       table_name,
                       false);

  // Add multi-unique constraint
  auto table = catalog->GetTableWithName(txn,
                                         db_name,
                                         DEFAULT_SCHEMA_NAME,
                                         table_name);
  catalog->AddUniqueConstraint(txn,
                               table->GetDatabaseOid(),
                               table->GetOid(),
                               cols,
                               "con_unique");
  txn_manager.CommitTransaction(txn);

  txn = txn_manager.BeginTransaction();
  // begin this transaction
  // Test1: insert a tuple with column  meet the unique requirment
  std::vector<type::Value> ccs;
  ccs.push_back(type::ValueFactory::GetIntegerValue(10));
  ccs.push_back(type::ValueFactory::GetIntegerValue(11));
  bool result = TestingConstraintsUtil::ExecuteMultiInsert(txn, table, ccs);
  EXPECT_TRUE(result);

  // Test2: insert not a valid column violate the constraint
  ccs.clear();
  ccs.push_back(type::ValueFactory::GetIntegerValue(10));
  ccs.push_back(type::ValueFactory::GetIntegerValue(11));
  result = TestingConstraintsUtil::ExecuteMultiInsert(txn, table, ccs);
  EXPECT_FALSE(result);

  ccs.clear();
  ccs.push_back(type::ValueFactory::GetIntegerValue(10));
  ccs.push_back(type::ValueFactory::GetIntegerValue(12));
  result = TestingConstraintsUtil::ExecuteMultiInsert(txn, table, ccs);
  EXPECT_TRUE(result);

  // commit this transaction
  txn_manager.CommitTransaction(txn);
  txn = txn_manager.BeginTransaction();
  catalog::Catalog::GetInstance()->DropDatabaseWithName(txn, db_name);
  txn_manager.CommitTransaction(txn);
}
#endif

#ifdef CONSTRAINT_FOREIGN_KEY_TEST
 TEST_F(ConstraintsTests, ForeignKeySingleInsertTest) {
  // First, initial 2 tables like following
  //     TABLE A -- src table          TABLE B -- sink table
  // a int(primary)     b int(ref B)     b int(primary)  c int
  //    0                    0               0             0
  //    1                    1               1             0
  //    2                    2               2             0
  //                                      .....
  //                                         9             0

  // create new db
  auto &txn_manager = concurrency::TransactionManagerFactory::GetInstance();
  auto txn = txn_manager.BeginTransaction();
  auto catalog = catalog::Catalog::GetInstance();
  std::string db_name = "db2";
  std::string table_a_name = "tableA";
  std::string table_b_name = "tableB";
  catalog::Catalog::GetInstance()->CreateDatabase(txn, db_name);

  // Table A
  auto column1 = catalog::Column(type::TypeId::INTEGER, 25, "a", false);
  auto column2 = catalog::Column(type::TypeId::INTEGER, 25, "b", false);
  std::unique_ptr<catalog::Schema> tableA_schema(
      new catalog::Schema({column1, column2}));
  catalog->CreateTable(txn,
                       db_name,
                       DEFAULT_SCHEMA_NAME,
                       std::move(tableA_schema),
                       table_a_name,
                       false);

  auto table_a = catalog->GetTableWithName(txn,
                                           db_name,
                                           DEFAULT_SCHEMA_NAME,
                                           table_a_name);
  catalog->AddPrimaryKeyConstraint(txn,
                                   table_a->GetDatabaseOid(),
                                   table_a->GetOid(),
                                   {0},
                                   "con_primary");
  txn_manager.CommitTransaction(txn);

  // Table B
  txn = txn_manager.BeginTransaction();
  auto column3 = catalog::Column(type::TypeId::INTEGER, 25, "b", false);
  auto column4 = catalog::Column(type::TypeId::INTEGER, 25, "c", false);
  std::unique_ptr<catalog::Schema> tableB_schema(
      new catalog::Schema({column3, column4}));

  catalog->CreateTable(txn,
                       db_name,
                       DEFAULT_SCHEMA_NAME,
                       std::move(tableB_schema),
                       table_b_name,
                       false);

  auto table_b = catalog->GetTableWithName(txn,
                                           db_name,
                                           DEFAULT_SCHEMA_NAME,
                                           table_b_name);
  catalog->AddPrimaryKeyConstraint(txn,
                                   table_b->GetDatabaseOid(),
                                   table_b->GetOid(),
                                   {0},
                                   "con_primary");

  oid_t sink_table_id = table_b->GetOid();
  std::vector<oid_t> sink_col_ids = { table_b->GetSchema()->GetColumnID("b") };
  std::vector<oid_t> source_col_ids = { table_a->GetSchema()->GetColumnID("b") };
  catalog->AddForeignKeyConstraint(txn,
                                   table_a->GetDatabaseOid(),
                                   table_a->GetOid(),
                                   source_col_ids,
                                   sink_table_id,
                                   sink_col_ids,
                                   FKConstrActionType::NOACTION,
                                   FKConstrActionType::NOACTION,
                                   "con_foreign");
  txn_manager.CommitTransaction(txn);

  txn = txn_manager.BeginTransaction();
  // begin this transaction
  // Test1: insert a tuple with column  meet the constraint requirement
  std::vector<type::Value> ccs;
  ccs.push_back(type::ValueFactory::GetIntegerValue(1));
  ccs.push_back(type::ValueFactory::GetIntegerValue(2));
  bool result = TestingConstraintsUtil::ExecuteMultiInsert(txn, table_b, ccs);
  EXPECT_TRUE(result);
  ccs.clear();
  ccs.push_back(type::ValueFactory::GetIntegerValue(2));
  ccs.push_back(type::ValueFactory::GetIntegerValue(1));
  result = TestingConstraintsUtil::ExecuteMultiInsert(txn, table_a, ccs);
  EXPECT_TRUE(result);

  ccs.clear();
  ccs.push_back(type::ValueFactory::GetIntegerValue(3));
  ccs.push_back(type::ValueFactory::GetIntegerValue(4));
  result = TestingConstraintsUtil::ExecuteMultiInsert(txn, table_b, ccs);
  EXPECT_TRUE(result);
  ccs.clear();
  ccs.push_back(type::ValueFactory::GetIntegerValue(2));
  ccs.push_back(type::ValueFactory::GetIntegerValue(5));
  result = TestingConstraintsUtil::ExecuteMultiInsert(txn, table_a, ccs);
  EXPECT_FALSE(result);

  // commit this transaction
  txn_manager.CommitTransaction(txn);
  txn = txn_manager.BeginTransaction();
  catalog::Catalog::GetInstance()->DropDatabaseWithName(txn, db_name);
  txn_manager.CommitTransaction(txn);
}

 TEST_F(ConstraintsTests, ForeignKeyMultiInsertTest) {
  // First, initial 2 tables like following
  //     TABLE A -- src table          TABLE B -- sink table
  // a int(ref B)      b int(ref B)     a int(primary)  b int(primary)
  //    0                  0                 0             0
  //    1                  0                 1             0
  //    2                  0                 2             0
  //                                      .....
  //                                         9             0

  // create new db
  auto &txn_manager = concurrency::TransactionManagerFactory::GetInstance();
  auto txn = txn_manager.BeginTransaction();
  auto catalog = catalog::Catalog::GetInstance();
  std::string db_name = "db2";
  std::string table_a_name = "tableA";
  std::string table_b_name = "tableB";
  catalog->CreateDatabase(txn, db_name);

  // TABLE A
  auto column1 = catalog::Column(type::TypeId::INTEGER, 25, "a", false);
  auto column2 = catalog::Column(type::TypeId::INTEGER, 25, "b", false);
  std::unique_ptr<catalog::Schema> tableA_schema(
      new catalog::Schema({column1, column2}));
  catalog->CreateTable(txn,
                       db_name,
                       DEFAULT_SCHEMA_NAME,
                       std::move(tableA_schema),
                       table_a_name,
                       false);

  auto table_a = catalog->GetTableWithName(txn,
                                           db_name,
                                           DEFAULT_SCHEMA_NAME,
                                           table_a_name);
  txn_manager.CommitTransaction(txn);

  // TABLE B
  txn = txn_manager.BeginTransaction();
  auto column3 = catalog::Column(type::TypeId::INTEGER, 25, "a", false);
  auto column4 = catalog::Column(type::TypeId::INTEGER, 25, "b", false);
  catalog::Schema *table_schema = new catalog::Schema({column3, column4});
  std::unique_ptr<catalog::Schema> tableB_schema(table_schema);

  catalog->CreateTable(txn,
                       db_name,
                       DEFAULT_SCHEMA_NAME,
                       std::move(tableB_schema),
                       table_b_name,
                       false);

  std::vector<oid_t> cols;
  cols.push_back(0);
  cols.push_back(1);
  auto table_b = catalog->GetTableWithName(txn,
                                           db_name,
                                           DEFAULT_SCHEMA_NAME,
                                           table_b_name);
  catalog->AddPrimaryKeyConstraint(txn,
                                   table_b->GetDatabaseOid(),
                                   table_b->GetOid(),
                                   cols,
                                   "con_primary");

  // Create foreign key tableA.B -> tableB.B
  oid_t sink_table_id = table_b->GetOid();
  std::vector<oid_t> sink_col_ids = { table_b->GetSchema()->GetColumnID("a"),
                                      table_b->GetSchema()->GetColumnID("b") };
  std::vector<oid_t> source_col_ids = { table_a->GetSchema()->GetColumnID("a"),
                                        table_a->GetSchema()->GetColumnID("b") };
  catalog->AddForeignKeyConstraint(txn,
                                   table_a->GetDatabaseOid(),
                                   table_a->GetOid(),
                                   source_col_ids,
                                   sink_table_id,
                                   sink_col_ids,
                                   FKConstrActionType::RESTRICT,
                                   FKConstrActionType::CASCADE,
                                   "con_foreign");
  txn_manager.CommitTransaction(txn);


  txn = txn_manager.BeginTransaction();
  // begin this transaction
  // Test1: insert a tuple with column  meet the constraint requirement
  std::vector<type::Value> ccs;
  ccs.push_back(type::ValueFactory::GetIntegerValue(1));
  ccs.push_back(type::ValueFactory::GetIntegerValue(2));
  bool result = TestingConstraintsUtil::ExecuteMultiInsert(txn, table_b, ccs);
  EXPECT_TRUE(result);
  ccs.clear();
  ccs.push_back(type::ValueFactory::GetIntegerValue(1));
  ccs.push_back(type::ValueFactory::GetIntegerValue(2));
  result = TestingConstraintsUtil::ExecuteMultiInsert(txn, table_a, ccs);
  EXPECT_TRUE(result);

  ccs.clear();
  ccs.push_back(type::ValueFactory::GetIntegerValue(3));
  ccs.push_back(type::ValueFactory::GetIntegerValue(4));
  result = TestingConstraintsUtil::ExecuteMultiInsert(txn, table_b, ccs);
  EXPECT_TRUE(result);
  ccs.clear();
  ccs.push_back(type::ValueFactory::GetIntegerValue(2));
  ccs.push_back(type::ValueFactory::GetIntegerValue(5));
  result = TestingConstraintsUtil::ExecuteMultiInsert(txn, table_a, ccs);
  EXPECT_FALSE(result);

  // commit this transaction
  txn_manager.CommitTransaction(txn);
  txn = txn_manager.BeginTransaction();
  catalog::Catalog::GetInstance()->DropDatabaseWithName(txn, db_name);
  txn_manager.CommitTransaction(txn);
}
#endif

// ========================================================

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

}  // namespace test
}  // namespace peloton
