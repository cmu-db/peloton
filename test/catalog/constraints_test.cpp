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

#include "sql/testing_sql_util.h"
#include "gtest/gtest.h"

#include "catalog/testing_constraints_util.h"

#include "common/internal_types.h"
#include "catalog/catalog.h"
#include "concurrency/testing_transaction_util.h"
#include "planner/plan_util.h"
#include "planner/create_plan.h"
#include "executor/executors.h"
#include "catalog/foreign_key.h"
#include "parser/postgresparser.h"
#include "optimizer/optimizer.h"

#define DEFAULT_VALUE 11111

#define CONSTRAINT_NOTNULL_TEST
#define CONSTRAINT_DEFAULT_TEST
//#define CONSTRAINT_CHECK_TEST

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
  std::vector<std::vector<catalog::Constraint>> constraints;
  for (int i = 0; i < CONSTRAINTS_NUM_COLS; i++) {
    constraints.push_back({ catalog::Constraint(ConstraintType::NOTNULL,
                                                "notnull_constraint") });
  }
  std::vector<catalog::MultiConstraint> multi_constraints;
  storage::DataTable *data_table =
      TestingConstraintsUtil::CreateAndPopulateTable(constraints, multi_constraints);

  // Bootstrap
  auto &txn_manager = concurrency::TransactionManagerFactory::GetInstance();

  std::vector<type::Value> values = {
      type::ValueFactory::GetIntegerValue(1),
      type::ValueFactory::GetIntegerValue(22),
      type::ValueFactory::GetDecimalValue(3.33),
      type::ValueFactory::GetVarcharValue("4444")
  };
  std::vector<type::Value> null_values = {
      type::ValueFactory::GetNullValueByType(type::TypeId::INTEGER),
      type::ValueFactory::GetNullValueByType(type::TypeId::INTEGER),
      type::ValueFactory::GetNullValueByType(type::TypeId::DECIMAL),
      type::ValueFactory::GetNullValueByType(type::TypeId::VARCHAR)
  };

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
  } // FOR
  
  // free the database just created
  txn = txn_manager.BeginTransaction();
  catalog::Catalog::GetInstance()->DropDatabaseWithName(DEFAULT_DB_NAME, txn);
  txn_manager.CommitTransaction(txn);
}
#endif

#ifdef CONSTRAINT_DEFAULT_TEST
TEST_F(ConstraintsTests, DEFAULTTEST) {
  // Set all of the columns to be NOT NULL
  std::vector<std::vector<catalog::Constraint>> constraints;
  for (int i = 0; i < CONSTRAINTS_NUM_COLS; i++) {
    // COL_A
    if (i == 0) {
      constraints.push_back(
          { catalog::Constraint(ConstraintType::PRIMARY, "pkey") });
    }
    // COL_B
    else if (i == 1) {
      catalog::Constraint default_const(ConstraintType::DEFAULT, "default");
      default_const.addDefaultValue(
          type::ValueFactory::GetIntegerValue(DEFAULT_VALUE));
      constraints.push_back({ });
    }
    // COL_C + COL_D
    else {
      constraints.push_back({ });
    }
  }
  std::vector<catalog::MultiConstraint> multi_constraints;
  TestingConstraintsUtil::CreateAndPopulateTable(constraints, multi_constraints);

  // Bootstrap
  std::vector<ResultValue> result;
  std::vector<FieldInfo> tuple_descriptor;
  std::string error_message;
  int rows_affected;

  // Test1: Insert a tuple without the second column defined
  // It should get set with the default value
  std::string sql = StringUtil::Format(
                    "INSERT INTO %s (col_a, col_c, col_d) "
                    "VALUES (9999, 2.2, 'xxx');", CONSTRAINTS_TEST_TABLE);
  auto status = TestingSQLUtil::ExecuteSQLQuery(
      sql, result, tuple_descriptor, rows_affected, error_message
  );
  EXPECT_EQ(ResultType::SUCCESS, status);

  sql = StringUtil::Format("SELECT col_d FROM %s WHERE col_a = 9999",
                           CONSTRAINTS_TEST_TABLE);
  status = TestingSQLUtil::ExecuteSQLQuery(
      sql, result, tuple_descriptor, rows_affected, error_message);
  EXPECT_EQ(ResultType::SUCCESS, status);
  std::string resultStr = TestingSQLUtil::GetResultValueAsString(result, 0);
  LOG_INFO("OUTPUT:\n%s", resultStr.c_str());
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

  auto column1 = catalog::Column(type::TypeId::INTEGER, 25, "A", false, 0);
  auto constraints = catalog::Constraint(ConstraintType::CHECK, "check1");
  type::Value tmp_value = type::ValueFactory::GetIntegerValue(0);
  constraints.AddCheck(ExpressionType::COMPARE_GREATERTHAN, tmp_value);
  column1.AddConstraint(constraints);
  LOG_DEBUG("%s %s", peloton::DOUBLE_STAR.c_str(), constraints.GetInfo().c_str());
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
    TestingConstraintsUtil::ExecuteOneInsert(
        txn, data_table.get(), type::ValueFactory::GetIntegerValue(10));
  } catch (ConstraintException e) {
    hasException = true;
  }
  EXPECT_FALSE(hasException);

  // Test2: insert not a valid column violate the constraint
  hasException = false;
  try {
    TestingConstraintsUtil::ExecuteOneInsert(
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

#ifdef CONSTRAINT_UNIQUE_TEST
TEST_F(ConstraintsTests, UNIQUETest) {
  auto &txn_manager = concurrency::TransactionManagerFactory::GetInstance();
  auto catalog = catalog::Catalog::GetInstance();
  auto txn = txn_manager.BeginTransaction();
  catalog->CreateDatabase(DEFAULT_DB_NAME, nullptr);
  auto column1 = catalog::Column(type::TypeId::INTEGER, 25, "A", false, 0);
  auto column2 = catalog::Column(type::TypeId::INTEGER, 25, "B", false, 1);

  auto constraints = catalog::Constraint(ConstraintType::UNIQUE, "unique1");
  column1.AddConstraint(constraints);
  LOG_DEBUG("%s %s", peloton::DOUBLE_STAR.c_str(), constraints.GetInfo().c_str());
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
    TestingConstraintsUtil::ExecuteOneInsert(
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
    TestingConstraintsUtil::ExecuteOneInsert(
        txn, table, type::ValueFactory::GetIntegerValue(10));
    // if (result == false) hasException = true;
  } catch (ConstraintException e) {
    hasException = true;
  }
  EXPECT_TRUE(hasException);

  hasException = false;
  try {
    TestingConstraintsUtil::ExecuteOneInsert(
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

//TEST_F(ConstraintsTests, MULTIUNIQUETest) {
//  auto &txn_manager = concurrency::TransactionManagerFactory::GetInstance();
//  auto catalog = catalog::Catalog::GetInstance();
//  auto txn = txn_manager.BeginTransaction();
//  std::string db_name = "db1";
//  catalog->CreateDatabase(db_name, nullptr);
//  auto column1 = catalog::Column(type::TypeId::INTEGER, 25, "A", false, 0);
//  auto column2 = catalog::Column(type::TypeId::INTEGER, 25, "B", false, 1);
//  auto column3 = catalog::Column(type::TypeId::INTEGER, 25, "C", false, 2);
//  std::vector<oid_t> cols;
//  cols.push_back(0);
//  cols.push_back(1);
//  std::vector<catalog::Column> columns;
//  columns.push_back(column1);
//  columns.push_back(column2);
//  columns.push_back(column3);
//  auto mc = catalog::MultiConstraint(ConstraintType::UNIQUE, "c1", cols);
//  LOG_DEBUG("%s MULTI CONSTRAINTS %s %s", peloton::DOUBLE_STAR.c_str(),
// peloton::DOUBLE_STAR.c_str(), mc.GetInfo().c_str());
//
//  std::unique_ptr<catalog::Schema> table_schema(new catalog::Schema(columns));
//  table_schema->AddMultiConstraints(mc);
//  std::string table_name("TEST_TABLE_1");
//  catalog->CreateTable(db_name, table_name, std::move(table_schema), txn);
//  txn_manager.CommitTransaction(txn);
//  storage::Database *database = catalog->GetDatabaseWithName(db_name);
//  storage::DataTable *table = database->GetTableWithName(table_name);
//
//  // table->AddUNIQUEIndex();
//
//  txn = txn_manager.BeginTransaction();
//  // begin this transaction
//  // Test1: insert a tuple with column  meet the unique requirment
//  bool hasException = false;
//  try {
//    std::vector<type::Value> ccs;
//    ccs.push_back(type::ValueFactory::GetIntegerValue(10));
//    ccs.push_back(type::ValueFactory::GetIntegerValue(11));
//    // bool result = true;
//    // result =
//    TestingConstraintsUtil::ExecuteMultiInsert(txn, table, ccs);
//    // if (result == false) hasException = true;
//  } catch (ConstraintException e) {
//    hasException = true;
//  }
//  EXPECT_FALSE(hasException);
//
//  // Test2: insert not a valid column violate the constraint
//  hasException = false;
//  try {
//    std::vector<type::Value> ccs;
//    ccs.push_back(type::ValueFactory::GetIntegerValue(10));
//    ccs.push_back(type::ValueFactory::GetIntegerValue(11));
//    // bool result = true;
//    // result =
//    TestingConstraintsUtil::ExecuteMultiInsert(txn, table, ccs);
//    // if (result == false) hasException = true;
//  } catch (ConstraintException e) {
//    hasException = true;
//  }
//  EXPECT_TRUE(hasException);
//
//  hasException = false;
//  try {
//    std::vector<type::Value> ccs;
//    ccs.push_back(type::ValueFactory::GetIntegerValue(10));
//    ccs.push_back(type::ValueFactory::GetIntegerValue(12));
//    TestingConstraintsUtil::ExecuteMultiInsert(txn, table, ccs);
//  } catch (ConstraintException e) {
//    hasException = true;
//  }
//  EXPECT_FALSE(hasException);
//
//  // commit this transaction
//  txn_manager.CommitTransaction(txn);
//  txn = txn_manager.BeginTransaction();
//  catalog::Catalog::GetInstance()->DropDatabaseWithName(db_name, txn);
//  txn_manager.CommitTransaction(txn);
//}

//TEST_F(ConstraintsTests, ForeignKeySingleInsertTest) {
//  // First, initial 2 tables like following
//  //     TABLE A -- src table          TABLE B -- sink table
//  // a int(primary, ref B)  b int      b int(primary)  c int
//  //    0                    0               0             0
//  //    1                    0               1             0
//  //    2                    0               2             0
//  //                                      .....
//  //                                         9             0
//
//  // create new db
//  auto &txn_manager = concurrency::TransactionManagerFactory::GetInstance();
//  auto txn = txn_manager.BeginTransaction();
//  auto catalog = catalog::Catalog::GetInstance();
//  std::string db_name = "db2";
//  std::string table_a_name = "tableA";
//  std::string table_b_name = "tableB";
//  catalog::Catalog::GetInstance()->CreateDatabase(db_name, nullptr);
//  // txn_manager.CommitTransaction(txn);
//
//  auto column1 = catalog::Column(type::TypeId::INTEGER, 25, "a", false, 0);
//  auto column2 = catalog::Column(type::TypeId::INTEGER, 25, "b", false, 1);
//
//  auto constraints = catalog::Constraint(ConstraintType::PRIMARY, "primary1");
//  column1.AddConstraint(constraints);
//  LOG_DEBUG("%s %s", peloton::DOUBLE_STAR.c_str(), constraints.GetInfo().c_str());
//  std::unique_ptr<catalog::Schema> tableA_schema(
//      new catalog::Schema({column1, column2}));
//
//  catalog->CreateTable(db_name, table_a_name, std::move(tableA_schema), txn);
//  txn_manager.CommitTransaction(txn);
//
//  txn = txn_manager.BeginTransaction();
//  auto column3 = catalog::Column(type::TypeId::INTEGER, 25, "b", false, 0);
//  column3.AddConstraint(constraints);
//  auto column4 = catalog::Column(type::TypeId::INTEGER, 25, "c", false, 1);
//  std::unique_ptr<catalog::Schema> tableB_schema(
//      new catalog::Schema({column3, column4}));
//
//  catalog->CreateTable(db_name, table_b_name, std::move(tableB_schema), txn);
//
//  auto table_a = catalog->GetTableWithName(db_name, table_a_name);
//  auto table_b = catalog->GetTableWithName(db_name, table_b_name);
//
//  oid_t sink_table_id = table_b->GetOid();
//  std::vector<oid_t> sink_col_ids = { table_b->GetSchema()->GetColumnID("b") };
//  std::vector<oid_t> source_col_ids = { table_a->GetSchema()->GetColumnID("a") };
//  catalog::ForeignKey *foreign_key = new catalog::ForeignKey(
//      sink_table_id, sink_col_ids, source_col_ids,
//      FKConstrActionType::NOACTION,
//      FKConstrActionType::NOACTION,
//      "foreign_constraint1");
//  table_a->AddForeignKey(foreign_key);
//
//  txn = txn_manager.BeginTransaction();
//  // begin this transaction
//  // Test1: insert a tuple with column  meet the unique requirment
//  bool hasException = false;
//  try {
//    std::vector<type::Value> ccs;
//    ccs.push_back(type::ValueFactory::GetIntegerValue(1));
//    ccs.push_back(type::ValueFactory::GetIntegerValue(2));
//    TestingConstraintsUtil::ExecuteMultiInsert(txn, table_b, ccs);
//    ccs.clear();
//    ccs.push_back(type::ValueFactory::GetIntegerValue(1));
//    ccs.push_back(type::ValueFactory::GetIntegerValue(2));
//    TestingConstraintsUtil::ExecuteMultiInsert(txn, table_a, ccs);
//  } catch (ConstraintException e) {
//    hasException = true;
//  }
//  EXPECT_FALSE(hasException);
//
//  hasException = true;
//  try {
//    std::vector<type::Value> ccs;
//    ccs.push_back(type::ValueFactory::GetIntegerValue(3));
//    ccs.push_back(type::ValueFactory::GetIntegerValue(4));
//    TestingConstraintsUtil::ExecuteMultiInsert(txn, table_b, ccs);
//    ccs.clear();
//    ccs.push_back(type::ValueFactory::GetIntegerValue(2));
//    ccs.push_back(type::ValueFactory::GetIntegerValue(5));
//    TestingConstraintsUtil::ExecuteMultiInsert(txn, table_a, ccs);
//  } catch (ConstraintException e) {
//    hasException = true;
//  }
//  EXPECT_TRUE(hasException);
//
//  // commit this transaction
//  txn_manager.CommitTransaction(txn);
//  txn = txn_manager.BeginTransaction();
//  catalog::Catalog::GetInstance()->DropDatabaseWithName(db_name, txn);
//  txn_manager.CommitTransaction(txn);
//  delete foreign_key;
//}

//TEST_F(ConstraintsTests, ForeignKeyMultiInsertTest) {
//  // First, initial 2 tables like following
//  //     TABLE A -- src table          TABLE B -- sink table
//  // a int(primary, ref B)  b int      b int(primary)  c int
//  //    0                    0               0             0
//  //    1                    0               1             0
//  //    2                    0               2             0
//  //                                      .....
//  //                                         9             0
//
//  // create new db
//  auto &txn_manager = concurrency::TransactionManagerFactory::GetInstance();
//  auto txn = txn_manager.BeginTransaction();
//  auto catalog = catalog::Catalog::GetInstance();
//  std::string db_name = "db2";
//  std::string table_a_name = "tableA";
//  std::string table_b_name = "tableB";
//  catalog->CreateDatabase(db_name, txn);
//
//  // TABLE A
//  auto column1 = catalog::Column(type::TypeId::INTEGER, 25, "a", false, 0);
//  auto column2 = catalog::Column(type::TypeId::INTEGER, 25, "b", false, 1);
//  std::unique_ptr<catalog::Schema> tableA_schema(
//      new catalog::Schema({column1, column2}));
//  catalog->CreateTable(db_name, table_a_name, std::move(tableA_schema), txn);
//  txn_manager.CommitTransaction(txn);
//  auto table_A = catalog->GetTableWithName(db_name, table_a_name);
//
//  txn = txn_manager.BeginTransaction();
//  auto column3 = catalog::Column(type::TypeId::INTEGER, 25, "b", false, 0);
//  auto column4 = catalog::Column(type::TypeId::INTEGER, 25, "c", false, 1);
//  std::vector<oid_t> cols;
//  cols.push_back(0);
//  cols.push_back(1);
//  auto mc =
//      catalog::MultiConstraint(ConstraintType::PRIMARY, "multiprimary1", cols);
//  LOG_DEBUG("%s MULTI CONSTRAINTS %s %s", peloton::DOUBLE_STAR.c_str(),
// peloton::DOUBLE_STAR.c_str(), mc.GetInfo().c_str());
//
//  // TABLE B
//  catalog::Schema *table_schema = new catalog::Schema({column3, column4});
//  table_schema->AddMultiConstraints(mc);
//  std::unique_ptr<catalog::Schema> tableB_schema(table_schema);
//
//  catalog->CreateTable(db_name, table_b_name, std::move(tableB_schema), txn);
//  auto table_a = catalog->GetTableWithName(db_name, table_a_name);
//  auto table_b = catalog->GetTableWithName(db_name, table_b_name);
//  txn_manager.CommitTransaction(txn);
//
//  // Create foreign key tableA.B -> tableB.B
//  oid_t sink_table_id = table_b->GetOid();
//  std::vector<oid_t> sink_col_ids = { table_b->GetSchema()->GetColumnID("b") };
//  std::vector<oid_t> source_col_ids = { table_a->GetSchema()->GetColumnID("b") };
//  catalog::ForeignKey *foreign_key = new catalog::ForeignKey(
//      sink_table_id, sink_col_ids, source_col_ids,
//      FKConstrActionType::RESTRICT,
//      FKConstrActionType::CASCADE,
//      "foreign_constraint1");
//  table_A->AddForeignKey(foreign_key);
//
//  // Test1: insert a tuple with column  meet the constraint requirment
//
//  txn = txn_manager.BeginTransaction();
//  // begin this transaction
//  // Test1: insert a tuple with column  meet the unique requirment
//  bool hasException = false;
//  try {
//    std::vector<type::Value> ccs;
//    ccs.push_back(type::ValueFactory::GetIntegerValue(1));
//    ccs.push_back(type::ValueFactory::GetIntegerValue(2));
//    TestingConstraintsUtil::ExecuteMultiInsert(txn, table_b, ccs);
//    ccs.clear();
//    ccs.push_back(type::ValueFactory::GetIntegerValue(2));
//    ccs.push_back(type::ValueFactory::GetIntegerValue(1));
//    TestingConstraintsUtil::ExecuteMultiInsert(txn, table_a, ccs);
//  } catch (ConstraintException e) {
//    hasException = true;
//  }
//  EXPECT_FALSE(hasException);
//
//  hasException = true;
//  try {
//    std::vector<type::Value> ccs;
//    ccs.push_back(type::ValueFactory::GetIntegerValue(3));
//    ccs.push_back(type::ValueFactory::GetIntegerValue(4));
//    TestingConstraintsUtil::ExecuteMultiInsert(txn, table_b, ccs);
//    ccs.clear();
//    ccs.push_back(type::ValueFactory::GetIntegerValue(2));
//    ccs.push_back(type::ValueFactory::GetIntegerValue(5));
//    TestingConstraintsUtil::ExecuteMultiInsert(txn, table_a, ccs);
//  } catch (ConstraintException e) {
//    hasException = true;
//  }
//  EXPECT_TRUE(hasException);
//
//  // commit this transaction
//  txn_manager.CommitTransaction(txn);
//  txn = txn_manager.BeginTransaction();
//  catalog::Catalog::GetInstance()->DropDatabaseWithName(db_name, txn);
//  txn_manager.CommitTransaction(txn);
//  delete foreign_key;
//}

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
