//===----------------------------------------------------------------------===//
//
//                         PelotonDB
//
// catalog_test.cpp
//
// Identification: tests/catalog/constraints_test.cpp
//
// Copyright (c) 2016, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "gtest/gtest.h"
#include "harness.h"

#include "backend/catalog/schema.h"
#include "backend/common/value.h"
#include "backend/concurrency/transaction.h"
#include "backend/concurrency/transaction_manager_factory.h"
#include "backend/executor/abstract_executor.h"
#include "backend/storage/tile_group_factory.h"
#include "backend/storage/tuple.h"
#include "backend/storage/table_factory.h"
#include "backend/index/index_factory.h"
#include "backend/bridge/ddl/bridge.h"

#include "catalog/constraints_tests_util.h"
#include "concurrency/transaction_tests_util.h"

#define NOTNULL_TEST
#define PRIMARY_UNIQUEKEY_TEST
#define FOREIGHN_KEY_TEST

namespace peloton {
namespace test {

//===--------------------------------------------------------------------===//
// Constraints Tests
//===--------------------------------------------------------------------===//

class ConstraintsTests : public PelotonTest {};

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

  std::unique_ptr<storage::DataTable> data_table(
      ConstraintsTestsUtil::CreateAndPopulateTable());

  auto &txn_manager = concurrency::TransactionManagerFactory::GetInstance();

  // begin this transaction
  auto txn = txn_manager.BeginTransaction();

  // Test1: insert a tuple with column 1 = null
  bool hasException = false;
  try {
    ConstraintsTestsUtil::ExecuteInsert(
        txn, data_table.get(), ValueFactory::GetNullValue(),
        ValueFactory::GetIntegerValue(
            ConstraintsTestsUtil::PopulatedValue(15, 1)),
        ValueFactory::GetIntegerValue(
            ConstraintsTestsUtil::PopulatedValue(15, 2)),
        ValueFactory::GetStringValue(
            std::to_string(ConstraintsTestsUtil::PopulatedValue(15, 3))));

  } catch (ConstraintException e) {
    hasException = true;
  }
  EXPECT_TRUE(hasException);

  // Test2: insert a legal tuple
  hasException = false;
  try {
    ConstraintsTestsUtil::ExecuteInsert(
        txn, data_table.get(), ValueFactory::GetIntegerValue(
                                   ConstraintsTestsUtil::PopulatedValue(15, 0)),
        ValueFactory::GetIntegerValue(
            ConstraintsTestsUtil::PopulatedValue(15, 1)),
        ValueFactory::GetIntegerValue(
            ConstraintsTestsUtil::PopulatedValue(15, 2)),
        ValueFactory::GetStringValue(
            std::to_string(ConstraintsTestsUtil::PopulatedValue(15, 3))));
  } catch (ConstraintException e) {
    hasException = true;
  }
  EXPECT_FALSE(hasException);

  // commit this transaction
  txn_manager.CommitTransaction();
}
#endif

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

    EXPECT_TRUE(RESULT_SUCCESS == scheduler.schedules[0].txn_result);
    EXPECT_TRUE(RESULT_ABORTED == scheduler.schedules[1].txn_result);
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

    EXPECT_TRUE((RESULT_SUCCESS == scheduler.schedules[0].txn_result &&
                 RESULT_ABORTED == scheduler.schedules[1].txn_result) ||
                (RESULT_SUCCESS == scheduler.schedules[1].txn_result &&
                 RESULT_ABORTED == scheduler.schedules[0].txn_result));
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

    EXPECT_TRUE(RESULT_ABORTED == scheduler.schedules[0].txn_result);
    EXPECT_TRUE(RESULT_SUCCESS == scheduler.schedules[1].txn_result);
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

    EXPECT_TRUE(RESULT_SUCCESS == scheduler.schedules[0].txn_result);
    EXPECT_TRUE(RESULT_SUCCESS == scheduler.schedules[1].txn_result);
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
  oid_t current_db_oid = bridge::Bridge::GetCurrentDatabaseOid();
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

    EXPECT_TRUE(RESULT_ABORTED == scheduler.schedules[0].txn_result);
    EXPECT_TRUE(RESULT_SUCCESS == scheduler.schedules[1].txn_result);
  }

  // this will also indirectly delete all tables in this database
  delete newdb;
}
#endif

}  // End test namespace
}  // End peloton namespace
