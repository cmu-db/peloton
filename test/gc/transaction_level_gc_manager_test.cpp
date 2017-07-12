//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// transaction_level_gc_manager_test.cpp
//
// Identification: test/gc/transaction_level_gc_manager_test.cpp
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "concurrency/testing_transaction_util.h"
#include "executor/testing_executor_util.h"
#include "common/harness.h"
#include "gc/transaction_level_gc_manager.h"
#include "concurrency/epoch_manager.h"

#include "storage/data_table.h"
#include "storage/tile_group.h"
#include "storage/database.h"
#include "storage/storage_manager.h"


namespace peloton {

namespace test {

//===--------------------------------------------------------------------===//
// Transaction-Level GC Manager Tests
//===--------------------------------------------------------------------===//

class TransactionLevelGCManagerTests : public PelotonTest {};

void UpdateTuple(storage::DataTable *table, const int update_num, const int total_num) {
  srand(15721);

  auto &txn_manager = concurrency::TransactionManagerFactory::GetInstance();
  TransactionScheduler scheduler(1, table, &txn_manager);
  for (int i = 0; i < update_num; i++) {
    scheduler.Txn(0).Update(rand() % total_num, rand() % 15721);
  }
  scheduler.Txn(0).Commit();
  scheduler.Run();

  EXPECT_TRUE(scheduler.schedules[0].txn_result == ResultType::SUCCESS);
}

void DeleteTuple(storage::DataTable *table, const int delete_num, const int total_num) {
  srand(15721);

  auto &txn_manager = concurrency::TransactionManagerFactory::GetInstance();
  TransactionScheduler scheduler(1, table, &txn_manager);
  for (int i = 0; i < delete_num; i++) {
    scheduler.Txn(0).Delete(rand() % total_num);
  }
  scheduler.Txn(0).Commit();
  scheduler.Run();

  EXPECT_TRUE(scheduler.schedules[0].txn_result == ResultType::SUCCESS);
}

void SelectTuple(storage::DataTable *table, const int select_num, const int total_num) {
  srand(15721);

  auto &txn_manager = concurrency::TransactionManagerFactory::GetInstance();
  TransactionScheduler scheduler(1, table, &txn_manager);
  for (int i = 0; i < select_num; i++) {
    scheduler.Txn(0).Read(rand() % total_num);
  }
  scheduler.Txn(0).Commit();
  scheduler.Run();

  EXPECT_TRUE(scheduler.schedules[0].txn_result == ResultType::SUCCESS);
}

TEST_F(TransactionLevelGCManagerTests, GCTest) {

  auto &epoch_manager = concurrency::EpochManagerFactory::GetInstance();
  epoch_manager.Reset(1);

  std::vector<std::unique_ptr<std::thread>> gc_threads;

  gc::GCManagerFactory::Configure(1);
  auto &gc_manager = gc::TransactionLevelGCManager::GetInstance();

  
  auto storage_manager = storage::StorageManager::GetInstance();
  // create database
  auto database = TestingExecutorUtil::InitializeDatabase("DATABASE");
  oid_t db_id = database->GetOid();
  EXPECT_TRUE(storage_manager->HasDatabase(db_id));

  // create a table with only one key
  const int num_key = 1;
  std::unique_ptr<storage::DataTable> table(
    TestingTransactionUtil::CreateTable(num_key, "TABLE", db_id, INVALID_OID, 1234, true));

  EXPECT_TRUE(gc_manager.GetTableCount() == 1);

  //===========================
  // update a version here.
  //===========================
  const int update_num = 1;
  UpdateTuple(table.get(), update_num, num_key);
  
  epoch_manager.SetCurrentEpochId(2);

  // get expired epoch id.
  // as the current epoch id is set to 2, 
  // the expected expired epoch id should be 1.
  auto expired_eid = epoch_manager.GetExpiredEpochId();
  
  EXPECT_EQ(1, expired_eid);

  auto current_eid = epoch_manager.GetCurrentEpochId();

  EXPECT_EQ(2, current_eid);

  auto reclaimed_count = gc_manager.Reclaim(0, expired_eid);

  auto unlinked_count = gc_manager.Unlink(0, expired_eid);

  EXPECT_EQ(0, reclaimed_count);

  EXPECT_EQ(1, unlinked_count);

  epoch_manager.SetCurrentEpochId(3);


  expired_eid = epoch_manager.GetExpiredEpochId();
  
  EXPECT_EQ(2, expired_eid);

  current_eid = epoch_manager.GetCurrentEpochId();

  EXPECT_EQ(3, current_eid);

  reclaimed_count = gc_manager.Reclaim(0, expired_eid);

  unlinked_count = gc_manager.Unlink(0, expired_eid);

  EXPECT_EQ(1, reclaimed_count);

  EXPECT_EQ(0, unlinked_count);

  //===========================
  // delete a version here.
  //===========================
  const int delete_num = 1;
  DeleteTuple(table.get(), delete_num, num_key);

  epoch_manager.SetCurrentEpochId(4);

  // get expired epoch id.
  // as the current epoch id is set to 4, 
  // the expected expired epoch id should be 3.
  expired_eid = epoch_manager.GetExpiredEpochId();
  
  EXPECT_EQ(3, expired_eid);

  current_eid = epoch_manager.GetCurrentEpochId();

  EXPECT_EQ(4, current_eid);

  reclaimed_count = gc_manager.Reclaim(0, expired_eid);

  unlinked_count = gc_manager.Unlink(0, expired_eid);

  EXPECT_EQ(0, reclaimed_count);

  EXPECT_EQ(1, unlinked_count);

  epoch_manager.SetCurrentEpochId(5);


  expired_eid = epoch_manager.GetExpiredEpochId();
  
  EXPECT_EQ(4, expired_eid);

  current_eid = epoch_manager.GetCurrentEpochId();

  EXPECT_EQ(5, current_eid);

  reclaimed_count = gc_manager.Reclaim(0, expired_eid);

  unlinked_count = gc_manager.Unlink(0, expired_eid);

  EXPECT_EQ(1, reclaimed_count);

  EXPECT_EQ(0, unlinked_count);
  

  table.release();

  // DROP!
  TestingExecutorUtil::DeleteDatabase("DATABASE");
  EXPECT_FALSE(storage_manager->HasDatabase(db_id));

}


}  // End test namespace
}  // End peloton namespace

