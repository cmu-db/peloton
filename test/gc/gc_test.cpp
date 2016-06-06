//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// gc_test.cpp
//
// Identification: test/gc/gc_test.cpp
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "common/harness.h"
#include "concurrency/transaction_tests_util.h"
#include "gc/gc_manager.h"
#include "gc/gc_manager_factory.h"
#include "concurrency/epoch_manager.h"

namespace peloton {
namespace test {

//===--------------------------------------------------------------------===//
// Transaction Tests
//===--------------------------------------------------------------------===//

class GCTest : public PelotonTest {};

/*
int UpdateTable(storage::DataTable *table, const int scale, const int num_key,
const int num_txn) {
  srand(15721);


  auto &txn_manager = concurrency::TransactionManagerFactory::GetInstance();

  TransactionScheduler scheduler(num_txn, table, &txn_manager);
  scheduler.SetConcurrent(true);
  for (int i = 0; i < num_txn; i++) {
    for (int j = 0; j < scale; j++) {
      // randomly select two uniq keys
      int key1 = rand() % num_key;
      int key2 = rand() % num_key;
      int delta = rand() % 1000;
      // Store substracted value
      scheduler.Txn(i).ReadStore(key1, -delta);
      scheduler.Txn(i).Update(key1, TXN_STORED_VALUE);
      // Store increased value
      scheduler.Txn(i).ReadStore(key2, delta);
      scheduler.Txn(i).Update(key2, TXN_STORED_VALUE);
    }
    scheduler.Txn(i).Commit();
  }
  scheduler.Run();

  // stats
  int nabort = 0;
  for (auto &schedule : scheduler.schedules) {
    if (schedule.txn_result == RESULT_ABORTED) nabort += 1;
  }
  LOG_INFO("Abort: %d out of %d", nabort, num_txn);
  return num_txn - nabort;
}


void ScanAndGC(storage::DataTable *table, const int num_key) {
  auto &txn_manager = concurrency::TransactionManagerFactory::GetInstance();
  TransactionScheduler scheduler(1, table, &txn_manager);
  for (int i = 0; i < num_key; i++) {
    scheduler.Txn(0).Read(i);
  }
  scheduler.Txn(0).Commit();
  scheduler.Run();

  EXPECT_TRUE(scheduler.schedules[0].txn_result == RESULT_SUCCESS);
}

int GarbageNum(storage::DataTable *table) {
  auto table_tile_group_count_ = table->GetTileGroupCount();
  auto current_tile_group_offset_ = START_OID;

  int old_num = 0;

  while (current_tile_group_offset_ < table_tile_group_count_) {
    auto tile_group =
      table->GetTileGroup(current_tile_group_offset_++);
    auto tile_group_header = tile_group->GetHeader();
    oid_t active_tuple_count = tile_group->GetNextTupleSlot();

    for (oid_t tuple_id = 0; tuple_id < active_tuple_count; tuple_id++) {
      auto tuple_txn_id = tile_group_header->GetTransactionId(tuple_id);
      auto tuple_end_cid = tile_group_header->GetEndCommitId(tuple_id);
      if(tuple_txn_id == INITIAL_TXN_ID && tuple_end_cid != MAX_CID) {
        old_num++;
      }
    }
  }

  LOG_INFO("old version num %d", old_num);
  return old_num;
}

// get tuple recycled by GC
int RecycledNum(storage::DataTable *table) {
  int count = 0;
  auto table_id = table->GetOid();
  while(!gc::GCManagerFactory::GetInstance().ReturnFreeSlot(table_id).IsNull())
    count++;

  return count;
}


//
TEST_F(GCTest, SimpleTest) {

  concurrency::EpochManagerFactory::GetInstance().Reset();
  // create a table with only one key
  const int num_key = 1;
  std::unique_ptr<storage::DataTable> table(
    TransactionTestsUtil::CreateTable(num_key, "TEST_TABLE", INVALID_OID,
INVALID_OID, 1234, true));

  // update this key 1 times, using only one thread
  const int scale = 1;
  const int thread_num = 1;
  auto succ_num = UpdateTable(table.get(), scale, num_key, thread_num);

  // transaction must success
  EXPECT_EQ(succ_num, 1);

  // count garbage num
  auto old_num = GarbageNum(table.get());

  // there should be only one garbage
  // genereated by the last update
  EXPECT_EQ(old_num, 1);


  ScanAndGC(table.get(), num_key);

  std::this_thread::sleep_for(
    3 * std::chrono::milliseconds(EPOCH_LENGTH));
  ScanAndGC(table.get(), num_key);

  std::this_thread::sleep_for(
    3 * std::chrono::milliseconds(EPOCH_LENGTH));
  ScanAndGC(table.get(), num_key);


  // there should be garbage
  old_num = GarbageNum(table.get());
  EXPECT_EQ(old_num, 0);

  // sleep a while for gc to finish its job
  std::this_thread::sleep_for(
    10 * std::chrono::milliseconds(GC_PERIOD_MILLISECONDS));

  // there should be 1 tuple recycled
  EXPECT_EQ(1, RecycledNum(table.get()));
}


TEST_F(GCTest, StressTest) {
  concurrency::EpochManagerFactory::GetInstance().Reset();

  const int num_key = 256;
  const int scale = 1;
  std::unique_ptr<storage::DataTable> table(
    TransactionTestsUtil::CreateTable(num_key, "TEST_TABLE", INVALID_OID,
INVALID_OID, 1234, true));

  // stress db to create garbage
  auto succ_num = UpdateTable(table.get(), scale, num_key, 16);

  // count garbage number
  auto old_num = GarbageNum(table.get());

  // sleep for a while to increase epoch



  // EXPECT_EQ(scale * succ_num * 2, old_num);
  EXPECT_TRUE(old_num > 0);
  ScanAndGC(table.get(), num_key);

  std::this_thread::sleep_for(
    3 * std::chrono::milliseconds(EPOCH_LENGTH));
  ScanAndGC(table.get(), num_key);

  std::this_thread::sleep_for(
    3 * std::chrono::milliseconds(EPOCH_LENGTH));
  ScanAndGC(table.get(), num_key);

  old_num = GarbageNum(table.get());

  EXPECT_EQ(old_num, 0);

  // sleep a while to wait gc to finish its work
  std::this_thread::sleep_for(
    10 * std::chrono::milliseconds(GC_PERIOD_MILLISECONDS));

  // garbage number should be scale * succ update time * how many tuples updated
each time
  EXPECT_EQ(scale * succ_num * 2, RecycledNum(table.get()));

}

*/

}  // End test namespace
}  // End peloton namespace
