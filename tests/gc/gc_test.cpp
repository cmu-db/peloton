//===----------------------------------------------------------------------===//
//
//                         PelotonDB
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "harness.h"
#include "concurrency/transaction_tests_util.h"

namespace peloton {

namespace test {

//===--------------------------------------------------------------------===//
// Transaction Tests
//===--------------------------------------------------------------------===//

class GCTest : public PelotonTest {};

int StressDB(storage::DataTable *table, const int scale, const int num_key) {
  const int num_txn = 16;
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

  // Read all values
  TransactionScheduler scheduler2(1, table, &txn_manager);
  for (int i = 0; i < num_key; i++) {
    scheduler2.Txn(0).Read(i);
  }
  scheduler2.Txn(0).Commit();
  scheduler2.Run();
  // The sum should be zero
  int sum = 0;
  for (auto result : scheduler2.schedules[0].results) {
    sum += result;
  }

  EXPECT_EQ(0, sum);

  // stats
  int nabort = 0;
  for (auto &schedule : scheduler.schedules) {
    if (schedule.txn_result == RESULT_ABORTED) nabort += 1;
  }
  LOG_INFO("Abort: %d out of %d", nabort, num_txn);
  return num_txn - nabort;
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

TEST_F(GCTest, StressTest) {
  const int num_key = 256;
  const int scale = 4;
  std::unique_ptr<storage::DataTable> table(
    TransactionTestsUtil::CreateTable(num_key));

  // firstly, stress db to create garbage
  auto succ_num = StressDB(table.get(), scale, num_key);

  // count garbage number
  auto old_num = GarbageNum(table.get());

  // garbage number should be scale * succ update time * how many tuples updated each time
  EXPECT_EQ(scale * succ_num * 2, old_num);


}
}  // End test namespace
}  // End peloton namespace
