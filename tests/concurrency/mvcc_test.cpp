//===----------------------------------------------------------------------===//
//
//                         PelotonDB
//
// mvcc_test.cpp
//
// Identification: tests/concurrency/mvcc_test.cpp
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

class MVCCTest : public PelotonTest {};

// Validate that MVCC storage is correct, it assumes an old-to-new chain
// Invariants
// 1. Transaction id should either be INVALID_TXNID or INITIAL_TXNID
// 2. Begin commit id should <= end commit id
// 3. Timestamp consistence
// 4. Version doubly linked list consistency
static void ValidateMVCC_OldToNew(storage::DataTable *table) {
  auto &gc_manager = gc::GCManagerFactory::GetInstance();
  auto &catalog_manager = catalog::Manager::GetInstance();
  gc_manager.StopGC();
  LOG_INFO("Validating MVCC storage");
  int tile_group_count = table->GetTileGroupCount();
  LOG_INFO("The table has %d tile groups in the table", tile_group_count);

  for (int tile_group_offset = 0; tile_group_offset < tile_group_count; tile_group_offset++) {
    LOG_INFO("Validate tile group #%d", tile_group_offset);
    auto tile_group = table->GetTileGroup(tile_group_offset);
    auto tile_group_header = tile_group->GetHeader();
    size_t tuple_count = tile_group->GetAllocatedTupleCount();
    LOG_INFO("Tile group #%d has allocated %lu tuples", tile_group_offset, tuple_count);

    // 1. Transaction id should either be INVALID_TXNID or INITIAL_TXNID
    for (oid_t tuple_slot = 0; tuple_slot < tuple_count; tuple_slot++) {
      txn_id_t txn_id = tile_group_header->GetTransactionId(tuple_slot);
      EXPECT_TRUE(txn_id == INVALID_TXN_ID || txn_id == INITIAL_TXN_ID) << "Transaction id is not INVALID_TXNID or INITIAL_TXNID";
    }

    LOG_INFO("[OK] All tuples have valid txn id");

    // double avg_version_chain_length = 0.0;
    for (oid_t tuple_slot = 0; tuple_slot < tuple_count; tuple_slot++) {
      txn_id_t txn_id = tile_group_header->GetTransactionId(tuple_slot);
      cid_t begin_cid = tile_group_header->GetBeginCommitId(tuple_slot);
      cid_t end_cid = tile_group_header->GetEndCommitId(tuple_slot);
      ItemPointer next_location = tile_group_header->GetNextItemPointer(tuple_slot);
      ItemPointer prev_location = tile_group_header->GetPrevItemPointer(tuple_slot);

      // 2. Begin commit id should <= end commit id
      EXPECT_TRUE(begin_cid <= end_cid) << "Tuple begin commit id is less than or equal to end commit id";

      // This test assumes a oldest-to-newest version chain
      if (txn_id != INVALID_TXN_ID) {
        EXPECT_TRUE(begin_cid != MAX_CID) << "Non invalid txn shouldn't have a MAX_CID begin commit id";
        
        // The version is an oldest version
        if (prev_location.IsNull()) {
          if (next_location.IsNull()) {
            EXPECT_EQ(end_cid, MAX_CID) << "Single version has a non MAX_CID end commit time";
          } else {
            cid_t prev_end_cid = end_cid;
            ItemPointer prev_location(tile_group_offset, tuple_slot);
            while (!next_location.IsNull()) {
              auto next_tile_group = catalog_manager.GetTileGroup(next_location.block);
              auto next_tile_group_header = next_tile_group->GetHeader();

              txn_id_t next_txn_id = next_tile_group_header->GetTransactionId(next_location.offset);

              if (next_txn_id == INVALID_TXN_ID) {
                // If a version in the version chain has a INVALID_TXN_ID, it must be at the tail
                // of the chain. It is either because we have deleted a tuple (so append a invalid tuple),
                // or because this new version is aborted.
                EXPECT_TRUE(next_tile_group_header->GetNextItemPointer(next_location.offset).IsNull()) << "Invalid version in a version chain and is not delete";
              }

              cid_t next_begin_cid = next_tile_group_header->GetBeginCommitId(next_location.offset);
              cid_t next_end_cid = next_tile_group_header->GetEndCommitId(next_location.offset);

              // 3. Timestamp consistence
              if (next_begin_cid == MAX_CID) {
                // It must be an aborted version, it should be at the end of the chain
                EXPECT_TRUE(next_tile_group_header->GetNextItemPointer(next_location.offset).IsNull()) << "Version with MAX_CID begin cid is not version tail";
              } else {
                EXPECT_EQ(prev_end_cid, next_begin_cid) << "Prev end commit id should equal net begin commit id";  
                ItemPointer next_prev_location = next_tile_group_header->GetPrevItemPointer(next_location.offset);

                // 4. Version doubly linked list consistency
                EXPECT_TRUE(next_prev_location.offset == prev_location.offset &&
                        next_prev_location.block == prev_location.block) << "Next version's prev version does not match";
              }
              
              prev_location = next_location;
              prev_end_cid = next_end_cid;
              next_location = next_tile_group_header->GetNextItemPointer(next_location.offset);
            }

            // Now prev_location is at the tail of the version chain
            ItemPointer last_location = prev_location;
            auto last_tile_group = catalog_manager.GetTileGroup(last_location.block);
            auto last_tile_group_header = last_tile_group->GetHeader();
            // txn_id_t last_txn_id = last_tile_group_header->GetTransactionId(last_location.offset);
            cid_t last_end_cid = last_tile_group_header->GetEndCommitId(last_location.offset);
            EXPECT_TRUE(last_tile_group_header->GetNextItemPointer(last_location.offset).IsNull()) << "Last version has a next pointer";

            EXPECT_EQ(last_end_cid , MAX_CID) << "Last version doesn't end with MAX_CID";
          }
        }
      } else {
        EXPECT_TRUE(tile_group_header->GetNextItemPointer(tuple_slot).IsNull()) << "Invalid tuple must not have next item pointer";
      }
    }
    LOG_INFO("[OK] oldest-to-newest version chain validated");
  }

  gc_manager.StartGC();
}

TEST_F(MVCCTest, VersionChainTest) {
  const int num_txn = 5;
  const int scale = 20;
  const int num_key = 256;
  srand(15721);

  std::unique_ptr<storage::DataTable> table(
      TransactionTestsUtil::CreateTable(num_key));
  auto &txn_manager = concurrency::TransactionManagerFactory::GetInstance();

  TransactionScheduler scheduler(num_txn, table.get(), &txn_manager);
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
  TransactionScheduler scheduler2(1, table.get(), &txn_manager);
  for (int i = 0; i < num_key; i++) {
    scheduler2.Txn(0).Read(i);
  }
  scheduler2.Txn(0).Commit();
  scheduler2.Run();

  ValidateMVCC_OldToNew(table.get());
}


}  // End test namespace
}  // End peloton namespace
