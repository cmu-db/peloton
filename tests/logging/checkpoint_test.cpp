//===----------------------------------------------------------------------===//
//
//                         PelotonDB
//
// checkpoint_test.cpp
//
// Identification: tests/logging/checkpoint_test.cpp
//
// Copyright (c) 2015, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "harness.h"
#include "backend/logging/checkpoint.h"
#include "backend/logging/logging_util.h"
#include "backend/logging/loggers/wal_backend_logger.h"
#include "backend/logging/checkpoint/simple_checkpoint.h"
#include "backend/bridge/dml/mapper/mapper.h"

#include "backend/concurrency/transaction_manager_factory.h"
#include "backend/executor/logical_tile_factory.h"
#include "backend/index/index.h"

#include "executor/mock_executor.h"
#include "logging/logging_tests_util.h"

#define DEFAULT_RECOVERY_CID 15

using ::testing::NotNull;
using ::testing::Return;
using ::testing::InSequence;

namespace peloton {
namespace test {

//===--------------------------------------------------------------------===//
// Checkpoint Tests
//===--------------------------------------------------------------------===//

class CheckpointTests : public PelotonTest {};

oid_t GetTotalTupleCount(size_t table_tile_group_count, cid_t next_cid) {
  auto &txn_manager = concurrency::TransactionManagerFactory::GetInstance();

  txn_manager.SetNextCid(next_cid);

  txn_manager.BeginTransaction();

  auto &catalog_manager = catalog::Manager::GetInstance();
  oid_t total_tuple_count = 0;
  for (size_t tile_group_id = 1; tile_group_id <= table_tile_group_count;
       tile_group_id++) {
    auto tile_group = catalog_manager.GetTileGroup(tile_group_id);
    total_tuple_count += tile_group->GetActiveTupleCount();
  }
  txn_manager.CommitTransaction();
  return total_tuple_count;
}

TEST_F(CheckpointTests, CheckpointIntegrationTest) {
  logging::LoggingUtil::RemoveDirectory("pl_checkpoint", false);
  auto &txn_manager = concurrency::TransactionManagerFactory::GetInstance();
  txn_manager.BeginTransaction();

  // Create a table and wrap it in logical tile
  size_t tile_group_size = TESTS_TUPLES_PER_TILEGROUP;
  size_t table_tile_group_count = 3;

  oid_t default_table_oid = 13;
  // table has 3 tile groups
  storage::DataTable *target_table =
      ExecutorTestsUtil::CreateTable(tile_group_size, true, default_table_oid);
  ExecutorTestsUtil::PopulateTable(target_table,
                                   tile_group_size * table_tile_group_count,
                                   false, false, false);
  txn_manager.CommitTransaction();

  // add table to catalog
  auto &catalog_manager = catalog::Manager::GetInstance();
  storage::Database *db(new storage::Database(DEFAULT_DB_ID));
  db->AddTable(target_table);
  catalog_manager.AddDatabase(db);

  // create checkpoint
  auto &checkpoint_manager = logging::CheckpointManager::GetInstance();
  auto &log_manager = logging::LogManager::GetInstance();
  log_manager.SetGlobalMaxFlushedCommitId(txn_manager.GetNextCommitId());
  checkpoint_manager.Configure(CHECKPOINT_TYPE_NORMAL, false, 1);
  checkpoint_manager.DestroyCheckpointers();
  checkpoint_manager.InitCheckpointers();
  auto checkpointer = checkpoint_manager.GetCheckpointer(0);

  checkpointer->DoCheckpoint();

  auto most_recent_checkpoint_cid = checkpointer->GetMostRecentCheckpointCid();
  EXPECT_EQ(most_recent_checkpoint_cid != INVALID_CID, true);

  // destroy and restart
  checkpoint_manager.DestroyCheckpointers();
  checkpoint_manager.InitCheckpointers();

  // recovery from checkpoint
  log_manager.PrepareRecovery();
  auto recovery_checkpointer = checkpoint_manager.GetCheckpointer(0);
  recovery_checkpointer->DoRecovery();

  EXPECT_EQ(db->GetTableCount(), 1);
  EXPECT_EQ(db->GetTable(0)->GetNumberOfTuples(),
            tile_group_size * table_tile_group_count);
  catalog_manager.DropDatabaseWithOid(db->GetOid());
  logging::LoggingUtil::RemoveDirectory("pl_checkpoint", false);
}

TEST_F(CheckpointTests, CheckpointScanTest) {
  logging::LoggingUtil::RemoveDirectory("pl_checkpoint", false);

  auto &txn_manager = concurrency::TransactionManagerFactory::GetInstance();
  txn_manager.BeginTransaction();

  // Create a table and wrap it in logical tile
  size_t tile_group_size = TESTS_TUPLES_PER_TILEGROUP;
  size_t table_tile_group_count = 3;

  // table has 3 tile groups
  std::unique_ptr<storage::DataTable> target_table(
      ExecutorTestsUtil::CreateTable(tile_group_size));
  ExecutorTestsUtil::PopulateTable(target_table.get(),
                                   tile_group_size * table_tile_group_count,
                                   false, false, false);
  txn_manager.CommitTransaction();

  auto cid = txn_manager.GetNextCommitId() - 1;
  LOG_INFO("Scan with cid = %d. MaxCommittedCid = %d", (int) cid, (int) txn_manager.GetMaxCommittedCid());
  auto schema = target_table->GetSchema();
  std::vector<oid_t> column_ids;
  column_ids.resize(schema->GetColumnCount());
  std::iota(column_ids.begin(), column_ids.end(), 0);

  // create checkpoint
  auto &checkpoint_manager = logging::CheckpointManager::GetInstance();
  checkpoint_manager.Configure(CHECKPOINT_TYPE_NORMAL, true, 1);
  checkpoint_manager.DestroyCheckpointers();
  checkpoint_manager.InitCheckpointers();
  auto checkpointer = checkpoint_manager.GetCheckpointer(0);

  auto simple_checkpointer =
      reinterpret_cast<logging::SimpleCheckpoint *>(checkpointer);

  simple_checkpointer->SetLogger(new logging::WriteAheadBackendLogger());
  simple_checkpointer->SetStartCommitId(cid);
  simple_checkpointer->Scan(target_table.get(), DEFAULT_DB_ID);

  // verify results
  auto records = simple_checkpointer->GetRecords();
  EXPECT_EQ(records.size(),
            TESTS_TUPLES_PER_TILEGROUP * table_tile_group_count);
  for (unsigned int i = 0; i < records.size(); i++) {
    EXPECT_EQ(records[i]->GetType(), LOGRECORD_TYPE_WAL_TUPLE_INSERT);
  }
}



TEST_F(CheckpointTests, CheckpointRecoveryTest) {
  logging::LoggingUtil::RemoveDirectory("pl_checkpoint", false);

  size_t tile_group_size = TESTS_TUPLES_PER_TILEGROUP;
  size_t table_tile_group_count = 3;

  std::unique_ptr<storage::DataTable> recovery_table(
      ExecutorTestsUtil::CreateTable(tile_group_size));

  // prepare tuples
  auto mutate = true;
  auto random = false;
  int num_rows = tile_group_size * table_tile_group_count;
  std::vector<std::shared_ptr<storage::Tuple>> tuples =
      LoggingTestsUtil::BuildTuples(recovery_table.get(), num_rows, mutate,
                                    random);
  std::vector<logging::TupleRecord> records =
      LoggingTestsUtil::BuildTupleRecords(tuples, tile_group_size,
                                          table_tile_group_count);

  // recovery tuples from checkpoint
  logging::SimpleCheckpoint simple_checkpoint(true);
  for (auto record : records) {
    auto tuple = record.GetTuple();
    auto target_location = record.GetInsertLocation();
    // recovery checkpoint from these records
    simple_checkpoint.RecoverTuple(tuple, recovery_table.get(), target_location,
                                   DEFAULT_RECOVERY_CID);
  }

  // recovered tuples are visible from DEFAULT_RECOVERY_CID
  auto total_tuple_count =
      GetTotalTupleCount(table_tile_group_count, DEFAULT_RECOVERY_CID);
  EXPECT_EQ(total_tuple_count, tile_group_size * table_tile_group_count);

  // Clean up
  for (auto &tuple : tuples) {
    tuple.reset();
  }
}

TEST_F(CheckpointTests, CheckpointModeTransitionTest) {
  logging::LoggingUtil::RemoveDirectory("pl_checkpoint", false);

  auto &log_manager = logging::LogManager::GetInstance();
  auto &checkpoint_manager = logging::CheckpointManager::GetInstance();
  checkpoint_manager.DestroyCheckpointers();

  checkpoint_manager.Configure(CHECKPOINT_TYPE_NORMAL, true, 1);

  // launch checkpoint thread, wait for standby mode
  auto thread = std::thread(&logging::CheckpointManager::StartStandbyMode,
                            &checkpoint_manager);

  checkpoint_manager.WaitForModeTransition(peloton::CHECKPOINT_STATUS_STANDBY,
                                           true);

  // Clean up table tile state before recovery from checkpoint
  log_manager.PrepareRecovery();

  // Do any recovery
  checkpoint_manager.StartRecoveryMode();

  // Wait for standby mode
  checkpoint_manager.WaitForModeTransition(CHECKPOINT_STATUS_DONE_RECOVERY,
                                           true);

  // Now, enter CHECKPOINTING mode
  checkpoint_manager.SetCheckpointStatus(CHECKPOINT_STATUS_CHECKPOINTING);
  auto checkpointer = checkpoint_manager.GetCheckpointer(0);
  while (checkpointer->GetCheckpointStatus() !=
         CHECKPOINT_STATUS_CHECKPOINTING) {
    std::chrono::milliseconds sleep_time(10);
    std::this_thread::sleep_for(sleep_time);
  }
  checkpoint_manager.SetCheckpointStatus(CHECKPOINT_STATUS_INVALID);
  thread.join();
}

}  // End test namespace
}  // End peloton namespace
