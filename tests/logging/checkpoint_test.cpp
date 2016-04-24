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

TEST_F(CheckpointTests, BasicCheckpointCreationTest) {
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
  auto schema = target_table->GetSchema();
  std::vector<oid_t> column_ids;
  column_ids.resize(schema->GetColumnCount());
  std::iota(column_ids.begin(), column_ids.end(), 0);

  // create checkpoint
  logging::SimpleCheckpoint simple_checkpoint(true);
  std::unique_ptr<logging::WriteAheadBackendLogger> logger(
      new logging::WriteAheadBackendLogger());
  simple_checkpoint.SetLogger(logger.get());
  simple_checkpoint.SetStartCommitId(cid);
  simple_checkpoint.Scan(target_table.get(), DEFAULT_DB_ID);

  // verify results
  auto records = simple_checkpoint.GetRecords();
  EXPECT_EQ(records.size(),
            TESTS_TUPLES_PER_TILEGROUP * table_tile_group_count);
  for (unsigned int i = 0; i < records.size(); i++) {
    EXPECT_EQ(records[i]->GetType(), LOGRECORD_TYPE_WAL_TUPLE_INSERT);
  }
}

TEST_F(CheckpointTests, BasicCheckpointRecoveryTest) {
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

  // TODO create test mode for checkpoint to avoid file write
  // TODO clean up file directory at startup

  // Clean up
  for (auto &tuple : tuples) {
    tuple.reset();
  }
}

}  // End test namespace
}  // End peloton namespace
