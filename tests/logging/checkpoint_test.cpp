//===----------------------------------------------------------------------===//
//
//                         PelotonDB
//
// checkpoint_test.cpp
//
// Identification: tests/planner/checkpoint_test.cpp
//
// Copyright (c) 2015, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "harness.h"
#include "backend/logging/checkpoint.h"
#include "backend/logging/loggers/wal_backend_logger.h"

#include "backend/concurrency/transaction_manager_factory.h"
#include "backend/executor/logical_tile_factory.h"
#include "backend/storage/data_table.h"
#include "backend/storage/tile.h"

#include "executor/mock_executor.h"
#include "executor/executor_tests_util.h"

#include "backend/logging/checkpoint/simple_checkpoint.h"

using ::testing::NotNull;
using ::testing::Return;
using ::testing::InSequence;

namespace peloton {
namespace test {

//===--------------------------------------------------------------------===//
// Checkpoint Tests
//===--------------------------------------------------------------------===//

class CheckpointTests : public PelotonTest {};

void ExpectNormalTileResults(
    size_t table_tile_group_count, MockExecutor *table_scan_executor,
    std::vector<std::unique_ptr<executor::LogicalTile>> &
        table_logical_tile_ptrs) {
  // Return true for the first table_tile_group_count times
  // Then return false after that
  {
    testing::Sequence execute_sequence;
    for (size_t table_tile_group_itr = 0;
         table_tile_group_itr < table_tile_group_count + 1;
         table_tile_group_itr++) {
      // Return true for the first table_tile_group_count times
      if (table_tile_group_itr < table_tile_group_count) {
        EXPECT_CALL(*table_scan_executor, DExecute())
            .InSequence(execute_sequence)
            .WillOnce(Return(true));
      } else  // Return false after that
      {
        EXPECT_CALL(*table_scan_executor, DExecute())
            .InSequence(execute_sequence)
            .WillOnce(Return(false));
      }
    }
  }
  // Return the appropriate logical tiles for the first table_tile_group_count
  // times
  {
    testing::Sequence get_output_sequence;
    for (size_t table_tile_group_itr = 0;
         table_tile_group_itr < table_tile_group_count;
         table_tile_group_itr++) {
      EXPECT_CALL(*table_scan_executor, GetOutput())
          .InSequence(get_output_sequence)
          .WillOnce(
              Return(table_logical_tile_ptrs[table_tile_group_itr].release()));
    }
  }
}

TEST_F(CheckpointTests, BasicTest) {
  MockExecutor table_scan_executor;

  // Create a table and wrap it in logical tile
  size_t tile_group_size = TESTS_TUPLES_PER_TILEGROUP;
  size_t table_tile_group_count = 3;

  auto &txn_manager = concurrency::TransactionManagerFactory::GetInstance();
  auto txn = txn_manager.BeginTransaction();
  auto txn_id = txn->GetTransactionId();

  // Left table has 3 tile groups
  std::unique_ptr<storage::DataTable> table(
      ExecutorTestsUtil::CreateTable(tile_group_size));
  ExecutorTestsUtil::PopulateTable(txn, table.get(),
                                   tile_group_size * table_tile_group_count,
                                   false, false, false);
  txn_manager.CommitTransaction();

  // Wrap the input tables with logical tiles
  std::vector<std::unique_ptr<executor::LogicalTile>> table_logical_tile_ptrs;
  for (size_t table_tile_group_itr = 0;
       table_tile_group_itr < table_tile_group_count; table_tile_group_itr++) {
    std::unique_ptr<executor::LogicalTile> table_logical_tile(
        executor::LogicalTileFactory::WrapTileGroup(
            table->GetTileGroup(table_tile_group_itr), txn_id));
    table_logical_tile_ptrs.push_back(std::move(table_logical_tile));
  }

  // scan executor returns logical tiles from the left table
  EXPECT_CALL(table_scan_executor, DInit()).WillOnce(Return(true));

  ExpectNormalTileResults(table_tile_group_count, &table_scan_executor,
                          table_logical_tile_ptrs);

  logging::SimpleCheckpoint simple_checkpoint;
  logging::WriteAheadBackendLogger *logger =
      new logging::WriteAheadBackendLogger();

  simple_checkpoint.SetLogger(logger);
  auto checkpoint_txn = txn_manager.BeginTransaction();
  simple_checkpoint.Execute(&table_scan_executor, checkpoint_txn, table.get(),
                            1);
  auto records = simple_checkpoint.GetRecords();
  EXPECT_EQ(records.size(),
            TESTS_TUPLES_PER_TILEGROUP * table_tile_group_count);
  for (unsigned int i = 0; i < records.size(); i++) {
    EXPECT_EQ(records[i]->GetType(), LOGRECORD_TYPE_WAL_TUPLE_INSERT);
  }

  // Clean up
  for (auto record : records) {
	  delete record;
  }

  delete logger;
}

}  // End test namespace
}  // End peloton namespace
