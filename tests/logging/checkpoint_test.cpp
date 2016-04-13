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
#include "backend/logging/records/tuple_record.h"
#include "backend/bridge/dml/mapper/mapper.h"

#include "backend/concurrency/transaction_manager_factory.h"
#include "backend/executor/logical_tile_factory.h"
#include "backend/storage/data_table.h"
#include "backend/storage/tile.h"
#include "backend/index/index.h"

#include "executor/mock_executor.h"
#include "executor/executor_tests_util.h"

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

std::vector<logging::TupleRecord> BuildTupleRecords(
    std::vector<std::shared_ptr<storage::Tuple>> &tuples,
    size_t tile_group_size, size_t table_tile_group_count) {
  std::vector<logging::TupleRecord> records;
  for (size_t block = 1; block <= table_tile_group_count; ++block) {
    for (size_t offset = 0; offset < tile_group_size; ++offset) {
      ItemPointer location(block, offset);
      auto &tuple = tuples[(block - 1) * tile_group_size + offset];
      logging::TupleRecord record(LOGRECORD_TYPE_WAL_TUPLE_INSERT,
                                  INITIAL_TXN_ID, INVALID_OID, location,
                                  INVALID_ITEMPOINTER, nullptr, DEFAULT_DB_ID);
      record.SetTuple(tuple.get());
      records.push_back(record);
    }
  }
  LOG_INFO("Built a vector of %lu tuple WAL insert records", records.size());
  return records;
}

std::vector<std::shared_ptr<storage::Tuple>> BuildTuples(
    storage::DataTable *table, int num_rows, bool mutate, bool random) {
  std::vector<std::shared_ptr<storage::Tuple>> tuples;
  LOG_INFO("build a vector of %d tuples", num_rows);

  // Random values
  std::srand(std::time(nullptr));
  const catalog::Schema *schema = table->GetSchema();
  // Ensure that the tile group is as expected.
  assert(schema->GetColumnCount() == 4);

  // Insert tuples into tile_group.
  const bool allocate = true;
  auto testing_pool = TestingHarness::GetInstance().GetTestingPool();

  for (int rowid = 0; rowid < num_rows; rowid++) {
    int populate_value = rowid;
    if (mutate) populate_value *= 3;

    std::shared_ptr<storage::Tuple> tuple(new storage::Tuple(schema, allocate));

    // First column is unique in this case
    tuple->SetValue(0,
                    ValueFactory::GetIntegerValue(
                        ExecutorTestsUtil::PopulatedValue(populate_value, 0)),
                    testing_pool);

    // In case of random, make sure this column has duplicated values
    tuple->SetValue(
        1, ValueFactory::GetIntegerValue(ExecutorTestsUtil::PopulatedValue(
               random ? std::rand() % (num_rows / 3) : populate_value, 1)),
        testing_pool);

    tuple->SetValue(
        2, ValueFactory::GetDoubleValue(ExecutorTestsUtil::PopulatedValue(
               random ? std::rand() : populate_value, 2)),
        testing_pool);

    // In case of random, make sure this column has duplicated values
    Value string_value = ValueFactory::GetStringValue(
        std::to_string(ExecutorTestsUtil::PopulatedValue(
            random ? std::rand() % (num_rows / 3) : populate_value, 3)));
    tuple->SetValue(3, string_value, testing_pool);
    tuples.push_back(std::move(tuple));
  }
  return tuples;
}

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
  auto txn = txn_manager.BeginTransaction();

  // Create a table and wrap it in logical tile
  size_t tile_group_size = TESTS_TUPLES_PER_TILEGROUP;
  size_t table_tile_group_count = 3;

  // table has 3 tile groups
  std::unique_ptr<storage::DataTable> target_table(
      ExecutorTestsUtil::CreateTable(tile_group_size));
  ExecutorTestsUtil::PopulateTable(txn, target_table.get(),
                                   tile_group_size * table_tile_group_count,
                                   false, false, false);
  txn_manager.CommitTransaction();

  auto checkpoint_txn = txn_manager.BeginTransaction();
  // create scan executor
  std::unique_ptr<executor::ExecutorContext> executor_context(
      new executor::ExecutorContext(
          checkpoint_txn, bridge::PlanTransformer::BuildParams(nullptr)));
  auto schema = target_table->GetSchema();
  assert(schema);
  std::vector<oid_t> column_ids;
  column_ids.resize(schema->GetColumnCount());
  std::iota(column_ids.begin(), column_ids.end(), 0);

  /* Construct the Peloton plan node */
  LOG_TRACE("Initializing the executor tree");
  std::unique_ptr<planner::SeqScanPlan> scan_plan_node(
      new planner::SeqScanPlan(target_table.get(), nullptr, column_ids));
  std::unique_ptr<executor::SeqScanExecutor> scan_executor(
      new executor::SeqScanExecutor(scan_plan_node.get(),
                                    executor_context.get()));

  // create checkpoint
  logging::SimpleCheckpoint simple_checkpoint;
  std::unique_ptr<logging::WriteAheadBackendLogger> logger(
      new logging::WriteAheadBackendLogger());
  simple_checkpoint.SetLogger(logger.get());
  simple_checkpoint.Execute(scan_executor.get(), checkpoint_txn,
                            target_table.get(), DEFAULT_DB_ID);
  txn_manager.CommitTransaction();

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
      BuildTuples(recovery_table.get(), num_rows, mutate, random);
  std::vector<logging::TupleRecord> records =
      BuildTupleRecords(tuples, tile_group_size, table_tile_group_count);

  // recovery tuples from checkpoint
  logging::SimpleCheckpoint simple_checkpoint;
  for (auto record : records) {
    auto tuple = record.GetTuple();
    auto target_location = record.GetInsertLocation();
    // recovery checkpoint from these records
    simple_checkpoint.RecoverTuple(tuple, recovery_table.get(), target_location,
                                   DEFAULT_RECOVERY_CID);
    simple_checkpoint.RecoverIndex(tuple, recovery_table.get(),
                                   target_location);
  }

  // TODO: fix this bug in the future.
  // recovered tuples are not visible until DEFAULT_RECOVERY_CID - 1
  // auto total_tuple_count =
  //     GetTotalTupleCount(table_tile_group_count, DEFAULT_RECOVERY_CID - 1);
  // EXPECT_EQ(total_tuple_count, 0);

  // recovered tuples are visible from DEFAULT_RECOVERY_CID
  auto total_tuple_count =
      GetTotalTupleCount(table_tile_group_count, DEFAULT_RECOVERY_CID);
  EXPECT_EQ(total_tuple_count, tile_group_size * table_tile_group_count);

  EXPECT_EQ(recovery_table->GetIndex(0)->GetNumberOfTuples(),
            tile_group_size * table_tile_group_count);
  EXPECT_EQ(recovery_table->GetIndex(1)->GetNumberOfTuples(),
            tile_group_size * table_tile_group_count);

  // Clean up
  for (auto &tuple : tuples) {
    tuple.reset();
  }
}

}  // End test namespace
}  // End peloton namespace
