//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// data_table_test.cpp
//
// Identification: tests/storage/basic_stats_test.cpp
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "harness.h"

#include "statistics/stats_tests_util.h"
#include "backend/statistics/stats_aggregator.h"
#include "backend/statistics/backend_stats_context.h"
#include "backend/storage/data_table.h"
#include "backend/concurrency/transaction_manager_factory.h"
#include "executor/executor_tests_util.h"
#include "backend/storage/tuple.h"
#include "backend/common/value.h"
#include "backend/common/value_factory.h"



#include <iostream>
using std::cout;
using std::endl;

extern StatsType peloton_stats_mode;

namespace peloton {
namespace test {
    
class StatsTest : public PelotonTest {};


  TEST_F(StatsTest, PerThreadStatsTest) {
    peloton_stats_mode = STATS_TYPE_ENABLE;

    /*
     * Register to StatsAggregator
     */
    peloton::stats::BackendStatsContext* stats_ =
        peloton::stats::StatsAggregator::GetInstanceForTest()
        .GetBackendStatsContext();

    //int tuple_count = 10;
    int tups_per_tile_group = 100;
    int num_rows = 10;

    // Create a table and wrap it in logical tiles
    auto &txn_manager = concurrency::TransactionManagerFactory::GetInstance();
    __attribute__((unused)) auto txn = txn_manager.BeginTransaction();
    std::unique_ptr<storage::DataTable> data_table(
               ExecutorTestsUtil::CreateTable(tups_per_tile_group, true));

    // Ensure that the tile group is as expected.
    const catalog::Schema *schema = data_table->GetSchema();
    assert(schema->GetColumnCount() == 4);

    // Insert tuples into tile_group.
    std::vector<ItemPointer> tuple_slot_ids;

    for (int rowid = 0; rowid < num_rows; rowid++) {
      int populate_value = rowid;

      storage::Tuple tuple = StatsTestsUtil::PopulateTuple(schema,
          ExecutorTestsUtil::PopulatedValue(populate_value, 0),
          ExecutorTestsUtil::PopulatedValue(populate_value, 1),
          ExecutorTestsUtil::PopulatedValue(populate_value, 2),
          ExecutorTestsUtil::PopulatedValue(populate_value, 3));

      ItemPointer tuple_slot_id = data_table->InsertTuple(&tuple);
      tuple_slot_ids.push_back(tuple_slot_id);
      EXPECT_TRUE(tuple_slot_id.block != INVALID_OID);
      EXPECT_TRUE(tuple_slot_id.offset != INVALID_OID);
      txn_manager.PerformInsert(tuple_slot_id);
    }
    txn_manager.CommitTransaction();
    oid_t database_id = data_table->GetDatabaseOid();
    oid_t table_id = data_table->GetOid();

    // Check: # transactions committed = 1, # table inserts = 10
    int64_t txn_commited = stats_->GetDatabaseMetric(database_id)
        ->GetTxnCommitted().GetCounter();
    int64_t inserts = stats_->GetTableMetric(database_id, table_id)
        ->GetTableAccess().GetInserts();
    EXPECT_EQ(1, txn_commited);
    EXPECT_EQ(num_rows, inserts);

    // Read every other tuple
    txn = txn_manager.BeginTransaction();
    for (size_t i = 0; i < tuple_slot_ids.size(); i += 2) {
      txn_manager.PerformRead(tuple_slot_ids[i]);
    }
    txn_manager.CommitTransaction();

    // Check: # transactions committed = 2, # inserts = 10, # reads = 5
    txn_commited = stats_->GetDatabaseMetric(database_id)
        ->GetTxnCommitted().GetCounter();
    inserts = stats_->GetTableMetric(database_id, table_id)
        ->GetTableAccess().GetInserts();
    int64_t reads = stats_->GetTableMetric(database_id, table_id)
        ->GetTableAccess().GetReads();
    EXPECT_EQ(2, txn_commited);
    EXPECT_EQ(num_rows, inserts);
    EXPECT_EQ(5, reads);

    // Do a single read and abort
    txn = txn_manager.BeginTransaction();
    txn_manager.PerformRead(tuple_slot_ids[0]);
    txn_manager.AbortTransaction();

    // Check: # txns committed = 2, # txns aborted = 1, # reads = 6
    txn_commited = stats_->GetDatabaseMetric(database_id)
        ->GetTxnCommitted().GetCounter();
    int64_t txn_aborted = stats_->GetDatabaseMetric(database_id)
        ->GetTxnAborted().GetCounter();
    reads = stats_->GetTableMetric(database_id, table_id)
            ->GetTableAccess().GetReads();
    EXPECT_EQ(2, txn_commited);
    EXPECT_EQ(1, txn_aborted);
    EXPECT_EQ(6, reads);

    // Update the third tuple
//    txn = txn_manager.BeginTransaction();
//    storage::Tuple new_tuple = StatsTestsUtil::PopulateTuple(schema, 0, 1, 2, 3);
//    ItemPointer new_tuple_slot_id = data_table->InsertTuple(&new_tuple);
//    EXPECT_TRUE(new_tuple_slot_id.block != INVALID_OID);
//    EXPECT_TRUE(new_tuple_slot_id.offset != INVALID_OID);
//    txn_manager.PerformUpdate(tuple_slot_ids[0], new_tuple_slot_id);
//    txn_manager.CommitTransaction();
//
//    // Check: # txns committed = 3, # updates = 1
//    txn_commited = stats_->GetDatabaseMetric(database_id)
//        ->GetTxnCommitted().GetCounter();
//    int64_t updates = stats_->GetTableMetric(database_id, table_id)
//        ->GetTableAccess().GetUpdates();
//    EXPECT_EQ(3, txn_commited);
//    EXPECT_EQ(1, updates);

  }
}
}
