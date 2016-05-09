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

#include "backend/statistics/stats_aggregator.h"
#include "backend/statistics/backend_stats_context.h"
#include "backend/storage/data_table.h"
#include "backend/concurrency/transaction_manager_factory.h"
#include "executor/executor_tests_util.h"
#include "statistics/stats_tests_util.h"

#include <iostream>
using std::cout;
using std::endl;

extern StatsType peloton_stats_mode;

namespace peloton {
namespace test {
    
class StatsTest : public PelotonTest {};
    
TEST_F(StatsTest, BasicStatsTest) {

  concurrency::TransactionManagerFactory::Configure(CONCURRENCY_TYPE_OPTIMISTIC);
  auto &txn_manager = concurrency::TransactionManagerFactory::GetInstance();
  std::unique_ptr<storage::DataTable> table(StatsTestsUtil::CreateTable());

  peloton_stats_mode = STATS_TYPE_ENABLE;
//  stats::StatsAggregator& aggregator = peloton::stats::StatsAggregator::GetInstance();
//  stats::StatsAggregator aggregator;
//  __attribute__((unused)) peloton::stats::BackendStatsContext* stats =
//      aggregator.GetBackendStatsContext();

//  // read, read, read, read, update, read, read not exist
//  // another txn read
//  {
//    TransactionScheduler scheduler(2, table.get(), &txn_manager);
//    scheduler.Txn(0).Read(0);
//    scheduler.Txn(0).Read(0);
//    scheduler.Txn(0).Read(0);
//    scheduler.Txn(0).Insert(0, 0);
//    scheduler.Txn(0).Read(0);
//    scheduler.Txn(0).Update(0, 1);
//    scheduler.Txn(0).Read(0);
//    scheduler.Txn(0).Read(100);
//    scheduler.Txn(0).Commit();
////    scheduler.Txn(1).Read(0);
////    scheduler.Txn(1).Commit();
//
//    scheduler.Run();
//
//    EXPECT_EQ(RESULT_SUCCESS, scheduler.schedules[0].txn_result);
//    EXPECT_EQ(RESULT_SUCCESS, scheduler.schedules[1].txn_result);
//    EXPECT_EQ(0, scheduler.schedules[0].results[0]);
//    EXPECT_EQ(0, scheduler.schedules[0].results[1]);
//    EXPECT_EQ(0, scheduler.schedules[0].results[2]);
//    EXPECT_EQ(0, scheduler.schedules[0].results[3]);
//    EXPECT_EQ(1, scheduler.schedules[0].results[4]);
//    EXPECT_EQ(-1, scheduler.schedules[0].results[5]);
//    EXPECT_EQ(1, scheduler.schedules[1].results[0]);
//  }
//
//  // update, update, update, update, read
//  {
//    TransactionScheduler scheduler(1, table.get(), &txn_manager);
//    scheduler.Txn(0).Update(0, 1);
//    scheduler.Txn(0).Update(0, 2);
//    scheduler.Txn(0).Update(0, 3);
//    scheduler.Txn(0).Update(0, 4);
//    scheduler.Txn(0).Read(0);
//    scheduler.Txn(0).Commit();
//
//    scheduler.Run();
//
//    EXPECT_EQ(RESULT_SUCCESS, scheduler.schedules[0].txn_result);
//    EXPECT_EQ(4, scheduler.schedules[0].results[0]);
//  }
//
//  // delete not exist, delete exist, read deleted, update deleted,
//  // read deleted, insert back, update inserted, read newly updated
//  {
//    TransactionScheduler scheduler(1, table.get(), &txn_manager);
//    scheduler.Txn(0).Delete(100);
//    scheduler.Txn(0).Delete(0);
//    scheduler.Txn(0).Read(0);
//    scheduler.Txn(0).Update(0, 1);
//    scheduler.Txn(0).Read(0);
//    scheduler.Txn(0).Insert(0, 2);
//    scheduler.Txn(0).Update(0, 3);
//    scheduler.Txn(0).Read(0);
//    scheduler.Txn(0).Commit();
//
//    scheduler.Run();
//
//    EXPECT_EQ(RESULT_SUCCESS, scheduler.schedules[0].txn_result);
//    EXPECT_EQ(-1, scheduler.schedules[0].results[0]);
//    EXPECT_EQ(-1, scheduler.schedules[0].results[1]);
//    EXPECT_EQ(3, scheduler.schedules[0].results[2]);
//  }

  // insert, delete inserted, read deleted, insert again, delete again
  // read deleted, insert again, read inserted, update inserted, read updated
  {
    TransactionScheduler scheduler(1, table.get(), &txn_manager);

    scheduler.Txn(0).Insert(1000, 0);
    scheduler.Txn(0).Delete(1000);
    scheduler.Txn(0).Read(1000);
    scheduler.Txn(0).Insert(1000, 1);
    scheduler.Txn(0).Delete(1000);
    scheduler.Txn(0).Read(1000);
    scheduler.Txn(0).Insert(1000, 2);
    scheduler.Txn(0).Read(1000);
    scheduler.Txn(0).Update(1000, 3);
    scheduler.Txn(0).Read(1000);
    scheduler.Txn(0).Commit();

    scheduler.Run();

//    EXPECT_EQ(RESULT_SUCCESS, scheduler.schedules[0].txn_result);
//    EXPECT_EQ(-1, scheduler.schedules[0].results[0]);
//    EXPECT_EQ(-1, scheduler.schedules[0].results[1]);
//    EXPECT_EQ(2, scheduler.schedules[0].results[2]);
//    EXPECT_EQ(3, scheduler.schedules[0].results[3]);

//    int64_t interval_cnt = 0;
//    double alpha = 0.4;
//    double weighted_avg_throughput = 0.0;

//    peloton::stats::StatsAggregator::GetInstance().Aggregate(interval_cnt,alpha,weighted_avg_throughput);
//    stats::BackendStatsContext& agg_stats = peloton::stats::StatsAggregator::GetInstance().GetAggregatedStats();

    std::vector<stats::BackendStatsContext*>& stats = scheduler.GetThreadStats();
    EXPECT_EQ(1, stats.size());

//    // Check database level metrics
//    oid_t database_id = table->GetDatabaseOid();
//    int64_t txns_committed = agg_stats.GetDatabaseMetric(database_id)
//        ->GetTxnCommitted().GetCounter();
//    int64_t txns_aborted = agg_stats.GetDatabaseMetric(database_id)
//        ->GetTxnAborted().GetCounter();
//
//    EXPECT_EQ(1, txns_committed);
//    EXPECT_EQ(0, txns_aborted);
//
//    // Check table level metrics
//    oid_t table_id = table->GetOid();
//    int64_t num_reads = agg_stats.GetTableMetric(database_id, table_id)
//        ->GetTableAccess().GetReads();
//    int64_t num_updates = agg_stats.GetTableMetric(database_id, table_id)
//        ->GetTableAccess().GetUpdates();
//    int64_t num_inserts = agg_stats.GetTableMetric(database_id, table_id)
//        ->GetTableAccess().GetInserts();
//    int64_t num_deletes = agg_stats.GetTableMetric(database_id, table_id)
//        ->GetTableAccess().GetDeletes();
//
//    EXPECT_EQ(4, num_reads);
//    EXPECT_EQ(1, num_updates);
//    EXPECT_EQ(3, num_inserts);
//    EXPECT_EQ(2, num_deletes);

  }

}

//  TEST_F(BasicStatsTest, CreateTest) {
//    peloton_stats_mode = STATS_TYPE_ENABLE;
//
//    /*
//     * Register to StatsAggregator
//     */
//    peloton::stats::BackendStatsContext* stats_ = peloton::stats::StatsAggregator::GetInstance().GetBackendStatsContext();
//    printf("have a stats context? %p\n", stats_);
//
//    int tuple_count = 10;
//
//    // Create a table and wrap it in logical tiles
//    auto &txn_manager = concurrency::TransactionManagerFactory::GetInstance();
//    auto txn = txn_manager.BeginTransaction();
//    std::unique_ptr<storage::DataTable> data_table(
//               ExecutorTestsUtil::CreateTable(tuple_count, false));
//
//    peloton::stats::StatsAggregator * stats_agg = new peloton::stats::StatsAggregator();
//
//    ExecutorTestsUtil::PopulateTable(txn, data_table.get(), tuple_count, false,
//             false, true);
//    txn_manager.CommitTransaction();
//
//    int64_t interval_cnt = 0;
//    double alpha = 0.4;
//    double weighted_avg_throughput = 0.0;
//    stats_agg->Aggregate(interval_cnt, alpha, weighted_avg_throughput);
//    //peloton::stats::BackendStatsContext context_ = stats_agg->GetAggregatedStats();
//
//
//    // Check if transacions commited is correct
//    int64_t txn_commited = stats_->GetDatabaseMetric(data_table->GetDatabaseOid())->GetTxnCommitted().GetCounter();
//    EXPECT_EQ(txn_commited, 1);
//
//    int inserts = (int)stats_->GetTableMetric(data_table->GetDatabaseOid(), data_table->GetOid())->GetTableAccess().GetInserts();
//
//    EXPECT_EQ(tuple_count, inserts);
//    delete stats_agg;
//  }
}
}
