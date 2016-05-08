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
#include "concurrency/transaction_tests_util.h"

#include <iostream>
using std::cout;
using std::endl;

extern StatsType peloton_stats_mode;

namespace peloton {
  namespace test {
    
    class BasicStatsTest : public PelotonTest {};
    
    TEST_F(BasicStatsTest, CreateTest) {
      peloton_stats_mode = STATS_TYPE_ENABLE;

      /*
       * Register to StatsAggregator
       */
      peloton::stats::BackendStatsContext* stats_ = peloton::stats::StatsAggregator::GetInstance().GetBackendStatsContext();
      printf("have a stats context? %p\n", stats_);
      
      int tuple_count = 10;

      // Create a table and wrap it in logical tiles
      auto &txn_manager = concurrency::TransactionManagerFactory::GetInstance();
      auto txn = txn_manager.BeginTransaction();
      std::unique_ptr<storage::DataTable> data_table(
						     ExecutorTestsUtil::CreateTable(tuple_count, false));

      peloton::stats::StatsAggregator * stats_agg = new peloton::stats::StatsAggregator();
      
      ExecutorTestsUtil::PopulateTable(txn, data_table.get(), tuple_count, false,
				       false, true);
      txn_manager.CommitTransaction();

      int64_t interval_cnt = 0;
      double alpha = 0.4;
      double weighted_avg_throughput = 0.0;
      stats_agg->Aggregate(interval_cnt, alpha, weighted_avg_throughput);
      //peloton::stats::BackendStatsContext context_ = stats_agg->GetAggregatedStats();
      

      // Check if transacions commited is correct 
      int64_t txn_commited = stats_->GetDatabaseMetric(data_table->GetDatabaseOid())->GetTxnCommitted().GetCounter();
      EXPECT_EQ(txn_commited, 1);

      int inserts = (int)stats_->GetTableMetric(data_table->GetDatabaseOid(), data_table->GetOid())->GetTableAccess().GetInserts();
      
      EXPECT_EQ(tuple_count, inserts);
      delete stats_agg;
    }
  }
}
