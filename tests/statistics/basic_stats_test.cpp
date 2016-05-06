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
#include "backend/storage/data_table.h"
#include "backend/concurrency/transaction_manager_factory.h"
#include "executor/executor_tests_util.h"
#include "concurrency/transaction_tests_util.h"

namespace peloton {
  namespace test {
    
    class BasicStatsTest : public PelotonTest {};
    
    TEST_F(BasicStatsTest, CreateTest) {
      peloton::stats::StatsAggregator * stats_ = new peloton::stats::StatsAggregator();
      int tuple_count = 10;

      // Create a table and wrap it in logical tiles                                                                          
      auto &txn_manager = concurrency::TransactionManagerFactory::GetInstance();
      auto txn = txn_manager.BeginTransaction();
      std::unique_ptr<storage::DataTable> data_table(
						     ExecutorTestsUtil::CreateTable(tuple_count, false));
      ExecutorTestsUtil::PopulateTable(txn, data_table.get(), tuple_count, false,
				       false, true);
      txn_manager.CommitTransaction();

      delete stats_;
    }
  }
}
