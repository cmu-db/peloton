//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// index_tuner_test.cpp
//
// Identification: test/brain/index_tuner_test.cpp
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//


#include <cstdio>
#include <random>
#include <chrono>

#include "common/harness.h"

#include "brain/index_tuner.h"

#include "executor/testing_executor_util.h"
#include "brain/sample.h"
#include "common/generator.h"

#include "storage/data_table.h"
#include "storage/tile_group.h"
#include "concurrency/transaction_manager_factory.h"

namespace peloton {
namespace test {

//===--------------------------------------------------------------------===//
// Index Tuner Tests
//===--------------------------------------------------------------------===//

class IndexTunerTests : public PelotonTest {};

TEST_F(IndexTunerTests, BasicTest) {

  const int tuple_count = TESTS_TUPLES_PER_TILEGROUP;

  // Create a table and populate it
  auto &txn_manager = concurrency::TransactionManagerFactory::GetInstance();
  auto txn = txn_manager.BeginTransaction();
  std::unique_ptr<storage::DataTable> data_table(
      TestingExecutorUtil::CreateTable(tuple_count, false));
  TestingExecutorUtil::PopulateTable(data_table.get(), tuple_count, false, false,
                                   true, txn);
  txn_manager.CommitTransaction(txn);

  // Check column count
  oid_t column_count = data_table->GetSchema()->GetColumnCount();
  EXPECT_EQ(column_count, 4);

  // Index tuner
  brain::IndexTuner &index_tuner = brain::IndexTuner::GetInstance();

  // Attach table to index tuner
  index_tuner.AddTable(data_table.get());

  // Check old index count
  auto old_index_count = data_table->GetIndexCount();
  EXPECT_TRUE(old_index_count == 0);
  LOG_INFO("Index Count: %u", old_index_count);

  // Start index tuner
  index_tuner.Start();

  std::vector<double> columns_accessed(column_count, 0);
  double sample_weight;

  UniformGenerator generator;
  for (int sample_itr = 0; sample_itr < 10000; sample_itr++) {
    auto rng_val = generator.GetSample();

    if (rng_val < 0.6) {
      columns_accessed = {0, 1, 2};
      sample_weight = 100;
    } else if (rng_val < 0.9) {
      columns_accessed = {0, 2};
      sample_weight = 10;
    } else {
      columns_accessed = {0, 1};
      sample_weight = 10;
    }

    // Create a table access sample
    // Indicates the columns present in predicate, query weight, and selectivity
    brain::Sample sample(columns_accessed,
                         sample_weight,
                         brain::SampleType::ACCESS);

    // Collect index sample in table
    data_table->RecordIndexSample(sample);

    // Periodically sleep a bit
    // Index tuner thread will process the index samples periodically,
    // and materialize the appropriate ad-hoc indexes
    if(sample_itr % 100 == 0 ){
      std::this_thread::sleep_for(std::chrono::microseconds(10000));
    }
  }

  // Wait for tuner to build indexes
  sleep(5);

  // Stop index tuner
  index_tuner.Stop();

  // Detach all tables from index tuner
  index_tuner.ClearTables();

  // Check new index count
  auto new_index_count = data_table->GetIndexCount();
  LOG_INFO("Index Count: %u", new_index_count);

  EXPECT_TRUE(new_index_count != old_index_count);
  EXPECT_TRUE(new_index_count == 3);

  // Check candidate indices to ensure that
  // all the ad-hoc indexes are materialized
  std::vector<std::set<oid_t>> candidate_indices;
  candidate_indices.push_back({0, 1, 2});
  candidate_indices.push_back({0, 2});
  candidate_indices.push_back({0, 1});

  for (auto candidate_index : candidate_indices) {
    bool candidate_index_found = false;
    oid_t index_itr;
    for (index_itr = 0; index_itr < new_index_count; index_itr++) {
      // Check attributes
      auto index_attrs = data_table->GetIndexAttrs(index_itr);
      if (index_attrs != candidate_index) {
        continue;
      }
      // Exact match
      candidate_index_found = true;
      break;
    }

    EXPECT_TRUE(candidate_index_found);
  }

}

}  // namespace test
}  // namespace peloton
