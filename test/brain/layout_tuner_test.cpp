//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// layout_tuner_test.cpp
//
// Identification: test/brain/layout_tuner_test.cpp
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//


#include <cstdio>
#include <random>
#include <chrono>

#include "common/harness.h"

#include "brain/layout_tuner.h"
#include "common/generator.h"

#include "storage/data_table.h"
#include "storage/tile_group.h"
#include "concurrency/transaction_manager_factory.h"
#include "executor/executor_tests_util.h"

namespace peloton {
namespace test {

//===--------------------------------------------------------------------===//
// Layout Tuner Tests
//===--------------------------------------------------------------------===//

class LayoutTunerTests : public PelotonTest {};

TEST_F(LayoutTunerTests, BasicTest) {

  const int tuple_count = TESTS_TUPLES_PER_TILEGROUP;

  // Create a table and populate it
  auto &txn_manager = concurrency::TransactionManagerFactory::GetInstance();
  auto txn = txn_manager.BeginTransaction();
  std::unique_ptr<storage::DataTable> data_table(
      ExecutorTestsUtil::CreateTable(tuple_count, false));
  ExecutorTestsUtil::PopulateTable(data_table.get(), tuple_count, false, false,
                                   true, txn);
  txn_manager.CommitTransaction(txn);

  oid_t column_count = data_table->GetSchema()->GetColumnCount();
  EXPECT_EQ(column_count, 4);

  // Layout tuner
  brain::LayoutTuner &layout_tuner = brain::LayoutTuner::GetInstance();

  // Run layout tuner
  layout_tuner.AddTable(data_table.get());

  auto old_default_layout = data_table->GetDefaultLayout();
  LOG_INFO("Layout: %s", layout_tuner.GetColumnMapInfo(old_default_layout).c_str());

  // Start layout tuner
  layout_tuner.Start();

  std::vector<double> columns_accessed(column_count, 0);
  double sample_weight;

  // initialize a uniform distribution between 0 and 1
  UniformGenerator generator;

  for (int sample_itr = 0; sample_itr < 10000; sample_itr++) {
    auto rng_val = generator.GetSample();

    if (rng_val < 0.6) {
      columns_accessed = {1, 1, 0, 0};
      sample_weight = 100;
    } else if (rng_val < 0.9) {
      columns_accessed = {0, 0, 1, 1};
      sample_weight = 10;
    } else {
      columns_accessed = {0, 0, 0, 1};
      sample_weight = 10;
    }

    brain::Sample sample(columns_accessed, sample_weight);
    data_table->RecordLayoutSample(sample);

    // Sleep a bit
    if(sample_itr % 100 == 0 ){
      std::this_thread::sleep_for(std::chrono::microseconds(1000));
    }
  }

  // Stop layout tuner
  layout_tuner.Stop();

  layout_tuner.ClearTables();

  auto new_default_layout = data_table->GetDefaultLayout();
  LOG_INFO("Layout: %s", layout_tuner.GetColumnMapInfo(new_default_layout).c_str());

  EXPECT_TRUE(new_default_layout != old_default_layout);

}

}  // End test namespace
}  // End peloton namespace
