//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// layout_tuner_test.cpp
//
// Identification: test/tuning/layout_tuner_test.cpp
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <chrono>
#include <cstdio>
#include <random>

#include "common/harness.h"

#include "tuning/layout_tuner.h"

#include "executor/testing_executor_util.h"
#include "common/generator.h"

#include "concurrency/transaction_manager_factory.h"
#include "storage/data_table.h"
#include "storage/tile_group.h"

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
      TestingExecutorUtil::CreateTable(tuple_count, false));
  TestingExecutorUtil::PopulateTable(data_table.get(), tuple_count, false,
  false,
                                   true, txn);
  txn_manager.CommitTransaction(txn);

  // Check column count
  oid_t column_count = data_table->GetSchema()->GetColumnCount();
  EXPECT_EQ(column_count, 4);

  // Layout tuner
  tuning::LayoutTuner &layout_tuner = tuning::LayoutTuner::GetInstance();

  // Attach table to index tuner
  layout_tuner.AddTable(data_table.get());

  // Check old default tile group layout
  auto old_default_layout = data_table->GetDefaultLayout();
  LOG_INFO("Layout: %s",
  layout_tuner.GetColumnMapInfo(old_default_layout).c_str());

  // Start layout tuner
  layout_tuner.Start();

  std::vector<double> columns_accessed(column_count, 0);
  double sample_weight;

  // initialize a uniform distribution between 0 and 1
  UniformGenerator generator;
  for (int sample_itr = 0; sample_itr < 10000; sample_itr++) {
    auto rng_val = generator.GetSample();

    if (rng_val < 0.9) {
      columns_accessed = {0, 1, 2};
      sample_weight = 100;
    }
    else {
      columns_accessed = {3};
      sample_weight = 10;
    }

    // Create a table access sample
    // Indicates the columns accessed (bitmap), and query weight
    tuning::Sample sample(columns_accessed, sample_weight);

    // Collect layout sample in table
    data_table->RecordLayoutSample(sample);

    // Periodically sleep a bit
    // Layout tuner thread will process the layout samples periodically,
    // derive the new table layout, and
    // transform the layout of existing tile groups in the table
    if(sample_itr % 100 == 0 ){
      std::this_thread::sleep_for(std::chrono::microseconds(1000));
    }
  }

  // Stop layout tuner
  layout_tuner.Stop();

  // Detach all tables from layout tuner
  layout_tuner.ClearTables();

  // Check new default tile group layout
  auto new_default_layout = data_table->GetDefaultLayout();
  LOG_INFO("Layout: %s",
  layout_tuner.GetColumnMapInfo(new_default_layout).c_str());

  // Ensure that the layout has been changed
  EXPECT_NE(new_default_layout, old_default_layout);

  // Check the new default table layout
  column_count = new_default_layout.size();
  EXPECT_EQ(column_count, 4);
  auto first_column_tile = new_default_layout[0].first;
  auto second_column_tile = new_default_layout[1].first;
  auto third_column_tile = new_default_layout[2].first;
  auto fourth_column_tile = new_default_layout[3].first;
  EXPECT_EQ(first_column_tile, 0);
  EXPECT_EQ(second_column_tile, 0);
  EXPECT_EQ(third_column_tile, 0);
  EXPECT_EQ(fourth_column_tile, 1);


}

}  // namespace test
}  // namespace peloton
