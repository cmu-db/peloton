//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// lspi_test.cpp
//
// Identification: test/brain/lspi_test.cpp
//
// Copyright (c) 2015-2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "brain/indextune/lspi/lspi_tuner.h"
#include "brain/indextune/lspi/lstdq.h"
#include "brain/indextune/lspi/rlse.h"
#include "brain/util/eigen_util.h"
#include "common/harness.h"
#include "brain/testing_index_selection_util.h"
#include "brain/what_if_index.h"

namespace peloton {
namespace test {

//===--------------------------------------------------------------------===//
// Tensorflow Tests
//===--------------------------------------------------------------------===//

class LSPITests : public PelotonTest {};

/**
 * @brief: Attempt to fit y = m*x with Recursive Least Squares
 */
TEST_F(LSPITests, RLSETest) {
  //
  int NUM_SAMPLES = 500;
  int LOG_INTERVAL = 100;
  int m = 3;
  vector_eig data_in = vector_eig::LinSpaced(NUM_SAMPLES, 0, NUM_SAMPLES - 1);
  vector_eig data_out = data_in.array() * m;
  vector_eig loss_vector = vector_eig::Zero(LOG_INTERVAL);
  float prev_loss = std::numeric_limits<float>::max();
  auto model = brain::RLSEModel(1);
  for (int i = 0; i < NUM_SAMPLES; i++) {
    vector_eig feat_vec = data_in.segment(i, 1);
    double value_true = data_out(i);
    double value_pred = model.Predict(feat_vec);
    double loss = fabs(value_pred - value_true);
    loss_vector(i % LOG_INTERVAL) = loss;
    model.Update(feat_vec, value_true);
    if ((i + 1) % LOG_INTERVAL == 0) {
      float curr_loss = loss_vector.array().mean();
      LOG_DEBUG("Loss at %d: %.5f", i + 1, curr_loss);
      EXPECT_LE(curr_loss, prev_loss);
      prev_loss = curr_loss;
    }
  }
}

/**
 * @brief: The suite of simple tuning tests uses the
 * `testing_index_selection_util` to build a cyclic query workload
 * and observe improvement in cost metrics over time.
 * As a sanity check, at every CATALOG_SYNC_INTERVAL,
 * we perform a hard check that the average cost metric
 * in this interval has been lower than previous upto a threshold.
 *
 * In addition these microworkloads serve as a useful way to analyze the behavior
 * of the tuner.
 *
 * Observations:
 * W1/2: Seems to pick correct set of indexes as it sees corresponding queries.
 * Its able to pick sc indexes when it sees queries using 1 column only and mc
 * indexes when it sees queries using multiple columns
 */

/**
 * @brief: Tuning test applied to the cyclic workload - SingleTableTwoColW1
 */
TEST_F(LSPITests, TuneTestTwoColTable1) {

  std::string database_name = DEFAULT_DB_NAME;
  size_t MAX_INDEX_SIZE = 3;
  int CATALOG_SYNC_INTERVAL = 2;
  // This threshold depends on #rows in the tables
  double MIN_COST_THRESH = 0.04;
  int TBL_ROWS = 100;

  index_selection::TestingIndexSelectionUtil testing_util(database_name);

  std::set<oid_t> ori_table_oids;
  brain::CompressedIndexConfigUtil::GetIgnoreTables(database_name,
                                                    ori_table_oids);

  auto config = testing_util.GetCyclicWorkload({index_selection::QueryStringsWorkloadType::SingleTableTwoColW1}, 2);
  auto table_schemas = config.first;
  auto query_strings = config.second;

  // Create all the required tables for this workloads.
  for (auto &table_schema : table_schemas) {
    testing_util.CreateTable(table_schema);
    testing_util.InsertIntoTable(table_schema, TBL_ROWS);
  }

  brain::LSPIIndexTuner index_tuner(database_name, ori_table_oids, false,
                                    MAX_INDEX_SIZE);



  std::vector<double> batch_costs;
  std::vector<std::string> batch_queries;
  double prev_cost = DBL_MAX;
  vector_eig cost_vector = vector_eig::Zero(CATALOG_SYNC_INTERVAL);

  for (size_t i = 1; i <= query_strings.size(); i++) {
    auto query = query_strings[i - 1];

    auto index_config = brain::CompressedIndexConfigUtil::ToIndexConfiguration(
        *index_tuner.GetConfigContainer());

    // Measure the What-If Index cost
    auto cost = testing_util.WhatIfIndexCost(query, index_config, database_name);

    LOG_DEBUG("Iter %zu", i);
    LOG_DEBUG("query: %s", query.c_str());
    LOG_DEBUG("index config(compressed): %s", index_tuner.GetConfigContainer()->ToString().c_str());
    LOG_DEBUG("cost: %f", cost);

    batch_queries.push_back(query);
    batch_costs.push_back(cost);
    cost_vector[(i - 1) % CATALOG_SYNC_INTERVAL] = cost;

    // Perform tuning
    if (i % CATALOG_SYNC_INTERVAL == 0) {
      LOG_DEBUG("Tuning...");
      index_tuner.Tune(batch_queries, batch_costs);
      batch_queries.clear();
      batch_costs.clear();
      double mean_cost = cost_vector.array().mean();
      LOG_DEBUG("Iter: %zu, Avg Cost: %f", i, mean_cost);
      EXPECT_LE(mean_cost, prev_cost);
      prev_cost = std::max<double>(mean_cost, MIN_COST_THRESH);
    }
  }
}

/**
 * @brief: Tuning test applied to the cyclic workload - SingleTableTwoColW2
 */
TEST_F(LSPITests, TuneTestTwoColTable2) {

  std::string database_name = DEFAULT_DB_NAME;
  size_t MAX_INDEX_SIZE = 3;
  int CATALOG_SYNC_INTERVAL = 2;
  // This threshold depends on #rows in the tables
  // Tuning it a little high for now to observe algorithm behavior
  double MIN_COST_THRESH = 0.05;
  int TBL_ROWS = 100;

  index_selection::TestingIndexSelectionUtil testing_util(database_name);

  std::set<oid_t> ori_table_oids;
  brain::CompressedIndexConfigUtil::GetIgnoreTables(database_name,
                                                    ori_table_oids);

  auto config = testing_util.GetCyclicWorkload({index_selection::QueryStringsWorkloadType::SingleTableTwoColW2}, 2);
  auto table_schemas = config.first;
  auto query_strings = config.second;

  // Create all the required tables for this workloads.
  for (auto &table_schema : table_schemas) {
    testing_util.CreateTable(table_schema);
    testing_util.InsertIntoTable(table_schema, TBL_ROWS);
  }

  brain::LSPIIndexTuner index_tuner(database_name, ori_table_oids, false,
                                    MAX_INDEX_SIZE);



  std::vector<double> batch_costs;
  std::vector<std::string> batch_queries;
  double prev_cost = DBL_MAX;
  vector_eig cost_vector = vector_eig::Zero(CATALOG_SYNC_INTERVAL);

  for (size_t i = 1; i <= query_strings.size(); i++) {
    auto query = query_strings[i - 1];

    auto index_config = brain::CompressedIndexConfigUtil::ToIndexConfiguration(
        *index_tuner.GetConfigContainer());

    // Measure the What-If Index cost
    auto cost = testing_util.WhatIfIndexCost(query, index_config, database_name);

    LOG_DEBUG("Iter %zu", i);
    LOG_DEBUG("query: %s", query.c_str());
    LOG_DEBUG("index config(compressed): %s", index_tuner.GetConfigContainer()->ToString().c_str());
    LOG_DEBUG("cost: %f", cost);

    batch_queries.push_back(query);
    batch_costs.push_back(cost);
    cost_vector[(i - 1) % CATALOG_SYNC_INTERVAL] = cost;

    // Perform tuning
    if (i % CATALOG_SYNC_INTERVAL == 0) {
      LOG_DEBUG("Tuning...");
      index_tuner.Tune(batch_queries, batch_costs);
      batch_queries.clear();
      batch_costs.clear();
      double mean_cost = cost_vector.array().mean();
      LOG_DEBUG("Iter: %zu, Avg Cost: %f", i, mean_cost);
      EXPECT_LE(mean_cost, prev_cost);
      prev_cost = std::max<double>(mean_cost, MIN_COST_THRESH);
    }
  }
}

/**
 * @brief: Tuning test applied to the cyclic workload - SingleTableFiveColW
 */
TEST_F(LSPITests, TuneTestFiveColTable) {

  std::string database_name = DEFAULT_DB_NAME;
  size_t MAX_INDEX_SIZE = 3;
  int CATALOG_SYNC_INTERVAL = 2;
  // This threshold depends on #rows in the tables
  // Tuning it a little high for now to observe algorithm behavior
  double MIN_COST_THRESH = 0.05;
  int TBL_ROWS = 100;

  index_selection::TestingIndexSelectionUtil testing_util(database_name);

  std::set<oid_t> ori_table_oids;
  brain::CompressedIndexConfigUtil::GetIgnoreTables(database_name,
                                                    ori_table_oids);

  auto config = testing_util.GetCyclicWorkload({index_selection::QueryStringsWorkloadType::SingleTableFiveColW}, 2);
  auto table_schemas = config.first;
  auto query_strings = config.second;

  // Create all the required tables for this workloads.
  for (auto &table_schema : table_schemas) {
    testing_util.CreateTable(table_schema);
    testing_util.InsertIntoTable(table_schema, TBL_ROWS);
  }

  brain::LSPIIndexTuner index_tuner(database_name, ori_table_oids, false,
                                    MAX_INDEX_SIZE);



  std::vector<double> batch_costs;
  std::vector<std::string> batch_queries;
  double prev_cost = DBL_MAX;
  vector_eig cost_vector = vector_eig::Zero(CATALOG_SYNC_INTERVAL);

  for (size_t i = 1; i <= query_strings.size(); i++) {
    auto query = query_strings[i - 1];

    auto index_config = brain::CompressedIndexConfigUtil::ToIndexConfiguration(
        *index_tuner.GetConfigContainer());

    // Measure the What-If Index cost
    auto cost = testing_util.WhatIfIndexCost(query, index_config, database_name);

    LOG_DEBUG("Iter %zu", i);
    LOG_DEBUG("query: %s", query.c_str());
    LOG_DEBUG("index config(compressed): %s", index_tuner.GetConfigContainer()->ToString().c_str());
    LOG_DEBUG("cost: %f", cost);

    batch_queries.push_back(query);
    batch_costs.push_back(cost);
    cost_vector[(i - 1) % CATALOG_SYNC_INTERVAL] = cost;

    // Perform tuning
    if (i % CATALOG_SYNC_INTERVAL == 0) {
      LOG_DEBUG("Tuning...");
      index_tuner.Tune(batch_queries, batch_costs);
      batch_queries.clear();
      batch_costs.clear();
      double mean_cost = cost_vector.array().mean();
      LOG_DEBUG("Iter: %zu, Avg Cost: %f", i, mean_cost);
      EXPECT_LE(mean_cost, prev_cost);
      prev_cost = std::max<double>(mean_cost, MIN_COST_THRESH);
    }
  }
}

/**
 * @brief: Tuning test applied to the cyclic workload - MultiColMultiTable
 */
 // TODO(wiechenl): Segfault inside `AddCandidates`
//TEST_F(LSPITests, TuneTestMultiColMultiTable) {
//
//  std::string database_name = DEFAULT_DB_NAME;
//  size_t MAX_INDEX_SIZE = 3;
//  int CATALOG_SYNC_INTERVAL = 5;
//  // This threshold depends on #rows in the tables
//  // Tuning it a little high for now to observe algorithm behavior
//  double MIN_COST_THRESH = 100.0;
//  int TBL_ROWS = 100;
//
//  index_selection::TestingIndexSelectionUtil testing_util(database_name);
//
//  std::set<oid_t> ori_table_oids;
//  brain::CompressedIndexConfigUtil::GetIgnoreTables(database_name,
//                                                    ori_table_oids);
//
//  auto config = testing_util.GetCyclicWorkload({index_selection::QueryStringsWorkloadType::MultiTableMultiColW}, 2);
//  auto table_schemas = config.first;
//  auto query_strings = config.second;
//
//  // Create all the required tables for this workloads.
//  for (auto &table_schema : table_schemas) {
//    testing_util.CreateTable(table_schema);
//    testing_util.InsertIntoTable(table_schema, TBL_ROWS);
//  }
//
//  brain::LSPIIndexTuner index_tuner(database_name, ori_table_oids, false,
//                                    MAX_INDEX_SIZE);
//
//
//
//  std::vector<double> batch_costs;
//  std::vector<std::string> batch_queries;
//  double prev_cost = DBL_MAX;
//  vector_eig cost_vector = vector_eig::Zero(CATALOG_SYNC_INTERVAL);
//
//  for (size_t i = 1; i <= query_strings.size(); i++) {
//    auto query = query_strings[i - 1];
//
//    auto index_config = brain::CompressedIndexConfigUtil::ToIndexConfiguration(
//        *index_tuner.GetConfigContainer());
//
//    // Measure the What-If Index cost
//    auto cost = testing_util.WhatIfIndexCost(query, index_config, database_name);
//
//    LOG_DEBUG("Iter %zu", i);
//    LOG_DEBUG("query: %s", query.c_str());
//    LOG_DEBUG("index config(compressed): %s", index_tuner.GetConfigContainer()->ToString().c_str());
//    LOG_DEBUG("cost: %f", cost);
//
//    batch_queries.push_back(query);
//    batch_costs.push_back(cost);
//    cost_vector[(i - 1) % CATALOG_SYNC_INTERVAL] = cost;
//
//    // Perform tuning
//    if (i % CATALOG_SYNC_INTERVAL == 0) {
//      LOG_DEBUG("Tuning...");
//      index_tuner.Tune(batch_queries, batch_costs);
//      batch_queries.clear();
//      batch_costs.clear();
//      double mean_cost = cost_vector.array().mean();
//      LOG_DEBUG("Iter: %zu, Avg Cost: %f", i, mean_cost);
//      EXPECT_LE(mean_cost, prev_cost);
//      prev_cost = std::max<double>(mean_cost, MIN_COST_THRESH);
//    }
//  }
//}

}  // namespace test
}  // namespace peloton
