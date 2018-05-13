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
 * @brief: Simple tuning test -I
 * The suite of simple tuning tests run o
 */
TEST_F(LSPITests, SimpleTuneTest1) {

  std::string database_name = DEFAULT_DB_NAME;
  size_t max_index_size = 3;

  index_selection::TestingIndexSelectionUtil testing_util(database_name);

  std::set<oid_t> ori_table_oids;
  brain::CompressedIndexConfigUtil::GetIgnoreTables(database_name,
                                                    ori_table_oids);

  auto config = testing_util.GetQueryStringsWorkload(
      index_selection::QueryStringsWorkloadType::SingleTableTwoColW1);
  auto table_schemas = config.first;
  auto query_strings = config.second;

  // Create all the required tables for this workloads.
  for (auto &table_schema : table_schemas) {
    testing_util.CreateTable(table_schema);
  }

  brain::LSPIIndexTuner index_tuner(database_name, ori_table_oids, false,
                                    max_index_size);

  int CATALOG_SYNC_INTERVAL = 2;

  std::vector<double> batch_costs;
  std::vector<std::string> batch_queries;
  for (size_t i = 1; i <= query_strings.size(); i++) {
    auto query = query_strings[i - 1];

    auto index_config = brain::CompressedIndexConfigUtil::ToIndexConfiguration(
        *index_tuner.GetConfigContainer());

    // Measure the What-If Index cost
    auto cost = testing_util.WhatIfIndexCost(query, index_config, database_name);

    LOG_DEBUG("Iter %zu", i);
    LOG_DEBUG("query: %s", query.c_str());
    LOG_DEBUG("index config(compressed): %s", index_tuner.GetConfigContainer()->ToString().c_str());
    LOG_DEBUG("index config: %s", index_config.ToString().c_str());
    LOG_DEBUG("cost: %f", cost);

    batch_queries.push_back(query);
    batch_costs.push_back(cost);
    if (i % CATALOG_SYNC_INTERVAL == 0) {
      LOG_DEBUG("Tuning...");
      index_tuner.Tune(batch_queries, batch_costs);
      batch_queries.clear();
      batch_costs.clear();
    }
  }
}

}  // namespace test
}  // namespace peloton
