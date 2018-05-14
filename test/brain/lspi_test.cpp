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
#include "common/timer.h"

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
 * We also perform a run of the workload with and without the tuning enabled
 * and perform a hard check that the overall cost should be lower with tuning.
 *
 * In addition these microworkloads serve as a useful way to analyze the
 * behavior
 * of the tuner.
 * TODO(saatviks): Add analysis and observations here?
 */

// TEST_F(LSPITests, BenchmarkTest) {
//  // ** Initialization ** //
//  std::string database_name = DEFAULT_DB_NAME;
//  size_t MAX_INDEX_SIZE = 3;
//  int CATALOG_SYNC_INTERVAL = 2;
//  // This threshold depends on #rows in the tables
//  double MIN_COST_THRESH = 1000.0;
//  size_t MAX_NUMINDEXES_WHATIF = 100;
//  bool DRY_RUN_MODE = true;
//  int TBL_ROWS = 1000;
//  auto timer = Timer<std::ratio<1>>();
//  std::vector<double> batch_costs;
//  std::vector<std::string> batch_queries;
//
//  index_selection::TestingIndexSelectionUtil testing_util(database_name);
//
//  std::set<oid_t> ignore_table_oids;
//  brain::CompressedIndexConfigUtil::GetIgnoreTables(database_name,
//                                                    ignore_table_oids);
//
//  auto config = testing_util.GetCyclicWorkload(
//      {index_selection::QueryStringsWorkloadType::SingleTableTwoColW2}, 2);
//  auto table_schemas = config.first;
//  auto query_strings = config.second;
//
//  // Create all the required tables for this workloads.
//  for (auto &table_schema : table_schemas) {
//    testing_util.CreateTable(table_schema);
//    testing_util.InsertIntoTable(table_schema, TBL_ROWS);
//  }
//
//  // ** No Tuning ** //
//  brain::LSPIIndexTuner index_tuner(database_name, ignore_table_oids, false,
//                                    MAX_INDEX_SIZE, DRY_RUN_MODE);
//  vector_eig query_costs_notuning = vector_eig::Zero(query_strings.size());
//  vector_eig search_time_notuning = vector_eig::Zero(query_strings.size());
//
//  LOG_DEBUG("Run without Tuning:");
//  for (size_t i = 1; i <= query_strings.size(); i++) {
//    auto query = query_strings[i - 1];
//
//    auto index_config =
//    brain::CompressedIndexConfigUtil::ToIndexConfiguration(
//        *index_tuner.GetConfigContainer());
//
//    // Measure the What-If Index cost
//    auto cost =
//        testing_util.WhatIfIndexCost(query, index_config, database_name);
//
//    // No tuning performed here
//    query_costs_notuning[i - 1] = cost;
//  }
//
//  // ** Exhaustive What-If Tuning Setup(Closest to Ideal) ** //
//
//  size_t max_index_cols = MAX_INDEX_SIZE;         // multi-column index limit
//  size_t enumeration_threshold = MAX_INDEX_SIZE;  // naive enumeration
//  threshold
//  size_t num_indexes =
//      MAX_NUMINDEXES_WHATIF;  // top num_indexes will be returned.
//
//  brain::IndexSelectionKnobs knobs = {max_index_cols, enumeration_threshold,
//                                      num_indexes};
//  brain::IndexConfiguration best_config;
//  vector_eig query_costs_exhaustivewhatif =
//      vector_eig::Zero(query_strings.size());
//  vector_eig search_time_exhaustivewhatif =
//      vector_eig::Zero(query_strings.size());
//  auto &txn_manager = concurrency::TransactionManagerFactory::GetInstance();
//
//  // Cleanup
//  batch_queries.clear();
//
//  brain::Workload w(database_name);
//  auto txn = txn_manager.BeginTransaction();
//  brain::IndexSelection is = {w, knobs, txn};
//  is.GetBestIndexes(best_config);
//  txn_manager.CommitTransaction(txn);
//  LOG_DEBUG("Index: %s", best_config.ToString().c_str());
//  LOG_DEBUG("Run with Exhaustive What-If Search:");
//  for (size_t i = 1; i <= query_strings.size(); i++) {
//    auto query = query_strings[i - 1];
//
//    // Measure the What-If Index cost
//
//    batch_queries.push_back(query);
//    double cost =
//        testing_util.WhatIfIndexCost(query, best_config, database_name);
//    query_costs_exhaustivewhatif[i - 1] = cost;
//
//    // Perform tuning
//    if (i % CATALOG_SYNC_INTERVAL == 0) {
//      LOG_DEBUG("Exhaustive What-If Tuning...");
//      timer.Reset();
//      timer.Start();
//      txn = txn_manager.BeginTransaction();
//      brain::Workload workload(batch_queries, database_name, txn);
//      is = {workload, knobs, txn};
//      is.GetBestIndexes(best_config);
//      txn_manager.CommitTransaction(txn);
//      timer.Stop();
//      search_time_exhaustivewhatif[i-1] = timer.GetDuration();
//      batch_queries.clear();
//      batch_costs.clear();
//    }
//  }
//  batch_costs.clear();
//  batch_queries.clear();
//
//  // ** Exhaustive What-If Tuning Setup without dropping indexes (Closest to
//  // Ideal) ** //
//
//  brain::IndexConfiguration best_config_nodropping;
//  brain::IndexConfiguration prev_config_nodropping;
//  vector_eig query_costs_exhaustivewhatif_nodropping =
//      vector_eig::Zero(query_strings.size());
//  vector_eig search_time_exhaustivewhatif_nodropping =
//      vector_eig::Zero(query_strings.size());
//
//  brain::Workload w_nodropping(database_name);
//  txn = txn_manager.BeginTransaction();
//  brain::IndexSelection is_nodropping = {w_nodropping, knobs, txn};
//  is_nodropping.GetBestIndexes(best_config_nodropping);
//  txn_manager.CommitTransaction(txn);
//
//  LOG_DEBUG("Index: %s", best_config_nodropping.ToString().c_str());
//  LOG_DEBUG("Run with Exhaustive What-If Search without dropping indexes:");
//  for (size_t i = 1; i <= query_strings.size(); i++) {
//    auto query = query_strings[i - 1];
//
//    // Measure the What-If Index cost
//
//    batch_queries.push_back(query);
//    double cost = testing_util.WhatIfIndexCost(query, best_config_nodropping,
//                                               database_name);
//    query_costs_exhaustivewhatif_nodropping[i - 1] = cost;
//
//    // Perform tuning
//    if (i % CATALOG_SYNC_INTERVAL == 0) {
//      LOG_DEBUG("Exhaustive What-If Tuning...");
//      txn = txn_manager.BeginTransaction();
//      prev_config_nodropping.Set(best_config_nodropping);
//      timer.Reset();
//      timer.Start();
//      brain::Workload workload(batch_queries, database_name, txn);
//      is_nodropping = {workload, knobs, txn};
//      is_nodropping.GetBestIndexes(best_config_nodropping);
//      timer.Stop();
//      best_config_nodropping.Merge(prev_config_nodropping);
//      txn_manager.CommitTransaction(txn);
//
//      search_time_exhaustivewhatif_nodropping[i - 1] = timer.GetDuration();
//      batch_queries.clear();
//      batch_costs.clear();
//    }
//  }
//  batch_costs.clear();
//  batch_queries.clear();
//
//  // ** LSPI Tuning Setup(Exhaustive: with max add candidate search) ** //
//  brain::LSPIIndexTuner index_tuner_exhaustive(
//      database_name, ignore_table_oids, false, MAX_INDEX_SIZE, DRY_RUN_MODE);
//  double prev_cost = DBL_MAX;
//  vector_eig cost_vector_lspiexhaustive =
//      vector_eig::Zero(CATALOG_SYNC_INTERVAL);
//  vector_eig query_costs_lspiexhaustive =
//      vector_eig::Zero(query_strings.size());
//  vector_eig search_time_lspiexhaustive =
//      vector_eig::Zero(query_strings.size());
//  vector_eig numconfigadds_lspiexhaustive =
//      vector_eig::Zero(query_strings.size());
//  vector_eig numconfigdrops_lspiexhaustive =
//      vector_eig::Zero(query_strings.size());
//
//
//  LOG_DEBUG("Run with LSPI(Exhaustive) Tuning:");
//  for (size_t i = 1; i <= query_strings.size(); i++) {
//    auto query = query_strings[i - 1];
//
//    auto index_config =
//    brain::CompressedIndexConfigUtil::ToIndexConfiguration(
//        *index_tuner_exhaustive.GetConfigContainer());
//
//    // Measure the What-If Index cost
//    auto cost =
//        testing_util.WhatIfIndexCost(query, index_config, database_name);
//
//    batch_queries.push_back(query);
//    batch_costs.push_back(cost);
//    query_costs_lspiexhaustive[i - 1] = cost;
//    cost_vector_lspiexhaustive[(i - 1) % CATALOG_SYNC_INTERVAL] = cost;
//
//    // Perform tuning
//    if (i % CATALOG_SYNC_INTERVAL == 0) {
//      const boost::dynamic_bitset<> prev_config(
//          *index_tuner_exhaustive.GetConfigContainer()
//               ->GetCurrentIndexConfig());
//      LOG_DEBUG("COREIL Tuning...");
//      timer.Reset();
//      timer.Start();
//      index_tuner_exhaustive.Tune(batch_queries, batch_costs);
//      timer.Stop();
//      search_time_lspiexhaustive[i - 1] = timer.GetDuration();
//      const boost::dynamic_bitset<> cur_config(
//          *index_tuner_exhaustive.GetConfigContainer()
//               ->GetCurrentIndexConfig());
//      const auto drop_bitset = prev_config - cur_config;
//      const auto add_bitset = cur_config - prev_config;
//      numconfigadds_lspiexhaustive[i - 1] = add_bitset.count();
//      numconfigdrops_lspiexhaustive[i - 1] = drop_bitset.count();
//      LOG_DEBUG("#Dropped Indexes: %lu, #Added Indexes: %lu",
//                drop_bitset.count(), add_bitset.count());
//
//      batch_queries.clear();
//      batch_costs.clear();
//      double mean_cost = cost_vector_lspiexhaustive.array().mean();
//      LOG_DEBUG("Iter: %zu, Avg Cost: %f", i, mean_cost);
//      EXPECT_LE(mean_cost, prev_cost);
//      prev_cost = std::max<double>(mean_cost, MIN_COST_THRESH);
//    }
//  }
//  batch_costs.clear();
//  batch_queries.clear();
//
//  // ** LSPI Tuning Setup(Non-Exhaustive: with only single-column indexes) **
//  //
//  brain::LSPIIndexTuner index_tuner_nonexhaustive(
//      database_name, ignore_table_oids, true, MAX_INDEX_SIZE, DRY_RUN_MODE);
//  prev_cost = DBL_MAX;
//  vector_eig cost_vector_lspinonexhaustive =
//      vector_eig::Zero(CATALOG_SYNC_INTERVAL);
//  vector_eig query_costs_lspinonexhaustive =
//      vector_eig::Zero(query_strings.size());
//  vector_eig search_time_lspinonexhaustive =
//      vector_eig::Zero(query_strings.size());
//  vector_eig numconfigadds_lspinonexhaustive =
//      vector_eig::Zero(query_strings.size());
//  vector_eig numconfigdrops_lspinonexhaustive =
//      vector_eig::Zero(query_strings.size());
//
//  LOG_DEBUG("Run with LSPI(Non-Exhaustive) Tuning:");
//  for (size_t i = 1; i <= query_strings.size(); i++) {
//    auto query = query_strings[i - 1];
//
//    auto index_config =
//    brain::CompressedIndexConfigUtil::ToIndexConfiguration(
//        *index_tuner_nonexhaustive.GetConfigContainer());
//
//    // Measure the What-If Index cost
//    auto cost =
//        testing_util.WhatIfIndexCost(query, index_config, database_name);
//
//    batch_queries.push_back(query);
//    batch_costs.push_back(cost);
//    query_costs_lspinonexhaustive[i - 1] = cost;
//    cost_vector_lspinonexhaustive[(i - 1) % CATALOG_SYNC_INTERVAL] = cost;
//
//    // Perform tuning
//    if (i % CATALOG_SYNC_INTERVAL == 0) {
//      const boost::dynamic_bitset<> prev_config(
//          *index_tuner_nonexhaustive.GetConfigContainer()
//               ->GetCurrentIndexConfig());
//      LOG_DEBUG("LSPI Tuning(Non-Exhaustive)...");
//      timer.Reset();
//      timer.Start();
//      index_tuner_nonexhaustive.Tune(batch_queries, batch_costs);
//      timer.Stop();
//      search_time_lspinonexhaustive[i - 1] = timer.GetDuration();
//      const boost::dynamic_bitset<> cur_config(
//          *index_tuner_nonexhaustive.GetConfigContainer()
//               ->GetCurrentIndexConfig());
//      const auto drop_bitset = prev_config - cur_config;
//      const auto add_bitset = cur_config - prev_config;
//      numconfigadds_lspinonexhaustive[i-1] = add_bitset.count();
//      numconfigdrops_lspinonexhaustive[i-1] = drop_bitset.count();
//      LOG_DEBUG("#Dropped Indexes: %lu, #Added Indexes: %lu",
//                drop_bitset.count(), add_bitset.count());
//      batch_queries.clear();
//      batch_costs.clear();
//      double mean_cost = cost_vector_lspinonexhaustive.array().mean();
//      LOG_DEBUG("Iter: %zu, Avg Cost: %f", i, mean_cost);
//      EXPECT_LE(mean_cost, prev_cost);
//      prev_cost = std::max<double>(mean_cost, MIN_COST_THRESH);
//    }
//  }
//
//  // For analysis
//  // TODO: This is tooooooooooooo overloaded!!
//  LOG_DEBUG("Overall Cost Trend for SingleTableTwoColW1 Workload:");
//  for (size_t i = 0; i < query_strings.size(); i++) {
//    LOG_DEBUG(
//        "%zu\t"
//        "No Tuning Cost: %f\tLSPI(Exhaustive) Tuning Cost: "
//        "%f\tWhatIf(Exhaustive) Tuning Cost: %f\tLSPI(Non-Exhaustive) Tuning "
//        "Cost: %f\tWhatIf(Exhaustive No-Dropping) Tuning Cost: %f\t"
//        "No Tuning Time: %f\tLSPI(Exhaustive) Tuning Time: "
//        "%f\tWhatIf(Exhaustive) Tuning Time: %f\tLSPI(Non-Exhaustive) Tuning "
//        "Time: %f\tWhatIf(Exhaustive No-Dropping) Tuning Time: %f\t"
//        "LSPI(Exhaustive) Adds: %f\tLSPI(Exhaustive) Drops: %f\t"
//        "LSPI(Non-Exhaustive) Adds: %f\tLSPI(Non-Exhaustive) Drops: %f\t"
//        "%s",
//        i, query_costs_notuning[i], query_costs_lspiexhaustive[i],
//        query_costs_exhaustivewhatif[i], query_costs_lspinonexhaustive[i],
//        query_costs_exhaustivewhatif_nodropping[i], search_time_notuning[i],
//        search_time_lspiexhaustive[i], search_time_exhaustivewhatif[i],
//        search_time_lspinonexhaustive[i],
//        search_time_exhaustivewhatif_nodropping[i],
//        numconfigadds_lspiexhaustive[i], numconfigdrops_lspiexhaustive[i],
//        numconfigadds_lspinonexhaustive[i],
//        numconfigdrops_lspinonexhaustive[i],
//        query_strings[i].c_str());
//  }
//  float tuning_overall_cost_lspiexhaustive =
//  query_costs_lspiexhaustive.array().sum();
//  float tuning_overall_cost_lspinonexhaustive =
//      query_costs_lspinonexhaustive.array().sum();
//  float notuning_overall_cost = query_costs_notuning.array().sum();
//  float tuning_overall_cost_exhaustivewhatif =
//  query_costs_exhaustivewhatif.array().sum();
//  float tuning_overall_cost_exhaustivewhatif_nodropping =
//  query_costs_exhaustivewhatif_nodropping.array().sum();
//  LOG_DEBUG(
//      "No Tuning Cost Total: %f, LSPI(Exhaustive) Tuning Cost Total: %f, "
//      "WhatIf(Exhaustive) Tuning Cost: %f, LSPI(Non-Exhaustive) Tuning Cost
//      Total: %f,"
//      "WhatIf(Exhaustive No-Dropping) Tuning Cost: %f",
//      notuning_overall_cost, tuning_overall_cost_lspiexhaustive,
//      tuning_overall_cost_exhaustivewhatif,
//      tuning_overall_cost_lspinonexhaustive,
//      tuning_overall_cost_exhaustivewhatif_nodropping);
//  EXPECT_LT(tuning_overall_cost_lspiexhaustive, notuning_overall_cost);
//  EXPECT_LT(tuning_overall_cost_lspinonexhaustive, notuning_overall_cost);
//}

/**
* @brief: The suite of simple tuning tests uses the
* `testing_index_selection_util` to build a cyclic query workload
* and observe improvement in cost metrics over time.
* As a sanity check, at every CATALOG_SYNC_INTERVAL,
* we perform a hard check that the average cost metric
* in this interval has been lower than previous upto a threshold.
*
* We also perform a run of the workload with and without the tuning enabled
* and perform a hard check that the overall cost should be lower with tuning.
*
* In addition these microworkloads serve as a useful way to analyze the behavior
* of the tuner.
* TODO(saatviks): Add analysis and observations here?
*/

TEST_F(LSPITests, TuneTestTwoColTable1) {
  std::string database_name = DEFAULT_DB_NAME;
  size_t MAX_INDEX_SIZE = 3;
  int CATALOG_SYNC_INTERVAL = 2;
  // This threshold depends on #rows in the tables
  double MIN_COST_THRESH = 0.04;
  int TBL_ROWS = 100;

  index_selection::TestingIndexSelectionUtil testing_util(database_name);

  std::set<oid_t> ignore_table_oids;
  brain::CompressedIndexConfigUtil::GetIgnoreTables(database_name,
                                                    ignore_table_oids);

  auto config = testing_util.GetCyclicWorkload(
      {index_selection::QueryStringsWorkloadType::SingleTableTwoColW1}, 2);
  auto table_schemas = config.first;
  auto query_strings = config.second;

  // Create all the required tables for this workloads.
  for (auto &table_schema : table_schemas) {
    testing_util.CreateTable(table_schema);
    testing_util.InsertIntoTable(table_schema, TBL_ROWS);
  }

  brain::LSPIIndexTuner index_tuner(database_name, ignore_table_oids, false,
                                    MAX_INDEX_SIZE);
  vector_eig query_costs_no_tuning = vector_eig::Zero(query_strings.size());

  LOG_DEBUG("Run without Tuning:");
  for (size_t i = 1; i <= query_strings.size(); i++) {
    auto query = query_strings[i - 1];

    auto index_config = brain::CompressedIndexConfigUtil::ToIndexConfiguration(
        *index_tuner.GetConfigContainer());

    // Measure the What-If Index cost
    auto cost =
        testing_util.WhatIfIndexCost(query, index_config, database_name);

    // No tuning performed here
    query_costs_no_tuning[i - 1] = cost;
  }

  std::vector<double> batch_costs;
  std::vector<std::string> batch_queries;
  double prev_cost = DBL_MAX;
  vector_eig cost_vector = vector_eig::Zero(CATALOG_SYNC_INTERVAL);
  vector_eig query_costs_tuning = vector_eig::Zero(query_strings.size());

  LOG_DEBUG("Run with Tuning:");
  for (size_t i = 1; i <= query_strings.size(); i++) {
    auto query = query_strings[i - 1];

    auto index_config = brain::CompressedIndexConfigUtil::ToIndexConfiguration(
        *index_tuner.GetConfigContainer());

    // Measure the What-If Index cost
    auto cost =
        testing_util.WhatIfIndexCost(query, index_config, database_name);

    batch_queries.push_back(query);
    batch_costs.push_back(cost);
    query_costs_tuning[i - 1] = cost;
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

  // For analysis
  LOG_DEBUG("Overall Cost Trend for SingleTableTwoColW1 Workload:");
  for (size_t i = 0; i < query_strings.size(); i++) {
    LOG_DEBUG("%zu\tWithout Tuning: %f\tWith Tuning: %f\t%s", i,
              query_costs_no_tuning[i], query_costs_tuning[i],
              query_strings[i].c_str());
  }
  float tuning_overall_cost = query_costs_tuning.array().sum();
  float notuning_overall_cost = query_costs_no_tuning.array().sum();
  LOG_DEBUG("With Tuning: %f, Without Tuning: %f", tuning_overall_cost,
            notuning_overall_cost);
  EXPECT_LT(tuning_overall_cost, notuning_overall_cost);
}

TEST_F(LSPITests, TuneTestTwoColTable2) {
  std::string database_name = DEFAULT_DB_NAME;
  size_t MAX_INDEX_SIZE = 3;
  int CATALOG_SYNC_INTERVAL = 2;
  // This threshold depends on #rows in the tables
  double MIN_COST_THRESH = 0.05;
  int TBL_ROWS = 100;

  index_selection::TestingIndexSelectionUtil testing_util(database_name);

  std::set<oid_t> ignore_table_oids;
  brain::CompressedIndexConfigUtil::GetIgnoreTables(database_name,
                                                    ignore_table_oids);

  auto config = testing_util.GetCyclicWorkload(
      {index_selection::QueryStringsWorkloadType::SingleTableTwoColW2}, 2);
  auto table_schemas = config.first;
  auto query_strings = config.second;

  // Create all the required tables for this workloads.
  for (auto &table_schema : table_schemas) {
    testing_util.CreateTable(table_schema);
    testing_util.InsertIntoTable(table_schema, TBL_ROWS);
  }

  brain::LSPIIndexTuner index_tuner(database_name, ignore_table_oids, false,
                                    MAX_INDEX_SIZE);
  vector_eig query_costs_no_tuning = vector_eig::Zero(query_strings.size());

  LOG_DEBUG("Run without Tuning:");
  for (size_t i = 1; i <= query_strings.size(); i++) {
    auto query = query_strings[i - 1];

    auto index_config = brain::CompressedIndexConfigUtil::ToIndexConfiguration(
        *index_tuner.GetConfigContainer());

    // Measure the What-If Index cost
    auto cost =
        testing_util.WhatIfIndexCost(query, index_config, database_name);

    // No tuning performed here
    query_costs_no_tuning[i - 1] = cost;
  }

  std::vector<double> batch_costs;
  std::vector<std::string> batch_queries;
  double prev_cost = DBL_MAX;
  vector_eig cost_vector = vector_eig::Zero(CATALOG_SYNC_INTERVAL);
  vector_eig query_costs_tuning = vector_eig::Zero(query_strings.size());

  LOG_DEBUG("Run with Tuning:");
  for (size_t i = 1; i <= query_strings.size(); i++) {
    auto query = query_strings[i - 1];

    auto index_config = brain::CompressedIndexConfigUtil::ToIndexConfiguration(
        *index_tuner.GetConfigContainer());

    // Measure the What-If Index cost
    auto cost =
        testing_util.WhatIfIndexCost(query, index_config, database_name);

    batch_queries.push_back(query);
    batch_costs.push_back(cost);
    query_costs_tuning[i - 1] = cost;
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

  // For analysis
  LOG_DEBUG("Overall Cost Trend for SingleTableTwoColW2 Workload:");
  for (size_t i = 0; i < query_strings.size(); i++) {
    LOG_DEBUG("%zu\tWithout Tuning: %f\tWith Tuning: %f\t%s", i,
              query_costs_no_tuning[i], query_costs_tuning[i],
              query_strings[i].c_str());
  }
  float tuning_overall_cost = query_costs_tuning.array().sum();
  float notuning_overall_cost = query_costs_no_tuning.array().sum();
  LOG_DEBUG("With Tuning: %f, Without Tuning: %f", tuning_overall_cost,
            notuning_overall_cost);
  EXPECT_LT(tuning_overall_cost, notuning_overall_cost);
}

TEST_F(LSPITests, TuneTestThreeColTable) {
  std::string database_name = DEFAULT_DB_NAME;
  size_t MAX_INDEX_SIZE = 3;
  int CATALOG_SYNC_INTERVAL = 2;
  // This threshold depends on #rows in the tables
  double MIN_COST_THRESH = 0.05;
  int TBL_ROWS = 100;

  index_selection::TestingIndexSelectionUtil testing_util(database_name);

  std::set<oid_t> ignore_table_oids;
  brain::CompressedIndexConfigUtil::GetIgnoreTables(database_name,
                                                    ignore_table_oids);

  auto config = testing_util.GetCyclicWorkload(
      {index_selection::QueryStringsWorkloadType::SingleTableThreeColW}, 2);
  auto table_schemas = config.first;
  auto query_strings = config.second;

  // Create all the required tables for this workloads.
  for (auto &table_schema : table_schemas) {
    testing_util.CreateTable(table_schema);
    testing_util.InsertIntoTable(table_schema, TBL_ROWS);
  }

  brain::LSPIIndexTuner index_tuner(database_name, ignore_table_oids, false,
                                    MAX_INDEX_SIZE);
  vector_eig query_costs_no_tuning = vector_eig::Zero(query_strings.size());

  LOG_DEBUG("Run without Tuning:");
  for (size_t i = 1; i <= query_strings.size(); i++) {
    auto query = query_strings[i - 1];

    auto index_config = brain::CompressedIndexConfigUtil::ToIndexConfiguration(
        *index_tuner.GetConfigContainer());

    // Measure the What-If Index cost
    auto cost =
        testing_util.WhatIfIndexCost(query, index_config, database_name);

    // No tuning performed here
    query_costs_no_tuning[i - 1] = cost;
  }

  std::vector<double> batch_costs;
  std::vector<std::string> batch_queries;
  double prev_cost = DBL_MAX;
  vector_eig cost_vector = vector_eig::Zero(CATALOG_SYNC_INTERVAL);
  vector_eig query_costs_tuning = vector_eig::Zero(query_strings.size());

  LOG_DEBUG("Run with Tuning:");
  for (size_t i = 1; i <= query_strings.size(); i++) {
    auto query = query_strings[i - 1];

    auto index_config = brain::CompressedIndexConfigUtil::ToIndexConfiguration(
        *index_tuner.GetConfigContainer());

    // Measure the What-If Index cost
    auto cost =
        testing_util.WhatIfIndexCost(query, index_config, database_name);

    batch_queries.push_back(query);
    batch_costs.push_back(cost);
    query_costs_tuning[i - 1] = cost;
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

  // For analysis
  LOG_DEBUG("Overall Cost Trend for SingleTableThreeColW Workload:");
  for (size_t i = 0; i < query_strings.size(); i++) {
    LOG_DEBUG("%zu\tWithout Tuning: %f\tWith Tuning: %f\t%s", i,
              query_costs_no_tuning[i], query_costs_tuning[i],
              query_strings[i].c_str());
  }
  float tuning_overall_cost = query_costs_tuning.array().sum();
  float notuning_overall_cost = query_costs_no_tuning.array().sum();
  LOG_DEBUG("With Tuning: %f, Without Tuning: %f", tuning_overall_cost,
            notuning_overall_cost);
  EXPECT_LT(tuning_overall_cost, notuning_overall_cost);
}

TEST_F(LSPITests, TuneTestMultiColMultiTable) {
  std::string database_name = DEFAULT_DB_NAME;
  size_t MAX_INDEX_SIZE = 3;
  int CATALOG_SYNC_INTERVAL = 2;
  // This threshold depends on #rows in the tables
  double MIN_COST_THRESH = 100.0;
  int TBL_ROWS = 100;

  index_selection::TestingIndexSelectionUtil testing_util(database_name);

  std::set<oid_t> ignore_table_oids;
  brain::CompressedIndexConfigUtil::GetIgnoreTables(database_name,
                                                    ignore_table_oids);

  auto config = testing_util.GetCyclicWorkload(
      {index_selection::QueryStringsWorkloadType::MultiTableMultiColW}, 2);
  auto table_schemas = config.first;
  auto query_strings = config.second;

  // Create all the required tables for this workloads.
  for (auto &table_schema : table_schemas) {
    testing_util.CreateTable(table_schema);
    testing_util.InsertIntoTable(table_schema, TBL_ROWS);
  }

  brain::LSPIIndexTuner index_tuner(database_name, ignore_table_oids, false,
                                    MAX_INDEX_SIZE);
  vector_eig query_costs_no_tuning = vector_eig::Zero(query_strings.size());

  LOG_DEBUG("Run without Tuning:");
  for (size_t i = 1; i <= query_strings.size(); i++) {
    auto query = query_strings[i - 1];

    auto index_config = brain::CompressedIndexConfigUtil::ToIndexConfiguration(
        *index_tuner.GetConfigContainer());

    // Measure the What-If Index cost
    auto cost =
        testing_util.WhatIfIndexCost(query, index_config, database_name);

    // No tuning performed here
    query_costs_no_tuning[i - 1] = cost;
  }

  std::vector<double> batch_costs;
  std::vector<std::string> batch_queries;
  double prev_cost = DBL_MAX;
  vector_eig cost_vector = vector_eig::Zero(CATALOG_SYNC_INTERVAL);
  vector_eig query_costs_tuning = vector_eig::Zero(query_strings.size());

  LOG_DEBUG("Run with Tuning:");
  for (size_t i = 1; i <= query_strings.size(); i++) {
    auto query = query_strings[i - 1];

    auto index_config = brain::CompressedIndexConfigUtil::ToIndexConfiguration(
        *index_tuner.GetConfigContainer());

    // Measure the What-If Index cost
    auto cost =
        testing_util.WhatIfIndexCost(query, index_config, database_name);

    batch_queries.push_back(query);
    batch_costs.push_back(cost);
    query_costs_tuning[i - 1] = cost;
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

  // For analysis
  LOG_DEBUG("Overall Cost Trend for MultiTableMultiColW Workload:");
  for (size_t i = 0; i < query_strings.size(); i++) {
    LOG_DEBUG("%zu\tWithout Tuning: %f\tWith Tuning: %f\t%s", i,
              query_costs_no_tuning[i], query_costs_tuning[i],
              query_strings[i].c_str());
  }
  float tuning_overall_cost = query_costs_tuning.array().sum();
  float notuning_overall_cost = query_costs_no_tuning.array().sum();
  LOG_DEBUG("With Tuning: %f, Without Tuning: %f", tuning_overall_cost,
            notuning_overall_cost);
  EXPECT_LT(tuning_overall_cost, notuning_overall_cost);
}

}  // namespace test
}  // namespace peloton
