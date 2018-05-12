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
#include "brain/testing_index_suggestion_util.h"
#include "brain/what_if_index.h"

namespace peloton {
namespace test {

//===--------------------------------------------------------------------===//
// Tensorflow Tests
//===--------------------------------------------------------------------===//

class LSPITests : public PelotonTest {};

TEST_F(LSPITests, RLSETest) {
  // Attempt to fit y = m*x
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

TEST_F(LSPITests, TuneTest) {
  // Sanity test that all components are running
  // Need more ri
  std::string database_name = DEFAULT_DB_NAME;

  index_suggestion::TestingIndexSuggestionUtil testing_util(database_name);

  std::set<oid_t> ori_table_oids;
  brain::CompressedIndexConfigUtil::GetIgnoreTables(database_name,
                                                    ori_table_oids);

  auto config = testing_util.GetQueryStringsWorkload(
      index_suggestion::QueryStringsWorkloadType::A);
  auto table_schemas = config.first;
  auto query_strings = config.second;

  // Create all the required tables for this workloads.
  for (auto table_schema : table_schemas) {
    testing_util.CreateTable(table_schema);
  }

  brain::LSPIIndexTuner index_tuner(database_name, ori_table_oids);

  brain::IndexConfiguration index_config;

  int CATALOG_SYNC_INTERVAL = 2;

  std::vector<double> query_latencies;
  std::vector<std::string> query_strs;
  for (size_t i = 1; i <= query_strings.size(); i++) {
    auto query = query_strings[i - 1];

    std::unique_ptr<parser::SQLStatementList> stmt_list(
        parser::PostgresParser::ParseSQLString(query));

    auto &txn_manager = concurrency::TransactionManagerFactory::GetInstance();
    auto txn = txn_manager.BeginTransaction();

    std::unique_ptr<binder::BindNodeVisitor> binder(
        new binder::BindNodeVisitor(txn, database_name));

    // Get the first statement.
    auto sql_statement = std::shared_ptr<parser::SQLStatement>(
        stmt_list.get()->PassOutStatement(0));

    binder->BindNameToNode(sql_statement.get());
    txn_manager.CommitTransaction(txn);

    auto result = brain::WhatIfIndex::GetCostAndBestPlanTree(
        sql_statement, index_config, database_name);
    auto latency = result->cost;

    LOG_DEBUG("query: %s", query.c_str());
    LOG_DEBUG("latency: %f", latency);

    query_strs.push_back(query);
    query_latencies.push_back(latency);
    if (i % CATALOG_SYNC_INTERVAL == 0) {
      index_tuner.Tune(query_strs, query_latencies);
      query_strs.clear();
      query_latencies.clear();
    }
  }
}

}  // namespace test
}  // namespace peloton
