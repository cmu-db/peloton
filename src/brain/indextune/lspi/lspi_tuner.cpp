//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// lspi_tuner.cpp
//
// Identification: src/brain/indextune/lspi/lspi_tuner.cpp
//
// Copyright (c) 2015-2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "brain/indextune/lspi/lspi_tuner.h"

namespace peloton {
namespace brain {
LSPIIndexTuner::LSPIIndexTuner(
    const std::string &db_name, const std::set<oid_t> &ori_table_oids,
    size_t max_index_size, peloton::catalog::Catalog *catalog,
    peloton::concurrency::TransactionManager *txn_manager)
    : db_name_{db_name}, max_index_size_{max_index_size} {
  index_config_ = std::unique_ptr<CompressedIndexConfigContainer>(
      new CompressedIndexConfigContainer(db_name, ori_table_oids,
                                         max_index_size, catalog, txn_manager));
  size_t feat_len = index_config_->GetConfigurationCount();
  rlse_model_ = std::unique_ptr<RLSEModel>(new RLSEModel(2 * feat_len));
  lstdq_model_ = std::unique_ptr<LSTDQModel>(new LSTDQModel(feat_len));
  prev_config_vec = vector_eig::Zero(feat_len);
  // Empty config
  prev_config_vec[0] = 1.0;
}

void LSPIIndexTuner::Tune(const std::vector<std::string> &queries,
                          const std::vector<double> &query_latencies) {
  size_t num_queries = queries.size();
  std::vector<boost::dynamic_bitset<>> add_candidate_sets;
  std::vector<boost::dynamic_bitset<>> drop_candidate_sets;
  double latency_avg = 0.0;
  const boost::dynamic_bitset<> &curr_config_set =
      *index_config_->GetCurrentIndexConfig();
  // Be careful about not duplicating bitsets anywhere since they can
  // be potentially huge
  // Step 1: Populate the add and drop candidates per query
  boost::dynamic_bitset<> add_candidate_set, drop_candidate_set;
  for (size_t i = 0; i < num_queries; i++) {
    CompressedIndexConfigUtil::AddCandidates(*index_config_, queries[i],
                                             add_candidate_set);
    add_candidate_sets.push_back(std::move(add_candidate_set));
    CompressedIndexConfigUtil::DropCandidates(*index_config_, queries[i],
                                              drop_candidate_set);
    drop_candidate_sets.push_back(std::move(drop_candidate_set));
    latency_avg += query_latencies[i];
  }
  latency_avg /= num_queries;
  // Step 2: Update the RLSE model with the new samples
  for (size_t i = 0; i < num_queries; i++) {
    vector_eig query_config_feat;
    CompressedIndexConfigUtil::ConstructQueryConfigFeature(
        curr_config_set, add_candidate_sets[i], drop_candidate_sets[i],
        query_config_feat);
    rlse_model_->Update(query_config_feat, query_latencies[i]);
  }
  // Step 3: Iterate through the queries/latencies and obtain a new optimal
  // config
  auto optimal_config_set = boost::dynamic_bitset<>(curr_config_set);
  for (size_t i = 0; i < num_queries; i++) {
    FindOptimalConfig(query_latencies[i], curr_config_set,
                      add_candidate_sets[i], drop_candidate_sets[i],
                      optimal_config_set);
  }

  vector_eig new_config_vec;
  CompressedIndexConfigUtil::ConstructStateConfigFeature(optimal_config_set,
                                                         new_config_vec);
  // Step 4: Update the LSPI model based on current most optimal query config
  lstdq_model_->Update(prev_config_vec, new_config_vec, latency_avg);
  // Step 5: Adjust to the most optimal query config
  index_config_->AdjustIndexes(optimal_config_set);
}

void LSPIIndexTuner::FindOptimalConfig(
    double max_cost, const boost::dynamic_bitset<> &curr_config_set,
    const boost::dynamic_bitset<> &add_candidate_set,
    const boost::dynamic_bitset<> &drop_candidate_set,
    boost::dynamic_bitset<> &optimal_config_set) {
  // Iterate through add candidates
  size_t index_id_rec = add_candidate_set.find_first();
  vector_eig query_config_vec, config_vec;
  while (index_id_rec != boost::dynamic_bitset<>::npos) {
    if (!optimal_config_set.test(index_id_rec)) {
      // Make a copy of the current config
      auto hypothetical_config = curr_config_set;
      hypothetical_config.set(index_id_rec);
      CompressedIndexConfigUtil::ConstructQueryConfigFeature(
          hypothetical_config, add_candidate_set, drop_candidate_set,
          query_config_vec);
      /**
       * The paper converts the current representation
       */
      CompressedIndexConfigUtil::ConstructStateConfigFeature(
          *index_config_->GetCurrentIndexConfig(), config_vec);
      double hypothetical_exec_cost = rlse_model_->Predict(query_config_vec);
      double hypothetical_config_cost = lstdq_model_->Predict(config_vec);
      double cost = hypothetical_config_cost + hypothetical_exec_cost;
      if (cost < max_cost) {
        optimal_config_set.set(index_id_rec);
      }
    }
    // We are done go to next
    index_id_rec = add_candidate_set.find_next(index_id_rec);
  }
  // Iterate through drop candidates
  size_t index_id_drop = drop_candidate_set.find_first();
  while (index_id_drop != boost::dynamic_bitset<>::npos) {
    if (optimal_config_set.test(index_id_drop)) {
      // Make a copy of the current config
      auto hypothetical_config = curr_config_set;
      hypothetical_config.reset(index_id_drop);
      CompressedIndexConfigUtil::ConstructQueryConfigFeature(
          hypothetical_config, add_candidate_set, drop_candidate_set,
          query_config_vec);
      double hypothetical_exec_cost = rlse_model_->Predict(query_config_vec);
      double hypothetical_config_cost = lstdq_model_->Predict(config_vec);
      double cost = hypothetical_config_cost + hypothetical_exec_cost;
      if (cost < max_cost) {
        optimal_config_set.reset(index_id_drop);
      }
    }
    // We are done go to next
    index_id_drop = drop_candidate_set.find_next(index_id_drop);
  }
}
}  // namespace brain
}  // namespace peloton