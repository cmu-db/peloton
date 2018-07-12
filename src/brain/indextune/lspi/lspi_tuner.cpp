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
    const std::string &db_name, const std::set<oid_t> &ignore_table_oids,
    CandidateSelectionType cand_sel_type, size_t max_index_size,
    double variance_init, double reg_coeff,
    peloton::catalog::Catalog *catalog,
    peloton::concurrency::TransactionManager *txn_manager)
    : db_name_{db_name},
      cand_sel_type_{cand_sel_type},
      max_index_size_{max_index_size}{
  index_config_ = std::unique_ptr<CompressedIndexConfigContainer>(
      new CompressedIndexConfigContainer(db_name, ignore_table_oids,
                                         max_index_size, catalog,
                                         txn_manager));
  size_t feat_len = index_config_->GetConfigurationCount();
  rlse_model_ = std::unique_ptr<RLSEModel>(new RLSEModel(2 * feat_len, variance_init, reg_coeff));
  lstdq_model_ = std::unique_ptr<LSTDQModel>(new LSTDQModel(feat_len));
  prev_config_vec = vector_eig::Zero(feat_len);
  // Empty config
  prev_config_vec[0] = 1.0;
}

const CompressedIndexConfigContainer *LSPIIndexTuner::GetConfigContainer()
    const {
  return index_config_.get();
}

void LSPIIndexTuner::Tune(const std::vector<std::string> &queries,
                          const std::vector<double> &query_costs,
                          std::set<std::shared_ptr<brain::HypotheticalIndexObject>>& add_set,
                          std::set<std::shared_ptr<brain::HypotheticalIndexObject>>& drop_set) {
  size_t num_queries = queries.size();
  std::vector<boost::dynamic_bitset<>> add_candidate_sets;
  std::vector<boost::dynamic_bitset<>> drop_candidate_sets;
  double cost_avg = 0.0;
  const boost::dynamic_bitset<> &curr_config_set =
      *index_config_->GetCurrentIndexConfig();
  // Be careful about not duplicating bitsets anywhere since they can
  // be potentially huge
  // Step 1: Populate the add and drop candidates per query
  boost::dynamic_bitset<> add_candidate_set, drop_candidate_set;
  for (size_t i = 0; i < num_queries; i++) {
    CompressedIndexConfigUtil::AddCandidates(*index_config_, queries[i],
                                             add_candidate_set, cand_sel_type_,
                                             max_index_size_);
    add_candidate_sets.push_back(std::move(add_candidate_set));
    CompressedIndexConfigUtil::DropCandidates(*index_config_, queries[i],
                                              drop_candidate_set);
    drop_candidate_sets.push_back(std::move(drop_candidate_set));
    cost_avg += query_costs[i];
  }
  cost_avg /= num_queries;
  // Step 2: Update the RLSE model with the new samples
  for (size_t i = 0; i < num_queries; i++) {
    vector_eig query_config_feat;
    CompressedIndexConfigUtil::ConstructQueryConfigFeature(
        curr_config_set, add_candidate_sets[i], drop_candidate_sets[i],
        query_config_feat);
    rlse_model_->Update(query_config_feat, query_costs[i]);
  }
  // Step 3: Iterate through the queries/latencies and obtain a new optimal
  // config
  auto optimal_config_set = curr_config_set;
  for (size_t i = 0; i < num_queries; i++) {
    FindOptimalConfig(curr_config_set, add_candidate_sets[i],
                      drop_candidate_sets[i], optimal_config_set);
  }

  vector_eig new_config_vec;
  CompressedIndexConfigUtil::ConstructStateConfigFeature(optimal_config_set,
                                                         new_config_vec);
  // Step 4: Update the LSPI model based on current most optimal query config
  lstdq_model_->Update(prev_config_vec, new_config_vec, cost_avg);

  // Step 5: Adjust to the most optimal query config
  index_config_->AdjustIndexes(optimal_config_set, add_set, drop_set);
  // TODO(saatviks, weichenl): Is this a heavy op?
  PELOTON_ASSERT(optimal_config_set == *index_config_->GetCurrentIndexConfig());
}

void LSPIIndexTuner::FindOptimalConfig(
    const boost::dynamic_bitset<> &curr_config_set,
    const boost::dynamic_bitset<> &add_candidate_set,
    const boost::dynamic_bitset<> &drop_candidate_set,
    boost::dynamic_bitset<> &optimal_config_set) {
  // Iterate through add candidates
  size_t index_id_rec = add_candidate_set.find_first();
  vector_eig query_config_vec, config_vec;
  // Find current cost
  CompressedIndexConfigUtil::ConstructQueryConfigFeature(
      curr_config_set, add_candidate_set, drop_candidate_set, query_config_vec);
  CompressedIndexConfigUtil::ConstructStateConfigFeature(
      *index_config_->GetCurrentIndexConfig(), config_vec);
  double max_exec_cost = rlse_model_->Predict(query_config_vec);
  double max_config_cost = lstdq_model_->Predict(config_vec);
  double max_cost = max_exec_cost + max_config_cost;
  while (index_id_rec != boost::dynamic_bitset<>::npos) {
    if (!optimal_config_set.test(index_id_rec)) {
      // Make a copy of the current config
      auto hypothetical_config = boost::dynamic_bitset<>(curr_config_set);
      // Set the corresponding bit for candidate
      hypothetical_config.set(index_id_rec);
      LOG_DEBUG("Prev: %s", index_config_->ToString(curr_config_set).c_str());
      LOG_DEBUG("Trying Add Cand: %s",
                index_config_->ToString(hypothetical_config).c_str());
      LOG_DEBUG("QueryConfig Vector: %s",
                CompressedIndexConfigUtil::ToString(query_config_vec).c_str());
      LOG_DEBUG("RLSE Wts: %s",
                CompressedIndexConfigUtil::ToString(rlse_model_->GetWeights()).c_str());
      // Construct the query-state and state feature
      CompressedIndexConfigUtil::ConstructQueryConfigFeature(
          hypothetical_config, add_candidate_set, drop_candidate_set,
          query_config_vec);
      CompressedIndexConfigUtil::ConstructStateConfigFeature(
          hypothetical_config, config_vec);
      // Get the new hypothetical configs overall cost
      double hypothetical_exec_cost = rlse_model_->Predict(query_config_vec);
      double hypothetical_config_cost = lstdq_model_->Predict(config_vec);
      double cost = hypothetical_config_cost + hypothetical_exec_cost;

      LOG_DEBUG("Candidate Cost: %f, Max Cost: %f", cost, max_cost);
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
      CompressedIndexConfigUtil::ConstructStateConfigFeature(
          hypothetical_config, config_vec);
      double hypothetical_exec_cost = rlse_model_->Predict(query_config_vec);
      double hypothetical_config_cost = lstdq_model_->Predict(config_vec);
      double cost = hypothetical_config_cost + hypothetical_exec_cost;
      LOG_DEBUG("Prev: %s", index_config_->ToString(curr_config_set).c_str());
      LOG_DEBUG("Trying Drop Cand: %s",
                index_config_->ToString(hypothetical_config).c_str());
      LOG_DEBUG("Candidate Cost: %f, Max Cost: %f", cost, max_cost);
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