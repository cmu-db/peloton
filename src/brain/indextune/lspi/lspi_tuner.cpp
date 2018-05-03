#include "brain/indextune/lspi/lspi_tuner.h"

namespace peloton {
namespace brain {
LSPIIndexTuner::LSPIIndexTuner(
    const std::string &db_name, peloton::catalog::Catalog *cat,
    peloton::concurrency::TransactionManager *txn_manager)
    : db_name_(db_name) {
  index_config_ = std::unique_ptr<CompressedIndexConfigContainer>(
      new CompressedIndexConfigContainer(db_name, cat, txn_manager));
  feat_len_ = index_config_->GetConfigurationCount();
  rlse_model_ = std::unique_ptr<RLSEModel>(new RLSEModel(2 * feat_len_));
  lstd_model_ = std::unique_ptr<LSTDModel>(new LSTDModel(feat_len_));
}

void LSPIIndexTuner::Tune(const std::vector<std::string> &queries,
                          const std::vector<double> &query_latencies) {
  size_t num_queries = queries.size();
  std::vector<boost::dynamic_bitset<>> add_candidate_sets;
  std::vector<boost::dynamic_bitset<>> drop_candidate_sets;
  boost::dynamic_bitset<> curr_config_set =
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
  }
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
  // Step 4: Update the LSPI model based on current most optimal query config
  // TODO(saatviks): Finish step 4
  // Step 5: Adjust to the most optimal query config
  // TODO(weichenl): Call AdjustConfig on 'optimal_config_set'
}

void LSPIIndexTuner::FindOptimalConfig(
    double max_cost, const boost::dynamic_bitset<> &curr_config_set,
    const boost::dynamic_bitset<> &add_candidate_set,
    const boost::dynamic_bitset<> &drop_candidate_set,
    boost::dynamic_bitset<> &optimal_config_set) {
  // Iterate through add candidates
  size_t index_id_rec = add_candidate_set.find_first();
  vector_eig query_config_vec;
  while (index_id_rec != boost::dynamic_bitset<>::npos) {
    if (!optimal_config_set.test(index_id_rec)) {
      // Make a copy of the current config
      auto hypothetical_config = boost::dynamic_bitset<>(curr_config_set);
      hypothetical_config.set(index_id_rec);
      CompressedIndexConfigUtil::ConstructQueryConfigFeature(
          hypothetical_config, add_candidate_set, drop_candidate_set,
          query_config_vec);
      double hypothetical_config_cost = rlse_model_->Predict(query_config_vec);
      if (hypothetical_config_cost < max_cost) {
        optimal_config_set.set(index_id_rec);
      }
    }
    // We are done go to next
    index_id_rec = add_candidate_set.find_next(index_id_rec);
  }
  // Iterate through add candidates
  size_t index_id_drop = add_candidate_set.find_first();
  while (index_id_drop != boost::dynamic_bitset<>::npos) {
    if (optimal_config_set.test(index_id_drop)) {
      // Make a copy of the current config
      auto hypothetical_config = boost::dynamic_bitset<>(curr_config_set);
      hypothetical_config.reset(index_id_drop);
      CompressedIndexConfigUtil::ConstructQueryConfigFeature(
          hypothetical_config, add_candidate_set, drop_candidate_set,
          query_config_vec);
      double hypothetical_config_cost = rlse_model_->Predict(query_config_vec);
      if (hypothetical_config_cost < max_cost) {
        optimal_config_set.reset(index_id_drop);
      }
    }
    // We are done go to next
    index_id_drop = add_candidate_set.find_next(index_id_drop);
  }
}
}
}