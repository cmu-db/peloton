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

void LSPIIndexTuner::Tune(
    const std::vector<std::string> &queries,
    UNUSED_ATTRIBUTE const std::vector<double> &query_latencies) {
  size_t num_queries = queries.size();
  std::vector<std::unique_ptr<boost::dynamic_bitset<>>> add_candidates;
  std::vector<std::unique_ptr<boost::dynamic_bitset<>>> drop_candidates;
  // Step 1: Populate the add and drop candidates per query
  for (size_t i = 0; i < num_queries; i++) {
    add_candidates.push_back(index_config_->AddCandidates(queries[i]));
    drop_candidates.push_back(index_config_->DropCandidates(queries[i]));
  }
  // Step 2: Update the RLSE model with the new samples
  for (size_t i = 0; i < num_queries; i++) {
    vector_eig query_config_feat;
    ConstructQueryConfigFeature(add_candidates[i], drop_candidates[i],
                                query_config_feat);
    rlse_model_->Update(query_config_feat, query_latencies[i]);
  }
  // Step 3: Iterate through the queries - Per query obtain optimal add/drop
  // candidates
  // Step 4:
}

void LSPIIndexTuner::ConstructQueryConfigFeature(
    std::unique_ptr<boost::dynamic_bitset<>> &add_candidates,
    std::unique_ptr<boost::dynamic_bitset<>> &drop_candidates,
    vector_eig &query_config_vec) const {
  size_t num_configs = feat_len_;
  auto curr_config_set = index_config_->GetCurrentIndexConfig();
  query_config_vec = vector_eig::Zero(2 * num_configs);
  size_t offset_rec = 0;
  size_t config_id_rec = add_candidates->find_first();
  query_config_vec[offset_rec] = 1.0;
  while (config_id_rec != boost::dynamic_bitset<>::npos) {
    if (curr_config_set->test(config_id_rec)) {
      query_config_vec[offset_rec + config_id_rec] = 1.0f;
    } else {
      query_config_vec[offset_rec + config_id_rec] = -1.0f;
    }
    config_id_rec = add_candidates->find_next(config_id_rec);
  }
  size_t offset_drop = num_configs;
  size_t config_id_drop = drop_candidates->find_first();
  query_config_vec[offset_drop] = 1.0;
  while (config_id_drop != boost::dynamic_bitset<>::npos) {
    if (curr_config_set->test(config_id_drop)) {
      query_config_vec[offset_drop + config_id_drop] = 1.0f;
    }
    // else case shouldnt happen
    config_id_drop = drop_candidates->find_next(config_id_drop);
  }
}

void LSPIIndexTuner::ConstructConfigFeature(
    peloton::vector_eig &config_vec) const {
  index_config_->ToCoveredEigen(config_vec);
}

// void LSPIIndexTuner::FindOptimal(vector_eig &optimal_next) const {
//  auto curr_config = index_config_->GetCurrentIndexConfig();
////  auto add_candidates = index_config_->AddCandidates()
//
//}
}
}