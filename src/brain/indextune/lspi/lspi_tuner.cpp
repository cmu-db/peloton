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
    add_candidates.push_back(
        CompressedIndexConfigUtil::AddCandidates(*index_config_, queries[i]));
    drop_candidates.push_back(
        CompressedIndexConfigUtil::DropCandidates(*index_config_, queries[i]));
  }
  // Step 2: Update the RLSE model with the new samples
  for (size_t i = 0; i < num_queries; i++) {
    vector_eig query_config_feat;
    CompressedIndexConfigUtil::ConstructQueryConfigFeature(*index_config_,
        add_candidates[i], drop_candidates[i], query_config_feat);
    rlse_model_->Update(query_config_feat, query_latencies[i]);
  }
  // Step 3: Iterate through the queries - Per query obtain optimal add/drop
  // candidates

  // Step 4:
}

// void LSPIIndexTuner::FindOptimal(vector_eig &optimal_next) const {
//  auto curr_config = index_config_->GetCurrentIndexConfig();
////  auto add_candidates = index_config_->AddCandidates()
//
//}
}
}