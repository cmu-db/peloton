#include "brain/indextune/lspi/lspi_tuner.h"

namespace peloton {
namespace brain {
LSPIIndexTuner::LSPIIndexTuner(
    const std::string &db_name, peloton::catalog::Catalog *cat,
    peloton::concurrency::TransactionManager *txn_manager)
    : db_name_(db_name) {
  index_config_ = std::unique_ptr<CompressedIndexConfiguration>(
      new CompressedIndexConfiguration(db_name, cat, txn_manager));
  feat_len_ = index_config_->GetConfigurationCount();
  rlse_model_ = std::unique_ptr<RLSEModel>(new RLSEModel(feat_len_));
  lstd_model_ = std::unique_ptr<LSTDModel>(new LSTDModel(feat_len_));
}

void LSPIIndexTuner::Tune(const std::vector<std::string>& queries,
                          const std::vector<double>& query_latencies) {
  size_t num_queries = queries.size();
  // Step 1: Update the RLSE model with more samples
  for(int i = 0; i < num_queries; i++) {

  }
  // Step 2: Iterate through the queries - Per query obtain optimal add/drop candidates
  // Step 3:
}

//void LSPIIndexTuner::FindOptimal(vector_eig &optimal_next) const {
//  auto curr_config = index_config_->GetCurrentIndexConfig();
////  auto add_candidates = index_config_->AddCandidates()
//
//}
}
}