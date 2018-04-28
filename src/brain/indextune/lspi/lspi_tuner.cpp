#include "brain/indextune/lspi/lspi_tuner.h"

namespace peloton{
namespace brain{
LSPIIndexTuner::LSPIIndexTuner(const std::string &db_name,
                               peloton::catalog::Catalog *cat,
                               peloton::concurrency::TransactionManager *txn_manager): db_name_(db_name) {
  index_config_ = std::make_shared<CompressedIndexConfiguration>(cat, txn_manager);
  feat_len_ = index_config_->GetConfigurationCount();
  rlse_model_ = std::unique_ptr<RLSEModel>(new RLSEModel(feat_len_));
  lstd_model_ = std::unique_ptr<LSTDModel>(new LSTDModel(feat_len_));
}

//void LSPIIndexTuner::Tune(UNUSED_ATTRIBUTE std::vector<std::pair<std::string, double>> query_latency_pairs) {
//  UNUSED_ATTRIBUTE auto current_config = index_config_->GetCurrentIndexConfig();
//}
}
}