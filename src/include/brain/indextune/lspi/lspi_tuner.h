#pragma once

#include <string>
#include <memory>
#include "brain/util/eigen_util.h"
#include "brain/indextune/lspi/rlse.h"
#include "brain/indextune/lspi/lstd.h"
#include "brain/indextune/compressed_index_config.h"

/**
 * Least-Squares Policy Iteration based Index tuning
 * (Derived from Cost Model Oblivious DB Tuning by Basu et. al.)
 * This can be extended to any configuration knob tuning problem.
 * For now, we assume one instance of the tuner per database.
 * We apply TD(0): V(St)=V(St)+α[Rt+1+γV(St+1)−V(St)] with alpha = 0.
 */
namespace peloton {
namespace brain {
class LSPIIndexTuner {
 public:
  explicit LSPIIndexTuner(
      const std::string &db_name, catalog::Catalog *cat,
      concurrency::TransactionManager *txn_manager = nullptr);
  /**
   * Given a recent set of queries and their latency on the current
   * configuration
   * this function will automatically tune the database for future workloads.
   * Currently it only supports IndexTuning but should be relatively simple to
   * support
   * more utility functions.
   * @param query_latency_pairs: vector of <query string, latency> pairs
   */
  void Tune(const std::vector<std::string> &queries,
            const std::vector<double> &query_latencies);

 private:
  // Database to tune
  std::string db_name_;
  // Feature Length == All possible configurations
  size_t feat_len_;
  // Index configuration object - Represents current set of indexes compactly
  // and exposes APIs for generating a search space for our RL algorithm
  std::unique_ptr<CompressedIndexConfigContainer> index_config_;
  // RLSE model for computing immediate cost of an action
  std::unique_ptr<RLSEModel> rlse_model_;
  // LSTD model for computing
  std::unique_ptr<LSTDModel> lstd_model_;
  // Feature constructors
  /**
   * Constructs the feature vector representing the SQL query running on the
   * current
   * index configuration. This is done by using the following feature vector:
   * = 0.0 if not in f(query)
   * = 1.0 if in f(query) and belongs to current config
   * = -1 if in f(query) but not in current config
   * where f(query) is first recommended_index(query)(0->n), then
   * drop_index(query)(n->2*n)
   * @param add_candidates: add candidate suggestions
   * @param drop_candidates: drop candidate suggestions
   * @param query_config_vec: query configuration vector to construct
   * // TODO: not in f(query) should split into:  (i)!f(query) &&
   * belongs(config) (ii) !(f(query) && belongs(config))?
   */
  void ConstructQueryConfigFeature(
      std::unique_ptr<boost::dynamic_bitset<>> &add_candidates,
      std::unique_ptr<boost::dynamic_bitset<>> &drop_candidates,
      vector_eig &query_config_vec) const;
  /**
   * Get the covered index configuration feature vector.
   * The difference between this and `GetCurrentIndexConfig` is that
   * all single column index configurations by a multicolumn index are
   * considered covered and set to 1.
   * @param config_vec: configuration vector to construct
   */
  void ConstructConfigFeature(vector_eig &config_vec) const;
};
}
}