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
namespace peloton{
namespace brain{
class LSPIIndexTuner{
 public:
  explicit LSPIIndexTuner(const std::string& db_name,
                          catalog::Catalog *cat,
                          concurrency::TransactionManager *txn_manager);
  /**
   * Given a recent set of queries and their latency on the current configuration
   * this function will automatically tune the database for future workloads.
   * Currently it only supports IndexTuning but should be relatively simple to support
   * more utility functions.
   * @param query_latency_pairs: vector of <query string, latency> pairs
   */
  void Tune(std::vector<std::pair<std::string,double>> query_latency_pairs);

 private:
  // Database to tune
  std::string db_name_;
  // Feature Length == All possible configurations
  int feat_len_;
  // Index configuration object - Represents current set of indexes compactly
  // and exposes APIs for generating a search space for our RL algorithm
  std::shared_ptr<CompressedIndexConfiguration> index_config_;
  // RLSE model for computing immediate cost of an action
  std::unique_ptr<RLSEModel> rlse_model_;
  // LSTD model for computing
  std::unique_ptr<LSTDModel> lstd_model_;
  // Feature Generation
  // Feature representing running a SQL query with a given IndexConfig
  // (0->n): 1 for Add cand(and its prefix closure), -1 otherwise |
  // (n->2*n): 1 for Drop cand(and its prefix closure??), -1 otherwise
  vector_eig GenQueryStateFeature(std::shared_ptr<boost::dynamic_bitset<>> index_config,
                                  std::shared_ptr<boost::dynamic_bitset<>> add_candidates,
                                  std::shared_ptr<boost::dynamic_bitset<>> drop_candidates);
  // Feature representing current IndexConfig
  // 1 for all covered(prefix closure) index configs, -1 otherwise
  vector_eig GenStateFeature(std::shared_ptr<boost::dynamic_bitset<>> index_config);
};
}
}