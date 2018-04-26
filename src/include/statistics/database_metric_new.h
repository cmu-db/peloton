//
// Created by Tianyu Li on 4/26/18.
//
#include "statistics/abstract_metric_new.h"

namespace peloton {
namespace stats {
class DatabaseMetricNew: public AbstractMetricNew {
 public:
  // TODO(tianyu): fill argument
  void OnTransactionCommit() override {
    txn_committed_++;
  }
  void OnTransactionAbort() override {
    txn_aborted_++;
  }

  void CollectIntoCatalog() override {
    // TODO(tianyu): implement this
  }

  const std::string GetInfo() const override {
    // TODO(tianyu): implement this
    return nullptr;
  }



 private:
  std::atomic<int64_t> txn_committed_, txn_aborted_;
};
}
}

