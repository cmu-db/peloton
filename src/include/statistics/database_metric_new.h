//
// Created by Tianyu Li on 4/26/18.
//
#include "statistics/abstract_metric_new.h"

namespace peloton {
namespace stats {
class DatabaseMetricNew: public AbstractMetricNew {
 public:
  template <StatInsertionPoint type, typename... Args>
  void OnStatAvailable(Args... args) override {
    AbstractMetricNew::OnStatAvailable(args);
  }

  // TODO(tianyu): fill argument
  template <> void OnStatAvailable<StatInsertionPoint::TXN_COMMIT>(){
    txn_committed_++;
  }

  template <> void OnStatAvailable<StatInsertionPoint::TXN_ABORT>() {
    txn_aborted_++;
  }

  void CollectIntoCatalog() override {
    // TODO(tianyu): implement this
  }

  const std::string Info() const override {
    // TODO(tianyu): implement this
    return nullptr;
  }

 private:
  std::atomic<int64_t> txn_committed_, txn_aborted_;
};
}
}

