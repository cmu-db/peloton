//
// Created by Tianyu Li on 4/26/18.
//
#include "statistics/abstract_metric_new.h"

namespace peloton {
namespace stats {
class DatabaseMetricNew: public AbstractMetricNew {
 public:
  // TODO(tianyu): fill argument
  void OnTransactionCommit(oid_t db_id) override {
    std::atomic<int64_t> *value;
    txn_committed_.Find(db_id, value);
    (*value).fetch_add(1);
  }

  void OnTransactionAbort(oid_t db_id) override {
    std::atomic<int64_t> *value;
    txn_aborted_.Find(db_id, value);
    (*value).fetch_add(1);
  }

  void CollectIntoCatalog() override {
    // TODO(tianyu): implement this
  }

  const std::string GetInfo() const override {
    // TODO(tianyu): implement this
    return nullptr;
  }



 private:
   CuckooMap<oid_t, std::atomic<int64_t> *> txn_committed_, txn_aborted_;
};
}
}
