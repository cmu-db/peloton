//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// counter_metric.h
//
// Identification: src/backend/statistics/counter_metric.h
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <mutex>
#include <map>
#include <vector>
#include <iostream>
#include <string>
#include <sstream>


#include "backend/common/types.h"
#include "backend/statistics/counter_metric.h"
#include "backend/statistics/abstract_metric.h"

//===--------------------------------------------------------------------===//
// GUC Variables
//===--------------------------------------------------------------------===//


namespace peloton {
namespace stats {


/**
 * Metric as a counter. E.g. # txns committed
 */
class DatabaseMetric : public AbstractMetric {
 public:

  DatabaseMetric(MetricType type, oid_t database_id);

  inline void IncrementTxnCommitted() {
    txn_committed_.Increment();
  }

  inline void IncrementTxnAborted() {
    txn_aborted_.Increment();
  }

  inline CounterMetric& GetTxnCommitted() {
    return txn_committed_;
  }

  inline CounterMetric& GetTxnAborted() {
    return txn_aborted_;
  }

  inline oid_t GetDatabaseId() {
    return database_id_;
  }

  inline void Reset() {
    txn_committed_.Reset();
    txn_aborted_.Reset();
  }

  void Aggregate(AbstractMetric &source);

  inline std::string ToString() {
    std::stringstream ss;
    ss << "//===--------------------------------------------------------------------===//" << std::endl;
    ss << "// DATABASE_ID " << database_id_ << std::endl;
    ss << "//===--------------------------------------------------------------------===//" << std::endl;
    ss <<  "# transactions committed: " << txn_committed_.ToString() << std::endl;
    ss <<  "# transactions aborted:   " << txn_aborted_.ToString() << std::endl;
    return ss.str();
  }

 private:
  oid_t database_id_;
  CounterMetric txn_committed_{MetricType::COUNTER_METRIC};
  CounterMetric txn_aborted_{MetricType::COUNTER_METRIC};

};

}  // namespace stats
}  // namespace peloton
