//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// database_metric.h
//
// Identification: src/statistics/database_metric.h
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <string>
#include <sstream>

#include "common/internal_types.h"
#include "statistics/counter_metric.h"
#include "statistics/abstract_metric.h"

namespace peloton {
namespace stats {

/**
 * Database-specific metrics, including the number of committed/aborted txns.
 */
class DatabaseMetric : public AbstractMetric {
 public:
  DatabaseMetric(MetricType type, oid_t database_id);

  //===--------------------------------------------------------------------===//
  // ACCESSORS
  //===--------------------------------------------------------------------===//

  inline void IncrementTxnCommitted() { txn_committed_.Increment(); }

  inline void IncrementTxnAborted() { txn_aborted_.Increment(); }

  inline CounterMetric &GetTxnCommitted() { return txn_committed_; }

  inline CounterMetric &GetTxnAborted() { return txn_aborted_; }

  inline oid_t GetDatabaseId() { return database_id_; }

  //===--------------------------------------------------------------------===//
  // HELPER METHODS
  //===--------------------------------------------------------------------===//

  inline void Reset() {
    txn_committed_.Reset();
    txn_aborted_.Reset();
  }

  inline bool operator==(const DatabaseMetric &other) {
    return database_id_ == other.database_id_ &&
           txn_committed_ == other.txn_committed_ &&
           txn_aborted_ == other.txn_aborted_;
  }

  inline bool operator!=(const DatabaseMetric &other) {
    return !(*this == other);
  }

  void Aggregate(AbstractMetric &source);

  const std::string GetInfo() const;

 private:
  //===--------------------------------------------------------------------===//
  // MEMBERS
  //===--------------------------------------------------------------------===//

  // The ID of this database
  oid_t database_id_;

  // Count of the number of transactions committed
  CounterMetric txn_committed_{MetricType::COUNTER};

  // Count of the number of transactions aborted
  CounterMetric txn_aborted_{MetricType::COUNTER};
};

}  // namespace stats
}  // namespace peloton
