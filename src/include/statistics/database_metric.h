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
class DatabaseMetricRawData : public AbstractRawData {
 public:
  inline void IncrementTxnCommited(oid_t database_id) {
    counters_[database_id].first++;
  }

  inline void IncrementTxnAborted(oid_t database_id){
    counters_[database_id].second++;
  }

  void Aggregate(AbstractRawData &other) override {
    auto &other_db_metric = dynamic_cast<DatabaseMetricRawData &>(other);
    for (auto &entry: other_db_metric.counters_) {
      auto &this_counter = counters_[entry.first];
      auto &other_counter = entry.second;
      this_counter.first += other_counter.first;
      this_counter.second += other_counter.second;
    }
  }

  // TODO(tianyu) Implement
  void WriteToCatalog() override{}

  const std::string GetInfo() const override {
    return "";
  }

 private:
  /**
   * Maps from database id to a pair of counters.
   *
   * First counter represents number of transactions committed and the second
   * one represents the number of transactions aborted.
   */
  std::unordered_map<oid_t, std::pair<uint64_t, uint64_t>> counters_;

};

class DatabaseMetric: public AbstractMetric<DatabaseMetricRawData> {
 public:
  inline void OnTransactionCommit(oid_t database_id) override {
    DatabaseMetricRawData *raw_data = raw_data_.load();
    raw_data->MarkUnsafe();
    raw_data->IncrementTxnCommited(database_id);
    raw_data->MarkSafe();
  }

  inline void OnTransactionAbort(oid_t database_id) override {
    DatabaseMetricRawData *raw_data = raw_data_.load();
    raw_data->MarkUnsafe();
    raw_data->IncrementTxnAborted(database_id);
    raw_data->MarkSafe();
  }
};

class DatabaseMetricOld : public AbstractMetricOld {
 public:
  DatabaseMetricOld(MetricType type, oid_t database_id);

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

  inline bool operator==(const DatabaseMetricOld &other) {
    return database_id_ == other.database_id_ &&
        txn_committed_ == other.txn_committed_ &&
        txn_aborted_ == other.txn_aborted_;
  }

  inline bool operator!=(const DatabaseMetricOld &other) {
    return !(*this == other);
  }

  void Aggregate(AbstractMetricOld &source);

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
