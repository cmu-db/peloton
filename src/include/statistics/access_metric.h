//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// access_metric.h
//
// Identification: src/statistics/access_metric.h
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <vector>

#include "common/internal_types.h"
#include "statistics/abstract_metric.h"
#include "statistics/counter_metric.h"

namespace peloton {
namespace stats {

/**
 * Metric that counts the number of reads, updates,
 * inserts, and deletes for a given storage type
 * (e.g. index or table).
 */
class AccessMetric : public AbstractMetric {
 public:
  AccessMetric(MetricType type) : AbstractMetric(type) {}

  //===--------------------------------------------------------------------===//
  // ACCESSORS
  //===--------------------------------------------------------------------===//

  inline void IncrementReads() { access_counters_[READ_COUNTER].Increment(); }

  inline void IncrementUpdates() {
    access_counters_[UPDATE_COUNTER].Increment();
  }

  inline void IncrementInserts() {
    access_counters_[INSERT_COUNTER].Increment();
  }

  inline void IncrementDeletes() {
    access_counters_[DELETE_COUNTER].Increment();
  }

  inline void IncrementReads(int64_t count) {
    access_counters_[READ_COUNTER].Increment(count);
  }

  inline void IncrementUpdates(int64_t count) {
    access_counters_[UPDATE_COUNTER].Increment(count);
  }

  inline void IncrementInserts(int64_t count) {
    access_counters_[INSERT_COUNTER].Increment(count);
  }

  inline void IncrementDeletes(int64_t count) {
    access_counters_[DELETE_COUNTER].Increment(count);
  }

  inline int64_t GetReads() {
    return access_counters_[READ_COUNTER].GetCounter();
  }

  inline int64_t GetUpdates() {
    return access_counters_[UPDATE_COUNTER].GetCounter();
  }

  inline int64_t GetInserts() {
    return access_counters_[INSERT_COUNTER].GetCounter();
  }

  inline int64_t GetDeletes() {
    return access_counters_[DELETE_COUNTER].GetCounter();
  }

  inline CounterMetric &GetAccessCounter(size_t counter_type) {
    return access_counters_[counter_type];
  }

  //===--------------------------------------------------------------------===//
  // HELPER METHODS
  //===--------------------------------------------------------------------===//

  inline bool operator==(const AccessMetric &other) {
    for (size_t access_counter_itr = 0;
         access_counter_itr < access_counters_.size(); ++access_counter_itr) {
      if (access_counters_[access_counter_itr] !=
          other.access_counters_[access_counter_itr]) {
        return false;
      }
    }
    return true;
  }

  inline bool operator!=(const AccessMetric &other) {
    return !(*this == other);
  }

  // Resets all access counters to zero
  inline void Reset() {
    for (auto &counter : access_counters_) {
      counter.Reset();
    }
  }

  // Returns a string representation of this access metric
  inline const std::string GetInfo() const {
    std::stringstream ss;
    ss << "[ reads=" << access_counters_[READ_COUNTER].GetInfo()
       << ", updates=" << access_counters_[UPDATE_COUNTER].GetInfo()
       << ", inserts=" << access_counters_[INSERT_COUNTER].GetInfo()
       << ", deletes=" << access_counters_[DELETE_COUNTER].GetInfo() << " ]";
    return ss.str();
  }

  // Adds the counters from the source access metric
  // to the counters in this access metric
  void Aggregate(AbstractMetric &source);

 private:
  //===--------------------------------------------------------------------===//
  // MEMBERS
  //===--------------------------------------------------------------------===//

  // Vector containing all access types
  std::vector<CounterMetric> access_counters_{
      CounterMetric(MetricType::COUNTER),  // READ_COUNTER
      CounterMetric(MetricType::COUNTER),  // UPDATE_COUNTER
      CounterMetric(MetricType::COUNTER),  // INSERT_COUNTER
      CounterMetric(MetricType::COUNTER)   // DELETE_COUNTER
  };

  // The different types of accesses. These also
  // serve as indexes into the access_counters_
  // vector.
  static const size_t READ_COUNTER = 0;
  static const size_t UPDATE_COUNTER = 1;
  static const size_t INSERT_COUNTER = 2;
  static const size_t DELETE_COUNTER = 3;
  static const size_t NUM_COUNTERS = 4;
};

}  // namespace stats
}  // namespace peloton
