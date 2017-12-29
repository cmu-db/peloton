//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// counter_metric.h
//
// Identification: src/statistics/counter_metric.h
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <string>
#include <sstream>

#include "common/internal_types.h"
#include "statistics/abstract_metric.h"

namespace peloton {
namespace stats {

/**
 * Metric as a counter. E.g. # txns committed, # tuples read, etc.
 */
class CounterMetric : public AbstractMetric {
 public:
  CounterMetric(MetricType type);

  //===--------------------------------------------------------------------===//
  // ACCESSORS
  //===--------------------------------------------------------------------===//

  inline void Increment() { count_++; }

  inline void Increment(int64_t count) { count_ += count; }

  inline void Decrement() { count_--; }

  inline void Decrement(int64_t count) { count_ -= count; }

  //===--------------------------------------------------------------------===//
  // HELPER METHODS
  //===--------------------------------------------------------------------===//

  inline void Reset() { count_ = 0; }

  inline int64_t GetCounter() { return count_; }

  inline bool operator==(const CounterMetric &other) {
    return count_ == other.count_;
  }

  inline bool operator!=(const CounterMetric &other) {
    return !(*this == other);
  }

  // Adds the source counter to this counter
  void Aggregate(AbstractMetric &source);

  // Returns a string representation of this counter
  inline const std::string GetInfo() const {
    std::stringstream ss;
    ss << count_;
    return ss.str();
  }

 private:
  //===--------------------------------------------------------------------===//
  // MEMBERS
  //===--------------------------------------------------------------------===//

  // The current count
  int64_t count_;
};

}  // namespace stats
}  // namespace peloton
