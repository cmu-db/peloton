//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// memory_metric.h
//
// Identification: src/statistics/memory_metric.h
//
// Copyright (c) 2015-18, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include "common/internal_types.h"
#include "statistics/abstract_metric.h"
#include "statistics/counter_metric.h"

namespace peloton {
namespace stats {
class MemoryMetric : public AbstractMetric {
 public:
  MemoryMetric(MetricType type)
      : AbstractMetric(type),
        alloc_(MetricType::COUNTER),
        usage_(MetricType::COUNTER) {}

  //===--------------------------------------------------------------------===//
  // ACCESSORS
  //===--------------------------------------------------------------------===//
  inline void IncrementAllocation(int64_t bytes) { alloc_.Increment(bytes); }

  inline void IncrementUsage(int64_t bytes) { usage_.Increment(bytes); }

  inline void DecreaseAllocation(int64_t bytes) { alloc_.Increment(bytes); }

  inline void DecreaseUsage(int64_t bytes) { usage_.Decrement(bytes); }

  inline int64_t GetAllocation() { return alloc_.GetCounter(); }

  inline int64_t GetUsage() { return usage_.GetCounter(); }

  //===--------------------------------------------------------------------===//
  // HELPER METHODS
  //===--------------------------------------------------------------------===//

  inline bool operator==(const MemoryMetric &other) {
    return (this->alloc_ == other.alloc_ && this->usage_ == other.usage_);
  }

  inline bool operator!=(const MemoryMetric &other) {
    return !(*this == other);
  }

  // Resets all access counters to zero
  inline void Reset() {
    alloc_.Reset();
    usage_.Reset();
  }
  // Returns a string representation of this access metric
  inline const std::string GetInfo() const {
    std::stringstream ss;
    ss << "[ memory allocation = " << alloc_.GetInfo() << " bytes"
       << ", memory usage = " << usage_.GetInfo() << " bytes ]";
    return ss.str();
  }

  void Aggregate(AbstractMetric &source);

 private:
  //===--------------------------------------------------------------------===//
  // MEMBERS
  //===--------------------------------------------------------------------===//

  CounterMetric alloc_;
  CounterMetric usage_;
};
}  // namespace stats
}  // namespace peloton