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
#include "backend/statistics/abstract_metric.h"

//===--------------------------------------------------------------------===//
// GUC Variables
//===--------------------------------------------------------------------===//


namespace peloton {
namespace stats {


/**
 * Metric as a counter. E.g. # txns committed
 */
class CounterMetric : public AbstractMetric {
 public:

  CounterMetric(MetricType type);

  inline void Increment() {
    count++;
  }

  inline void Increment(int64_t count_) {
    count += count_;
  }

  inline void Decrement() {
    count--;
  }

  inline void Reset() {
    count = 0;
  }

  inline int64_t GetCounter() {
    return count;
  }

  void Aggregate(AbstractMetric &source);

  inline std::string ToString() {
    std::stringstream ss;
    ss << count << std::endl;
    return ss.str();
  }

 private:
  int64_t count;

};

}  // namespace stats
}  // namespace peloton
