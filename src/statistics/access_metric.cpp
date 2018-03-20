//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// access_metric.cpp
//
// Identification: src/statistics/access_metric.cpp
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "statistics/access_metric.h"
#include "common/macros.h"

namespace peloton {
namespace stats {

void AccessMetric::Aggregate(AbstractMetric &source) {
  PL_ASSERT(source.GetType() == MetricType::ACCESS);

  auto access_metric = static_cast<AccessMetric &>(source);
  for (size_t i = 0; i < NUM_COUNTERS; ++i) {
    access_counters_[i].Aggregate(
        static_cast<CounterMetric &>(access_metric.GetAccessCounter(i)));
  }
}

}  // namespace stats
}  // namespace peloton
