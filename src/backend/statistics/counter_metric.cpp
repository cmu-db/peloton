//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// counter_metric.cpp
//
// Identification: src/backend/statistics/counter_metric.cpp
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "backend/statistics/counter_metric.h"

namespace peloton {
namespace stats {

CounterMetric::CounterMetric(MetricType type) : AbstractMetric(type) {
  count_ = 0;
}

void CounterMetric::Aggregate(AbstractMetric &source) {
  assert(source.GetType() == COUNTER_METRIC);
  count_ += static_cast<CounterMetric &>(source).GetCounter();
}

}  // namespace stats
}  // namespace peloton
