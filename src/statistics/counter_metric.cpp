//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// counter_metric.cpp
//
// Identification: src/statistics/counter_metric.cpp
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "statistics/counter_metric.h"
#include "common/macros.h"

namespace peloton {
namespace stats {

CounterMetric::CounterMetric(MetricType type) : AbstractMetric(type) {
  count_ = 0;
}

void CounterMetric::Aggregate(AbstractMetric &source) {
  PL_ASSERT(source.GetType() == MetricType::COUNTER);
  count_ += static_cast<CounterMetric &>(source).GetCounter();
}

}  // namespace stats
}  // namespace peloton
