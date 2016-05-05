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

//===--------------------------------------------------------------------===//
// GUC Variables
//===--------------------------------------------------------------------===//


namespace peloton {
namespace stats {


CounterMetric::CounterMetric(MetricType type) : AbstractMetric(type) {
  count = 0;
}

void CounterMetric::Aggregate(CounterMetric &source) {
    count += source.GetCounter();
}

}  // namespace stats
}  // namespace peloton
