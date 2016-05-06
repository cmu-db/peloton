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


#include "backend/statistics/access_metric.h"

//===--------------------------------------------------------------------===//
// GUC Variables
//===--------------------------------------------------------------------===//


namespace peloton {
namespace stats {


void AccessMetric::Aggregate(AbstractMetric &source) {
  assert(source.GetType() == ACCESS_METRIC);

  auto access_metric = static_cast<AccessMetric&>(source);
  for (size_t i = 0; i < NUM_COUNTERS; ++i) {
    access_counters_[i].Aggregate(static_cast<CounterMetric&>(
        access_metric.GetAccessCounter(i)));
  }
}

}  // namespace stats
}  // namespace peloton
