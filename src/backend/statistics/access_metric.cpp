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


AccessMetric::AccessMetric(MetricType type) : AbstractMetric(type) {
  read_counts_ = 0;
  update_counts_ = 0;
  insert_counts_ = 0;
  delete_counts_ = 0;
}

void AccessMetric::Aggregate(AbstractMetric &source) {
  assert(source.GetType() == ACCESS_METRIC);

  auto access_metric = static_cast<AccessMetric&>(source);
  read_counts_ += access_metric.GetReads();
  update_counts_ += access_metric.GetUpdates();
  insert_counts_ += access_metric.GetInserts();
  delete_counts_ += access_metric.GetDeletes();
}

}  // namespace stats
}  // namespace peloton
