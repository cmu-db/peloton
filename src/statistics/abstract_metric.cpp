//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// abstract_metric.cpp
//
// Identification: src/statistics/abstract_metric.cpp
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "statistics/abstract_metric.h"

namespace peloton {
namespace stats {

AbstractMetricOld::AbstractMetricOld(MetricType type) { type_ = type; }

AbstractMetricOld::~AbstractMetricOld() {}

}  // namespace stats
}  // namespace peloton
