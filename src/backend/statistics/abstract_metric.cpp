//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// abstract_metric.cpp
//
// Identification: src/backend/statistics/abstract_metric.cpp
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <mutex>
#include <map>
#include <vector>

#include "backend/statistics/abstract_metric.h"

//===--------------------------------------------------------------------===//
// GUC Variables
//===--------------------------------------------------------------------===//


namespace peloton {
namespace stats {


AbstractMetric::AbstractMetric(MetricType type_) {
    type = type_;
}

AbstractMetric::~AbstractMetric() {}

}  // namespace stats
}  // namespace peloton
