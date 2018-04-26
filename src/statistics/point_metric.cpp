//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// point_metric.h
//
// Identification: src/statistics/point_metric.cpp
//
// Copyright (c) 2017-18, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <sstream>

#include "statistics/point_metric.h"

namespace peloton {
namespace stats {

PointMetric::PointMetric() {

}

void PointMetric::Reset() {
  // clear all oid->count mappings
  // TODO: would it be better to zero them all?
  counts_.Clear();
}

const std::string PointMetric::GetInfo() const {
  std::stringstream ss;
  // what is the right thing to display?
  return ss.str();
}

void PointMetric::Collect(oid_t id) {
  std::atomic<int64_t> *value;
  // get pointer to atomic counter
  counts_.Find(id, value);
  // atomically increment
  (*value).fetch_add(1);
}

} // namespace stats
} // namespace peloton
