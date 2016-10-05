//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// query_metric.h
//
// Identification: src/statistics/query_metric.h
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <string>
#include <sstream>

#include "common/types.h"
#include "statistics/abstract_metric.h"
#include "statistics/access_metric.h"
#include "statistics/latency_metric.h"
#include "statistics/processor_metric.h"

namespace peloton {
namespace stats {

/**
 * Metric for the access of a query
 */
class QueryMetric : public AbstractMetric {
 public:
  QueryMetric(MetricType type, std::string query_name, oid_t database_id);

  //===--------------------------------------------------------------------===//
  // ACCESSORS
  //===--------------------------------------------------------------------===//

  inline AccessMetric &GetQueryAccess() { return query_access_; }

  inline LatencyMetric &GetQueryLatency() { return latency_metric_; }

  inline ProcessorMetric &GetProcessorMetric() { return processor_metric_; }

  inline std::string GetName() { return query_name_; }

  inline oid_t GetDatabaseId() { return database_id_; }

  //===--------------------------------------------------------------------===//
  // HELPER FUNCTIONS
  //===--------------------------------------------------------------------===//

  inline void Reset() { query_access_.Reset(); }

  void Aggregate(AbstractMetric &source);

  inline const std::string GetInfo() const {
    std::stringstream ss;
    ss << "-----------------------------" << std::endl;
    ss << "  QUERY " << query_name_ << std::endl;
    ss << "-----------------------------" << std::endl;
    ss << query_access_.GetInfo() << std::endl;
    return ss.str();
  }

 private:
  //===--------------------------------------------------------------------===//
  // MEMBERS
  //===--------------------------------------------------------------------===//

  // The database ID of this query
  oid_t database_id_;

  // The name of this query
  std::string query_name_;

  // The number of tuple accesses
  AccessMetric query_access_{ACCESS_METRIC};

  // Latency metric
  LatencyMetric latency_metric_{LATENCY_METRIC, 2};

  // Processor metric
  ProcessorMetric processor_metric_{PROCESSOR_METRIC};
};

}  // namespace stats
}  // namespace peloton
