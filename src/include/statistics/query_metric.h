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
#include <vector>
#include "common/types.h"
#include "statistics/abstract_metric.h"
#include "statistics/access_metric.h"
#include "statistics/latency_metric.h"
#include "statistics/processor_metric.h"

namespace peloton {

namespace stats {

// Same type defined in wire/marshal.h
typedef unsigned char uchar;

/**
 * Metric for the access of a query
 */
class QueryMetric : public AbstractMetric {
 public:
  class QueryParams {

   public:
    QueryParams(std::vector<uchar> &format_buf_copy,
                std::vector<int32_t> &param_types_,
                std::shared_ptr<std::vector<uchar>> val_buf_copy,
                int num_params);

    // TODO constructor from common::Value

    inline int GetNumParams() { return num_params_; }

    inline std::vector<uchar> &GetFormatBuf() { return format_buf_copy_; }

    inline std::shared_ptr<std::vector<uchar>> GetValueBuf() {
      return val_buf_copy_;
    }

   private:
    // A copy of paramter format buffer
    std::vector<uchar> format_buf_copy_;

    // The types of the params
    std::vector<int32_t> param_types_;

    // A copy of paramter value buffer
    std::shared_ptr<std::vector<uchar>> val_buf_copy_;

    // Number of parameters
    int num_params_ = 0;
  };

  QueryMetric(MetricType type, const std::string &query_name,
              std::shared_ptr<QueryParams> query_params,
              const oid_t database_id);

  //===--------------------------------------------------------------------===//
  // ACCESSORS
  //===--------------------------------------------------------------------===//

  inline AccessMetric &GetQueryAccess() { return query_access_; }

  inline LatencyMetric &GetQueryLatency() { return latency_metric_; }

  inline ProcessorMetric &GetProcessorMetric() { return processor_metric_; }

  inline std::string GetName() const { return query_name_; }

  inline oid_t GetDatabaseId() const { return database_id_; }

  inline std::shared_ptr<QueryParams> const GetQueryParams() {
    return query_params_;
  }

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

  // The parameter of this query
  std::shared_ptr<QueryParams> query_params_;

  // The number of tuple accesses
  AccessMetric query_access_{ACCESS_METRIC};

  // Latency metric
  LatencyMetric latency_metric_{LATENCY_METRIC, 2};

  // Processor metric
  ProcessorMetric processor_metric_{PROCESSOR_METRIC};
};

}  // namespace stats
}  // namespace peloton
