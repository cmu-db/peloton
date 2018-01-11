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
#include "common/internal_types.h"
#include "statistics/abstract_metric.h"
#include "statistics/access_metric.h"
#include "statistics/latency_metric.h"
#include "statistics/processor_metric.h"
#include "util/string_util.h"

namespace peloton {

namespace stats {

// Same type defined in network/marshal.h
typedef unsigned char uchar;

/**
 * Metric for the access of a query
 */
class QueryMetric : public AbstractMetric {
 public:
  // A wrapper of the query param buffer copy
  struct QueryParamBuf {
    uchar *buf = nullptr;
    int len = 0;

    // Default constructor
    QueryParamBuf() {}
    QueryParamBuf(uchar *buf, int len) : buf(buf), len(len) {}
  };

  // A wrapper of the query params used in prepared statement
  struct QueryParams {

    QueryParams(QueryParamBuf format_buf_copy, QueryParamBuf type_buf_copy,
                QueryParamBuf val_buf_copy, int num_params);

    // A copy of paramter format buffer
    QueryParamBuf format_buf_copy;

    // A copy of the types of the params
    QueryParamBuf type_buf_copy;

    // A copy of paramter value buffer
    QueryParamBuf val_buf_copy;

    // Number of parameters
    int num_params = 0;
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
    ss << peloton::GETINFO_SINGLE_LINE << std::endl;
    ss << "  QUERY " << query_name_ << std::endl;
    ss << peloton::GETINFO_SINGLE_LINE << std::endl;
    ss << query_access_.GetInfo();
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
  AccessMetric query_access_{MetricType::ACCESS};

  // Latency metric
  LatencyMetric latency_metric_{MetricType::LATENCY, 2};

  // Processor metric
  ProcessorMetric processor_metric_{MetricType::PROCESSOR};
};

}  // namespace stats
}  // namespace peloton
