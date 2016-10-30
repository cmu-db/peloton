//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// table_metric.cpp
//
// Identification: src/statistics/table_metric.cpp
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "common/macros.h"
#include "common/logger.h"
#include "statistics/query_metric.h"
#include "catalog/catalog.h"
#include "storage/data_table.h"

namespace peloton {
namespace stats {

QueryMetric::QueryMetric(MetricType type, const std::string& query_name,
                         std::shared_ptr<QueryParams> query_params,
                         const oid_t database_id)
    : AbstractMetric(type),
      database_id_(database_id),
      query_name_(query_name),
      query_params_(query_params) {
  latency_metric_.StartTimer();
  processor_metric_.StartTimer();
  LOG_TRACE("Query metric initialized");
}

QueryMetric::QueryParams::QueryParams(
    std::vector<uchar>& format_buf_copy, std::vector<int32_t>& param_types_,
    std::shared_ptr<std::vector<uchar>> val_buf_copy, int num_params)
    : format_buf_copy_(format_buf_copy),
      param_types_(param_types_),
      val_buf_copy_(val_buf_copy),
      num_params_(num_params) {}

void QueryMetric::Aggregate(AbstractMetric& source UNUSED_ATTRIBUTE) {}

}  // namespace stats
}  // namespace peloton
