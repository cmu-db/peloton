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

#include "statistics/query_metric.h"
#include "catalog/catalog.h"
#include "storage/data_table.h"

namespace peloton {
namespace stats {

QueryMetric::QueryMetric(MetricType type, std::string query_name,
                         oid_t database_id)
    : AbstractMetric(type),
      database_id_(database_id),
      query_name_(query_name) {}

void QueryMetric::Aggregate(AbstractMetric& source UNUSED_ATTRIBUTE) {}

}  // namespace stats
}  // namespace peloton
