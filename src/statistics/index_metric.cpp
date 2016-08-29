//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// index_metric.cpp
//
// Identification: src/backend/statistics/index_metric.cpp
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "statistics/index_metric.h"
#include "catalog/manager.h"
#include "index/index.h"

namespace peloton {
namespace stats {

IndexMetric::IndexMetric(MetricType type, oid_t database_id, oid_t table_id,
                         oid_t index_id)
    : AbstractMetric(type),
      database_id_(database_id),
      table_id_(table_id),
      index_id_(index_id) {
  index_name_ = "";
  auto index = catalog::Manager::GetInstance().GetIndexWithOid(
      database_id, table_id, index_id);
  if (index == nullptr) {
    index_name_ = "";
  } else {
    index_name_ = index->GetName();
    for (auto& ch : index_name_) {
      ch = toupper(ch);
    }
  }
}

void IndexMetric::Aggregate(AbstractMetric& source) {
  assert(source.GetType() == INDEX_METRIC);

  IndexMetric& index_metric = static_cast<IndexMetric&>(source);
  index_access_.Aggregate(index_metric.GetIndexAccess());
}

}  // namespace stats
}  // namespace peloton
