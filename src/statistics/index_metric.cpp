//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// index_metric.cpp
//
// Identification: src/statistics/index_metric.cpp
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "statistics/index_metric.h"
#include "storage/storage_manager.h"
#include "index/index.h"

namespace peloton {
namespace stats {

IndexMetricOld::IndexMetricOld(MetricType type, oid_t database_id, oid_t table_id,
                         oid_t index_id)
    : AbstractMetricOld(type),
      database_id_(database_id),
      table_id_(table_id),
      index_id_(index_id) {
  index_name_ = "";
  try {
    auto index = storage::StorageManager::GetInstance()->GetIndexWithOid(
        database_id, table_id, index_id);
    index_name_ = index->GetName();
    for (auto& ch : index_name_) {
      ch = toupper(ch);
    }
  } catch (CatalogException& e) {
  }
}

void IndexMetricOld::Aggregate(AbstractMetricOld& source) {
  assert(source.GetType() == MetricType::INDEX);

  IndexMetricOld& index_metric = static_cast<IndexMetricOld&>(source);
  index_access_.Aggregate(index_metric.GetIndexAccess());
}

}  // namespace stats
}  // namespace peloton
