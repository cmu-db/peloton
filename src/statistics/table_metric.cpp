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

#include "statistics/table_metric.h"
#include "storage/data_table.h"
#include "storage/storage_manager.h"

namespace peloton {
namespace stats {

TableMetricOld::TableMetricOld(MetricType type, oid_t database_id,
                               oid_t table_id)
    : AbstractMetricOld(type), database_id_(database_id), table_id_(table_id) {
  try {
    auto table = storage::StorageManager::GetInstance()->GetTableWithOid(
        database_id, table_id);
    table_name_ = table->GetName();
    for (auto &ch : table_name_) ch = toupper(ch);
  } catch (CatalogException &e) {
    table_name_ = "";
  }
}

void TableMetricOld::Aggregate(AbstractMetricOld &source) {
  assert(source.GetType() == MetricType::TABLE);

  TableMetricOld &table_metric = static_cast<TableMetricOld &>(source);
  table_access_.Aggregate(table_metric.GetTableAccess());
  table_memory_.Aggregate(table_metric.GetTableMemory());
}

void TableMetric::Aggregate(AbstractMetric &other) {
  auto &other_index_metric = dynamic_cast<TableMetricRawData &>(other);
  for (auto &entry : other_index_metric.counters_) {
    auto &this_counter = counters_[entry.first];
    auto &other_counter = entry.second;
    for (size_t i = 0; i < NUM_COUNTERS; i++) {
      this_counter[i] += other_counter[i];
    }
  }
}

}  // namespace stats
}  // namespace peloton
