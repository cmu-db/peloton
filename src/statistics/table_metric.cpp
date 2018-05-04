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

#include "catalog/table_metrics_catalog.h"
#include "concurrency/transaction_manager_factory.h"
#include "statistics/table_metric.h"
#include "storage/data_table.h"
#include "storage/storage_manager.h"

namespace peloton {
namespace stats {

void TableMetricRawData::Aggregate(AbstractRawData &other) {
  auto &other_table_data = dynamic_cast<TableMetricRawData &>(other);
  // Collect counters
  for (auto &entry : other_table_data.counters_) {
    if (counters_.find(entry.first) == counters_.end())
      counters_[entry.first] = std::vector<int64_t>(NUM_COUNTERS);

    auto &this_counter = counters_[entry.first];
    auto &other_counter = entry.second;
    for (size_t i = 0; i < NUM_COUNTERS; i++) {
      this_counter[i] += other_counter[i];
    }
  }

  // Collect referenced TileGroups
  for (auto &tile_groups : other_table_data.modified_tile_group_id_set_) {
    if (modified_tile_group_id_set_.find(tile_groups.first) == modified_tile_group_id_set_.end())
      modified_tile_group_id_set_[tile_groups.first] = std::unordered_set<oid_t>();

    auto &this_set = modified_tile_group_id_set_[tile_groups.first];
    auto &other_set = tile_groups.second;
    for (auto tile_group_id : other_set) {
      this_set.insert(tile_group_id);
    }
  }
}

void TableMetricRawData::WriteToCatalog() {
  auto &txn_manager = concurrency::TransactionManagerFactory::GetInstance();
  auto txn = txn_manager.BeginTransaction();
  auto time_since_epoch = std::chrono::system_clock::now().time_since_epoch();
  auto time_stamp = std::chrono::duration_cast<std::chrono::seconds>(
                        time_since_epoch).count();

  for (auto &entry : counters_) {
    oid_t database_oid = entry.first.first;
    oid_t table_oid = entry.first.second;
    auto &counts = entry.second;
    // TODO (Justin): currently incorrect, should actually read and then
    // increment,
    // since each aggregation period only knows the delta
    catalog::TableMetricsCatalog::GetInstance()->InsertTableMetrics(
        database_oid, table_oid, counts[READ], counts[UPDATE], counts[DELETE],
        counts[INSERT], counts[INLINE_MEMORY_ALLOC], counts[INLINE_MEMORY_USAGE], time_stamp,
        nullptr, txn);
  }

  txn_manager.CommitTransaction(txn);
}

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
}  // namespace stats
}  // namespace peloton
