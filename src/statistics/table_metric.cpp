//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// table_metric.cpp
//
// Identification: src/statistics/table_metric.cpp
//
// Copyright (c) 2015-2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "catalog/table_metrics_catalog.h"
#include "catalog/catalog.h"
#include "concurrency/transaction_manager_factory.h"
#include "statistics/table_metric.h"
#include "storage/data_table.h"
#include "storage/storage_manager.h"
#include "storage/tile.h"

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
    if (modified_tile_group_id_set_.find(tile_groups.first) ==
        modified_tile_group_id_set_.end())
      modified_tile_group_id_set_[tile_groups.first] =
          std::unordered_set<oid_t>();

    auto &this_set = modified_tile_group_id_set_[tile_groups.first];
    auto &other_set = tile_groups.second;
    for (auto tile_group_id : other_set) {
      this_set.insert(tile_group_id);
    }
  }
}

void TableMetricRawData::FetchData() {
  auto &tile_group_manager = catalog::Manager::GetInstance();
  auto pg_catalog = catalog::Catalog::GetInstance();
  auto &txn_manager = concurrency::TransactionManagerFactory::GetInstance();
  auto storage_manager = storage::StorageManager::GetInstance();
  for (auto &entry : modified_tile_group_id_set_) {
    auto &db_table_id = entry.first;
    auto &tile_group_ids = entry.second;

    // Begin a txn to avoid concurrency issue (i.e. Other people delete the
    // table)
    auto txn = txn_manager.BeginTransaction();
    try {
      auto tb_object = pg_catalog->GetTableObject(db_table_id.first,
                                                  db_table_id.second, txn);
    } catch (CatalogException &e) {
      txn_manager.CommitTransaction(txn);
      continue;
    }
    size_t inline_tuple_size =
        storage_manager->GetTableWithOid(db_table_id.first, db_table_id.second)
            ->GetSchema()
            ->GetLength();
    txn_manager.CommitTransaction(txn);

    for (oid_t tile_group_id : tile_group_ids) {
      auto tile_group = tile_group_manager.GetTileGroup(tile_group_id);
      if (tile_group == nullptr) continue;

      // Collect inline table
      counters_[db_table_id][INLINE_MEMORY_USAGE] +=
          tile_group->GetActiveTupleCount() *
          (inline_tuple_size + storage::TileGroupHeader::header_entry_size);

      // Colelct Varlen Memory stats
      for (size_t i = 0; i < tile_group->NumTiles(); i++) {
        counters_[db_table_id][VARLEN_MEMORY_ALLOC] +=
            tile_group->GetTile(i)->GetPool()->GetMemoryAlloc();
        counters_[db_table_id][VARLEN_MEMORY_USAGE] +=
            tile_group->GetTile(i)->GetPool()->GetMemoryUsage();
      }
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
        counts[INSERT], counts[INLINE_MEMORY_ALLOC],
        counts[INLINE_MEMORY_USAGE], time_stamp, nullptr, txn);
  }

  txn_manager.CommitTransaction(txn);
}

}  // namespace stats
}  // namespace peloton
