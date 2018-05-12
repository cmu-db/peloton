//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// table_metric.h
//
// Identification: src/include/statistics/table_metric.h
//
// Copyright (c) 2015-2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <sstream>
#include <string>

#include "catalog/manager.h"
#include "common/internal_types.h"
#include "statistics/abstract_metric.h"
#include "storage/tile_group.h"
#include "util/string_util.h"

namespace peloton {
namespace stats {
class TableMetricRawData : public AbstractRawData {
  // this serves as an index into each table's counter vector
  enum CounterType {
    READ = 0,
    UPDATE,
    INSERT,
    DELETE,
    INLINE_MEMORY_ALLOC,
    INLINE_MEMORY_USAGE,
    VARLEN_MEMORY_ALLOC,
    VARLEN_MEMORY_USAGE
  };

 public:
  inline void IncrementTableReads(std::pair<oid_t, oid_t> db_table_id) {
    GetCounter(db_table_id, READ)++;
  }

  inline void IncrementTableUpdates(std::pair<oid_t, oid_t> db_table_id) {
    GetCounter(db_table_id, UPDATE)++;
  }

  inline void IncrementTableInserts(std::pair<oid_t, oid_t> db_table_id) {
    GetCounter(db_table_id, INSERT)++;
  }

  inline void IncrementTableDeletes(std::pair<oid_t, oid_t> db_table_id) {
    GetCounter(db_table_id, DELETE)++;
  }

  inline void IncrementTableMemAlloc(std::pair<oid_t, oid_t> db_table_id,
                                     int64_t bytes) {
    GetCounter(db_table_id, INLINE_MEMORY_ALLOC) += bytes;
  }

  inline void DecrementTableMemAlloc(std::pair<oid_t, oid_t> db_table_id,
                                     int64_t bytes) {
    GetCounter(db_table_id, INLINE_MEMORY_ALLOC) -= bytes;
  }

  inline void AddModifiedTileGroup(std::pair<oid_t, oid_t> db_table_id,
                                   oid_t tile_group_id) {
    auto tile_group_set = modified_tile_group_id_set_.find(db_table_id);
    if (tile_group_set == modified_tile_group_id_set_.end())
      modified_tile_group_id_set_[db_table_id] = std::unordered_set<oid_t>();

    modified_tile_group_id_set_[db_table_id].insert(tile_group_id);
  }

  void Aggregate(AbstractRawData &other) override;

  void WriteToCatalog() override;

  const std::string GetInfo() const override { return "table metric"; }

  /**
   * Fetch Usage for inlined tile memory and both allocation and usage for
   * varlen pool
   */
  void FetchData() override;

 private:
  inline int64_t &GetCounter(std::pair<oid_t, oid_t> db_table_id,
                             CounterType type) {
    auto entry = counters_.find(db_table_id);
    if (entry == counters_.end())
      counters_[db_table_id] = std::vector<int64_t>(NUM_COUNTERS);
    return counters_[db_table_id][type];
  }
  std::unordered_map<std::pair<oid_t, oid_t>, std::vector<int64_t>, pair_hash>
      counters_;

  // list of counter types (used by Aggregate)
  static const std::vector<CounterType> COUNTER_TYPES;
  // should be number of possible CounterType values
  static const size_t NUM_COUNTERS = 8;

  std::unordered_map<std::pair<oid_t, oid_t>, std::unordered_set<oid_t>,
                     pair_hash> modified_tile_group_id_set_;
};

class TableMetric : public AbstractMetric<TableMetricRawData> {
 public:
  inline void OnTupleRead(const concurrency::TransactionContext *, oid_t tile_group_id) override {
    auto db_table_id = GetDBTableIdFromTileGroupOid(tile_group_id);
    if (db_table_id.second == INVALID_OID) return;
    GetRawData()->IncrementTableReads(db_table_id);
  }

  inline void OnTupleUpdate(const concurrency::TransactionContext *, oid_t tile_group_id) override {
    auto db_table_id = GetDBTableIdFromTileGroupOid(tile_group_id);
    if (db_table_id.second == INVALID_OID) return;
    GetRawData()->AddModifiedTileGroup(db_table_id, tile_group_id);
    GetRawData()->IncrementTableUpdates(db_table_id);
  }

  inline void OnTupleInsert(const concurrency::TransactionContext *, oid_t tile_group_id) override {
    auto db_table_id = GetDBTableIdFromTileGroupOid(tile_group_id);
    if (db_table_id.second == INVALID_OID) return;
    GetRawData()->AddModifiedTileGroup(db_table_id, tile_group_id);
    GetRawData()->IncrementTableInserts(db_table_id);
  }

  inline void OnTupleDelete(const concurrency::TransactionContext *, oid_t tile_group_id) override {
    auto db_table_id = GetDBTableIdFromTileGroupOid(tile_group_id);
    if (db_table_id.second == INVALID_OID) return;
    GetRawData()->AddModifiedTileGroup(db_table_id, tile_group_id);
    GetRawData()->IncrementTableDeletes(db_table_id);
  }

  inline void OnMemoryAlloc(std::pair<oid_t, oid_t> db_table_id,
                            size_t bytes) override {
    GetRawData()->IncrementTableMemAlloc(db_table_id, bytes);
  }

  inline void OnMemoryFree(std::pair<oid_t, oid_t> db_table_id,
                           size_t bytes) override {
    GetRawData()->DecrementTableMemAlloc(db_table_id, bytes);
  }

 private:
  inline static std::pair<oid_t, oid_t> GetDBTableIdFromTileGroupOid(
      oid_t tile_group_id) {
    auto tile_group =
        catalog::Manager::GetInstance().GetTileGroup(tile_group_id);
    if (tile_group == nullptr) {
      return std::pair<oid_t, oid_t>(INVALID_OID, INVALID_OID);
    }
    return std::pair<oid_t, oid_t>(tile_group->GetDatabaseId(),
                                   tile_group->GetTableId());
  }
};

}  // namespace stats
}  // namespace peloton
