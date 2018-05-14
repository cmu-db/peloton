//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// database_metric.h
//
// Identification: src/include/statistics/database_metric.h
//
// Copyright (c) 2015-2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <string>
#include <sstream>

#include "catalog/manager.h"
#include "catalog/database_metrics_catalog.h"
#include "common/internal_types.h"
#include "statistics/abstract_metric.h"
#include "storage/tile_group.h"
#include "type/ephemeral_pool.h"

namespace peloton {

namespace concurrency {
class TransactionContext;
} // namespace concurrency

namespace stats {
class DatabaseMetricRawData : public AbstractRawData {
 public:
  inline void IncrementTxnCommited(oid_t database_id) {
    counters_[database_id].first++;
  }

  inline void IncrementTxnAborted(oid_t database_id) {
    counters_[database_id].second++;
  }

  void Aggregate(AbstractRawData &other) override {
    auto &other_db_metric = dynamic_cast<DatabaseMetricRawData &>(other);
    for (auto &entry : other_db_metric.counters_) {
      auto &this_counter = counters_[entry.first];
      auto &other_counter = entry.second;
      this_counter.first += other_counter.first;
      this_counter.second += other_counter.second;
    }
  }

  void UpdateAndPersist() override;

  // TODO(Tianyu): Pretty Print
  const std::string GetInfo() const override { return ""; }

 private:
  inline static std::pair<oid_t, oid_t> GetDBTableIdFromTileGroupOid(
      oid_t tile_group_id) {
    auto tile_group =
        catalog::Manager::GetInstance().GetTileGroup(tile_group_id);
    if (tile_group == nullptr) return {INVALID_OID, INVALID_OID};
    return {tile_group->GetDatabaseId(), tile_group->GetTableId()};
  }
  /**
   * Maps from database id to a pair of counters.
   *
   * First counter represents number of transactions committed and the second
   * one represents the number of transactions aborted.
   */
  std::unordered_map<oid_t, std::pair<int64_t, int64_t>> counters_;
};

class DatabaseMetric : public AbstractMetric<DatabaseMetricRawData> {
 public:
  inline void OnTransactionCommit(const concurrency::TransactionContext *, oid_t tile_group_id) override {
    oid_t database_id = GetDBTableIdFromTileGroupOid(tile_group_id).first;
    GetRawData()->IncrementTxnCommited(database_id);
  }

  inline void OnTransactionAbort(const concurrency::TransactionContext *, oid_t tile_group_id) override {
    oid_t database_id = GetDBTableIdFromTileGroupOid(tile_group_id).first;
    GetRawData()->IncrementTxnAborted(database_id);
  }

 private:
  inline static std::pair<oid_t, oid_t> GetDBTableIdFromTileGroupOid(
      oid_t tile_group_id) {
    auto tile_group =
        catalog::Manager::GetInstance().GetTileGroup(tile_group_id);
    if (tile_group == nullptr) return {INVALID_OID, INVALID_OID};
    return {tile_group->GetDatabaseId(), tile_group->GetTableId()};
  }
};

}  // namespace stats
}  // namespace peloton
