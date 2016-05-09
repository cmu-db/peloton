//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// backend_stats_context.h
//
// Identification: src/backend/statistics/backend_stats_context.h
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <mutex>
#include <map>
#include <thread>
#include <unordered_map>
#include <iostream>
#include <sstream>

#include "backend/catalog/manager.h"
#include "backend/common/types.h"
#include "backend/index/index.h"
#include "backend/statistics/counter_metric.h"
#include "backend/statistics/database_metric.h"
#include "backend/statistics/index_metric.h"
#include "backend/statistics/latency_metric.h"
#include "backend/statistics/table_metric.h"
#include "backend/storage/database.h"

//===--------------------------------------------------------------------===//
// GUC Variables
//===--------------------------------------------------------------------===//


namespace peloton {
namespace stats {


/**
 * Context of backend stats as a singleton per thread
 */
class BackendStatsContext {
 public:

  BackendStatsContext(size_t max_latency_history);
  ~BackendStatsContext();

  inline std::thread::id GetThreadId() {
    return thread_id;
  }

  inline TableMetric* GetTableMetric(oid_t database_id, oid_t table_id) {
    TableMetric::TableKey table_key = TableMetric::GetKey(database_id, table_id);
    if (table_metrics_.find(table_key) == table_metrics_.end()) {
      table_metrics_[table_key] = new TableMetric{TABLE_METRIC, database_id, table_id};
    }
    return table_metrics_[table_key];
  }

  inline DatabaseMetric* GetDatabaseMetric(oid_t database_id) {
    if (database_metrics_.find(database_id) == database_metrics_.end()) {
      database_metrics_[database_id] = new DatabaseMetric(
          DATABASE_METRIC, database_id);
    }
    return database_metrics_[database_id];
  }

  inline IndexMetric* GetIndexMetric(oid_t database_id, oid_t table_id, oid_t index_id) {
    IndexMetric::IndexKey index_key = IndexMetric::GetKey(database_id, table_id, index_id);
    if (index_metrics_.find(index_key) == index_metrics_.end()) {
      index_metrics_[index_key] = new IndexMetric{INDEX_METRIC, database_id, table_id, index_id};
    }
    return index_metrics_[index_key];
  }

  inline LatencyMetric& GetTxnLatencyMetric() {
    return txn_latencies_;
  }

  /**
   * Aggregate another BackendStatsContext to myself
   */
  void Aggregate(BackendStatsContext &source);

  inline bool operator==(const BackendStatsContext &other) {
    return database_metrics_ == other.database_metrics_ &&
              table_metrics_ == other.table_metrics_    &&
              index_metrics_ == other.index_metrics_;
  }

  inline bool operator!=(const BackendStatsContext &other) {
    return !(*this == other);
  }

  /**
   * Reset all metrics
   * Including create new submetrics according to current database
   * status.
   */
  inline void Reset() {
    txn_latencies_.Reset();

    for (auto& database_item : database_metrics_) {
      database_item.second->Reset();
    }
    for (auto table_item : table_metrics_) {
      table_item.second->Reset();
    }
    for (auto index_item : index_metrics_) {
      index_item.second->Reset();
    }

    oid_t num_databases = catalog::Manager::GetInstance().GetDatabaseCount();
    for (oid_t i = 0; i < num_databases; ++i) {
      auto database = catalog::Manager::GetInstance().GetDatabase(i);
      oid_t database_id = database->GetOid();

      // Reset database metrics
      if (database_metrics_.find(database_id) == database_metrics_.end()) {
        database_metrics_[database_id] = new DatabaseMetric(
            DATABASE_METRIC, database_id);
      }

      // Reset table metrics
      oid_t num_tables = database->GetTableCount();
      for (oid_t j = 0; j < num_tables; ++j) {
        auto table = database->GetTable(j);
        oid_t table_id = table->GetOid();
        TableMetric::TableKey table_key = TableMetric::GetKey(
            database_id, table_id);

        if (table_metrics_.find(table_key) == table_metrics_.end()) {
          table_metrics_[table_key] = new TableMetric(TABLE_METRIC,
              database_id, table_id);
        }

        // Reset indexes metrics
        oid_t num_indexes = table->GetIndexCount();
        for (oid_t k = 0; k < num_indexes; ++k) {
          auto index = table->GetIndex(k);
          oid_t index_id = index->GetOid();
          IndexMetric::IndexKey index_key = IndexMetric::GetKey(
              database_id, table_id, index_id);

          if (index_metrics_.find(index_key) == index_metrics_.end()) {
            index_metrics_[index_key] = new IndexMetric(INDEX_METRIC,
                database_id, table_id, index_id);
          }
        }
      }
    }
  }

  inline std::string ToString() {
    std::stringstream ss;

    ss << txn_latencies_.ToString() << std::endl;

    for (auto database_item : database_metrics_) {
      oid_t database_id = database_item.second->GetDatabaseId();
      ss << database_item.second->ToString();

      for (auto table_item : table_metrics_) {
        if (table_item.second->GetDatabaseId() == database_id) {
          ss << table_item.second->ToString();

          oid_t table_id = table_item.second->GetTableId();
          for (auto index_item : index_metrics_) {
            if (index_item.second->GetDatabaseId() == database_id &&
                index_item.second->GetTableId() == table_id) {
              ss << index_item.second->ToString();
            }
          }
          if (!index_metrics_.empty()) {
            ss << std::endl;
          }
        }
        if (!table_metrics_.empty()) {
          ss << std::endl;
        }
      }
      if (!database_metrics_.empty()) {
        ss << std::endl;
      }
    }

    return ss.str();
  }


  // Global metrics

  // Database metrics
  std::unordered_map<oid_t, DatabaseMetric*> database_metrics_;

  // Table metrics
  std::unordered_map<TableMetric::TableKey, TableMetric*> table_metrics_;

  // Index metrics
  std::unordered_map<IndexMetric::IndexKey, IndexMetric*> index_metrics_;

 private:
  std::thread::id thread_id;
  LatencyMetric txn_latencies_;

};

}  // namespace stats
}  // namespace peloton
