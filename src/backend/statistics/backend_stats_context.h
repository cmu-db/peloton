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
#include "backend/statistics/access_metric.h"
#include "backend/statistics/counter_metric.h"
#include "backend/storage/database.h"

//===--------------------------------------------------------------------===//
// GUC Variables
//===--------------------------------------------------------------------===//


namespace peloton {
namespace stats {


/**
 * Context of backend stats as a singleton
 */
class BackendStatsContext {
 public:

  BackendStatsContext();
  ~BackendStatsContext();

  inline std::thread::id GetThreadId() {
    return thread_id;
  }

  inline AccessMetric* GetTableAccessMetric(__attribute__((unused)) oid_t database_id,
      oid_t table_id) {
    auto table_item = table_accesses_.find(table_id);
    if (table_item == table_accesses_.end() ) {
      table_accesses_[table_id] = new AccessMetric{MetricType::ACCESS_METRIC};
    }
    return table_accesses_[table_id];
  }

  void Aggregate(BackendStatsContext &source);

  inline void Reset() {
    txn_committed.Reset();
    txn_aborted.Reset();
    for (auto table_item : table_accesses_) {
      table_item.second->Reset();
    }
  }

  inline std::string ToString() {
    std::stringstream ss;
    ss <<  "txn_committed: " << txn_committed.ToString() << std::endl;
    ss <<  "txn_aborted: " << txn_aborted.ToString() << std::endl;

    //oid_t database_id = catalog::Manager::GetInstance().GetDatabase(0)->GetOid();
    for (auto table_item : table_accesses_) {
      //std::string table_name = catalog::Manager::GetInstance().GetTableWithOid(database_id, table_item.first)->GetName();
      ss << "Num Databases: " << catalog::Manager::GetInstance().GetDatabaseCount() << std::endl;
      ss << "Table " << table_item.first << ": " << table_item.second->ToString()
          << std::endl << std::endl;
    }
    return ss.str();
  }


  // Global metrics
  CounterMetric txn_committed{MetricType::COUNTER_METRIC};
  CounterMetric txn_aborted{MetricType::COUNTER_METRIC};

  // Table metrics
  std::unordered_map<oid_t, AccessMetric*> table_accesses_;

  // Index metrics
  std::unordered_map<oid_t, AccessMetric*> index_accesses_;

 private:
  std::thread::id thread_id;

};

}  // namespace stats
}  // namespace peloton
