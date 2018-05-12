//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// index_metric.cpp
//
// Identification: src/statistics/index_metric.cpp
//
// Copyright (c) 2015-2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "catalog/catalog.h"
#include "catalog/system_catalogs.h"
#include "catalog/index_metrics_catalog.h"
#include "concurrency/transaction_manager_factory.h"
#include "statistics/index_metric.h"
#include "storage/storage_manager.h"
#include "index/index.h"

namespace peloton {
namespace stats {

const std::vector<IndexMetricRawData::CounterType>
    IndexMetricRawData::COUNTER_TYPES = {
        IndexMetricRawData::CounterType::READ,
        IndexMetricRawData::CounterType::UPDATE,
        IndexMetricRawData::CounterType::INSERT,
        IndexMetricRawData::CounterType::DELETE,
        IndexMetricRawData::CounterType::MEMORY_ALLOC,
        IndexMetricRawData::CounterType::MEMORY_USAGE};

void IndexMetricRawData::Aggregate(AbstractRawData &other) {
  auto &other_index_metric = dynamic_cast<IndexMetricRawData &>(other);
  for (auto &entry : other_index_metric.counters_) {
    for (auto &counter_type : COUNTER_TYPES) {
      GetCounter(entry.first, counter_type) +=
          other_index_metric.GetCounter(entry.first, counter_type);
    }
  }
}

void IndexMetricRawData::WriteToCatalog() {
  auto &txn_manager = concurrency::TransactionManagerFactory::GetInstance();
  auto txn = txn_manager.BeginTransaction();
  auto time_since_epoch = std::chrono::system_clock::now().time_since_epoch();
  auto time_stamp = std::chrono::duration_cast<std::chrono::seconds>(
                        time_since_epoch).count();

  for (auto &entry : counters_) {
    oid_t database_oid = entry.first.first;
    oid_t index_oid = entry.first.second;
    oid_t table_oid = 0;  // FIXME!!

    auto &counts = entry.second;
    auto system_catalogs =
        catalog::Catalog::GetInstance()->GetSystemCatalogs(database_oid);
    auto index_metrics_catalog = system_catalogs->GetIndexMetricsCatalog();
    auto old_metric =
        index_metrics_catalog->GetIndexMetricsObject(index_oid, txn);

    if (old_metric == nullptr) {
      index_metrics_catalog->InsertIndexMetrics(
          index_oid, table_oid, counts[READ], counts[UPDATE], counts[INSERT],
          counts[DELETE], counts[MEMORY_ALLOC], counts[MEMORY_USAGE],
          time_stamp, nullptr, txn);
    } else {
      index_metrics_catalog->UpdateIndexMetrics(
          index_oid, table_oid, old_metric->GetReads() + counts[READ],
          old_metric->GetUpdates() + counts[UPDATE],
          old_metric->GetInserts() + counts[INSERT],
          old_metric->GetDeletes() + counts[DELETE],
          old_metric->GetMemoryAlloc() + counts[MEMORY_ALLOC],
          old_metric->GetMemoryUsage() + counts[MEMORY_USAGE], time_stamp, txn);
    }
  }

  txn_manager.CommitTransaction(txn);
}

}  // namespace stats
}  // namespace peloton
