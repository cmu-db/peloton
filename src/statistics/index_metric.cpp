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
    // TODO(tianyu): fix name
    catalog::IndexMetricsCatalog::GetInstance("")->InsertIndexMetrics(
        database_oid, table_oid, index_oid, counts[READ], counts[DELETE],
        counts[INSERT], time_stamp, nullptr, txn);
  }

  txn_manager.CommitTransaction(txn);
}

}  // namespace stats
}  // namespace peloton
