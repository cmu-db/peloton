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

#include "catalog/index_metrics_catalog.h"
#include "concurrency/transaction_manager_factory.h"
#include "statistics/index_metric.h"
#include "storage/storage_manager.h"
#include "index/index.h"

namespace peloton {
namespace stats {

void IndexMetricRawData::Aggregate(AbstractRawData &other) {
  auto &other_index_metric = dynamic_cast<IndexMetricRawData &>(other);
  for (auto &entry : other_index_metric.counters_) {
    auto &this_counter = counters_[entry.first];
    auto &other_counter = entry.second;
    for (size_t i = 0; i < NUM_COUNTERS; i++) {
      this_counter[i] += other_counter[i];
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
    catalog::IndexMetricsCatalog::GetInstance()->InsertIndexMetrics(
        database_oid, table_oid, index_oid, counters_[READ], counters_[DELETE],
        counters_[INSERT], time_stamp, nullptr, txn);
  }

  txn_manager.CommitTransaction(txn);
}

IndexMetricOld::IndexMetricOld(MetricType type, oid_t database_id,
                               oid_t table_id, oid_t index_id)
    : AbstractMetricOld(type),
      database_id_(database_id),
      table_id_(table_id),
      index_id_(index_id) {
  index_name_ = "";
  try {
    auto index = storage::StorageManager::GetInstance()->GetIndexWithOid(
        database_id, table_id, index_id);
    index_name_ = index->GetName();
    for (auto &ch : index_name_) {
      ch = toupper(ch);
    }
  } catch (CatalogException &e) {
  }
}

void IndexMetricOld::Aggregate(AbstractMetricOld &source) {
  assert(source.GetType() == MetricType::INDEX);

  IndexMetricOld &index_metric = static_cast<IndexMetricOld &>(source);
  index_access_.Aggregate(index_metric.GetIndexAccess());
}

}  // namespace stats
}  // namespace peloton
