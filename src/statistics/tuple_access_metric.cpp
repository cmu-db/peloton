//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// tuple_access_metric.cpp
//
// Identification: src/statistics/tuple_access_metric.cpp
//
// Copyright (c) 2015-18, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "catalog/catalog.h"
#include "concurrency/transaction_manager_factory.h"
#include "statistics/tuple_access_metric.h"
#include "catalog/tuple_access_catalog.h"

namespace peloton {
namespace stats {
void TupleAccessRawData::WriteToCatalog(txn_id_t tid,
                                        bool complete,
                                        bool commit,
                                        concurrency::TransactionContext *txn) {
  auto catalog = catalog::TupleAccessMetricsCatalog::GetInstance(txn);
  auto old = catalog->GetTupleAccessMetricsCatalogObject(tid, txn);
  if (old == nullptr)
    catalog->InsertAccessMetric(tid,
                                tuple_access_counters_[tid],
                                complete,
                                commit,
                                nullptr,
                                txn);
  else
    catalog->UpdateAccessMetrics(tid,
                                 old->GetReads() + tuple_access_counters_[tid],
                                 complete,
                                 commit,
                                 txn);
}
void TupleAccessRawData::UpdateAndPersist() {
  auto &txn_manager = concurrency::TransactionManagerFactory::GetInstance();
  auto txn = txn_manager.BeginTransaction();

  for (auto &entry : tuple_access_counters_) {
    auto tid = entry.first;
    if (!(commits_.find(tid) == commits_.end()))
      WriteToCatalog(tid, true, true, txn);
    else if (!(aborts_.find(tid) == aborts_.end()))
      WriteToCatalog(tid, true, false, txn);
    else
      WriteToCatalog(tid, false, false, txn);
  }
}

} // namespace stats
} // namespace peloton