//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// database_metric.cpp
//
// Identification: src/statistics/database_metric.cpp
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "concurrency/transaction_manager_factory.h"
#include "util/string_util.h"
#include "statistics/database_metric.h"
#include "common/macros.h"

namespace peloton {
namespace stats {

void DatabaseMetricRawData::WriteToCatalog() {
  // LOG_INFO("db metric write to catalog");
  // auto &txn_manager =
  // concurrency::TransactionManagerFactory::GetInstance();
  // auto txn = txn_manager.BeginTransaction();
  // auto time_since_epoch =
  // std::chrono::system_clock::now().time_since_epoch();
  // auto time_stamp =
  // std::chrono::duration_cast<std::chrono::seconds>(time_since_epoch).count();
  //
  // auto pool = new type::EphemeralPool();
  //
  // for (auto &entry : counters_) {
  //   oid_t database_oid = entry.first;
  //   auto &counts = entry.second;
  //   catalog::DatabaseMetricsCatalog::GetInstance()->InsertDatabaseMetrics(
  //     database_oid, counts.first, counts.second, time_stamp, pool, txn);
  // }
  //
  // txn_manager.CommitTransaction(txn);
}

DatabaseMetricOld::DatabaseMetricOld(MetricType type, oid_t database_id)
    : AbstractMetricOld(type), database_id_(database_id) {}

void DatabaseMetricOld::Aggregate(AbstractMetricOld &source) {
  PELOTON_ASSERT(source.GetType() == MetricType::DATABASE);

  DatabaseMetricOld &db_metric = static_cast<DatabaseMetricOld &>(source);
  txn_committed_.Aggregate(db_metric.GetTxnCommitted());
  txn_aborted_.Aggregate(db_metric.GetTxnAborted());
}

const std::string DatabaseMetricOld::GetInfo() const {
  std::stringstream ss;
  ss << peloton::GETINFO_THICK_LINE << std::endl;
  ss << "// DATABASE_ID " << database_id_ << std::endl;
  ss << peloton::GETINFO_THICK_LINE << std::endl;
  ss << "# transactions committed: " << txn_committed_.GetInfo() << std::endl;
  ss << "# transactions aborted:   " << txn_aborted_.GetInfo();
  return ss.str();
}

}  // namespace stats
}  // namespace peloton
