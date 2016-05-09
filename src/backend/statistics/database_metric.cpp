//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// database_metric.cpp
//
// Identification: src/backend/statistics/database_metric.cpp
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "backend/statistics/database_metric.h"

namespace peloton {
namespace stats {

DatabaseMetric::DatabaseMetric(MetricType type, oid_t database_id)
    : AbstractMetric(type) {
  database_id_ = database_id;
}

void DatabaseMetric::Aggregate(AbstractMetric& source) {
  assert(source.GetType() == DATABASE_METRIC);

  DatabaseMetric& db_metric = static_cast<DatabaseMetric&>(source);
  txn_committed_.Aggregate(db_metric.GetTxnCommitted());
  txn_aborted_.Aggregate(db_metric.GetTxnAborted());
}

std::string DatabaseMetric::ToString() {
  std::stringstream ss;
  ss << "//"
        "===-----------------------------------------------------------------"
        "---===//" << std::endl;
  ss << "// DATABASE_ID " << database_id_ << std::endl;
  ss << "//"
        "===-----------------------------------------------------------------"
        "---===//" << std::endl;
  ss << "# transactions committed: " << txn_committed_.ToString() << std::endl;
  ss << "# transactions aborted:   " << txn_aborted_.ToString() << std::endl;
  return ss.str();
}

}  // namespace stats
}  // namespace peloton
