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

#include "util/string_util.h"
#include "statistics/database_metric.h"
#include "common/macros.h"

namespace peloton {
namespace stats {

DatabaseMetric::DatabaseMetric(MetricType type, oid_t database_id)
    : AbstractMetric(type), database_id_(database_id) {}

void DatabaseMetric::Aggregate(AbstractMetric& source) {
  PL_ASSERT(source.GetType() == MetricType::DATABASE);

  DatabaseMetric& db_metric = static_cast<DatabaseMetric&>(source);
  txn_committed_.Aggregate(db_metric.GetTxnCommitted());
  txn_aborted_.Aggregate(db_metric.GetTxnAborted());
}

const std::string DatabaseMetric::GetInfo() const {
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
