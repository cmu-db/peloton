//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// database_metrics_catalog.cpp
//
// Identification: src/catalog/query_metrics_catalog.cpp
//
// Copyright (c) 2015-17, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "catalog/database_metrics_catalog.h"

#include "executor/logical_tile.h"
#include "storage/data_table.h"
#include "storage/tuple.h"

namespace peloton {
namespace catalog {

DatabaseMetricsCatalog *DatabaseMetricsCatalog::GetInstance(
    concurrency::Transaction *txn) {
  static std::unique_ptr<DatabaseMetricsCatalog> database_metrics_catalog(
      new DatabaseMetricsCatalog(txn));

  return database_metrics_catalog.get();
}

DatabaseMetricsCatalog::DatabaseMetricsCatalog(concurrency::Transaction *txn)
    : AbstractCatalog("CREATE TABLE " CATALOG_DATABASE_NAME
                      "." DATABASE_METRICS_CATALOG_NAME
                      " ("
                      "database_oid  INT NOT NULL PRIMARY KEY, "
                      "txn_committed INT NOT NULL, "
                      "txn_aborted   INT NOT NULL, "
                      "time_stamp    INT NOT NULL);",
                      txn) {
  // Add secondary index here if necessary
}

DatabaseMetricsCatalog::~DatabaseMetricsCatalog() {}

bool DatabaseMetricsCatalog::InsertDatabaseMetrics(
    oid_t database_oid, oid_t txn_committed, oid_t txn_aborted,
    oid_t time_stamp, type::AbstractPool *pool, concurrency::Transaction *txn) {
  std::unique_ptr<storage::Tuple> tuple(
      new storage::Tuple(catalog_table_->GetSchema(), true));

  auto val0 = type::ValueFactory::GetIntegerValue(database_oid);
  auto val1 = type::ValueFactory::GetIntegerValue(txn_committed);
  auto val2 = type::ValueFactory::GetIntegerValue(txn_aborted);
  auto val3 = type::ValueFactory::GetIntegerValue(time_stamp);

  tuple->SetValue(ColumnId::DATABASE_OID, val0, pool);
  tuple->SetValue(ColumnId::TXN_COMMITTED, val1, pool);
  tuple->SetValue(ColumnId::TXN_ABORTED, val2, pool);
  tuple->SetValue(ColumnId::TIME_STAMP, val3, pool);

  // Insert the tuple into catalog table
  return InsertTuple(std::move(tuple), txn);
}

bool DatabaseMetricsCatalog::DeleteDatabaseMetrics(
    oid_t database_oid, concurrency::Transaction *txn) {
  oid_t index_offset = IndexId::PRIMARY_KEY;  // Primary key index

  std::vector<type::Value> values;
  values.push_back(type::ValueFactory::GetIntegerValue(database_oid).Copy());

  return DeleteWithIndexScan(index_offset, values, txn);
}

oid_t DatabaseMetricsCatalog::GetTimeStamp(oid_t database_oid,
                                           concurrency::Transaction *txn) {
  std::vector<oid_t> column_ids(
      {ColumnId::TIME_STAMP});                // projection column is time_stamp
  oid_t index_offset = IndexId::PRIMARY_KEY;  // Primary key index

  std::vector<type::Value> values;
  values.push_back(type::ValueFactory::GetIntegerValue(database_oid).Copy());

  auto result_tiles =
      GetResultWithIndexScan(column_ids, index_offset, values, txn);

  oid_t time_stamp = INVALID_OID;
  PL_ASSERT(result_tiles->size() <= 1);  // unique

  if (result_tiles->size() != 0) {
    PL_ASSERT((*result_tiles)[0]->GetTupleCount() <= 1);
    if ((*result_tiles)[0]->GetTupleCount() != 0) {
      time_stamp = (*result_tiles)[0]->GetValue(0, 0).GetAs<oid_t>();
    }
  }

  return time_stamp;
}

}  // namespace catalog
}  // namespace peloton
