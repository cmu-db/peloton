//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// database_metrics_catalog.cpp
//
// Identification: src/catalog/database_metrics_catalog.cpp
//
// Copyright (c) 2015-17, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "catalog/database_metrics_catalog.h"

#include "executor/logical_tile.h"
#include "storage/data_table.h"
#include "type/value_factory.h"

namespace peloton {
namespace catalog {

DatabaseMetricsCatalogObject::DatabaseMetricsCatalogObject(
    executor::LogicalTile *tile, int tupleId)
    : database_oid_(
          tile->GetValue(tupleId,
                         DatabaseMetricsCatalog::ColumnId::DATABASE_OID)
              .GetAs<oid_t>()),
      txn_committed_(
          tile->GetValue(tupleId,
                         DatabaseMetricsCatalog::ColumnId::TXN_COMMITTED)
              .GetAs<int64_t>()),
      txn_aborted_(
          tile->GetValue(tupleId, DatabaseMetricsCatalog::ColumnId::TXN_ABORTED)
              .GetAs<int64_t>()),
      time_stamp_(
          tile->GetValue(tupleId, DatabaseMetricsCatalog::ColumnId::TIME_STAMP)
              .GetAs<int64_t>()) {}

DatabaseMetricsCatalog *DatabaseMetricsCatalog::GetInstance(
    concurrency::TransactionContext *txn) {
  static DatabaseMetricsCatalog database_metrics_catalog{txn};
  return &database_metrics_catalog;
}

DatabaseMetricsCatalog::DatabaseMetricsCatalog(
    concurrency::TransactionContext *txn)
    : AbstractCatalog("CREATE TABLE " CATALOG_DATABASE_NAME
                      "." CATALOG_SCHEMA_NAME "." DATABASE_METRICS_CATALOG_NAME
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
    oid_t time_stamp, type::AbstractPool *pool,
    concurrency::TransactionContext *txn) {
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
    oid_t database_oid, concurrency::TransactionContext *txn) {
  oid_t index_offset = IndexId::PRIMARY_KEY;  // Primary key index

  std::vector<type::Value> values;
  values.push_back(type::ValueFactory::GetIntegerValue(database_oid).Copy());

  return DeleteWithIndexScan(index_offset, values, txn);
}

bool DatabaseMetricsCatalog::UpdateDatabaseMetrics(
    oid_t database_oid, oid_t txn_committed, oid_t txn_aborted,
    oid_t time_stamp, concurrency::TransactionContext *txn) {
  std::vector<oid_t> update_columns(all_column_ids_);
  std::vector<type::Value> update_values;

  update_values.push_back(
      type::ValueFactory::GetIntegerValue(database_oid).Copy());
  update_values.push_back(
      type::ValueFactory::GetIntegerValue(txn_committed).Copy());
  update_values.push_back(
      type::ValueFactory::GetIntegerValue(txn_aborted).Copy());
  update_values.push_back(
      type::ValueFactory::GetIntegerValue(time_stamp).Copy());

  std::vector<type::Value> scan_values;
  scan_values.push_back(type::ValueFactory::GetIntegerValue(database_oid));
  oid_t index_offset = IndexId::PRIMARY_KEY;

  return UpdateWithIndexScan(update_columns, update_values, scan_values,
                             index_offset, txn);
}

std::shared_ptr<DatabaseMetricsCatalogObject>
DatabaseMetricsCatalog::GetDatabaseMetricsObject(
    oid_t database_oid, concurrency::TransactionContext *txn) {
  if (txn == nullptr) {
    throw CatalogException("Transaction is invalid!");
  }

  // set up query
  std::vector<oid_t> column_ids(all_column_ids_);
  oid_t index_offset = IndexId::PRIMARY_KEY;
  std::vector<type::Value> values;
  values.push_back(type::ValueFactory::GetIntegerValue(database_oid).Copy());

  auto result_tiles =
      GetResultWithIndexScan(column_ids, index_offset, values, txn);

  if (result_tiles->size() == 1 && (*result_tiles)[0]->GetTupleCount() == 1) {
    auto database_metric_object =
        std::make_shared<DatabaseMetricsCatalogObject>(
            (*result_tiles)[0].get());
    return database_metric_object;
  }

  return nullptr;
}

}  // namespace catalog
}  // namespace peloton
