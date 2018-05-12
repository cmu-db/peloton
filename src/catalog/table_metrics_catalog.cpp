//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// table_metrics_catalog.cpp
//
// Identification: src/catalog/table_metrics_catalog.cpp
//
// Copyright (c) 2015-17, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "catalog/table_metrics_catalog.h"

#include "executor/logical_tile.h"
#include "storage/data_table.h"
#include "type/value_factory.h"

namespace peloton {
namespace catalog {

TableMetricsCatalogObject::TableMetricsCatalogObject(
    executor::LogicalTile *tile, int tupleId)
    : table_oid_(
          tile->GetValue(tupleId, TableMetricsCatalog::ColumnId::TABLE_OID)
              .GetAs<oid_t>()),
      reads_(tile->GetValue(tupleId, TableMetricsCatalog::ColumnId::READS)
                 .GetAs<int64_t>()),
      updates_(tile->GetValue(tupleId, TableMetricsCatalog::ColumnId::UPDATES)
                   .GetAs<int64_t>()),
      inserts_(tile->GetValue(tupleId, TableMetricsCatalog::ColumnId::INSERTS)
                   .GetAs<int64_t>()),
      deletes_(tile->GetValue(tupleId, TableMetricsCatalog::ColumnId::DELETES)
                   .GetAs<int64_t>()),
      memory_alloc_(
          tile->GetValue(tupleId, TableMetricsCatalog::ColumnId::MEMORY_ALLOC)
              .GetAs<int64_t>()),
      memory_usage_(
          tile->GetValue(tupleId, TableMetricsCatalog::ColumnId::MEMORY_USAGE)
              .GetAs<int64_t>()),
      time_stamp_(
          tile->GetValue(tupleId, TableMetricsCatalog::ColumnId::TIME_STAMP)
              .GetAs<int64_t>()) {}

TableMetricsCatalog::TableMetricsCatalog(const std::string &database_name,
                                         concurrency::TransactionContext *txn)
    : AbstractCatalog("CREATE TABLE " + database_name +
                          "." CATALOG_SCHEMA_NAME "." TABLE_METRICS_CATALOG_NAME
                          " ("
                          "table_oid      INT NOT NULL PRIMARY KEY, "
                          "reads          INT NOT NULL, "
                          "updates        INT NOT NULL, "
                          "inserts        INT NOT NULL, "
                          "deletes        INT NOT NULL, "
                          "memory_alloc     INT NOT NULL, "
                          "memory_usage     INT NOT NULL, "
                          "time_stamp     INT NOT NULL);",
                      txn) {
  // Add secondary index here if necessary
}

TableMetricsCatalog::~TableMetricsCatalog() {}

bool TableMetricsCatalog::InsertTableMetrics(
    oid_t table_oid, int64_t reads, int64_t updates, int64_t inserts,
    int64_t deletes, int64_t memory_alloc, int64_t memory_usage,
    int64_t time_stamp, type::AbstractPool *pool,
    concurrency::TransactionContext *txn) {
  std::unique_ptr<storage::Tuple> tuple(
      new storage::Tuple(catalog_table_->GetSchema(), true));

  auto val1 = type::ValueFactory::GetIntegerValue(table_oid);
  auto val2 = type::ValueFactory::GetIntegerValue(reads);
  auto val3 = type::ValueFactory::GetIntegerValue(updates);
  auto val4 = type::ValueFactory::GetIntegerValue(inserts);
  auto val5 = type::ValueFactory::GetIntegerValue(deletes);
  auto val6 = type::ValueFactory::GetIntegerValue(memory_alloc);
  auto val7 = type::ValueFactory::GetIntegerValue(memory_usage);
  auto val8 = type::ValueFactory::GetIntegerValue(time_stamp);

  tuple->SetValue(ColumnId::TABLE_OID, val1, pool);
  tuple->SetValue(ColumnId::READS, val2, pool);
  tuple->SetValue(ColumnId::UPDATES, val3, pool);
  tuple->SetValue(ColumnId::INSERTS, val4, pool);
  tuple->SetValue(ColumnId::DELETES, val5, pool);
  tuple->SetValue(ColumnId::MEMORY_ALLOC, val6, pool);
  tuple->SetValue(ColumnId::MEMORY_USAGE, val7, pool);
  tuple->SetValue(ColumnId::TIME_STAMP, val8, pool);

  // Insert the tuple
  return InsertTuple(std::move(tuple), txn);
}

bool TableMetricsCatalog::DeleteTableMetrics(
    oid_t table_oid, concurrency::TransactionContext *txn) {
  oid_t index_offset = IndexId::PRIMARY_KEY;  // Primary key index

  std::vector<type::Value> values;
  values.push_back(type::ValueFactory::GetIntegerValue(table_oid).Copy());

  return DeleteWithIndexScan(index_offset, values, txn);
}

bool TableMetricsCatalog::UpdateTableMetrics(
    oid_t table_oid, int64_t reads, int64_t updates, int64_t inserts,
    int64_t deletes, int64_t memory_alloc, int64_t memory_usage,
    int64_t time_stamp, concurrency::TransactionContext *txn) {
  std::vector<oid_t> update_columns(all_column_ids_);
  std::vector<type::Value> update_values;

  update_values.push_back(
      type::ValueFactory::GetIntegerValue(table_oid).Copy());
  update_values.push_back(type::ValueFactory::GetIntegerValue(reads).Copy());
  update_values.push_back(type::ValueFactory::GetIntegerValue(updates).Copy());
  update_values.push_back(type::ValueFactory::GetIntegerValue(inserts).Copy());
  update_values.push_back(type::ValueFactory::GetIntegerValue(deletes).Copy());
  update_values.push_back(
      type::ValueFactory::GetIntegerValue(memory_alloc).Copy());
  update_values.push_back(
      type::ValueFactory::GetIntegerValue(memory_usage).Copy());
  update_values.push_back(
      type::ValueFactory::GetIntegerValue(time_stamp).Copy());

  std::vector<type::Value> scan_values;
  scan_values.push_back(type::ValueFactory::GetIntegerValue(table_oid));
  oid_t index_offset = IndexId::PRIMARY_KEY;

  // Update the tuple
  return UpdateWithIndexScan(update_columns, update_values, scan_values,
                             index_offset, txn);
}

std::shared_ptr<TableMetricsCatalogObject>
TableMetricsCatalog::GetTableMetricsObject(
    oid_t table_oid, concurrency::TransactionContext *txn) {
  if (txn == nullptr) {
    throw CatalogException("Transaction is invalid!");
  }

  // set up read query
  std::vector<oid_t> column_ids(all_column_ids_);
  oid_t index_offset = IndexId::PRIMARY_KEY;
  std::vector<type::Value> values;
  values.push_back(type::ValueFactory::GetIntegerValue(table_oid).Copy());

  auto result_tiles =
      GetResultWithIndexScan(column_ids, index_offset, values, txn);

  if (result_tiles->size() == 1 && (*result_tiles)[0]->GetTupleCount() == 1) {
    auto table_metric_object =
        std::make_shared<TableMetricsCatalogObject>((*result_tiles)[0].get());
    return table_metric_object;
  }

  return nullptr;
}

}  // namespace catalog
}  // namespace peloton
