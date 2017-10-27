//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// index_metrics_catalog.cpp
//
// Identification: src/catalog/index_metrics_catalog.cpp
//
// Copyright (c) 2015-17, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "catalog/index_metrics_catalog.h"

#include "executor/logical_tile.h"
#include "storage/data_table.h"
#include "type/value_factory.h"

namespace peloton {
namespace catalog {

IndexMetricsCatalog *IndexMetricsCatalog::GetInstance(
    concurrency::Transaction *txn) {
  static IndexMetricsCatalog index_metrics_catalog{txn};
  return &index_metrics_catalog;
}

IndexMetricsCatalog::IndexMetricsCatalog(concurrency::Transaction *txn)
    : AbstractCatalog("CREATE TABLE " CATALOG_DATABASE_NAME
                      "." INDEX_METRICS_CATALOG_NAME
                      " ("
                      "database_oid   INT NOT NULL, "
                      "table_oid      INT NOT NULL, "
                      "index_oid      INT NOT NULL, "
                      "reads          INT NOT NULL, "
                      "deletes        INT NOT NULL, "
                      "inserts        INT NOT NULL, "
                      "time_stamp     INT NOT NULL);",
                      txn) {
  // Add secondary index here if necessary
}

IndexMetricsCatalog::~IndexMetricsCatalog() {}

bool IndexMetricsCatalog::InsertIndexMetrics(
    oid_t database_oid, oid_t table_oid, oid_t index_oid, int64_t reads,
    int64_t deletes, int64_t inserts, int64_t time_stamp,
    type::AbstractPool *pool, concurrency::Transaction *txn) {
  std::unique_ptr<storage::Tuple> tuple(
      new storage::Tuple(catalog_table_->GetSchema(), true));

  auto val0 = type::ValueFactory::GetIntegerValue(database_oid);
  auto val1 = type::ValueFactory::GetIntegerValue(table_oid);
  auto val2 = type::ValueFactory::GetIntegerValue(index_oid);
  auto val3 = type::ValueFactory::GetIntegerValue(reads);
  auto val4 = type::ValueFactory::GetIntegerValue(deletes);
  auto val5 = type::ValueFactory::GetIntegerValue(inserts);
  auto val6 = type::ValueFactory::GetIntegerValue(time_stamp);

  tuple->SetValue(ColumnId::DATABASE_OID, val0, pool);
  tuple->SetValue(ColumnId::TABLE_OID, val1, pool);
  tuple->SetValue(ColumnId::INDEX_OID, val2, pool);
  tuple->SetValue(ColumnId::READS, val3, pool);
  tuple->SetValue(ColumnId::DELETES, val4, pool);
  tuple->SetValue(ColumnId::INSERTS, val5, pool);
  tuple->SetValue(ColumnId::TIME_STAMP, val6, pool);

  // Insert the tuple
  return InsertTuple(std::move(tuple), txn);
}

bool IndexMetricsCatalog::DeleteIndexMetrics(oid_t index_oid,
                                             concurrency::Transaction *txn) {
  oid_t index_offset = IndexId::PRIMARY_KEY;  // Primary key index

  std::vector<type::Value> values;
  values.push_back(type::ValueFactory::GetIntegerValue(index_oid).Copy());

  return DeleteWithIndexScan(index_offset, values, txn);
}

}  // namespace catalog
}  // namespace peloton
