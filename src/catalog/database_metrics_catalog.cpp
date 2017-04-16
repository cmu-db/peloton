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
#include "catalog/catalog.h"
#include "catalog/catalog_defaults.h"
#include "common/macros.h"

namespace peloton {
namespace catalog {

DatabaseMetricsCatalog *DatabaseMetricsCatalog::GetInstance(
    storage::Database *pg_catalog, type::AbstractPool *pool) {
  static std::unique_ptr<DatabaseMetricsCatalog> database_metrics_catalog(
      new DatabaseMetricsCatalog(pg_catalog, pool));

  return database_metrics_catalog.get();
}

DatabaseMetricsCatalog::DatabaseMetricsCatalog(
    UNUSED_ATTRIBUTE storage::Database *pg_catalog,
    UNUSED_ATTRIBUTE type::AbstractPool *pool)
    : AbstractCatalog(DATABASE_METRICS_CATALOG_NAME,
                      InitializeSchema().release()) {
  // Add secondary index here if necessary
}

DatabaseMetricsCatalog::~DatabaseMetricsCatalog() {}

// initialize schema for pg_databse_metrics
std::unique_ptr<catalog::Schema> Catalog::InitializeSchema() {
  const std::string not_null_constraint_name = "not_null";
  catalog::Constraint not_null_constraint(ConstraintType::NOTNULL,
                                          not_null_constraint_name);
  oid_t integer_type_size = type::Type::GetTypeSize(type::Type::INTEGER);
  type::Type::TypeId integer_type = type::Type::INTEGER;

  auto id_column =
      catalog::Column(integer_type, integer_type_size, "database_oid", true);
  id_column.AddConstraint(not_null_constraint);
  id_column.AddConstraint(
      catalog::Constraint(ConstraintType::PRIMARY, "primary_key"));

  auto txn_committed_column =
      catalog::Column(integer_type, integer_type_size, "txn_committed", true);
  txn_committed_column.AddConstraint(not_null_constraint);

  auto txn_aborted_column =
      catalog::Column(integer_type, integer_type_size, "txn_aborted", true);
  txn_aborted_column.AddConstraint(not_null_constraint);

  auto timestamp_column =
      catalog::Column(integer_type, integer_type_size, "time_stamp", true);
  timestamp_column.AddConstraint(not_null_constraint);

  std::unique_ptr<catalog::Schema> schema(new catalog::Schema(
      {id_column, txn_committed_column, txn_aborted_column, timestamp_column}));
  return schema;
}

bool DatabaseMetricsCatalog::InsertDatabaseMetrics(
    oid_t database_oid, oid_t txn_committed, oid_t txn_aborted,
    oid_t time_stamp, concurrency::Transaction *txn) {
  std::unique_ptr<storage::Tuple> tuple(
      new storage::Tuple(catalog_table_->GetSchema(), true));

  auto val0 = type::ValueFactory::GetIntegerValue(database_oid);
  auto val1 = type::ValueFactory::GetIntegerValue(txn_committed);
  auto val2 = type::ValueFactory::GetIntegerValue(txn_aborted);
  auto val3 = type::ValueFactory::GetIntegerValue(time_stamp);

  tuple->SetValue(0, val0, pool);
  tuple->SetValue(1, val1, pool);
  tuple->SetValue(2, val2, pool);
  tuple->SetValue(3, val3, pool);

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
      param_types = (*result_tiles)[0]->GetValue(0, 0).GetAs<oid_t>();
    }
  }

  return time_stamp;
}
