//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// database_catalog.cpp
//
// Identification: src/catalog/database_catalog.cpp
//
// Copyright (c) 2015-17, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "catalog/database_catalog.h"
#include "catalog/column_catalog.h"

namespace peloton {
namespace catalog {

DatabaseCatalog *DatabaseCatalog::GetInstance(storage::Database *pg_catalog,
                                              type::AbstractPool *pool,
                                              concurrency::Transaction *txn) {
  static std::unique_ptr<DatabaseCatalog> database_catalog(
      new DatabaseCatalog(pg_catalog, pool, txn));

  return database_catalog.get();
}

DatabaseCatalog::DatabaseCatalog(storage::Database *pg_catalog,
                                 type::AbstractPool *pool,
                                 concurrency::Transaction *txn)
    : AbstractCatalog(DATABASE_CATALOG_OID, DATABASE_CATALOG_NAME,
                      InitializeSchema().release(), pg_catalog) {
  // Insert columns into pg_attribute
  ColumnCatalog *pg_attribute = ColumnCatalog::GetInstance(pg_catalog, pool, txn);

  oid_t column_id = 0;
  for (auto column : catalog_table_->GetSchema()->GetColumns()) {
    pg_attribute->InsertColumn(DATABASE_CATALOG_OID, column.GetName(),
                               column_id, column.GetOffset(), column.GetType(),
                               column.IsInlined(), column.GetConstraints(),
                               pool, txn);
    column_id++;
  }
}

DatabaseCatalog::~DatabaseCatalog() {}

/*@brief   private function for initialize schema of pg_database
* @return  unqiue pointer to schema
*/
std::unique_ptr<catalog::Schema> DatabaseCatalog::InitializeSchema() {
  const std::string not_null_constraint_name = "not_null";
  const std::string primary_key_constraint_name = "primary_key";

  auto database_id_column = catalog::Column(
      type::Type::INTEGER, type::Type::GetTypeSize(type::Type::INTEGER),
      "database_oid", true);
  database_id_column.AddConstraint(catalog::Constraint(
      ConstraintType::PRIMARY, primary_key_constraint_name));
  database_id_column.AddConstraint(
      catalog::Constraint(ConstraintType::NOTNULL, not_null_constraint_name));

  auto database_name_column = catalog::Column(
      type::Type::VARCHAR, max_name_size, "database_name", false);
  database_name_column.AddConstraint(
      catalog::Constraint(ConstraintType::NOTNULL, not_null_constraint_name));

  std::unique_ptr<catalog::Schema> database_catalog_schema(
      new catalog::Schema({database_id_column, database_name_column}));
  return database_catalog_schema;
}

bool DatabaseCatalog::InsertDatabase(oid_t database_oid,
                                     const std::string &database_name,
                                     type::AbstractPool *pool,
                                     concurrency::Transaction *txn) {
  std::unique_ptr<storage::Tuple> tuple(
      new storage::Tuple(catalog_table_->GetSchema(), true));

  auto val0 = type::ValueFactory::GetIntegerValue(database_oid);
  auto val1 = type::ValueFactory::GetVarcharValue(database_name, nullptr);

  tuple->SetValue(0, val0, pool);
  tuple->SetValue(1, val1, pool);

  // Insert the tuple
  return InsertTuple(std::move(tuple), txn);
}

bool DatabaseCatalog::DeleteDatabase(oid_t database_oid,
                                     concurrency::Transaction *txn) {
  oid_t index_offset = 0;  // Index of database_oid
  std::vector<type::Value> values;
  values.push_back(type::ValueFactory::GetIntegerValue(database_oid).Copy());

  return DeleteWithIndexScan(index_offset, values, txn);
}

std::string DatabaseCatalog::GetDatabaseName(oid_t database_oid,
                                             concurrency::Transaction *txn) {
  std::vector<oid_t> column_ids({1});  // database_name
  oid_t index_offset = 0;              // Index of database_oid
  std::vector<type::Value> values;
  values.push_back(type::ValueFactory::GetIntegerValue(database_oid).Copy());

  auto result_tiles =
      GetResultWithIndexScan(column_ids, index_offset, values, txn);

  std::string database_name;
  PL_ASSERT(result_tiles->size() <= 1);  // database_oid is unique
  if (result_tiles->size() != 0) {
    PL_ASSERT((*result_tiles)[0]->GetTupleCount() <= 1);
    if ((*result_tiles)[0]->GetTupleCount() != 0) {
      database_name = (*result_tiles)[0]
                          ->GetValue(0, 0)
                          .ToString();  // After projection left 1 column
    }
  }

  return database_name;
}

oid_t DatabaseCatalog::GetDatabaseOid(const std::string &database_name,
                                      concurrency::Transaction *txn) {
  std::vector<oid_t> column_ids({0});  // database_oid
  oid_t index_offset = 1;              // Index of database_name
  std::vector<type::Value> values;
  values.push_back(
      type::ValueFactory::GetVarcharValue(database_name, nullptr).Copy());

  auto result_tiles =
      GetResultWithIndexScan(column_ids, index_offset, values, txn);

  oid_t database_oid = INVALID_OID;
  PL_ASSERT(result_tiles->size() <= 1);  // database_name is unique
  if (result_tiles->size() != 0) {
    PL_ASSERT((*result_tiles)[0]->GetTupleCount() <= 1);
    if ((*result_tiles)[0]->GetTupleCount() != 0) {
      database_oid = (*result_tiles)[0]
                         ->GetValue(0, 0)
                         .GetAs<oid_t>();  // After projection left 1 column
    }
  }

  return database_oid;
}

}  // End catalog namespace
}  // End peloton namespace
