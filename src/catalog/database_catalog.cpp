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

#pragma once

#include "catalog/database_catalog.h"

namespace peloton {
namespace catalog {

DatabaseCatalog *DatabaseCatalog::GetInstance(storage::Database *pg_catalog,
                                              type::AbstractPool *pool) {
  static std::unique_ptr<DatabaseCatalog> database_catalog(
      new DatabaseCatalog(pg_catalog, pool));
  return database_catalog.get();
}

DatabaseCatalog::DatabaseCatalog(storage::Database *pg_catalog,
                                 type::AbstractPool *pool)
    : AbstractCatalog(DATABASE_CATALOG_OID, DATABASE_CATALOG_NAME,
                      InitializeSchema().release(), pg_catalog, pool) {}

DatabaseCatalog::~DatabaseCatalog() {}

std::unique_ptr<catalog::Schema> DatabaseCatalog::InitializeSchema() {
  const std::string not_null_constraint_name = "not_null";
  const std::string primary_key_constraint_name = "primary_key";

  auto database_id_column = catalog::Column(
      type::Type::INTEGER, type::Type::GetTypeSize(type::Type::INTEGER),
      "database_id", true);
  database_id_column.AddConstraint(catalog::Constraint(
      ConstraintType::PRIMARY, primary_key_constraint_name));
  database_id_column.AddConstraint(
      catalog::Constraint(ConstraintType::NOTNULL, not_null_constraint_name));

  auto database_name_column = catalog::Column(
      type::Type::VARCHAR, max_name_size, "database_name", true);
  database_name_column.AddConstraint(
      catalog::Constraint(ConstraintType::NOTNULL, not_null_constraint_name));

  std::unique_ptr<catalog::Schema> database_catalog_schema(
      new catalog::Schema({database_id_column, database_name_column}));
  return database_catalog_schema;
}

bool DatabaseCatalog::Insert(oid_t database_id,
                             const std::string &database_name,
                             type::AbstractPool *pool,
                             concurrency::Transaction *txn) {
  std::unique_ptr<storage::Tuple> tuple(
      new storage::Tuple(catalog_table_->GetSchema(), true));

  auto val0 = type::ValueFactory::GetIntegerValue(database_id);
  auto val1 = type::ValueFactory::GetVarcharValue(database_name, nullptr);

  tuple->SetValue(0, val0, pool);
  tuple->SetValue(1, val1, pool);

  // Insert the tuple
  return InsertTuple(std::move(tuple), txn);
}

bool DatabaseCatalog::DeleteByOid(oid_t database_id,
                                  concurrency::Transaction *txn) {
  oid_t index_offset = 0;  // Index of database_id
  std::vector<type::Value> values;
  values.push_back(type::ValueFactory::GetIntegerValue(database_id).Copy());

  return DeleteWithIndexScan(index_offset, values, txn);
}

std::string DatabaseCatalog::GetNameByOid(oid_t database_id,
                                          concurrency::Transaction *txn) {
  std::vector<oid_t> column_ids({1});  // database_name
  oid_t index_offset = 0;              // Index of database_id
  std::vector<type::Value> values;
  values.push_back(type::ValueFactory::GetIntegerValue(database_id).Copy());

  auto result = GetResultWithIndexScan(column_ids, index_offset, values, txn);

  std::string database_name;
  PL_ASSERT(result->GetTupleCount() <= 1);  // database_id is unique
  if (result->GetTupleCount() != 0) {
    database_name =
        result->GetValue(0, 0)
            .GetAs<std::string>();  // After projection left 1 column
  }

  return database_name;
}

oid_t DatabaseCatalog::GetOidByName(const std::string &database_name,
                                    concurrency::Transaction *txn) {
  std::vector<oid_t> column_ids({0});  // database_id
  oid_t index_offset = 1;              // Index of database_name
  std::vector<type::Value> values;
  values.push_back(
      type::ValueFactory::GetVarcharValue(database_name, nullptr).Copy());

  auto result = GetResultWithIndexScan(column_ids, index_offset, values, txn);

  oid_t database_id = INVALID_OID;
  PL_ASSERT(result->GetTupleCount() <=
            1);  // table_name + database_name is unique
  if (result->GetTupleCount() != 0) {
    database_id = result->GetValue(0, 0)
                      .GetAs<oid_t>();  // After projection left 1 column
  }

  return database_id;
}

}  // End catalog namespace
}  // End peloton namespace