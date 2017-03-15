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

DatabaseCatalog *DatabaseCatalog::GetInstance(void) {
  static std::unique_ptr<DatabaseCatalog> database_catalog(
      new DatabaseCatalog());
  return database_catalog.get();
}

DatabaseCatalog::DatabaseCatalog() :
  AbstractCatalog(GetNextOid(), TABLE_CATALOG_NAME, InitializeTableCatalogSchema().release()) {}

std::unique_ptr<catalog::Schema> DatabaseCatalog::InitializeDatabaseCatalogSchema() {
  const std::string not_null_constraint_name = "not_null";
  const std::string primary_key_constraint_name = "primary_key";

  auto database_id_column = catalog::Column(type::Type::INTEGER,
                                   type::Type::GetTypeSize(type::Type::INTEGER),
                                   "database_id", true);
  database_id_column.AddConstraint(
      catalog::Constraint(ConstraintType::PRIMARY, primary_key_constraint_name));
  database_id_column.AddConstraint(
      catalog::Constraint(ConstraintType::NOTNULL, not_null_constraint_name));

  auto database_name_column = catalog::Column(type::Type::VARCHAR, max_name_size,
                                     "database_name", true);
  database_name_column.AddConstraint(
      catalog::Constraint(ConstraintType::NOTNULL, not_null_constraint_name));

  std::unique_ptr<catalog::Schema> database_catalog_schema(
      new catalog::Schema({database_id_column, database_name_column}));
  return database_catalog_schema;
}

std::unique_ptr<storage::Tuple> DatabaseCatalog::GetDatabaseCatalogTuple(
    oid_t database_id, std::string database_name,
    type::AbstractPool *pool) {
  std::unique_ptr<storage::Tuple> tuple(
      new storage::Tuple(catalog_table_->GetSchema(), true));
  
  auto val1 = type::ValueFactory::GetIntegerValue(database_id);
  auto val2 = type::ValueFactory::GetVarcharValue(database_name, nullptr);
  
  tuple->SetValue(0, val1, pool);
  tuple->SetValue(1, val2, pool);
  
  return std::move(tuple);
}

}  // End catalog namespace
}  // End peloton namespace