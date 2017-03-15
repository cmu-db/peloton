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

DatabaseCatalog::DatabaseCatalog() {
  bool own_schema = true;
  bool adapt_table = false;
  bool is_catalog = true;
  auto database_schema = DatabaseCatalogSchema().release();
  oid_t database_id = START_OID;
  std::string database_name = DATABASE_CATALOG_NAME;

  // TODO:

  catalog_table_ =
      std::unique_ptr<storage::DataTable>(storage::TableFactory::GetDataTable(
          database_id, GetNextOid(), database_schema, database_name,
          DEFAULT_TUPLES_PER_TILEGROUP, own_schema, adapt_table, is_catalog));
}

std::unique_ptr<catalog::Schema> DatabaseCatalog::DatabaseCatalogSchema() {
  const std::string not_null_constraint_name = "not_null";

  auto id_column = catalog::Column(type::Type::INTEGER,
                                   type::Type::GetTypeSize(type::Type::INTEGER),
                                   "database_id", true);
  id_column.AddConstraint(
      catalog::Constraint(ConstraintType::NOTNULL, not_null_constraint_name));

  auto name_column = catalog::Column(type::Type::VARCHAR, max_name_size,
                                     "database_name", true);
  name_column.AddConstraint(
      catalog::Constraint(ConstraintType::NOTNULL, not_null_constraint_name));

  std::unique_ptr<catalog::Schema> database_catalog_schema(
      new catalog::Schema({id_column, name_column}));
  return database_catalog_schema;
}

}  // End catalog namespace
}  // End peloton namespace