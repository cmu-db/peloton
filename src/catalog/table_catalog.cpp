//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// table_catalog.cpp
//
// Identification: src/catalog/table_catalog.cpp
//
// Copyright (c) 2015-17, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include "catalog/table_catalog.h"

namespace peloton {
namespace catalog {

TableCatalog *TableCatalog::GetInstance(void) {
  static std::unique_ptr<TableCatalog> table_catalog(new TableCatalog());
  return table_catalog.get();
}

TableCatalog::TableCatalog()
    : AbstractCatalog(TABLE_CATALOG_OID, TABLE_CATALOG_NAME,
                      InitializeTableCatalogSchema().release()) {}

std::unique_ptr<catalog::Schema> TableCatalog::InitializeTableCatalogSchema() {
  const std::string primary_key_constraint_name = "primary_key";
  const std::string not_null_constraint_name = "not_null";

  auto table_id_column = catalog::Column(
      type::Type::INTEGER, type::Type::GetTypeSize(type::Type::INTEGER),
      "table_id", true);
  table_id_column.AddConstraint(catalog::Constraint(
      ConstraintType::PRIMARY, primary_key_constraint_name));
  table_id_column.AddConstraint(
      catalog::Constraint(ConstraintType::NOTNULL, not_null_constraint_name));

  auto table_name_column =
      catalog::Column(type::Type::VARCHAR, max_name_size, "table_name", true);
  table_name_column.AddConstraint(
      catalog::Constraint(ConstraintType::NOTNULL, not_null_constraint_name));

  auto database_id_column = catalog::Column(
      type::Type::INTEGER, type::Type::GetTypeSize(type::Type::INTEGER),
      "database_id", true);
  database_id_column.AddConstraint(
      catalog::Constraint(ConstraintType::NOTNULL, not_null_constraint_name));

  auto database_name_column = catalog::Column(
      type::Type::VARCHAR, max_name_size, "database_name", true);
  database_name_column.AddConstraint(
      catalog::Constraint(ConstraintType::NOTNULL, not_null_constraint_name));

  std::unique_ptr<catalog::Schema> table_catalog_schema(
      new catalog::Schema({table_id_column, table_name_column,
                           database_id_column, database_name_column}));

  return table_catalog_schema;
}

std::unique_ptr<storage::Tuple> TableCatalog::GetTableCatalogTuple(
    oid_t table_id, std::string table_name, oid_t database_id,
    std::string database_name, type::AbstractPool *pool) {
  std::unique_ptr<storage::Tuple> tuple(
      new storage::Tuple(catalog_table_->GetSchema(), true));

  auto val1 = type::ValueFactory::GetIntegerValue(table_id);
  auto val2 = type::ValueFactory::GetVarcharValue(table_name, nullptr);
  auto val3 = type::ValueFactory::GetIntegerValue(database_id);
  auto val4 = type::ValueFactory::GetVarcharValue(database_name, nullptr);

  tuple->SetValue(0, val1, pool);
  tuple->SetValue(1, val2, pool);
  tuple->SetValue(2, val3, pool);
  tuple->SetValue(3, val4, pool);

  return std::move(tuple);
}

}  // End catalog namespace
}  // End peloton namespace
