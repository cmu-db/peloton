//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// column_catalog.cpp
//
// Identification: src/catalog/column_catalog.cpp
//
// Copyright (c) 2015-17, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include "catalog/column_catalog.h"

namespace peloton {
namespace catalog {

ColumnCatalog *ColumnCatalog::GetInstance(void) {
  static std::unique_ptr<ColumnCatalog> column_catalog(new ColumnCatalog());
  return column_catalog.get();
}

ColumnCatalog::ColumnCatalog()
    : AbstractCatalog(GetNextOid(), COLUMN_CATALOG_NAME,
                      InitializeColumnCatalogSchema().release()) {}

std::unique_ptr<catalog::Schema>
ColumnCatalog::InitializeColumnCatalogSchema() {
  // const std::string primary_key_constraint_name = "primary_key";
  const std::string not_null_constraint_name = "not_null";

  auto table_id_column = catalog::Column(
      type::Type::INTEGER, type::Type::GetTypeSize(type::Type::INTEGER),
      "table_id", true);
  // table_id_column.AddConstraint(catalog::Constraint(
  //     ConstraintType::PRIMARY, primary_key_constraint_name));
  table_id_column.AddConstraint(
      catalog::Constraint(ConstraintType::NOTNULL, not_null_constraint_name));

  auto column_name_column =
      catalog::Column(type::Type::VARCHAR, max_name_size, "column_name", true);
  column_name_column.AddConstraint(
      catalog::Constraint(ConstraintType::NOTNULL, not_null_constraint_name));

  auto column_type_column = catalog::Column(
      type::Type::INTEGER, type::Type::GetTypeSize(type::Type::INTEGER),
      "column_type", true);
  column_type_column.AddConstraint(
      catalog::Constraint(ConstraintType::NOTNULL, not_null_constraint_name));

  auto column_offset_column = catalog::Column(
      type::Type::INTEGER, type::Type::GetTypeSize(type::Type::INTEGER),
      "column_offset", true);
  column_offset_column.AddConstraint(
      catalog::Constraint(ConstraintType::NOTNULL, not_null_constraint_name));

  auto column_isPrimary_column = catalog::Column(
      type::Type::BOOLEAN, type::Type::GetTypeSize(type::Type::BOOLEAN),
      "column_isPrimary", true);
  column_isPrimary_column.AddConstraint(
      catalog::Constraint(ConstraintType::NOTNULL, not_null_constraint_name));

  auto column_hasConstrain_column = catalog::Column(
      type::Type::BOOLEAN, type::Type::GetTypeSize(type::Type::BOOLEAN),
      "column_hasConstrain", true);
  column_hasConstrain_column.AddConstraint(
      catalog::Constraint(ConstraintType::NOTNULL, not_null_constraint_name));

  std::unique_ptr<catalog::Schema> column_catalog_schema(new catalog::Schema(
      {table_id_column, column_name_column, column_type_column,
       column_offset_column, column_isPrimary_column,
       column_hasConstrain_column}));

  return column_catalog_schema;
}

std::unique_ptr<storage::Tuple> ColumnCatalog::GetColumnCatalogTuple(
    oid_t table_id, std::string column_name, oid_t column_type,
    oid_t column_offset, bool column_isPrimary, bool column_hasConstrain,
    type::AbstractPool *pool) {
  std::unique_ptr<storage::Tuple> tuple(
      new storage::Tuple(catalog_table->GetSchema(), true));

  auto val1 = type::ValueFactory::GetIntegerValue(table_id);
  auto val2 = type::ValueFactory::GetVarcharValue(column_name, nullptr);
  auto val3 = type::ValueFactory::GetIntegerValue(column_type);
  auto val4 = type::ValueFactory::GetIntegerValue(column_offset);
  auto val5 = type::ValueFactory::GetBooleanValue(column_isPrimary);
  auto val6 = type::ValueFactory::GetBooleanValue(column_hasConstrain);

  tuple->SetValue(0, val1, pool);
  tuple->SetValue(1, val2, pool);
  tuple->SetValue(2, val3, pool);
  tuple->SetValue(3, val4, pool);
  tuple->SetValue(4, val5, pool);
  tuple->SetValue(5, val6, pool);

  return std::move(tuple);
}

}  // End catalog namespace
}  // End peloton namespace
