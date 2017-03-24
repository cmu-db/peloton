//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// index_catalog.cpp
//
// Identification: src/catalog/index_catalog.cpp
//
// Copyright (c) 2015-17, Carnegie Mellon University Index Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include "catalog/index_catalog.h"

namespace peloton {
namespace catalog {

IndexCatalog *IndexCatalog::GetInstance(void) {
  static std::unique_ptr<IndexCatalog> index_catalog(
      new IndexCatalog());
  return index_catalog.get();
}

IndexCatalog::IndexCatalog() :
  AbstractCatalog(GetNextOid(), TABLE_CATALOG_NAME, InitializeTableCatalogSchema().release()) {}

std::unique_ptr<catalog::Schema> IndexCatalog::InitializeIndexCatalogSchema() {
  const std::string not_null_constraint_name = "not_null";
  const std::string primary_key_constraint_name = "primary_key";

  auto index_id_column = catalog::Column(type::Type::INTEGER,
                                   type::Type::GetTypeSize(type::Type::INTEGER),
                                   "index_oid", true);
  index_id_column.AddConstraint(
      catalog::Constraint(ConstraintType::PRIMARY, primary_key_constraint_name));
  id_column.AddConstraint(
      catalog::Constraint(ConstraintType::NOTNULL, not_null_constraint_name));

  auto index_name_column = catalog::Column(type::Type::VARCHAR, max_name_size, "index_name", true);
  index_name_column.AddConstraint(
      catalog::Constraint(ConstraintType::NOTNULL, not_null_constraint_name));

  auto table_id_column = catalog::Column(type::Type::INTEGER,
                                         type::Type::GetTypeSize(type::Type::INTEGER),
                                         "table_oid", true);
  table_id_column.AddConstraint(
      catalog::Constraint(ConstraintType::NOTNULL, not_null_constraint_name));

  auto database_id_column = catalog::Column(type::Type::INTEGER,
                                            type::Type::GetTypeSize(type::Type::INTEGER),
                                            "database_oid", true);
  database_id_column.AddConstraint(
      catalog::Constraint(ConstraintType::NOTNULL, not_null_constraint_name));

  auto unique_key = catalog::Column(type::Type::BOOLEAN,
                                            type::Type::GetTypeSize(type::Type::BOOLEAN),
                                            "unique_key", true);
  std::unique_ptr<catalog::Schema> index_schema(new catalog::Schema( {
      id_column, index_name_column, table_id_column , database_id_column, unique_key}));
  return index_schema;
}

std::unique_ptr<storage::Tuple> IndexCatalog::GetIndexCatalogTuple(
    oid_t index_oid, std::string index_name,
    oid_t table_oid, oid_t database_oid, bool unique_keys
    type::AbstractPool *pool) {
  std::unique_ptr<storage::Tuple> tuple(new storage::Tuple(catalog_table_->GetSchema(), true));

  auto val1 = type::ValueFactory::GetIntegerValue(index_oid);
  auto val2 = type::ValueFactory::GetVarcharValue(index_name, nullptr);
  auto val3 = type::ValueFactory::GetIntegerValue(table_oid);
  auto val4 = type::ValueFactory::GetIntegerValue(database_oid);
  auto val5 = type::ValueFactory::GetBooleanValue(unique_keys);

  tuple->SetValue(0, val1, pool);
  tuple->SetValue(1, val2, pool);
  tuple->SetValue(2, val3, pool);
  tuple->SetValue(3, val4, pool);
  tuple->SetValue(4, val5, pool);

  return std::move(tuple);
}

}  // End catalog namespace
}  // End peloton namespace