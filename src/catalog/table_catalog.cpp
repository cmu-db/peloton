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
                      InitializeSchema().release()) {}

std::unique_ptr<catalog::Schema> TableCatalog::InitializeSchema() {
  const std::string primary_key_constraint_name = "primary_key";
  const std::string not_null_constraint_name = "not_null";

  auto table_id_column = catalog::Column(
      type::Type::INTEGER, type::Type::GetTypeSize(type::Type::INTEGER),
      "table_id", true);
  table_id_column.AddConstraint(catalog::Constraint(
      ConstraintType::PRIMARY, primary_key_constraint_name));
  table_id_column.AddConstraint(
      catalog::Constraint(ConstraintType::NOTNULL, not_null_constraint_name));
  // Insert into pg_attribute
  ColumnCatalog::GetInstance()->Insert(
      TABLE_CATALOG_OID, 0, "table_id", type::Type::INTEGER, true,
      table_id_column.GetConstraints(), nullptr);

  auto table_name_column =
      catalog::Column(type::Type::VARCHAR, max_name_size, "table_name", true);
  table_name_column.AddConstraint(
      catalog::Constraint(ConstraintType::NOTNULL, not_null_constraint_name));
  // Insert into pg_attribute
  ColumnCatalog::GetInstance()->Insert(
      TABLE_CATALOG_OID, 1, "table_name", type::Type::VARCHAR, true,
      table_name_column.GetConstraints(), nullptr);

  auto database_id_column = catalog::Column(
      type::Type::INTEGER, type::Type::GetTypeSize(type::Type::INTEGER),
      "database_id", true);
  database_id_column.AddConstraint(
      catalog::Constraint(ConstraintType::NOTNULL, not_null_constraint_name));
  // Insert into pg_attribute
  ColumnCatalog::GetInstance()->Insert(
      TABLE_CATALOG_OID, 2, "database_id", type::Type::INTEGER, true,
      database_id_column.GetConstraints(), nullptr);

  auto database_name_column = catalog::Column(
      type::Type::VARCHAR, max_name_size, "database_name", true);
  database_name_column.AddConstraint(
      catalog::Constraint(ConstraintType::NOTNULL, not_null_constraint_name));
  // Insert into pg_attribute
  ColumnCatalog::GetInstance()->Insert(
      TABLE_CATALOG_OID, 3, "database_name", type::Type::VARCHAR, true,
      database_name_column.GetConstraints(), nullptr);

  std::unique_ptr<catalog::Schema> table_catalog_schema(
      new catalog::Schema({table_id_column, table_name_column,
                           database_id_column, database_name_column}));

  return table_catalog_schema;
}

bool TableCatalog::Insert(oid_t table_id, std::string table_name,
                          oid_t database_id, std::string database_name,
                          concurrency::Transaction *txn) {
  // Create the tuple first
  std::unique_ptr<storage::Tuple> tuple(
      new storage::Tuple(catalog_table_->GetSchema(), true));

  auto val1 = type::ValueFactory::GetIntegerValue(table_id);
  auto val2 = type::ValueFactory::GetVarcharValue(table_name, nullptr);
  auto val3 = type::ValueFactory::GetIntegerValue(database_id);
  auto val4 = type::ValueFactory::GetVarcharValue(database_name, nullptr);

  tuple->SetValue(0, val1, Catalog::pool_);
  tuple->SetValue(1, val2, Catalog::pool_);
  tuple->SetValue(2, val3, Catalog::pool_);
  tuple->SetValue(3, val4, Catalog::pool_);

  // Insert the tuple
  return InsertTuple(std::move(tuple), txn);
}

bool TableCatalog::DeleteByOid(oid_t oid, concurrency::Transaction *txn) {
  oid_t index_offset = 0;  // Index of oid
  std::vector<type::Value> values;
  values.push_back(type::ValueFactory::GetIntegerValue(oid).Copy());

  return DeleteWithIndexScan(index_offset, values, txn);
}

std::string TableCatalog::GetTableNameByOid(oid_t oid,
                                            concurrency::Transaction *txn) {
  std::vector<oid_t> column_ids({1});  // table_name
  oid_t index_offset = 0;              // Index of oid
  std::vector<type::Value> values;
  values.push_back(type::ValueFactory::GetIntegerValue(oid).Copy());

  auto result = GetWithIndexScan(column_ids, index_offset, values, txn);

  std::string table_name;
  PL_ASSERT(result->GetTupleCount() <= 1);  // Oid is unique
  if (result->GetTupleCount() != 0) {
    table_name = result->GetValue(0, 0);  // After projection left 1 column
  }

  return table_name;
}

std::string TableCatalog::GetDatabaseNameByOid(oid_t oid,
                                               concurrency::Transaction *txn) {
  std::vector<oid_t> column_ids({2});  // database_name
  oid_t index_offset = 0;              // Index of oid
  std::vector<type::Value> values;
  values.push_back(type::ValueFactory::GetIntegerValue(oid).Copy());

  auto result = GetWithIndexScan(column_ids, index_offset, values, txn);

  std::string database_name;
  PL_ASSERT(result->GetTupleCount() <= 1);  // Oid is unique
  if (result->GetTupleCount() != 0) {
    database_name = result->GetValue(0, 0);  // After projection left 1 column
  }

  return database_name;
}

oid_t TableCatalog::GetOidByName(std::string &table_name,
                                 std::string &database_name,
                                 concurrency::Transaction *txn) {
  std::vector<oid_t> column_ids({0});  // table_id
  oid_t index_offset = 1;              // Index of table_name + database_name
  std::vector<type::Value> values;
  values.push_back(
      type::ValueFactory::GetVarcharValue(table_name, nullptr).Copy());
  values.push_back(
      type::ValueFactory::GetVarcharValue(database_name, nullptr).Copy());

  auto result = GetWithIndexScan(column_ids, index_offset, values, txn);

  oid_t oid = INVALID_OID;
  PL_ASSERT(result->GetTupleCount() <=
            1);  // table_name + database_name is unique
  if (result->GetTupleCount() != 0) {
    oid = result->GetValue(0, 0);  // After projection left 1 column
  }

  return oid;
}

}  // End catalog namespace
}  // End peloton namespace