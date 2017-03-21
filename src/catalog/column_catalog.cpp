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

ColumnCatalog::ColumnCatalog(storage::Database *pg_catalog)
    : AbstractCatalog(pg_catalog, COLUMN_CATALOG_OID, COLUMN_CATALOG_NAME,
                      InitializeSchema().release()) {}

// This only constructs pg_attribute schema, it leaves the insertion job to the
// constructor
std::unique_ptr<catalog::Schema> ColumnCatalog::InitializeSchema() {
  const std::string primary_key_constraint_name = "primary_key";
  const std::string not_null_constraint_name = "not_null";

  auto table_id_column = catalog::Column(
      type::Type::INTEGER, type::Type::GetTypeSize(type::Type::INTEGER),
      "table_id", true);
  table_id_column.AddConstraint(catalog::Constraint(
      ConstraintType::PRIMARY, primary_key_constraint_name));
  table_id_column.AddConstraint(
      catalog::Constraint(ConstraintType::NOTNULL, not_null_constraint_name));

  auto column_name_column =
      catalog::Column(type::Type::VARCHAR, max_name_size, "column_name", true);
  column_name_column.AddConstraint(catalog::Constraint(
      ConstraintType::PRIMARY, primary_key_constraint_name));
  column_name_column.AddConstraint(
      catalog::Constraint(ConstraintType::NOTNULL, not_null_constraint_name));

  auto column_offset_column = catalog::Column(
      type::Type::INTEGER, type::Type::GetTypeSize(type::Type::INTEGER),
      "column_offset", true);
  column_offset_column.AddConstraint(
      catalog::Constraint(ConstraintType::NOTNULL, not_null_constraint_name));

  auto column_type_column =
      catalog::Column(type::Type::VARCHAR, max_name_size, "column_type", true);
  column_type_column.AddConstraint(
      catalog::Constraint(ConstraintType::NOTNULL, not_null_constraint_name));

  auto is_inlined_column = catalog::Column(
      type::Type::BOOLEAN, type::Type::GetTypeSize(type::Type::BOOLEAN),
      "is_inlined", true);
  is_inlined_column.AddConstraint(
      catalog::Constraint(ConstraintType::NOTNULL, not_null_constraint_name));

  auto is_primary_column = catalog::Column(
      type::Type::BOOLEAN, type::Type::GetTypeSize(type::Type::BOOLEAN),
      "is_primary", true);
  is_primary_column.AddConstraint(
      catalog::Constraint(ConstraintType::NOTNULL, not_null_constraint_name));

  auto is_not_null_column = catalog::Column(
      type::Type::BOOLEAN, type::Type::GetTypeSize(type::Type::BOOLEAN),
      "is_not_null", true);
  is_not_null_column.AddConstraint(
      catalog::Constraint(ConstraintType::NOTNULL, not_null_constraint_name));

  std::unique_ptr<catalog::Schema> column_catalog_schema(new catalog::Schema(
      {table_id_column, column_name_column, column_offset_column,
       column_type_column, is_inlined_column, is_primary_column,
       is_not_null_column}));

  return column_catalog_schema;
}

bool ColumnCatalog::Insert(oid_t table_id, const std::string &column_name,
                           oid_t column_offset, type::Type::TypeId column_type,
                           bool is_inlined,
                           std::vector<ConstraintType> constraints,
                           concurrency::Transaction *txn) {
  // Create the tuple first
  std::unique_ptr<storage::Tuple> tuple(
      new storage::Tuple(catalog_table_->GetSchema(), true));

  auto val0 = type::ValueFactory::GetIntegerValue(table_id);
  auto val1 = type::ValueFactory::GetVarcharValue(column_name, nullptr);
  auto val2 = type::ValueFactory::GetIntegerValue(column_offset);
  auto val3 =
      type::ValueFactory::GetVarcharValue(TypeIdToString(column_type), nullptr);
  auto val4 = type::ValueFactory::GetBooleanValue(is_inlined);
  bool is_primary = false, is_not_null = false;
  for (auto constraint : constraints) {
    if (constraint == ConstraintType::PRIMARY) {
      is_primary = true;
    } else if (constraint == ConstraintType::NOT_NULL ||
               constraint == ConstraintType::NOTNULL) {
      is_not_null = true;
    }
  }
  auto val5 = type::ValueFactory::GetBooleanValue(is_primary);
  auto val6 = type::ValueFactory::GetBooleanValue(is_not_null);

  tuple->SetValue(0, val0, Catalog::pool_);
  tuple->SetValue(1, val1, Catalog::pool_);
  tuple->SetValue(2, val2, Catalog::pool_);
  tuple->SetValue(3, val3, Catalog::pool_);
  tuple->SetValue(4, val4, Catalog::pool_);
  tuple->SetValue(5, val5, Catalog::pool_);
  tuple->SetValue(6, val6, Catalog::pool_);

  // Insert the tuple
  return InsertTuple(std::move(tuple), txn);
}

bool ColumnCatalog::DeleteByOidWithName(oid_t table_id,
                                        const std::string &column_name,
                                        concurrency::Transaction *txn) {
  oid_t index_offset = 0;  // Index of table_id & column_name
  std::vector<type::Value> values;
  values.push_back(type::ValueFactory::GetIntegerValue(table_id).Copy());
  values.push_back(
      type::ValueFactory::GetVarcharValue(column_name, nullptr).Copy());

  return DeleteWithIndexScan(index_offset, values, txn);
}

oid_t ColumnCatalog::GetOffsetByOidWithName(oid_t table_id,
                                            const std::string &column_name,
                                            concurrency::Transaction *txn) {
  std::vector<oid_t> column_ids({2});  // column_offset
  oid_t index_offset = 0;              // Index of table_id & column_name
  std::vector<type::Value> values;
  values.push_back(type::ValueFactory::GetIntegerValue(table_id).Copy());
  values.push_back(
      type::ValueFactory::GetVarcharValue(column_name, nullptr).Copy());

  auto result = GetWithIndexScan(column_ids, index_offset, values, txn);

  oid_t column_offset = INVALID_OID;
  PL_ASSERT(result->GetTupleCount() <= 1);  // table_id & column_name is unique
  if (result->GetTupleCount() != 0) {
    column_offset = result->GetValue(0, 0)
                        .GetAs<oid_t>();  // After projection left 1 column
  }

  return column_offset;
}

std::string ColumnCatalog::GetNameByOidWithOffset(
    oid_t table_id, oid_t column_offset, concurrency::Transaction *txn) {
  std::vector<oid_t> column_ids({1});  // column_name
  oid_t index_offset = 1;              // Index of table_id & column_offset
  std::vector<type::Value> values;
  values.push_back(type::ValueFactory::GetIntegerValue(table_id).Copy());
  values.push_back(type::ValueFactory::GetIntegerValue(column_offset).Copy());

  auto result = GetWithIndexScan(column_ids, index_offset, values, txn);

  std::string column_name;
  PL_ASSERT(result->GetTupleCount() <=
            1);  // table_id & column_offset is unique
  if (result->GetTupleCount() != 0) {
    column_name = result->GetValue(0, 0)
                      .GetAs<std::string>();  // After projection left 1 column
  }

  return column_name;
}

type::Type::TypeId ColumnCatalog::GetTypeByOidWithName(
    oid_t table_id, std::string column_name, concurrency::Transaction *txn) {
  std::vector<oid_t> column_ids({3});  // column_type
  oid_t index_offset = 0;              // Index of table_id & column_name
  std::vector<type::Value> values;
  values.push_back(type::ValueFactory::GetIntegerValue(table_id).Copy());
  values.push_back(
      type::ValueFactory::GetVarcharValue(column_name, nullptr).Copy());

  auto result = GetWithIndexScan(column_ids, index_offset, values, txn);

  type::Type::TypeId column_type = type::Type::TypeId::INVALID;
  PL_ASSERT(result->GetTupleCount() <= 1);  // table_id & column_name is unique
  if (result->GetTupleCount() != 0) {
    column_type =
        result->GetValue(0, 0)
            .GetAs<type::Type::TypeId>();  // After projection left 1 column
  }

  return column_type;
}

type::Type::TypeId ColumnCatalog::GetTypeByOidWithOffset(
    oid_t table_id, oid_t column_offset, concurrency::Transaction *txn) {
  std::vector<oid_t> column_ids({3});  // column_type
  oid_t index_offset = 1;              // Index of table_id & column_offset
  std::vector<type::Value> values;
  values.push_back(type::ValueFactory::GetIntegerValue(table_id).Copy());
  values.push_back(type::ValueFactory::GetIntegerValue(column_offset).Copy());

  auto result = GetWithIndexScan(column_ids, index_offset, values, txn);

  type::Type::TypeId column_type = type::Type::TypeId::INVALID;
  PL_ASSERT(result->GetTupleCount() <=
            1);  // table_id & column_offset is unique
  if (result->GetTupleCount() != 0) {
    column_type =
        result->GetValue(0, 0)
            .GetAs<type::Type::TypeId>();  // After projection left 1 column
  }

  return column_type;
}

}  // End catalog namespace
}  // End peloton namespace
