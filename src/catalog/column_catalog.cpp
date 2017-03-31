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

#include "catalog/column_catalog.h"
#include "type/catalog_type.h"
#include "type/types.h"
#include "index/index_factory.h"

namespace peloton {
namespace catalog {

ColumnCatalog *ColumnCatalog::GetInstance(storage::Database *pg_catalog,
                                          type::AbstractPool *pool) {
  static std::unique_ptr<ColumnCatalog> column_catalog(
      new ColumnCatalog(pg_catalog, pool));

  return column_catalog.get();
}

ColumnCatalog::ColumnCatalog(storage::Database *pg_catalog,
                             type::AbstractPool *pool)
    : AbstractCatalog(COLUMN_CATALOG_OID, COLUMN_CATALOG_NAME,
                      InitializeSchema().release(), pg_catalog) {
  AddIndex({0, 1}, COLUMN_CATALOG_PKEY_OID, COLUMN_CATALOG_NAME "_pkey",
           IndexConstraintType::PRIMARY_KEY);
  AddIndex({0, 2}, COLUMN_CATALOG_SKEY0_OID, COLUMN_CATALOG_NAME "_skey0",
           IndexConstraintType::UNIQUE);
  AddIndex({2}, COLUMN_CATALOG_SKEY1_OID, COLUMN_CATALOG_NAME "_skey1",
           IndexConstraintType::DEFAULT);

  // Insert columns into pg_attribute, note that insertion does not require
  // indexes on pg_attribute
  for (auto column : catalog_table_->GetSchema()->GetColumns()) {
    InsertColumn(COLUMN_CATALOG_OID, column.GetName(), column.GetOffset(),
                 column.GetType(), true, column.GetConstraints(), pool,
                 nullptr);
  }
}

void ColumnCatalog::AddIndex(const std::vector<oid_t> &key_attrs,
                             oid_t index_oid, const std::string &index_name,
                             IndexConstraintType index_constraint) {
  auto schema = catalog_table_->GetSchema();
  auto key_schema = catalog::Schema::CopySchema(schema, key_attrs);
  key_schema->SetIndexedColumns(key_attrs);
  bool unique_keys = (index_constraint == IndexConstraintType::PRIMARY_KEY) ||
                     (index_constraint == IndexConstraintType::UNIQUE);

  auto index_metadata = new index::IndexMetadata(
      index_name, index_oid, catalog_table_->GetOid(), CATALOG_DATABASE_OID,
      IndexType::BWTREE, index_constraint, schema, key_schema, key_attrs,
      unique_keys);

  std::shared_ptr<index::Index> key_index(
      index::IndexFactory::GetIndex(index_metadata));
  catalog_table_->AddIndex(key_index);

  // insert index record into index_catalog(pg_index) table
  // IndexCatalog::GetInstance()->InsertIndex(
  //     index_oid, index_name, COLUMN_CATALOG_OID, IndexType::BWTREE,
  //     index_constraint, unique_keys, pool_.get(), nullptr);

  LOG_DEBUG("Successfully created primary key index '%s' for table '%s'",
            index_name.c_str(), COLUMN_CATALOG_NAME);
}

ColumnCatalog::~ColumnCatalog() {}

std::unique_ptr<catalog::Schema> ColumnCatalog::InitializeSchema() {
  const std::string primary_key_constraint_name = "primary_key";
  const std::string not_null_constraint_name = "not_null";

  auto table_id_column = catalog::Column(
      type::Type::INTEGER, type::Type::GetTypeSize(type::Type::INTEGER),
      "table_oid", true);
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

bool ColumnCatalog::InsertColumn(
    oid_t table_oid, const std::string &column_name, oid_t column_offset,
    type::Type::TypeId column_type, bool is_inlined,
    const std::vector<Constraint> &constraints, type::AbstractPool *pool,
    concurrency::Transaction *txn) {
  // Create the tuple first
  std::unique_ptr<storage::Tuple> tuple(
      new storage::Tuple(catalog_table_->GetSchema(), true));

  auto val0 = type::ValueFactory::GetIntegerValue(table_oid);
  auto val1 = type::ValueFactory::GetVarcharValue(column_name, nullptr);
  auto val2 = type::ValueFactory::GetIntegerValue(column_offset);
  auto val3 =
      type::ValueFactory::GetVarcharValue(TypeIdToString(column_type), nullptr);
  auto val4 = type::ValueFactory::GetBooleanValue(is_inlined);
  bool is_primary = false, is_not_null = false;
  for (auto constraint : constraints) {
    if (constraint.GetType() == ConstraintType::PRIMARY) {
      is_primary = true;
    } else if (constraint.GetType() == ConstraintType::NOT_NULL ||
               constraint.GetType() == ConstraintType::NOTNULL) {
      is_not_null = true;
    }
  }
  auto val5 = type::ValueFactory::GetBooleanValue(is_primary);
  auto val6 = type::ValueFactory::GetBooleanValue(is_not_null);

  tuple->SetValue(0, val0, pool);
  tuple->SetValue(1, val1, pool);
  tuple->SetValue(2, val2, pool);
  tuple->SetValue(3, val3, pool);
  tuple->SetValue(4, val4, pool);
  tuple->SetValue(5, val5, pool);
  tuple->SetValue(6, val6, pool);

  // Insert the tuple
  return InsertTuple(std::move(tuple), txn);
}

bool ColumnCatalog::DeleteColumn(oid_t table_oid,
                                 const std::string &column_name,
                                 concurrency::Transaction *txn) {
  oid_t index_offset = 0;  // Index of table_oid & column_name
  std::vector<type::Value> values;
  values.push_back(type::ValueFactory::GetIntegerValue(table_oid).Copy());
  values.push_back(
      type::ValueFactory::GetVarcharValue(column_name, nullptr).Copy());

  return DeleteWithIndexScan(index_offset, values, txn);
}

bool ColumnCatalog::DeleteColumns(oid_t table_oid,
                                  concurrency::Transaction *txn) {
  oid_t index_offset = 2;  // Index of table_oid
  std::vector<type::Value> values;
  values.push_back(type::ValueFactory::GetIntegerValue(table_oid).Copy());

  return DeleteWithIndexScan(index_offset, values, txn);
}

oid_t ColumnCatalog::GetColumnOffset(oid_t table_oid,
                                     const std::string &column_name,
                                     concurrency::Transaction *txn) {
  std::vector<oid_t> column_ids({2});  // column_offset
  oid_t index_offset = 0;              // Index of table_oid & column_name
  std::vector<type::Value> values;
  values.push_back(type::ValueFactory::GetIntegerValue(table_oid).Copy());
  values.push_back(
      type::ValueFactory::GetVarcharValue(column_name, nullptr).Copy());

  auto result_tiles =
      GetResultWithIndexScan(column_ids, index_offset, values, txn);

  oid_t column_offset = INVALID_OID;
  PL_ASSERT(result_tiles->size() <= 1);  // table_oid & column_name is unique
  if (result_tiles->size() != 0) {
    PL_ASSERT((*result_tiles)[0]->GetTupleCount() <= 1);
    if ((*result_tiles)[0]->GetTupleCount() != 0) {
      column_offset = (*result_tiles)[0]
                          ->GetValue(0, 0)
                          .GetAs<oid_t>();  // After projection left 1 column
    }
  }

  return column_offset;
}

std::string ColumnCatalog::GetColumnName(oid_t table_oid, oid_t column_offset,
                                         concurrency::Transaction *txn) {
  std::vector<oid_t> column_ids({1});  // column_name
  oid_t index_offset = 1;              // Index of table_oid & column_offset
  std::vector<type::Value> values;
  values.push_back(type::ValueFactory::GetIntegerValue(table_oid).Copy());
  values.push_back(type::ValueFactory::GetIntegerValue(column_offset).Copy());

  auto result_tiles =
      GetResultWithIndexScan(column_ids, index_offset, values, txn);

  std::string column_name;
  PL_ASSERT(result_tiles->size() <= 1);  // table_oid & column_offset is unique
  if (result_tiles->size() != 0) {
    PL_ASSERT((*result_tiles)[0]->GetTupleCount() <= 1);
    if ((*result_tiles)[0]->GetTupleCount() != 0) {
      column_name =
          (*result_tiles)[0]
              ->GetValue(0, 0)
              .GetAs<std::string>();  // After projection left 1 column
    }
  }

  return column_name;
}

type::Type::TypeId ColumnCatalog::GetColumnType(oid_t table_oid,
                                                std::string column_name,
                                                concurrency::Transaction *txn) {
  std::vector<oid_t> column_ids({3});  // column_type
  oid_t index_offset = 0;              // Index of table_oid & column_name
  std::vector<type::Value> values;
  values.push_back(type::ValueFactory::GetIntegerValue(table_oid).Copy());
  values.push_back(
      type::ValueFactory::GetVarcharValue(column_name, nullptr).Copy());

  auto result_tiles =
      GetResultWithIndexScan(column_ids, index_offset, values, txn);

  type::Type::TypeId column_type = type::Type::TypeId::INVALID;
  PL_ASSERT(result_tiles->size() <= 1);  // table_oid & column_name is unique
  if (result_tiles->size() != 0) {
    PL_ASSERT((*result_tiles)[0]->GetTupleCount() <= 1);
    if ((*result_tiles)[0]->GetTupleCount() != 0) {
      column_type =
          (*result_tiles)[0]
              ->GetValue(0, 0)
              .GetAs<type::Type::TypeId>();  // After projection left 1 column
    }
  }

  return column_type;
}

type::Type::TypeId ColumnCatalog::GetColumnType(oid_t table_oid,
                                                oid_t column_offset,
                                                concurrency::Transaction *txn) {
  std::vector<oid_t> column_ids({3});  // column_type
  oid_t index_offset = 1;              // Index of table_oid & column_offset
  std::vector<type::Value> values;
  values.push_back(type::ValueFactory::GetIntegerValue(table_oid).Copy());
  values.push_back(type::ValueFactory::GetIntegerValue(column_offset).Copy());

  auto result_tiles =
      GetResultWithIndexScan(column_ids, index_offset, values, txn);

  type::Type::TypeId column_type = type::Type::TypeId::INVALID;
  PL_ASSERT(result_tiles->size() <= 1);  // table_oid & column_offset is unique
  if (result_tiles->size() != 0) {
    PL_ASSERT((*result_tiles)[0]->GetTupleCount() <= 1);
    if ((*result_tiles)[0]->GetTupleCount() != 0) {
      column_type =
          (*result_tiles)[0]
              ->GetValue(0, 0)
              .GetAs<type::Type::TypeId>();  // After projection left 1 column
    }
  }

  return column_type;
}

}  // End catalog namespace
}  // End peloton namespace
