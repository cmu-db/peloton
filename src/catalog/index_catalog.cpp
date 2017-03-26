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

IndexCatalog *IndexCatalog::GetInstance(storage::Database *pg_catalog,
                                        type::AbstractPool *pool) {
  static std::unique_ptr<IndexCatalog> index_catalog(
      new IndexCatalog(pg_catalog, pool));
  return index_catalog.get();
}

IndexCatalog::IndexCatalog(storage::Database *pg_catalog,
                           type::AbstractPool *pool)
    : AbstractCatalog(INDEX_CATALOG_OID, INDEX_CATALOG_NAME,
                      InitializeSchema().release(), pg_catalog, pool) {}

IndexCatalog::~IndexCatalog() {}

std::unique_ptr<catalog::Schema> IndexCatalog::InitializeSchema() {
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

  auto unique_keys = catalog::Column(type::Type::BOOLEAN,
                                            type::Type::GetTypeSize(type::Type::BOOLEAN),
                                            "unique_keys", true);
  std::unique_ptr<catalog::Schema> index_schema(new catalog::Schema( {
      id_column, index_name_column, table_id_column , database_id_column, unique_keys}));
  return index_schema;
}

bool IndexCatalog::InsertIndex(oid_t index_oid,
                               const std::string &index_name,
                               oid_t table_oid,
                               oid_t database_oid,
                               bool unique_keys
                               type::AbstractPool *pool,
                               concurrency::Transaction *txn) {
  // Create the tuple first
  std::unique_ptr<storage::Tuple> tuple(
      new storage::Tuple(catalog_table_->GetSchema(), true));

  auto val0 = type::ValueFactory::GetIntegerValue(index_oid);
  auto val1 = type::ValueFactory::GetVarcharValue(index_name, nullptr);
  auto val2 = type::ValueFactory::GetIntegerValue(table_oid);
  auto val3 = type::ValueFactory::GetIntegerValue(database_oid);
  auto val4 = type::ValueFactory::GetBooleanValue(unique_keys);

  tuple->SetValue(0, val0, pool);
  tuple->SetValue(1, val1, pool);
  tuple->SetValue(2, val2, pool);
  tuple->SetValue(3, val3, pool);
  tuple->SetValue(4, val4, pool);

  // Insert the tuple
  return InsertTuple(std::move(tuple), txn);
}

bool TableCatalog::DeleteIndex(oid_t index_oid, concurrency::Transaction *txn) {
  oid_t index_offset = 0;  // Index of index_oid
  std::vector<type::Value> values;
  values.push_back(type::ValueFactory::GetIntegerValue(index_oid).Copy());

  return DeleteWithIndexScan(index_offset, values, txn);
}

bool TableCatalog::DeleteIndexes(oid_t table_oid, oid_t database_oid,
                                concurrency::Transaction *txn) {
  oid_t index_offset = 2;  // Index of table_oid & database_oid
  std::vector<type::Value> values;
  values.push_back(type::ValueFactory::GetIntegerValue(table_oid).Copy());
  values.push_back(type::ValueFactory::GetIntegerValue(database_oid).Copy());

  return DeleteWithIndexScan(index_offset, values, txn);
}

std::string TableCatalog::GetIndexName(oid_t index_oid,
                                       concurrency::Transaction *txn) {
  std::vector<oid_t> column_ids({1});  // index_name
  oid_t index_offset = 0;              // Index of index_oid
  std::vector<type::Value> values;
  values.push_back(type::ValueFactory::GetIntegerValue(index_oid).Copy());

  auto result_tiles =
      GetResultWithIndexScan(column_ids, index_offset, values, txn);

  std::string index_name;
  PL_ASSERT(result_tiles.size() <= 1);  // index_oid is unique
  if (result_tiles.size() != 0) {
    PL_ASSERT(result_tiles[0]->GetTupleCount() <= 1);
    if (result_tiles[0]->GetTupleCount() != 0) {
      index_name = result_tiles[0]
                       ->GetValue(0, 0)
                       .GetAs<std::string>();  // After projection left 1 column
    }
  }

  return index_name;
}

oid_t GetTableOid(oid_t index_oid, concurrency::Transaction *txn) {
  std::vector<oid_t> column_ids({2});  // table_oid
  oid_t index_offset = 0;              // Index of index_oid
  std::vector<type::Value> values;
  values.push_back(type::ValueFactory::GetIntegerValue(index_oid).Copy());

  auto result_tiles =
      GetResultWithIndexScan(column_ids, index_offset, values, txn);

  oid_t table_oid;
  PL_ASSERT(result_tiles.size() <= 1);  // table_oid is unique
  if (result_tiles.size() != 0) {
    PL_ASSERT(result_tiles[0]->GetTupleCount() <= 1);
    if (result_tiles[0]->GetTupleCount() != 0) {
      table_oid = result_tiles[0]
                       ->GetValue(0, 0)
                       .GetAs<oid_t>();  // After projection left 1 column
    }
  }

  return table_oid;
}

oid_t GetDatabaseOid(oid_t index_oid, concurrency::Transaction *txn) {
  std::vector<oid_t> column_ids({3});  // database_oid
  oid_t index_offset = 0;              // Index of index_oid
  std::vector<type::Value> values;
  values.push_back(type::ValueFactory::GetIntegerValue(index_oid).Copy());

  auto result_tiles =
      GetResultWithIndexScan(column_ids, index_offset, values, txn);

  oid_t database_oid;
  PL_ASSERT(result_tiles.size() <= 1);  // table_oid is unique
  if (result_tiles.size() != 0) {
    PL_ASSERT(result_tiles[0]->GetTupleCount() <= 1);
    if (result_tiles[0]->GetTupleCount() != 0) {
      database_oid = result_tiles[0]
                       ->GetValue(0, 0)
                       .GetAs<oid_t>();  // After projection left 1 column
    }
  }

  return database_oid;
}

oid_t GetUniqueKeys(oid_t index_oid, concurrency::Transaction *txn) {
  std::vector<oid_t> column_ids({4});  // unique_keys
  oid_t index_offset = 0;              // Index of index_oid
  std::vector<type::Value> values;
  values.push_back(type::ValueFactory::GetIntegerValue(index_oid).Copy());

  auto result_tiles =
      GetResultWithIndexScan(column_ids, index_offset, values, txn);

  bool unique_keys = false;
  PL_ASSERT(result_tiles.size() <= 1);  // table_oid is unique
  if (result_tiles.size() != 0) {
    PL_ASSERT(result_tiles[0]->GetTupleCount() <= 1);
    if (result_tiles[0]->GetTupleCount() != 0) {
      unique_keys = result_tiles[0]
                       ->GetValue(0, 0)
                       .GetAs<bool>();  // After projection left 1 column
    }
  }

  return unique_keys;
}

std::vector<oid_t> GetIndexOids(oid_t table_oid, oid_t database_oid, concurrency::Transaction *txn) {
  std::vector<oid_t> column_ids({0});  // index_oid
  oid_t index_offset = 2;              // Index of table_oid & database_oid
  std::vector<type::Value> values;
  values.push_back(type::ValueFactory::GetIntegerValue(table_oid).Copy());
  values.push_back(type::ValueFactory::GetIntegerValue(database_oid).Copy());

  auto result_tiles =
      GetResultWithIndexScan(column_ids, index_offset, values, txn);

  std::vector<oid_t> index_oids;
  for (auto tile : result_tiles) {
    for (auto tuple_id : *tile) {
      index_oids.emplace_back(
          tile->GetValue(tuple_id, 0)
              .GetAs<oid_t>());  // After projection left 1 column
    }
  }

  return index_oids;
}


}  // End catalog namespace
}  // End peloton namespace