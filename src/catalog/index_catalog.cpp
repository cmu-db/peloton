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

#include "catalog/index_catalog.h"
#include "catalog/column_catalog.h"

namespace peloton {
namespace catalog {

IndexCatalog *IndexCatalog::GetInstance(storage::Database *pg_catalog,
                                        type::AbstractPool *pool) {
  static std::unique_ptr<IndexCatalog> index_catalog(
      new IndexCatalog(pg_catalog, pool));

  return index_catalog.get();
}

IndexCatalog::IndexCatalog(storage::Database *pg_catalog, type::AbstractPool *pool)
    : AbstractCatalog(INDEX_CATALOG_OID, INDEX_CATALOG_NAME,
                      InitializeSchema().release(), pg_catalog) {
  // Insert columns into pg_attribute, note that insertion does not require
  // indexes on pg_attribute
  ColumnCatalog *pg_attribute = ColumnCatalog::GetInstance(pg_catalog, pool);
  for (auto column : catalog_table_->GetSchema()->GetColumns()) {
    pg_attribute->InsertColumn(catalog_table_oid, column.GetName(),
                               column.GetOffset(), column.GetType(), true,
                               column.GetConstraints(), pool, nullptr);
  }
}

IndexCatalog::~IndexCatalog() {}

std::unique_ptr<catalog::Schema> IndexCatalog::InitializeSchema() {
  const std::string not_null_constraint_name = "not_null";
  const std::string primary_key_constraint_name = "primary_key";

  auto index_id_column = catalog::Column(
      type::Type::INTEGER, type::Type::GetTypeSize(type::Type::INTEGER),
      "index_oid", true);
  index_id_column.AddConstraint(catalog::Constraint(
      ConstraintType::PRIMARY, primary_key_constraint_name));
  index_id_column.AddConstraint(
      catalog::Constraint(ConstraintType::NOTNULL, not_null_constraint_name));

  auto index_name_column =
      catalog::Column(type::Type::VARCHAR, max_name_size, "index_name", true);
  index_name_column.AddConstraint(
      catalog::Constraint(ConstraintType::NOTNULL, not_null_constraint_name));

  auto table_id_column = catalog::Column(
      type::Type::INTEGER, type::Type::GetTypeSize(type::Type::INTEGER),
      "table_oid", true);
  table_id_column.AddConstraint(
      catalog::Constraint(ConstraintType::NOTNULL, not_null_constraint_name));

  auto index_type_column = catalog::Column(
      type::Type::INTEGER, type::Type::GetTypeSize(type::Type::INTEGER),
      "index_type", true);
  index_type_column.AddConstraint(
      catalog::Constraint(ConstraintType::NOTNULL, not_null_constraint_name));

  auto index_constraint_column = catalog::Column(
      type::Type::INTEGER, type::Type::GetTypeSize(type::Type::INTEGER),
      "index_constraint", true);

  auto unique_keys = catalog::Column(
      type::Type::BOOLEAN, type::Type::GetTypeSize(type::Type::BOOLEAN),
      "unique_keys", true);
  std::unique_ptr<catalog::Schema> index_schema(new catalog::Schema(
      {index_id_column, index_name_column, table_id_column, index_type_column,
       index_constraint_column, unique_keys}));
  return index_schema;
}

bool IndexCatalog::InsertIndex(oid_t index_oid, const std::string &index_name,
                               oid_t table_oid, IndexType index_type,
                               IndexConstraintType index_constraint,
                               bool unique_keys type::AbstractPool *pool,
                               concurrency::Transaction *txn) {
  // Create the tuple first
  std::unique_ptr<storage::Tuple> tuple(
      new storage::Tuple(catalog_table_->GetSchema(), true));

  auto val0 = type::ValueFactory::GetIntegerValue(index_oid);
  auto val1 = type::ValueFactory::GetVarcharValue(index_name, nullptr);
  auto val2 = type::ValueFactory::GetIntegerValue(table_oid);
  auto val3 = type::ValueFactory::GetIntegerValue(index_type);
  auto val4 = type::ValueFactory::GetIntegerValue(index_constraint);
  auto val5 = type::ValueFactory::GetBooleanValue(unique_keys);

  tuple->SetValue(0, val0, pool);
  tuple->SetValue(1, val1, pool);
  tuple->SetValue(2, val2, pool);
  tuple->SetValue(3, val3, pool);
  tuple->SetValue(4, val4, pool);
  tuple->SetValue(5, val5, pool);

  // Insert the tuple
  return InsertTuple(std::move(tuple), txn);
}

bool IndexCatalog::DeleteIndex(oid_t index_oid, concurrency::Transaction *txn) {
  oid_t index_offset = 0;  // Index of index_oid
  std::vector<type::Value> values;
  values.push_back(type::ValueFactory::GetIntegerValue(index_oid).Copy());

  return DeleteWithIndexScan(index_offset, values, txn);
}

std::string IndexCatalog::GetIndexName(oid_t index_oid,
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

oid_t IndexCatalog::GetTableOid(oid_t index_oid, concurrency::Transaction *txn) {
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

IndexType IndexCatalog::GetIndexType(oid_t index_oid, concurrency::Transaction *txn) {
  std::vector<oid_t> column_ids({3});  // index_type
  oid_t index_offset = 0;              // Index of index_oid
  std::vector<type::Value> values;
  values.push_back(type::ValueFactory::GetIntegerValue(index_oid).Copy());

  auto result_tiles =
      GetResultWithIndexScan(column_ids, index_offset, values, txn);

  IndexType index_type;
  PL_ASSERT(result_tiles.size() <= 1);  // table_oid is unique
  if (result_tiles.size() != 0) {
    PL_ASSERT(result_tiles[0]->GetTupleCount() <= 1);
    if (result_tiles[0]->GetTupleCount() != 0) {
      index_type = result_tiles[0]
                       ->GetValue(0, 0)
                       .GetAs<IndexType>();  // After projection left 1 column
    }
  }

  return index_type;
}

IndexConstraintType IndexCatalog::GetIndexConstraint(oid_t index_oid,
                                       concurrency::Transaction *txn) {
  std::vector<oid_t> column_ids({4});  // index_constraint
  oid_t index_offset = 0;              // Index of index_oid
  std::vector<type::Value> values;
  values.push_back(type::ValueFactory::GetIntegerValue(index_oid).Copy());

  auto result_tiles =
      GetResultWithIndexScan(column_ids, index_offset, values, txn);

  IndexConstraintType index_constraint;
  PL_ASSERT(result_tiles.size() <= 1);  // table_oid is unique
  if (result_tiles.size() != 0) {
    PL_ASSERT(result_tiles[0]->GetTupleCount() <= 1);
    if (result_tiles[0]->GetTupleCount() != 0) {
      index_constraint =
          result_tiles[0]
              ->GetValue(0, 0)
              .GetAs<IndexConstraintType>();  // After projection left 1 column
    }
  }

  return index_constraint;
}

bool IndexCatalog::IsUniqueKeys(oid_t index_oid, concurrency::Transaction *txn) {
  std::vector<oid_t> column_ids({5});  // unique_keys
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

std::vector<oid_t> IndexCatalog::GetIndexOids(oid_t table_oid,
                                concurrency::Transaction *txn) {
  std::vector<oid_t> column_ids({0});  // index_oid
  oid_t index_offset = 2;              // Index of table_oid
  std::vector<type::Value> values;
  values.push_back(type::ValueFactory::GetIntegerValue(table_oid).Copy());

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

oid_t IndexCatalog::GetIndexOid(std::string &index_name, oid_t table_oid,
                  concurrency::Transaction *txn) {
  std::vector<oid_t> column_ids({0});  // index_oid
  oid_t index_offset = 1;              // Index of index_name & table_oid
  std::vector<type::Value> values;
  values.push_back(
      type::ValueFactory::GetVarcharValue(index_name, nullptr).Copy());
  values.push_back(type::ValueFactory::GetIntegerValue(table_oid).Copy());

  auto result_tiles =
      GetResultWithIndexScan(column_ids, index_offset, values, txn);

  bool index_oid = false;
  PL_ASSERT(result_tiles.size() <= 1);  // index_name & table_oid is unique
  if (result_tiles.size() != 0) {
    PL_ASSERT(result_tiles[0]->GetTupleCount() <= 1);
    if (result_tiles[0]->GetTupleCount() != 0) {
      index_oid = result_tiles[0]
                      ->GetValue(0, 0)
                      .GetAs<oid_t>();  // After projection left 1 column
    }
  }

  return index_oid;
}

}  // End catalog namespace
}  // End peloton namespace