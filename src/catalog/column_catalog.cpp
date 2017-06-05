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

#include "executor/logical_tile.h"
#include "storage/data_table.h"
#include "storage/tuple.h"

namespace peloton {
namespace catalog {

ColumnCatalog *ColumnCatalog::GetInstance(storage::Database *pg_catalog,
                                          type::AbstractPool *pool,
                                          concurrency::Transaction *txn) {
  static std::unique_ptr<ColumnCatalog> column_catalog(
      new ColumnCatalog(pg_catalog, pool, txn));

  return column_catalog.get();
}

ColumnCatalog::ColumnCatalog(storage::Database *pg_catalog,
                             type::AbstractPool *pool,
                             concurrency::Transaction *txn)
    : AbstractCatalog(COLUMN_CATALOG_OID, COLUMN_CATALOG_NAME,
                      InitializeSchema().release(), pg_catalog) {
  // Add indexes for pg_attribute
  AddIndex({0, 1}, COLUMN_CATALOG_PKEY_OID, COLUMN_CATALOG_NAME "_pkey",
           IndexConstraintType::PRIMARY_KEY);
  AddIndex({0, 2}, COLUMN_CATALOG_SKEY0_OID, COLUMN_CATALOG_NAME "_skey0",
           IndexConstraintType::UNIQUE);
  AddIndex({0}, COLUMN_CATALOG_SKEY1_OID, COLUMN_CATALOG_NAME "_skey1",
           IndexConstraintType::DEFAULT);

  // Insert columns into pg_attribute
  oid_t column_id = 0;
  for (auto column : catalog_table_->GetSchema()->GetColumns()) {
    InsertColumn(COLUMN_CATALOG_OID, column.GetName(), column_id,
                 column.GetOffset(), column.GetType(), column.IsInlined(),
                 column.GetConstraints(), pool, txn);
    column_id++;
  }
}

ColumnCatalog::~ColumnCatalog() {}

/*@brief   private function for initialize schema of pg_attribute
* @return  unqiue pointer to schema
*/
std::unique_ptr<catalog::Schema> ColumnCatalog::InitializeSchema() {
  const std::string primary_key_constraint_name = "primary_key";
  const std::string not_null_constraint_name = "not_null";

  auto table_id_column = catalog::Column(
      type::TypeId::INTEGER, type::Type::GetTypeSize(type::TypeId::INTEGER),
      "table_oid", true);
  table_id_column.AddConstraint(catalog::Constraint(
      ConstraintType::PRIMARY, primary_key_constraint_name));
  table_id_column.AddConstraint(
      catalog::Constraint(ConstraintType::NOTNULL, not_null_constraint_name));

  auto column_name_column =
      catalog::Column(type::TypeId::VARCHAR, max_name_size, "column_name", false);
  column_name_column.AddConstraint(catalog::Constraint(
      ConstraintType::PRIMARY, primary_key_constraint_name));
  column_name_column.AddConstraint(
      catalog::Constraint(ConstraintType::NOTNULL, not_null_constraint_name));

  auto column_id_column = catalog::Column(
      type::TypeId::INTEGER, type::Type::GetTypeSize(type::TypeId::INTEGER),
      "column_id", true);
  column_id_column.AddConstraint(
      catalog::Constraint(ConstraintType::NOTNULL, not_null_constraint_name));

  auto column_offset_column = catalog::Column(
      type::TypeId::INTEGER, type::Type::GetTypeSize(type::TypeId::INTEGER),
      "column_offset", true);
  column_offset_column.AddConstraint(
      catalog::Constraint(ConstraintType::NOTNULL, not_null_constraint_name));

  auto column_type_column =
      catalog::Column(type::TypeId::VARCHAR, max_name_size, "column_type", false);
  column_type_column.AddConstraint(
      catalog::Constraint(ConstraintType::NOTNULL, not_null_constraint_name));

  auto is_inlined_column = catalog::Column(
      type::TypeId::BOOLEAN, type::Type::GetTypeSize(type::TypeId::BOOLEAN),
      "is_inlined", true);
  is_inlined_column.AddConstraint(
      catalog::Constraint(ConstraintType::NOTNULL, not_null_constraint_name));

  auto is_primary_column = catalog::Column(
      type::TypeId::BOOLEAN, type::Type::GetTypeSize(type::TypeId::BOOLEAN),
      "is_primary", true);
  is_primary_column.AddConstraint(
      catalog::Constraint(ConstraintType::NOTNULL, not_null_constraint_name));

  auto is_not_null_column = catalog::Column(
      type::TypeId::BOOLEAN, type::Type::GetTypeSize(type::TypeId::BOOLEAN),
      "is_not_null", true);
  is_not_null_column.AddConstraint(
      catalog::Constraint(ConstraintType::NOTNULL, not_null_constraint_name));

  std::unique_ptr<catalog::Schema> column_catalog_schema(new catalog::Schema(
      {table_id_column, column_name_column, column_id_column,
       column_offset_column, column_type_column, is_inlined_column,
       is_primary_column, is_not_null_column}));

  return column_catalog_schema;
}

bool ColumnCatalog::InsertColumn(
    oid_t table_oid, const std::string &column_name, oid_t column_id,
    oid_t column_offset, type::TypeId column_type, bool is_inlined,
    const std::vector<Constraint> &constraints, type::AbstractPool *pool,
    concurrency::Transaction *txn) {
  // Create the tuple first
  std::unique_ptr<storage::Tuple> tuple(
      new storage::Tuple(catalog_table_->GetSchema(), true));

  auto val0 = type::ValueFactory::GetIntegerValue(table_oid);
  auto val1 = type::ValueFactory::GetVarcharValue(column_name, nullptr);
  auto val2 = type::ValueFactory::GetIntegerValue(column_id);
  auto val3 = type::ValueFactory::GetIntegerValue(column_offset);
  auto val4 =
      type::ValueFactory::GetVarcharValue(TypeIdToString(column_type), nullptr);
  auto val5 = type::ValueFactory::GetBooleanValue(is_inlined);
  bool is_primary = false, is_not_null = false;
  for (auto constraint : constraints) {
    if (constraint.GetType() == ConstraintType::PRIMARY) {
      is_primary = true;
    } else if (constraint.GetType() == ConstraintType::NOT_NULL ||
               constraint.GetType() == ConstraintType::NOTNULL) {
      is_not_null = true;
    }
  }
  auto val6 = type::ValueFactory::GetBooleanValue(is_primary);
  auto val7 = type::ValueFactory::GetBooleanValue(is_not_null);

  tuple->SetValue(0, val0, pool);
  tuple->SetValue(1, val1, pool);
  tuple->SetValue(2, val2, pool);
  tuple->SetValue(3, val3, pool);
  tuple->SetValue(4, val4, pool);
  tuple->SetValue(5, val5, pool);
  tuple->SetValue(6, val6, pool);
  tuple->SetValue(7, val7, pool);

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

/*@brief   delete all column records from the same table
* this function is useful when calling DropTable
* @param   table_oid
* @param   txn  Transaction
* @return  a vector of table oid
*/
bool ColumnCatalog::DeleteColumns(oid_t table_oid,
                                  concurrency::Transaction *txn) {
  oid_t index_offset = 2;  // Index of table_oid
  std::vector<type::Value> values;
  values.push_back(type::ValueFactory::GetIntegerValue(table_oid).Copy());

  return DeleteWithIndexScan(index_offset, values, txn);
}

/*@brief   get physical offset of one column in the table(e.g this column store
* data that is integer type, then the offset of this column is 4. Since integer
* type has 32-bits)
* @param   table_oid
* @param   column_name
* @param   txn  Transaction
* @return  column offset
*/
oid_t ColumnCatalog::GetColumnOffset(oid_t table_oid,
                                     const std::string &column_name,
                                     concurrency::Transaction *txn) {
  std::vector<oid_t> column_ids({3});  // column_offset
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

/*@brief   get logical position of one column in the table(e.g column id = 0
* indicates this is the first column in the table,column id = 1 indicates this
* is the second column in the table.)
* @param   table_oid
* @param   column_name
* @param   txn  Transaction
* @return  column id
*/
oid_t ColumnCatalog::GetColumnId(oid_t table_oid,
                                 const std::string &column_name,
                                 concurrency::Transaction *txn) {
  std::vector<oid_t> column_ids({2});  // column_id
  oid_t index_offset = 0;              // Index of table_oid & column_name
  std::vector<type::Value> values;
  values.push_back(type::ValueFactory::GetIntegerValue(table_oid).Copy());
  values.push_back(
      type::ValueFactory::GetVarcharValue(column_name, nullptr).Copy());

  auto result_tiles =
      GetResultWithIndexScan(column_ids, index_offset, values, txn);

  oid_t column_id = INVALID_OID;
  PL_ASSERT(result_tiles->size() <= 1);  // table_oid & column_name is unique
  if (result_tiles->size() != 0) {
    PL_ASSERT((*result_tiles)[0]->GetTupleCount() <= 1);
    if ((*result_tiles)[0]->GetTupleCount() != 0) {
      column_id = (*result_tiles)[0]
                      ->GetValue(0, 0)
                      .GetAs<oid_t>();  // After projection left 1 column
    }
  }
  return column_id;
}

std::string ColumnCatalog::GetColumnName(oid_t table_oid, oid_t column_id,
                                         concurrency::Transaction *txn) {
  std::vector<oid_t> column_ids({1});  // column_name
  oid_t index_offset = 1;              // Index of table_oid & column_id
  std::vector<type::Value> values;
  values.push_back(type::ValueFactory::GetIntegerValue(table_oid).Copy());
  values.push_back(type::ValueFactory::GetIntegerValue(column_id).Copy());

  auto result_tiles =
      GetResultWithIndexScan(column_ids, index_offset, values, txn);

  std::string column_name;
  PL_ASSERT(result_tiles->size() <= 1);  // table_oid & column_offset is unique
  if (result_tiles->size() != 0) {
    PL_ASSERT((*result_tiles)[0]->GetTupleCount() <= 1);
    if ((*result_tiles)[0]->GetTupleCount() != 0) {
      column_name = (*result_tiles)[0]
                        ->GetValue(0, 0)
                        .ToString();  // After projection left 1 column
    }
  }

  return column_name;
}

type::TypeId ColumnCatalog::GetColumnType(oid_t table_oid,
                                                std::string column_name,
                                                concurrency::Transaction *txn) {
  std::vector<oid_t> column_ids({4});  // column_type
  oid_t index_offset = 0;              // Index of table_oid & column_name
  std::vector<type::Value> values;
  values.push_back(type::ValueFactory::GetIntegerValue(table_oid).Copy());
  values.push_back(
      type::ValueFactory::GetVarcharValue(column_name, nullptr).Copy());

  auto result_tiles =
      GetResultWithIndexScan(column_ids, index_offset, values, txn);

  type::TypeId column_type = type::TypeId::INVALID;
  PL_ASSERT(result_tiles->size() <= 1);  // table_oid & column_name is unique
  if (result_tiles->size() != 0) {
    PL_ASSERT((*result_tiles)[0]->GetTupleCount() <= 1);
    if ((*result_tiles)[0]->GetTupleCount() != 0) {
      column_type =
          (*result_tiles)[0]
              ->GetValue(0, 0)
              .GetAs<type::TypeId>();  // After projection left 1 column
    }
  }

  return column_type;
}

type::TypeId ColumnCatalog::GetColumnType(oid_t table_oid,
                                                oid_t column_id,
                                                concurrency::Transaction *txn) {
  std::vector<oid_t> column_ids({4});  // column_type
  oid_t index_offset = 1;              // Index of table_oid & column_id
  std::vector<type::Value> values;
  values.push_back(type::ValueFactory::GetIntegerValue(table_oid).Copy());
  values.push_back(type::ValueFactory::GetIntegerValue(column_id).Copy());

  auto result_tiles =
      GetResultWithIndexScan(column_ids, index_offset, values, txn);

  type::TypeId column_type = type::TypeId::INVALID;
  PL_ASSERT(result_tiles->size() <= 1);  // table_oid & column_offset is unique
  if (result_tiles->size() != 0) {
    PL_ASSERT((*result_tiles)[0]->GetTupleCount() <= 1);
    if ((*result_tiles)[0]->GetTupleCount() != 0) {
      column_type =
          (*result_tiles)[0]
              ->GetValue(0, 0)
              .GetAs<type::TypeId>();  // After projection left 1 column
    }
  }

  return column_type;
}

}  // End catalog namespace
}  // End peloton namespace
