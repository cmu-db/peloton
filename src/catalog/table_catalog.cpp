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

#include "catalog/table_catalog.h"

#include "executor/logical_tile.h"
#include "storage/data_table.h"
#include "storage/tuple.h"

namespace peloton {
namespace catalog {

std::unordered_map<oid_t, IndexCatalogObject>
TableCatalogObject::GetIndexObjects() {
  if (!valid_index_objects) {
    index_objects =
        IndexCatalog::GetInstance()->GetIndexesByTableOid(table_oid, txn);
  }
  return index_objects;
}

std::unordered_map<size_t, ColumnCatalogObject>
TableCatalogObject::GetColumnObjects() {
  if (!valid_column_objects) {
    ColumnCatalog::GetInstance()->GetColumnsByTableOid(table_oid, txn);
  }
  return column_objects;
}

TableCatalog *TableCatalog::GetInstance(storage::Database *pg_catalog,
                                        type::AbstractPool *pool,
                                        concurrency::Transaction *txn) {
  static TableCatalog table_catalog{pg_catalog, pool, txn};
  return &table_catalog;
}

TableCatalog::TableCatalog(storage::Database *pg_catalog,
                           type::AbstractPool *pool,
                           concurrency::Transaction *txn)
    : AbstractCatalog(TABLE_CATALOG_OID, TABLE_CATALOG_NAME,
                      InitializeSchema().release(), pg_catalog) {
  // Insert columns into pg_attribute
  ColumnCatalog *pg_attribute =
      ColumnCatalog::GetInstance(pg_catalog, pool, txn);

  oid_t column_id = 0;
  for (auto column : catalog_table_->GetSchema()->GetColumns()) {
    pg_attribute->InsertColumn(TABLE_CATALOG_OID, column.GetName(), column_id,
                               column.GetOffset(), column.GetType(),
                               column.IsInlined(), column.GetConstraints(),
                               pool, txn);
    column_id++;
  }
}

TableCatalog::~TableCatalog() {}

/*@brief   private function for initialize schema of pg_table
* @return  unqiue pointer to schema
*/
std::unique_ptr<catalog::Schema> TableCatalog::InitializeSchema() {
  const std::string primary_key_constraint_name = "primary_key";
  const std::string not_null_constraint_name = "not_null";

  auto table_id_column = catalog::Column(
      type::TypeId::INTEGER, type::Type::GetTypeSize(type::TypeId::INTEGER),
      "table_oid", true);
  table_id_column.AddConstraint(catalog::Constraint(
      ConstraintType::PRIMARY, primary_key_constraint_name));
  table_id_column.AddConstraint(
      catalog::Constraint(ConstraintType::NOTNULL, not_null_constraint_name));

  auto table_name_column = catalog::Column(type::TypeId::VARCHAR, max_name_size,
                                           "table_name", false);
  table_name_column.AddConstraint(
      catalog::Constraint(ConstraintType::NOTNULL, not_null_constraint_name));

  auto database_id_column = catalog::Column(
      type::TypeId::INTEGER, type::Type::GetTypeSize(type::TypeId::INTEGER),
      "database_oid", true);
  database_id_column.AddConstraint(
      catalog::Constraint(ConstraintType::NOTNULL, not_null_constraint_name));

  std::unique_ptr<catalog::Schema> table_catalog_schema(new catalog::Schema(
      {table_id_column, table_name_column, database_id_column}));

  return table_catalog_schema;
}

/*@brief   insert a tuple about table info into pg_table
* @param   table_oid
* @param   table_name
* @param   database_oid
* @param   txn     Transaction
* @return  Whether insertion is Successful
*/
bool TableCatalog::InsertTable(oid_t table_oid, const std::string &table_name,
                               oid_t database_oid, type::AbstractPool *pool,
                               concurrency::Transaction *txn) {
  // Create the tuple first
  std::unique_ptr<storage::Tuple> tuple(
      new storage::Tuple(catalog_table_->GetSchema(), true));

  auto val0 = type::ValueFactory::GetIntegerValue(table_oid);
  auto val1 = type::ValueFactory::GetVarcharValue(table_name, nullptr);
  auto val2 = type::ValueFactory::GetIntegerValue(database_oid);

  tuple->SetValue(0, val0, pool);
  tuple->SetValue(1, val1, pool);
  tuple->SetValue(2, val2, pool);

  // Insert the tuple
  return InsertTuple(std::move(tuple), txn);
}

/*@brief   delete a tuple about table info from pg_table(using index scan)
* @param   table_oid
* @param   txn     Transaction
* @return  Whether deletion is Successful
*/
bool TableCatalog::DeleteTable(oid_t table_oid, concurrency::Transaction *txn) {
  oid_t index_offset = 0;  // Index of table_oid
  std::vector<type::Value> values;
  values.push_back(type::ValueFactory::GetIntegerValue(table_oid).Copy());

  return DeleteWithIndexScan(index_offset, values, txn);
}

/*@brief   read table catalog object from pg_table using table oid
* @param   table_oid
* @param   txn     Transaction
* @return  table catalog object
*/
const TableCatalogObject TableCatalog::GetTableByOid(
    oid_t table_oid, concurrency::Transaction *txn) {
  std::vector<oid_t> column_ids({0, 1, 2});
  oid_t index_offset = 0;  // Index of table_oid
  std::vector<type::Value> values;
  values.push_back(type::ValueFactory::GetIntegerValue(table_oid).Copy());

  auto result_tiles =
      GetResultWithIndexScan(column_ids, index_offset, values, txn);

  if (result_tiles->size() == 1 && (*result_tiles)[0]->GetTupleCount() == 1) {
    return TableCatalogObject((*result_tiles)[0].get(), txn);
  } else {
    LOG_DEBUG("Found %lu table with oid %u", result_tiles->size(), table_oid);
  }

  return TableCatalogObject();  // return empty object with INVALID_OID
}

/*@brief   read table catalog object from pg_table using table name + database
* oid
* @param   table_name
* @param   database_oid
* @param   txn     Transaction
* @return  table catalog object
*/
const TableCatalogObject TableCatalog::GetTableByName(
    const std::string &table_name, oid_t database_oid,
    concurrency::Transaction *txn) {
  std::vector<oid_t> column_ids({0, 1, 2});
  oid_t index_offset = 1;  // Index of table_name & database_oid
  std::vector<type::Value> values;
  values.push_back(
      type::ValueFactory::GetVarcharValue(table_name, nullptr).Copy());
  values.push_back(type::ValueFactory::GetIntegerValue(database_oid).Copy());

  auto result_tiles =
      GetResultWithIndexScan(column_ids, index_offset, values, txn);

  if (result_tiles->size() == 1 && (*result_tiles)[0]->GetTupleCount() == 1) {
    return TableCatalogObject((*result_tiles)[0].get(), txn);
  } else {
    LOG_DEBUG("Found %lu table with name %s", result_tiles->size(),
              table_name.c_str());
  }

  return TableCatalogObject();  // return empty object with INVALID_OID
}

/*@brief   read table name from pg_table using table oid
* @param   table_oid
* @param   txn     Transaction
* @return  table name
*/
std::string TableCatalog::GetTableName(oid_t table_oid,
                                       concurrency::Transaction *txn) {
  std::vector<oid_t> column_ids({1});  // table_name
  oid_t index_offset = 0;              // Index of table_oid
  std::vector<type::Value> values;
  values.push_back(type::ValueFactory::GetIntegerValue(table_oid).Copy());

  auto result_tiles =
      GetResultWithIndexScan(column_ids, index_offset, values, txn);

  std::string table_name;
  PL_ASSERT(result_tiles->size() <= 1);  // table_oid is unique
  if (result_tiles->size() != 0) {
    PL_ASSERT((*result_tiles)[0]->GetTupleCount() <= 1);
    if ((*result_tiles)[0]->GetTupleCount() != 0) {
      table_name = (*result_tiles)[0]
                       ->GetValue(0, 0)
                       .ToString();  // After projection left 1 column
    }
  }

  return table_name;
}

/*@brief   read database oid one table belongs to using table oid
* @param   table_oid
* @param   txn     Transaction
* @return  database oid
*/
oid_t TableCatalog::GetDatabaseOid(oid_t table_oid,
                                   concurrency::Transaction *txn) {
  std::vector<oid_t> column_ids({2});  // database_oid
  oid_t index_offset = 0;              // Index of table_oid
  std::vector<type::Value> values;
  values.push_back(type::ValueFactory::GetIntegerValue(table_oid).Copy());

  auto result_tiles =
      GetResultWithIndexScan(column_ids, index_offset, values, txn);

  oid_t database_oid = INVALID_OID;
  PL_ASSERT(result_tiles->size() <= 1);  // table_oid is unique
  if (result_tiles->size() != 0) {
    PL_ASSERT((*result_tiles)[0]->GetTupleCount() <= 1);
    if ((*result_tiles)[0]->GetTupleCount() != 0) {
      database_oid = (*result_tiles)[0]
                         ->GetValue(0, 0)
                         .GetAs<oid_t>();  // After projection left 1 column
    }
  }

  return database_oid;
}

/*@brief   read table oid from pg_table using table name + database oid
* @param   table_name
* @param   database_oid
* @param   txn     Transaction
* @return  table oid
*/
oid_t TableCatalog::GetTableOid(const std::string &table_name,
                                oid_t database_oid,
                                concurrency::Transaction *txn) {
  std::vector<oid_t> column_ids({0});  // table_oid
  oid_t index_offset = 1;              // Index of table_name & database_oid
  std::vector<type::Value> values;
  values.push_back(
      type::ValueFactory::GetVarcharValue(table_name, nullptr).Copy());
  values.push_back(type::ValueFactory::GetIntegerValue(database_oid).Copy());

  auto result_tiles =
      GetResultWithIndexScan(column_ids, index_offset, values, txn);

  oid_t table_oid = INVALID_OID;
  PL_ASSERT(result_tiles->size() <= 1);  // table_name & database_oid is unique
  if (result_tiles->size() != 0) {
    PL_ASSERT((*result_tiles)[0]->GetTupleCount() <= 1);
    if ((*result_tiles)[0]->GetTupleCount() != 0) {
      table_oid = (*result_tiles)[0]
                      ->GetValue(0, 0)
                      .GetAs<oid_t>();  // After projection left 1 column
    }
  }

  return table_oid;
}

/*@brief   read all table oids from the same database using its oid
* @param   database_oid
* @param   txn  Transaction
* @return  a vector of table oid
*/
std::vector<oid_t> TableCatalog::GetTableOids(oid_t database_oid,
                                              concurrency::Transaction *txn) {
  std::vector<oid_t> column_ids({0});  // table_oid
  oid_t index_offset = 2;              // Index of database_oid
  std::vector<type::Value> values;
  values.push_back(type::ValueFactory::GetIntegerValue(database_oid).Copy());

  auto result_tiles =
      GetResultWithIndexScan(column_ids, index_offset, values, txn);

  std::vector<oid_t> table_ids;
  for (auto &tile : (*result_tiles)) {
    for (auto tuple_id : *tile) {
      table_ids.emplace_back(
          tile->GetValue(tuple_id, 0)
              .GetAs<oid_t>());  // After projection left 1 column
    }
  }

  return table_ids;
}

/*@brief   read all table names from the same database using its oid
* @param   database_oid
* @param   txn  Transaction
* @return  a vector of table name
*/
std::vector<std::string> TableCatalog::GetTableNames(
    oid_t database_oid, concurrency::Transaction *txn) {
  std::vector<oid_t> column_ids({1});  // table_name
  oid_t index_offset = 2;              // Index of database_oid
  std::vector<type::Value> values;
  values.push_back(type::ValueFactory::GetIntegerValue(database_oid).Copy());

  auto result_tiles =
      GetResultWithIndexScan(column_ids, index_offset, values, txn);

  std::vector<std::string> table_names;
  for (auto &tile : (*result_tiles)) {
    for (auto tuple_id : *tile) {
      table_names.emplace_back(
          tile->GetValue(tuple_id, 0)
              .ToString());  // After projection left 1 column
    }
  }

  return table_names;
}

}  // namespace catalog
}  // namespace peloton
