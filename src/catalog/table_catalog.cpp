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

#include <memory>

#include "catalog/table_catalog.h"

#include "catalog/database_catalog.h"
#include "catalog/index_catalog.h"
#include "catalog/column_catalog.h"
#include "concurrency/transaction_context.h"
#include "storage/data_table.h"
#include "type/value_factory.h"

namespace peloton {
namespace catalog {

TableCatalogObject::TableCatalogObject(executor::LogicalTile *tile,
                                       concurrency::TransactionContext *txn,
                                       int tupleId)
    : table_oid(tile->GetValue(tupleId, TableCatalog::ColumnId::TABLE_OID)
                    .GetAs<oid_t>()),
      table_name(tile->GetValue(tupleId, TableCatalog::ColumnId::TABLE_NAME)
                     .ToString()),
      database_oid(tile->GetValue(tupleId, TableCatalog::ColumnId::DATABASE_OID)
                       .GetAs<oid_t>()),
      index_objects(),
      index_names(),
      valid_index_objects(false),
      column_objects(),
      column_names(),
      valid_column_objects(false),
      txn(txn) {}

/* @brief   insert index catalog object into cache
 * @param   index_object
 * @return  false if index_name already exists in cache
 */
bool TableCatalogObject::InsertIndexObject(
    std::shared_ptr<IndexCatalogObject> index_object) {
  if (!index_object || index_object->GetIndexOid() == INVALID_OID) {
    return false;  // invalid object
  }

  // check if already in cache
  if (index_objects.find(index_object->GetIndexOid()) != index_objects.end()) {
    LOG_DEBUG("Index %u already exists in cache!", index_object->GetIndexOid());
    return false;
  }

  if (index_names.find(index_object->GetIndexName()) != index_names.end()) {
    LOG_DEBUG("Index %s already exists in cache!",
              index_object->GetIndexName().c_str());
    return false;
  }

  valid_index_objects = true;
  index_objects.insert(
      std::make_pair(index_object->GetIndexOid(), index_object));
  index_names.insert(
      std::make_pair(index_object->GetIndexName(), index_object));
  return true;
}

/* @brief   evict index catalog object from cache
 * @param   index_oid
 * @return  true if index_oid is found and evicted; false if not found
 */
bool TableCatalogObject::EvictIndexObject(oid_t index_oid) {
  if (!valid_index_objects) return false;

  // find index name from index name cache
  auto it = index_objects.find(index_oid);
  if (it == index_objects.end()) {
    return false;  // index oid not found in cache
  }

  auto index_object = it->second;
  PL_ASSERT(index_object);
  index_objects.erase(it);
  index_names.erase(index_object->GetIndexName());
  return true;
}

/* @brief   evict index catalog object from cache
 * @param   index_name
 * @return  true if index_name is found and evicted; false if not found
 */
bool TableCatalogObject::EvictIndexObject(const std::string &index_name) {
  if (!valid_index_objects) return false;

  // find index name from index name cache
  auto it = index_names.find(index_name);
  if (it == index_names.end()) {
    return false;  // index name not found in cache
  }

  auto index_object = it->second;
  PL_ASSERT(index_object);
  index_names.erase(it);
  index_objects.erase(index_object->GetIndexOid());
  return true;
}

/* @brief   evict all index catalog objects from cache
 */
void TableCatalogObject::EvictAllIndexObjects() {
  index_objects.clear();
  index_names.clear();
  valid_index_objects = false;
}

/* @brief   get all index objects of this table into cache
 * @return  map from index oid to cached index object
 */
std::unordered_map<oid_t, std::shared_ptr<IndexCatalogObject>>
TableCatalogObject::GetIndexObjects(bool cached_only) {
  if (!valid_index_objects && !cached_only) {
    // get index catalog objects from pg_index
    valid_index_objects = true;
    index_objects =
        IndexCatalog::GetInstance()->GetIndexObjects(table_oid, txn);
  }
  return index_objects;
}

/* @brief   get index object with index oid from cache
 * @param   index_oid
 * @param   cached_only   if cached only, return nullptr on a cache miss
 * @return  shared pointer to the cached index object, nullptr if not found
 */
std::shared_ptr<IndexCatalogObject> TableCatalogObject::GetIndexObject(
    oid_t index_oid, bool cached_only) {
  GetIndexObjects(cached_only);  // fetch index objects in case we have not
  auto it = index_objects.find(index_oid);
  if (it != index_objects.end()) {
    return it->second;
  }
  return nullptr;
}

/* @brief   get index object with index oid from cache
 * @param   index_name
 * @param   cached_only   if cached only, return nullptr on a cache miss
 * @return  shared pointer to the cached index object, nullptr if not found
 */
std::shared_ptr<IndexCatalogObject> TableCatalogObject::GetIndexObject(
    const std::string &index_name, bool cached_only) {
  GetIndexObjects(cached_only);  // fetch index objects in case we have not
  auto it = index_names.find(index_name);
  if (it != index_names.end()) {
    return it->second;
  }
  return nullptr;
}

/* @brief   insert column catalog object into cache
 * @param   column_object
 * @return  false if column_name already exists in cache
 */
bool TableCatalogObject::InsertColumnObject(
    std::shared_ptr<ColumnCatalogObject> column_object) {
  if (!column_object || column_object->GetTableOid() == INVALID_OID) {
    return false;  // invalid object
  }

  // check if already in cache
  if (column_objects.find(column_object->GetColumnId()) !=
      column_objects.end()) {
    LOG_DEBUG("Column %u already exists in cache!",
              column_object->GetColumnId());
    return false;
  }

  if (column_names.find(column_object->GetColumnName()) != column_names.end()) {
    LOG_DEBUG("Column %s already exists in cache!",
              column_object->GetColumnName().c_str());
    return false;
  }

  valid_column_objects = true;
  column_objects.insert(
      std::make_pair(column_object->GetColumnId(), column_object));
  column_names.insert(
      std::make_pair(column_object->GetColumnName(), column_object));
  return true;
}

/* @brief   evict column catalog object from cache
 * @param   column_id
 * @return  true if column_id is found and evicted; false if not found
 */
bool TableCatalogObject::EvictColumnObject(oid_t column_id) {
  if (!valid_column_objects) return false;

  // find column name from column name cache
  auto it = column_objects.find(column_id);
  if (it == column_objects.end()) {
    return false;  // column id not found in cache
  }

  auto column_object = it->second;
  PL_ASSERT(column_object);
  column_objects.erase(it);
  column_names.erase(column_object->GetColumnName());
  return true;
}

/* @brief   evict column catalog object from cache
 * @param   column_name
 * @return  true if column_name is found and evicted; false if not found
 */
bool TableCatalogObject::EvictColumnObject(const std::string &column_name) {
  if (!valid_column_objects) return false;

  // find column name from column name cache
  auto it = column_names.find(column_name);
  if (it == column_names.end()) {
    return false;  // column name not found in cache
  }

  auto column_object = it->second;
  PL_ASSERT(column_object);
  column_names.erase(it);
  column_objects.erase(column_object->GetColumnId());
  return true;
}

/* @brief   evict all column catalog objects from cache
 * @return  true if column_name is found and evicted; false if not found
 */
void TableCatalogObject::EvictAllColumnObjects() {
  column_objects.clear();
  column_names.clear();
  valid_column_objects = false;
}

/* @brief   get all column objects of this table into cache
 * @return  map from column id to cached column object
 */
std::unordered_map<oid_t, std::shared_ptr<ColumnCatalogObject>>
TableCatalogObject::GetColumnObjects(bool cached_only) {
  if (!valid_column_objects && !cached_only) {
    // get column catalog objects from pg_column
    ColumnCatalog::GetInstance()->GetColumnObjects(table_oid, txn);
    valid_column_objects = true;
  }
  return column_objects;
}

/* @brief   get column object with column id from cache
 * @param   column_id
 * @param   cached_only   if cached only, return nullptr on a cache miss
 * @return  shared pointer to the cached column object, nullptr if not found
 */
std::shared_ptr<ColumnCatalogObject> TableCatalogObject::GetColumnObject(
    oid_t column_id, bool cached_only) {
  GetColumnObjects(cached_only);  // fetch column objects in case we have not
  auto it = column_objects.find(column_id);
  if (it != column_objects.end()) {
    return it->second;
  }
  return nullptr;
}

/* @brief   get column object with column id from cache
 * @param   column_name
 * @param   cached_only   if cached only, return nullptr on a cache miss
 * @return  shared pointer to the cached column object, nullptr if not found
 */
std::shared_ptr<ColumnCatalogObject> TableCatalogObject::GetColumnObject(
    const std::string &column_name, bool cached_only) {
  GetColumnObjects(cached_only);  // fetch column objects in case we have not
  auto it = column_names.find(column_name);
  if (it != column_names.end()) {
    return it->second;
  }
  return nullptr;
}

TableCatalog *TableCatalog::GetInstance(storage::Database *pg_catalog,
                                        type::AbstractPool *pool,
                                        concurrency::TransactionContext *txn) {
  static TableCatalog table_catalog{pg_catalog, pool, txn};
  return &table_catalog;
}

TableCatalog::TableCatalog(storage::Database *pg_catalog,
                           type::AbstractPool *pool,
                           concurrency::TransactionContext *txn)
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
 * @param   txn     TransactionContext
 * @return  Whether insertion is Successful
 */
bool TableCatalog::InsertTable(oid_t table_oid, const std::string &table_name,
                               oid_t database_oid, type::AbstractPool *pool,
                               concurrency::TransactionContext *txn) {
  // Create the tuple first
  std::unique_ptr<storage::Tuple> tuple(
      new storage::Tuple(catalog_table_->GetSchema(), true));

  auto val0 = type::ValueFactory::GetIntegerValue(table_oid);
  auto val1 = type::ValueFactory::GetVarcharValue(table_name, nullptr);
  auto val2 = type::ValueFactory::GetIntegerValue(database_oid);

  tuple->SetValue(TableCatalog::ColumnId::TABLE_OID, val0, pool);
  tuple->SetValue(TableCatalog::ColumnId::TABLE_NAME, val1, pool);
  tuple->SetValue(TableCatalog::ColumnId::DATABASE_OID, val2, pool);

  // Insert the tuple
  return InsertTuple(std::move(tuple), txn);
}

/*@brief   delete a tuple about table info from pg_table(using index scan)
 * @param   table_oid
 * @param   txn     TransactionContext
 * @return  Whether deletion is Successful
 */
bool TableCatalog::DeleteTable(oid_t table_oid, concurrency::TransactionContext *txn) {
  oid_t index_offset = IndexId::PRIMARY_KEY;  // Index of table_oid
  std::vector<type::Value> values;
  values.push_back(type::ValueFactory::GetIntegerValue(table_oid).Copy());

  // evict from cache
  auto table_object = txn->catalog_cache.GetCachedTableObject(table_oid);
  if (table_object) {
    auto database_object = DatabaseCatalog::GetInstance()->GetDatabaseObject(
        table_object->GetDatabaseOid(), txn);
    database_object->EvictTableObject(table_oid);
  }

  return DeleteWithIndexScan(index_offset, values, txn);
}

/*@brief   read table catalog object from pg_table using table oid
 * @param   table_oid
 * @param   txn     TransactionContext
 * @return  table catalog object
 */
std::shared_ptr<TableCatalogObject> TableCatalog::GetTableObject(
    oid_t table_oid, concurrency::TransactionContext *txn) {
  if (txn == nullptr) {
    throw CatalogException("Transaction is invalid!");
  }
  // try get from cache
  auto table_object = txn->catalog_cache.GetCachedTableObject(table_oid);
  if (table_object) return table_object;

  // cache miss, get from pg_table
  std::vector<oid_t> column_ids(all_column_ids);
  oid_t index_offset = IndexId::PRIMARY_KEY;  // Index of table_oid
  std::vector<type::Value> values;
  values.push_back(type::ValueFactory::GetIntegerValue(table_oid).Copy());

  auto result_tiles =
      GetResultWithIndexScan(column_ids, index_offset, values, txn);

  if (result_tiles->size() == 1 && (*result_tiles)[0]->GetTupleCount() == 1) {
    auto table_object =
        std::make_shared<TableCatalogObject>((*result_tiles)[0].get(), txn);
    // insert into cache
    auto database_object = DatabaseCatalog::GetInstance()->GetDatabaseObject(
        table_object->GetDatabaseOid(), txn);
    PL_ASSERT(database_object);
    bool success = database_object->InsertTableObject(table_object);
    PL_ASSERT(success == true);
    (void)success;
    return table_object;
  } else {
    LOG_DEBUG("Found %lu table with oid %u", result_tiles->size(), table_oid);
  }

  // return empty object if not found
  return nullptr;
}

/*@brief   read table catalog object from pg_table using table name + database
 * oid
 * @param   table_name
 * @param   database_oid
 * @param   txn     TransactionContext
 * @return  table catalog object
 */
std::shared_ptr<TableCatalogObject> TableCatalog::GetTableObject(
    const std::string &table_name, oid_t database_oid,
    concurrency::TransactionContext *txn) {
  if (txn == nullptr) {
    throw CatalogException("Transaction is invalid!");
  }
  // try get from cache
  auto database_object = txn->catalog_cache.GetDatabaseObject(database_oid);
  if (database_object) {
    auto table_object = database_object->GetTableObject(table_name, true);
    if (table_object) return table_object;
  }

  // cache miss, get from pg_table
  std::vector<oid_t> column_ids(all_column_ids);
  oid_t index_offset =
      IndexId::SKEY_TABLE_NAME;  // Index of table_name & database_oid
  std::vector<type::Value> values;
  values.push_back(
      type::ValueFactory::GetVarcharValue(table_name, nullptr).Copy());
  values.push_back(type::ValueFactory::GetIntegerValue(database_oid).Copy());

  auto result_tiles =
      GetResultWithIndexScan(column_ids, index_offset, values, txn);

  if (result_tiles->size() == 1 && (*result_tiles)[0]->GetTupleCount() == 1) {
    auto table_object =
        std::make_shared<TableCatalogObject>((*result_tiles)[0].get(), txn);
    // insert into cache
    auto database_object = DatabaseCatalog::GetInstance()->GetDatabaseObject(
        table_object->GetDatabaseOid(), txn);
    PL_ASSERT(database_object);
    bool success = database_object->InsertTableObject(table_object);
    PL_ASSERT(success == true);
    (void)success;
    return table_object;
  }

  // return empty object if not found
  return nullptr;
}

/*@brief   read table catalog objects from pg_table using database oid
 * @param   database_oid
 * @param   txn     TransactionContext
 * @return  table catalog objects
 */
std::unordered_map<oid_t, std::shared_ptr<TableCatalogObject>>
TableCatalog::GetTableObjects(oid_t database_oid,
                              concurrency::TransactionContext *txn) {
  if (txn == nullptr) {
    throw CatalogException("Transaction is invalid!");
  }
  // try get from cache
  auto database_object =
      DatabaseCatalog::GetInstance()->GetDatabaseObject(database_oid, txn);
  PL_ASSERT(database_object != nullptr);
  if (database_object->IsValidTableObjects()) {
    return database_object->GetTableObjects(true);
  }

  // cache miss, get from pg_table
  std::vector<oid_t> column_ids(all_column_ids);
  oid_t index_offset = IndexId::SKEY_DATABASE_OID;  // Index of database_oid
  std::vector<type::Value> values;
  values.push_back(type::ValueFactory::GetIntegerValue(database_oid).Copy());

  auto result_tiles =
      GetResultWithIndexScan(column_ids, index_offset, values, txn);

  for (auto &tile : (*result_tiles)) {
    for (auto tuple_id : *tile) {
      auto table_object =
          std::make_shared<TableCatalogObject>(tile.get(), txn, tuple_id);
      database_object->InsertTableObject(table_object);
    }
  }

  database_object->SetValidTableObjects(true);
  return database_object->GetTableObjects();
}

}  // namespace catalog
}  // namespace peloton
