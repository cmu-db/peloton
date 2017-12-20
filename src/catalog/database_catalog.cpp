//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// database_catalog.cpp
//
// Identification: src/catalog/database_catalog.cpp
//
// Copyright (c) 2015-17, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <memory>

#include "catalog/database_catalog.h"

#include "concurrency/transaction_context.h"
#include "catalog/table_catalog.h"
#include "catalog/column_catalog.h"
#include "executor/logical_tile.h"
#include "storage/data_table.h"
#include "storage/tuple.h"
#include "type/value_factory.h"

namespace peloton {
namespace catalog {

DatabaseCatalogObject::DatabaseCatalogObject(executor::LogicalTile *tile,
                                             concurrency::TransactionContext *txn)
    : database_oid(tile->GetValue(0, DatabaseCatalog::ColumnId::DATABASE_OID)
                       .GetAs<oid_t>()),
      database_name(tile->GetValue(0, DatabaseCatalog::ColumnId::DATABASE_NAME)
                        .ToString()),
      table_objects_cache(),
      table_name_cache(),
      valid_table_objects(false),
      txn(txn) {}

/* @brief   insert table catalog object into cache
 * @param   table_object
 * @return  false if table_name already exists in cache
 */
bool DatabaseCatalogObject::InsertTableObject(
    std::shared_ptr<TableCatalogObject> table_object) {
  if (!table_object || table_object->GetTableOid() == INVALID_OID) {
    return false;  // invalid object
  }

  // check if already in cache
  if (table_objects_cache.find(table_object->GetTableOid()) !=
      table_objects_cache.end()) {
    LOG_DEBUG("Table %u already exists in cache!", table_object->GetTableOid());
    return false;
  }

  if (table_name_cache.find(table_object->GetTableName()) !=
      table_name_cache.end()) {
    LOG_DEBUG("Table %s already exists in cache!",
              table_object->GetTableName().c_str());
    return false;
  }

  table_objects_cache.insert(
      std::make_pair(table_object->GetTableOid(), table_object));
  table_name_cache.insert(
      std::make_pair(table_object->GetTableName(), table_object));
  return true;
}

/* @brief   evict table catalog object from cache
 * @param   table_oid
 * @return  true if table_oid is found and evicted; false if not found
 */
bool DatabaseCatalogObject::EvictTableObject(oid_t table_oid) {
  // find table name from table name cache
  auto it = table_objects_cache.find(table_oid);
  if (it == table_objects_cache.end()) {
    return false;  // table oid not found in cache
  }

  auto table_object = it->second;
  PL_ASSERT(table_object);
  table_objects_cache.erase(it);
  table_name_cache.erase(table_object->GetTableName());
  return true;
}

/* @brief   evict table catalog object from cache
 * @param   table_name
 * @return  true if table_name is found and evicted; false if not found
 */
bool DatabaseCatalogObject::EvictTableObject(const std::string &table_name) {
  // find table name from table name cache
  auto it = table_name_cache.find(table_name);
  if (it == table_name_cache.end()) {
    return false;  // table oid not found in cache
  }

  auto table_object = it->second;
  PL_ASSERT(table_object);
  table_name_cache.erase(it);
  table_objects_cache.erase(table_object->GetTableOid());
  return true;
}

/*@brief   evict all table catalog objects in this database from cache
*/
void DatabaseCatalogObject::EvictAllTableObjects() {
  table_objects_cache.clear();
  table_name_cache.clear();
}

/* @brief   Get table catalog object from cache or all the way from storage
 * @param   table_oid     table oid of the requested table catalog object
 * @param   cached_only   if cached only, return nullptr on a cache miss
 * @return  Shared pointer to the requested table catalog object
 */
std::shared_ptr<TableCatalogObject> DatabaseCatalogObject::GetTableObject(
    oid_t table_oid, bool cached_only) {
  auto it = table_objects_cache.find(table_oid);
  if (it != table_objects_cache.end()) return it->second;

  if (cached_only) {
    // cache miss return empty object
    return nullptr;
  } else {
    // cache miss get from pg_table
    return TableCatalog::GetInstance()->GetTableObject(table_oid, txn);
  }
}

/* @brief   Get table catalog object from cache (cached_only == true),
            or all the way from storage (cached_only == false)
 * @param   table_oid     table oid of the requested table catalog object
 * @param   cached_only   if cached only, return nullptr on a cache miss
 * @return  Shared pointer to the requested table catalog object
 */
std::shared_ptr<TableCatalogObject> DatabaseCatalogObject::GetTableObject(
    const std::string &table_name, bool cached_only) {
  auto it = table_name_cache.find(table_name);
  if (it != table_name_cache.end()) return it->second;

  if (cached_only) {
    // cache miss return empty object
    return nullptr;
  } else {
    // cache miss get from pg_table
    return TableCatalog::GetInstance()->GetTableObject(table_name, database_oid,
                                                       txn);
  }
}

/* @brief   Get table catalog object from cache (cached_only == true),
            or all the way from storage (cached_only == false)
 * @param   cached_only   if cached only, return nullptr on a cache miss
 * @return  Shared pointer to the requested table catalog object
 */
std::unordered_map<oid_t, std::shared_ptr<TableCatalogObject>>
DatabaseCatalogObject::GetTableObjects(bool cached_only) {
  if (!cached_only && !valid_table_objects) {
    // cache miss get from pg_table
    return TableCatalog::GetInstance()->GetTableObjects(database_oid, txn);
  }
  // make sure to check IsValidTableObjects() before getting table objects
  PL_ASSERT(valid_table_objects);
  return table_objects_cache;
}

/*@brief   search index catalog object from all cached database objects
* @param   index_oid
* @return  index catalog object; if not found return null
*/
std::shared_ptr<IndexCatalogObject> DatabaseCatalogObject::GetCachedIndexObject(
    oid_t index_oid) {
  for (auto it = table_objects_cache.begin(); it != table_objects_cache.end();
       ++it) {
    auto table_object = it->second;
    auto index_object = table_object->GetIndexObject(index_oid, true);
    if (index_object) return index_object;
  }
  return nullptr;
}

/*@brief   search index catalog object from all cached database objects
* @param   index_name
* @return  index catalog object; if not found return null
*/
std::shared_ptr<IndexCatalogObject> DatabaseCatalogObject::GetCachedIndexObject(
    const std::string &index_name) {
  for (auto it = table_objects_cache.begin(); it != table_objects_cache.end();
       ++it) {
    auto table_object = it->second;
    auto index_object = table_object->GetIndexObject(index_name, true);
    if (index_object) return index_object;
  }
  return nullptr;
}

DatabaseCatalog *DatabaseCatalog::GetInstance(storage::Database *pg_catalog,
                                              type::AbstractPool *pool,
                                              concurrency::TransactionContext *txn) {
  static DatabaseCatalog database_catalog{pg_catalog, pool, txn};
  return &database_catalog;
}

DatabaseCatalog::DatabaseCatalog(storage::Database *pg_catalog,
                                 type::AbstractPool *pool,
                                 concurrency::TransactionContext *txn)
    : AbstractCatalog(DATABASE_CATALOG_OID, DATABASE_CATALOG_NAME,
                      InitializeSchema().release(), pg_catalog) {
  // Insert columns into pg_attribute
  ColumnCatalog *pg_attribute =
      ColumnCatalog::GetInstance(pg_catalog, pool, txn);

  oid_t column_id = 0;
  for (auto column : catalog_table_->GetSchema()->GetColumns()) {
    pg_attribute->InsertColumn(DATABASE_CATALOG_OID, column.GetName(),
                               column_id, column.GetOffset(), column.GetType(),
                               column.IsInlined(), column.GetConstraints(),
                               pool, txn);
    column_id++;
  }
}

DatabaseCatalog::~DatabaseCatalog() {}

/*@brief   private function for initialize schema of pg_database
 * @return  unqiue pointer to schema
 */
std::unique_ptr<catalog::Schema> DatabaseCatalog::InitializeSchema() {
  const std::string not_null_constraint_name = "not_null";
  const std::string primary_key_constraint_name = "primary_key";

  auto database_id_column = catalog::Column(
      type::TypeId::INTEGER, type::Type::GetTypeSize(type::TypeId::INTEGER),
      "database_oid", true);
  database_id_column.AddConstraint(catalog::Constraint(
      ConstraintType::PRIMARY, primary_key_constraint_name));
  database_id_column.AddConstraint(
      catalog::Constraint(ConstraintType::NOTNULL, not_null_constraint_name));

  auto database_name_column = catalog::Column(
      type::TypeId::VARCHAR, max_name_size, "database_name", false);
  database_name_column.AddConstraint(
      catalog::Constraint(ConstraintType::NOTNULL, not_null_constraint_name));

  std::unique_ptr<catalog::Schema> database_catalog_schema(
      new catalog::Schema({database_id_column, database_name_column}));
  return database_catalog_schema;
}

bool DatabaseCatalog::InsertDatabase(oid_t database_oid,
                                     const std::string &database_name,
                                     type::AbstractPool *pool,
                                     concurrency::TransactionContext *txn) {
  std::unique_ptr<storage::Tuple> tuple(
      new storage::Tuple(catalog_table_->GetSchema(), true));

  auto val0 = type::ValueFactory::GetIntegerValue(database_oid);
  auto val1 = type::ValueFactory::GetVarcharValue(database_name, nullptr);

  tuple->SetValue(ColumnId::DATABASE_OID, val0, pool);
  tuple->SetValue(ColumnId::DATABASE_NAME, val1, pool);

  // Insert the tuple
  return InsertTuple(std::move(tuple), txn);
}

bool DatabaseCatalog::DeleteDatabase(oid_t database_oid,
                                     concurrency::TransactionContext *txn) {
  oid_t index_offset = IndexId::PRIMARY_KEY;  // Index of database_oid
  std::vector<type::Value> values;
  values.push_back(type::ValueFactory::GetIntegerValue(database_oid).Copy());

  // evict cache
  txn->catalog_cache.EvictDatabaseObject(database_oid);

  return DeleteWithIndexScan(index_offset, values, txn);
}

std::shared_ptr<DatabaseCatalogObject> DatabaseCatalog::GetDatabaseObject(
    oid_t database_oid, concurrency::TransactionContext *txn) {
  if (txn == nullptr) {
    throw CatalogException("Transaction is invalid!");
  }
  // try get from cache
  auto database_object = txn->catalog_cache.GetDatabaseObject(database_oid);
  if (database_object) return database_object;

  // cache miss, get from pg_database
  std::vector<oid_t> column_ids(all_column_ids);
  oid_t index_offset = IndexId::PRIMARY_KEY;  // Index of database_oid
  std::vector<type::Value> values;
  values.push_back(type::ValueFactory::GetIntegerValue(database_oid).Copy());

  auto result_tiles =
      GetResultWithIndexScan(column_ids, index_offset, values, txn);

  if (result_tiles->size() == 1 && (*result_tiles)[0]->GetTupleCount() == 1) {
    auto database_object =
        std::make_shared<DatabaseCatalogObject>((*result_tiles)[0].get(), txn);
    // insert into cache
    bool success = txn->catalog_cache.InsertDatabaseObject(database_object);
    PL_ASSERT(success == true);
    (void)success;
    return database_object;
  } else {
    LOG_DEBUG("Found %lu database tiles with oid %u", result_tiles->size(),
              database_oid);
  }

  // return empty object if not found
  return nullptr;
}

/* @brief   First try get cached object from transaction cache. If cache miss,
 *          construct database object from pg_database, and insert into the
 *          cache.
 */
std::shared_ptr<DatabaseCatalogObject> DatabaseCatalog::GetDatabaseObject(
    const std::string &database_name, concurrency::TransactionContext *txn) {
  if (txn == nullptr) {
    throw CatalogException("Transaction is invalid!");
  }
  // try get from cache
  auto database_object = txn->catalog_cache.GetDatabaseObject(database_name);
  if (database_object) return database_object;

  // cache miss, get from pg_database
  std::vector<oid_t> column_ids(all_column_ids);
  oid_t index_offset = IndexId::SKEY_DATABASE_NAME;  // Index of database_name
  std::vector<type::Value> values;
  values.push_back(
      type::ValueFactory::GetVarcharValue(database_name, nullptr).Copy());

  auto result_tiles =
      GetResultWithIndexScan(column_ids, index_offset, values, txn);

  if (result_tiles->size() == 1 && (*result_tiles)[0]->GetTupleCount() == 1) {
    auto database_object =
        std::make_shared<DatabaseCatalogObject>((*result_tiles)[0].get(), txn);
    if (database_object) {
      // insert into cache
      bool success = txn->catalog_cache.InsertDatabaseObject(database_object);
      PL_ASSERT(success == true);
      (void)success;
    }
    return database_object;
  }

  // return empty object if not found
  return nullptr;
}

}  // namespace catalog
}  // namespace peloton
