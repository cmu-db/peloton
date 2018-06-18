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

#include "catalog/database_catalog.h"

#include <memory>

#include "catalog/catalog.h"
#include "catalog/system_catalogs.h"
#include "concurrency/transaction_context.h"
#include "executor/logical_tile.h"
#include "storage/data_table.h"
#include "storage/tuple.h"
#include "type/value_factory.h"

namespace peloton {
namespace catalog {

DatabaseCatalogEntry::DatabaseCatalogEntry(concurrency::TransactionContext *txn,
                                           executor::LogicalTile *tile)
    : database_oid_(tile->GetValue(0, DatabaseCatalog::ColumnId::DATABASE_OID)
                       .GetAs<oid_t>()),
      database_name_(tile->GetValue(0, DatabaseCatalog::ColumnId::DATABASE_NAME)
                        .ToString()),
      table_objects_cache_(),
      table_name_cache_(),
      valid_table_objects_(false),
      txn_(txn) {}

/* @brief   insert table catalog object into cache
 * @param   table_object
 * @return  false if table_name already exists in cache
 */
bool DatabaseCatalogEntry::InsertTableObject(
    std::shared_ptr<TableCatalogEntry> table_object) {
  if (!table_object || table_object->GetTableOid() == INVALID_OID) {
    return false;  // invalid object
  }

  // check if already in cache
  if (table_objects_cache_.find(table_object->GetTableOid()) !=
      table_objects_cache_.end()) {
    LOG_DEBUG("Table %u already exists in cache!", table_object->GetTableOid());
    return false;
  }

  std::string key =
      table_object->GetSchemaName() + "." + table_object->GetTableName();
  if (table_name_cache_.find(key) != table_name_cache_.end()) {
    LOG_DEBUG("Table %s already exists in cache!",
              table_object->GetTableName().c_str());
    return false;
  }

  table_objects_cache_.insert(
      std::make_pair(table_object->GetTableOid(), table_object));
  table_name_cache_.insert(std::make_pair(key, table_object));
  return true;
}

/* @brief   evict table catalog object from cache
 * @param   table_oid
 * @return  true if table_oid is found and evicted; false if not found
 */
bool DatabaseCatalogEntry::EvictTableObject(oid_t table_oid) {
  // find table name from table name cache
  auto it = table_objects_cache_.find(table_oid);
  if (it == table_objects_cache_.end()) {
    return false;  // table oid not found in cache
  }

  auto table_object = it->second;
  PELOTON_ASSERT(table_object);
  table_objects_cache_.erase(it);
  // erase from table name cache
  std::string key =
      table_object->GetSchemaName() + "." + table_object->GetTableName();
  table_name_cache_.erase(key);
  return true;
}

/* @brief   evict table catalog object from cache
 * @param   table_name
 * @return  true if table_name is found and evicted; false if not found
 */
bool DatabaseCatalogEntry::EvictTableObject(const std::string &table_name,
                                             const std::string &schema_name) {
  std::string key = schema_name + "." + table_name;
  // find table name from table name cache
  auto it = table_name_cache_.find(key);
  if (it == table_name_cache_.end()) {
    return false;  // table name not found in cache
  }

  auto table_object = it->second;
  PELOTON_ASSERT(table_object);
  table_name_cache_.erase(it);
  table_objects_cache_.erase(table_object->GetTableOid());
  return true;
}

/*@brief   evict all table catalog objects in this database from cache
 */
void DatabaseCatalogEntry::EvictAllTableObjects() {
  table_objects_cache_.clear();
  table_name_cache_.clear();
}

/* @brief   Get table catalog object from cache or all the way from storage
 * @param   table_oid     table oid of the requested table catalog object
 * @param   cached_only   if cached only, return nullptr on a cache miss
 * @return  Shared pointer to the requested table catalog object
 */
std::shared_ptr<TableCatalogEntry> DatabaseCatalogEntry::GetTableObject(
    oid_t table_oid, bool cached_only) {
  auto it = table_objects_cache_.find(table_oid);
  if (it != table_objects_cache_.end()) return it->second;

  if (cached_only) {
    // cache miss return empty object
    return nullptr;
  } else {
    // cache miss get from pg_table
    auto pg_table = Catalog::GetInstance()
                        ->GetSystemCatalogs(database_oid_)
                        ->GetTableCatalog();
    return pg_table->GetTableObject(txn_, table_oid);
  }
}

/* @brief   Get table catalog object from cache (cached_only == true),
            or all the way from storage (cached_only == false)
 * @param   table_name     table name of the requested table catalog object
 * @param   schema_name    schema name of the requested table catalog object
 * @param   cached_only   if cached only, return nullptr on a cache miss
 * @return  Shared pointer to the requested table catalog object
 */
std::shared_ptr<TableCatalogEntry> DatabaseCatalogEntry::GetTableObject(
    const std::string &table_name, const std::string &schema_name,
    bool cached_only) {
  std::string key = schema_name + "." + table_name;
  auto it = table_name_cache_.find(key);
  if (it != table_name_cache_.end()) {
    return it->second;
  }

  if (cached_only) {
    // cache miss return empty object
    return nullptr;
  } else {
    // cache miss get from pg_table
    auto pg_table = Catalog::GetInstance()
                        ->GetSystemCatalogs(database_oid_)
                        ->GetTableCatalog();
    return pg_table->GetTableObject(txn_, schema_name, table_name);
  }
}

/*@brief    Get table catalog object from cache (cached_only == true),
            or all the way from storage (cached_only == false)
 * @param   schema_name
 * @return  table catalog objects
 */
std::vector<std::shared_ptr<TableCatalogEntry>>
DatabaseCatalogEntry::GetTableObjects(const std::string &schema_name) {
  // read directly from pg_table
  if (!valid_table_objects_) {
    auto pg_table = Catalog::GetInstance()
                        ->GetSystemCatalogs(database_oid_)
                        ->GetTableCatalog();
    // insert every table object into cache
    pg_table->GetTableObjects(txn_);
  }
  // make sure to check IsValidTableObjects() before getting table objects
  PELOTON_ASSERT(valid_table_objects_);
  std::vector<std::shared_ptr<TableCatalogEntry>> result;
  for (auto it : table_objects_cache_) {
    if (it.second->GetSchemaName() == schema_name) {
      result.push_back(it.second);
    }
  }

  return result;
}

/* @brief   Get table catalog object from cache (cached_only == true),
            or all the way from storage (cached_only == false)
 * @param   cached_only   if cached only, return nullptr on a cache miss
 * @return  Shared pointer to the requested table catalog object
 */
std::unordered_map<oid_t, std::shared_ptr<TableCatalogEntry>>
DatabaseCatalogEntry::GetTableObjects(bool cached_only) {
  if (!cached_only && !valid_table_objects_) {
    // cache miss get from pg_table
    auto pg_table = Catalog::GetInstance()
                        ->GetSystemCatalogs(database_oid_)
                        ->GetTableCatalog();
    return pg_table->GetTableObjects(txn_);
  }
  // make sure to check IsValidTableObjects() before getting table objects
  PELOTON_ASSERT(valid_table_objects_);
  return table_objects_cache_;
}

/*@brief   search index catalog object from all cached database objects
 * @param   index_oid
 * @return  index catalog object; if not found return null
 */
std::shared_ptr<IndexCatalogEntry> DatabaseCatalogEntry::GetCachedIndexObject(
    oid_t index_oid) {
  for (auto it = table_objects_cache_.begin(); it != table_objects_cache_.end();
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
std::shared_ptr<IndexCatalogEntry> DatabaseCatalogEntry::GetCachedIndexObject(
    const std::string &index_name, const std::string &schema_name) {
  for (auto it = table_objects_cache_.begin(); it != table_objects_cache_.end();
       ++it) {
    auto table_object = it->second;
    if (table_object != nullptr &&
        table_object->GetSchemaName() == schema_name) {
      auto index_object = table_object->GetIndexObject(index_name, true);
      if (index_object) return index_object;
    }
  }
  return nullptr;
}

DatabaseCatalog *DatabaseCatalog::GetInstance(concurrency::TransactionContext *txn,
                                             storage::Database *pg_catalog,
                                             type::AbstractPool *pool) {
  static DatabaseCatalog
      database_catalog{txn, pg_catalog, pool};
  return &database_catalog;
}

DatabaseCatalog::DatabaseCatalog(concurrency::TransactionContext *,
                                 storage::Database *pg_catalog,
                                 type::AbstractPool *)
    : AbstractCatalog(pg_catalog,
                      InitializeSchema().release(),
                      DATABASE_CATALOG_OID,
                      DATABASE_CATALOG_NAME) {
  // Add indexes for pg_database
  AddIndex(DATABASE_CATALOG_NAME "_pkey",
           DATABASE_CATALOG_PKEY_OID,
           {ColumnId::DATABASE_OID},
           IndexConstraintType::PRIMARY_KEY);
  AddIndex(DATABASE_CATALOG_NAME "_skey0",
           DATABASE_CATALOG_SKEY0_OID,
           {ColumnId::DATABASE_NAME},
           IndexConstraintType::UNIQUE);
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
      type::TypeId::VARCHAR, max_name_size_, "database_name", false);
  database_name_column.AddConstraint(
      catalog::Constraint(ConstraintType::NOTNULL, not_null_constraint_name));

  std::unique_ptr<catalog::Schema> database_catalog_schema(
      new catalog::Schema({database_id_column, database_name_column}));
  return database_catalog_schema;
}

bool DatabaseCatalog::InsertDatabase(concurrency::TransactionContext *txn,
                                     oid_t database_oid,
                                     const std::string &database_name,
                                     type::AbstractPool *pool) {
  std::unique_ptr<storage::Tuple> tuple(
      new storage::Tuple(catalog_table_->GetSchema(), true));

  auto val0 = type::ValueFactory::GetIntegerValue(database_oid);
  auto val1 = type::ValueFactory::GetVarcharValue(database_name, nullptr);

  tuple->SetValue(ColumnId::DATABASE_OID, val0, pool);
  tuple->SetValue(ColumnId::DATABASE_NAME, val1, pool);

  // Insert the tuple
  return InsertTuple(txn, std::move(tuple));
}

bool DatabaseCatalog::DeleteDatabase(concurrency::TransactionContext *txn, oid_t database_oid) {
  oid_t index_offset = IndexId::PRIMARY_KEY;  // Index of database_oid
  std::vector<type::Value> values;
  values.push_back(type::ValueFactory::GetIntegerValue(database_oid).Copy());

  // evict cache
  txn->catalog_cache.EvictDatabaseObject(database_oid);

  return DeleteWithIndexScan(txn, index_offset, values);
}

std::shared_ptr<DatabaseCatalogEntry> DatabaseCatalog::GetDatabaseObject(concurrency::TransactionContext *txn,
                                                                         oid_t database_oid) {
  if (txn == nullptr) {
    throw CatalogException("Transaction is invalid!");
  }
  // try get from cache
  auto database_object = txn->catalog_cache.GetDatabaseObject(database_oid);
  if (database_object) return database_object;

  // cache miss, get from pg_database
  std::vector<oid_t> column_ids(all_column_ids_);
  oid_t index_offset = IndexId::PRIMARY_KEY;  // Index of database_oid
  std::vector<type::Value> values;
  values.push_back(type::ValueFactory::GetIntegerValue(database_oid).Copy());

  auto result_tiles =
      GetResultWithIndexScan(txn,
                             column_ids,
                             index_offset,
                             values);

  if (result_tiles->size() == 1 && (*result_tiles)[0]->GetTupleCount() == 1) {
    auto database_object =
        std::make_shared<DatabaseCatalogEntry>(txn, (*result_tiles)[0].get());
    // insert into cache
    bool success = txn->catalog_cache.InsertDatabaseObject(database_object);
    PELOTON_ASSERT(success == true);
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
std::shared_ptr<DatabaseCatalogEntry> DatabaseCatalog::GetDatabaseObject(concurrency::TransactionContext *txn,
                                                                         const std::string &database_name) {
  if (txn == nullptr) {
    throw CatalogException("Transaction is invalid!");
  }
  // try get from cache
  auto database_object = txn->catalog_cache.GetDatabaseObject(database_name);
  if (database_object) return database_object;

  // cache miss, get from pg_database
  std::vector<oid_t> column_ids(all_column_ids_);
  oid_t index_offset = IndexId::SKEY_DATABASE_NAME;  // Index of database_name
  std::vector<type::Value> values;
  values.push_back(
      type::ValueFactory::GetVarcharValue(database_name, nullptr).Copy());

  auto result_tiles =
      GetResultWithIndexScan(txn,
                             column_ids,
                             index_offset,
                             values);

  if (result_tiles->size() == 1 && (*result_tiles)[0]->GetTupleCount() == 1) {
    auto database_object =
        std::make_shared<DatabaseCatalogEntry>(txn, (*result_tiles)[0].get());
    if (database_object) {
      // insert into cache
      bool success = txn->catalog_cache.InsertDatabaseObject(database_object);
      PELOTON_ASSERT(success == true);
      (void)success;
    }
    return database_object;
  }

  // return empty object if not found
  return nullptr;
}

}  // namespace catalog
}  // namespace peloton
