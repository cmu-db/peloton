//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// catalog_cache.cpp
//
// Identification: src/catalog/catalog_cache.cpp
//
// Copyright (c) 2015-17, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <memory>

#include "catalog/catalog_cache.h"

#include "catalog/database_catalog.h"
#include "common/logger.h"
#include "concurrency/transaction_context.h"

namespace peloton {
namespace catalog {

/*@brief   insert database catalog object into cache
 * @param   database_object
 * @return  false only if database_oid already exists in cache
 */
bool CatalogCache::InsertDatabaseCatalogEntry(
    std::shared_ptr<DatabaseCatalogEntry> database_object) {
  if (!database_object || database_object->GetDatabaseOid() == INVALID_OID) {
    return false;  // invalid object
  }

  // check if already in cache
  if (database_catalog_entries_cache_.find(database_object->GetDatabaseOid()) !=
      database_catalog_entries_cache_.end()) {
    LOG_TRACE("Database %u already exists in cache!",
              database_object->GetDatabaseOid());
    return false;
  }
  if (database_name_cache_.find(database_object->GetDatabaseName()) !=
      database_name_cache_.end()) {
    LOG_TRACE("Database %s already exists in cache!",
              database_object->GetDatabaseName().c_str());
    return false;
  }

  database_catalog_entries_cache_.insert(
      std::make_pair(database_object->GetDatabaseOid(), database_object));
  database_name_cache_.insert(
      std::make_pair(database_object->GetDatabaseName(), database_object));
  return true;
}

/*@brief   evict database catalog object from cache
 * @param   database_oid
 * @return  true if database_oid is found and evicted; false if not found
 */
bool CatalogCache::EvictDatabaseCatalogEntry(oid_t database_oid) {
  auto it = database_catalog_entries_cache_.find(database_oid);
  if (it == database_catalog_entries_cache_.end()) {
    return false;  // database oid not found in cache
  }

  auto database_object = it->second;
  PELOTON_ASSERT(database_object);
  database_catalog_entries_cache_.erase(it);
  database_name_cache_.erase(database_object->GetDatabaseName());
  return true;
}

/*@brief   evict database catalog object from cache
 * @param   database_name
 * @return  true if database_name is found and evicted; false if not found
 */
bool CatalogCache::EvictDatabaseCatalogEntry(const std::string &database_name) {
  auto it = database_name_cache_.find(database_name);
  if (it == database_name_cache_.end()) {
    return false;  // database name not found in cache
  }

  auto database_object = it->second;
  PELOTON_ASSERT(database_object);
  database_name_cache_.erase(it);
  database_catalog_entries_cache_.erase(database_object->GetDatabaseOid());
  return true;
}

/*@brief   get database catalog object from cache
 * @param   database_oid
 * @return  database catalog object; if not found return object with invalid oid
 */
std::shared_ptr<DatabaseCatalogEntry>
CatalogCache::GetDatabaseCatalogEntry(oid_t database_oid) {
  auto it = database_catalog_entries_cache_.find(database_oid);
  if (it == database_catalog_entries_cache_.end()) {
    return nullptr;
  }
  return it->second;
}

/*@brief   get database catalog object from cache
 * @param   database_name
 * @return  database catalog object; if not found return null
 */
std::shared_ptr<DatabaseCatalogEntry>
CatalogCache::GetDatabaseCatalogEntry(const std::string &database_name) {
  auto it = database_name_cache_.find(database_name);
  if (it == database_name_cache_.end()) {
    return nullptr;
  }
  return it->second;
}

/* @brief   Get database catalog object from cache,
            or all the way from storage
 * @param   txn   if nullptr, return nullptr on a cache miss
 * @return  Shared pointer to the requested database catalog object
 */
std::unordered_map<oid_t, std::shared_ptr<DatabaseCatalogEntry>>
CatalogCache::GetDatabaseCatalogEntries(concurrency::TransactionContext *txn) {
  if (!valid_database_catalog_entry_ && txn != nullptr) {
    // cache miss get from pg_database
    return DatabaseCatalog::GetInstance()->GetDatabaseCatalogEntries(txn);
  }
  // make sure to check IsValidTableObjects() before getting table objects
  PELOTON_ASSERT(valid_database_catalog_entry_);
  return database_catalog_entries_cache_;
}

/*@brief   search table catalog object from all cached database objects
 * @param   table_oid
 * @return  table catalog object; if not found return null
 */
std::shared_ptr<TableCatalogEntry>
CatalogCache::GetCachedTableCatalogEntry(oid_t database_oid,
                                         oid_t table_oid) {
  auto database_object = GetDatabaseCatalogEntry(database_oid);
  if (database_object == nullptr) return nullptr;
  auto table_object = database_object->GetTableCatalogEntry(table_oid, true);
  if (table_object) return table_object;
  return nullptr;
}

/*@brief   search index catalog object from all cached database objects
 * @param   index_oid
 * @return  index catalog object; if not found return null
 */
std::shared_ptr<IndexCatalogEntry>
CatalogCache::GetCachedIndexCatalogEntry(oid_t database_oid,
                                         oid_t index_oid) {
  auto database_object = GetDatabaseCatalogEntry(database_oid);
  if (database_object == nullptr) return nullptr;
  auto index_object = database_object->GetCachedIndexCatalogEntry(index_oid);
  if (index_object) return index_object;
  return nullptr;
}

/*@brief   search index catalog object from all cached database objects
 * @param   index_name
 * @return  index catalog object; if not found return null
 */
std::shared_ptr<IndexCatalogEntry>
CatalogCache::GetCachedIndexCatalogEntry(const std::string &database_name,
                                         const std::string &index_name,
                                         const std::string &schema_name) {
  auto database_object = GetDatabaseCatalogEntry(database_name);
  if (database_object == nullptr) return nullptr;
  auto index_object =
      database_object->GetCachedIndexCatalogEntry(index_name, schema_name);
  if (index_object) return index_object;

  return nullptr;
}

}  // namespace catalog
}  // namespace peloton
