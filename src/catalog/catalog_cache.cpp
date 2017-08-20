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
#include "common/logger.h"

namespace peloton {
namespace catalog {

/*@brief   insert database catalog object into cache
* @param   database_object
* @return  false only if database_oid already exists in cache
*/
bool CatalogCache::InsertDatabaseObject(
    std::shared_ptr<DatabaseCatalogObject> database_object) {
  if (!database_object || database_object->database_oid == INVALID_OID) {
    return false;  // invalid object
  }
  // std::lock_guard<std::mutex> lock(database_cache_lock);

  // check if already in cache
  if (database_objects_cache.find(database_object->database_oid) !=
      database_objects_cache.end()) {
    LOG_DEBUG("Database %u already exists in cache!",
              database_object->database_oid);
    return false;
  }
  if (database_name_cache.find(database_object->database_name) !=
      database_name_cache.end()) {
    LOG_DEBUG("Database %s already exists in cache!",
              database_object->database_name.c_str());
    return false;
  }

  database_objects_cache.insert(
      std::make_pair(database_object->database_oid, database_object));
  database_name_cache.insert(
      std::make_pair(database_object->database_name, database_object));
  return true;
}

/*@brief   evict database catalog object from cache
* @param   database_oid
* @return  true if database_oid is found and evicted; false if not found
*/
bool CatalogCache::EvictDatabaseObject(oid_t database_oid) {
  // std::lock_guard<std::mutex> lock(database_cache_lock);
  auto it = database_objects_cache.find(database_oid);
  if (it == database_objects_cache.end()) {
    return false;  // database oid not found in cache
  }

  auto database_object = it->second;
  PL_ASSERT(database_object);
  database_objects_cache.erase(it);
  database_name_cache.erase(database_object->database_name);
  return true;
}

/*@brief   evict database catalog object from cache
* @param   database_name
* @return  true if database_name is found and evicted; false if not found
*/
bool CatalogCache::EvictDatabaseObject(const std::string &database_name) {
  // std::lock_guard<std::mutex> lock(database_cache_lock);
  auto it = database_name_cache.find(database_name);
  if (it == database_name_cache.end()) {
    return false;  // database name not found in cache
  }

  auto database_object = it->second;
  PL_ASSERT(database_object);
  database_name_cache.erase(it);
  database_objects_cache.erase(database_object->database_oid);
  return true;
}

/*@brief   get database catalog object from cache
* @param   database_oid
* @return  database catalog object; if not found return object with invalid oid
*/
std::shared_ptr<DatabaseCatalogObject> CatalogCache::GetDatabaseObject(
    oid_t database_oid) {
  // std::lock_guard<std::mutex> lock(database_cache_lock);
  auto it = database_objects_cache.find(database_oid);
  if (it == database_objects_cache.end()) {
    return nullptr;
  }
  return it->second;
}

/*@brief   get database catalog object from cache
* @param   database_name
* @return  database catalog object; if not found return null
*/
std::shared_ptr<DatabaseCatalogObject> CatalogCache::GetDatabaseObject(
    const std::string &database_name) {
  // std::lock_guard<std::mutex> lock(database_cache_lock);
  auto it = database_name_cache.find(database_name);
  if (it == database_name_cache.end()) {
    return nullptr;
  }
  return it->second;
}

/*@brief   search table catalog object from all cached database objects
* @param   table_oid
* @return  table catalog object; if not found return null
*/
std::shared_ptr<TableCatalogObject> CatalogCache::GetCachedTableObject(
    oid_t table_oid) {
  // std::lock_guard<std::mutex> lock(database_cache_lock);
  for (auto it = database_objects_cache.begin();
       it != database_objects_cache.end(); ++it) {
    auto database_object = it->second;
    auto table_object = database_object->GetTableObject(table_oid, true);
    if (table_object) return table_object;
  }
  return nullptr;
}

/*@brief   search index catalog object from all cached database objects
* @param   index_oid
* @return  index catalog object; if not found return null
*/
std::shared_ptr<IndexCatalogObject> CatalogCache::GetCachedIndexObject(
    oid_t index_oid) {
  // std::lock_guard<std::mutex> lock(database_cache_lock);
  for (auto it = database_objects_cache.begin();
       it != database_objects_cache.end(); ++it) {
    auto database_object = it->second;
    auto index_object = database_object->GetCachedIndexObject(index_oid);
    if (index_object) return index_object;
  }
  return nullptr;
}

/*@brief   search index catalog object from all cached database objects
* @param   index_name
* @return  index catalog object; if not found return null
*/
std::shared_ptr<IndexCatalogObject> CatalogCache::GetCachedIndexObject(
    const std::string &index_name) {
  // std::lock_guard<std::mutex> lock(database_cache_lock);
  for (auto it = database_objects_cache.begin();
       it != database_objects_cache.end(); ++it) {
    auto database_object = it->second;
    auto index_object = database_object->GetCachedIndexObject(index_name);
    if (index_object) return index_object;
  }
  return nullptr;
}

}  // namespace catalog
}  // namespace peloton
