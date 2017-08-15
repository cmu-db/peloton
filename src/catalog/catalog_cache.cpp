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

namespace peloton {
namespace catalog {

/*@brief   insert database catalog object into cache
* @param   database_object
* @param   forced   if forced, replace existing object, else return false if oid
*          already exists
* @return  false only if not forced and database_oid already exists in cache
*/
bool CatalogCache::InsertDatabaseObject(
    std::shared_ptr<DatabaseCatalogObject> &database_object, bool forced) {
  if (!database_object || database_object->database_oid == INVALID_OID) {
    return false;  // invalid object
  }
  // std::lock_guard<std::mutex> lock(database_cache_lock);

  auto object_iter = database_objects_cache.find(database_object->database_oid);

  if (forced == true && object_iter != database_objects_cache.end()) {
    database_objects_cache.erase(object_iter);  // Evict old
    database_name_cache.erase(database_object->database_name);
    object_iter = database_objects_cache.find(database_object->database_oid);
  }
  if (object_iter == database_objects_cache.end()) {
    auto cached_object =
        std::shared_ptr<DatabaseCatalogObject>(database_object);
    database_objects_cache.insert(
        std::make_pair(database_object->database_oid, cached_object));
    database_name_cache.insert(std::make_pair(database_object->database_name,
                                              cached_object->database_oid));
    return true;
  } else {
    return false;
  }
}

/*@brief   evict database catalog object from cache
* @param   database_oid
* @return  true if database_oid is found and evicted; false if not found
*/
bool CatalogCache::EvictDatabaseObject(oid_t database_oid) {
  // std::lock_guard<std::mutex> lock(database_cache_lock);
  auto it = database_objects_cache.find(database_oid);
  if (it == database_objects_cache.end()) {
    return false;
  } else {
    database_name_cache.erase(it->second->database_name);
    database_objects_cache.erase(it);
    return true;
  }
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
* @return  database catalog object; if not found return object with invalid oid
*/
std::shared_ptr<DatabaseCatalogObject> CatalogCache::GetDatabaseObject(
    const std::string &database_name) {
  // std::lock_guard<std::mutex> lock(database_cache_lock);
  auto it = database_name_cache.find(database_name);
  if (it == database_name_cache.end()) {
    return nullptr;
  }
  oid_t database_oid = it->second;
  return database_objects_cache.find(database_oid)->second;
}

/*@brief   insert table catalog object into cache
* @param   table_object
* @param   forced   if forced, replace existing object, else return false if oid
*          already exists
* @return  false only if not forced and table_oid already exists in cache
*/
bool CatalogCache::InsertTableObject(
    std::shared_ptr<TableCatalogObject> table_object, bool forced) {
  if (!table_object || table_object->table_oid == INVALID_OID) {
    return false;  // invalid object
  }

  // find old table catalog object
  // std::lock_guard<std::mutex> lock(table_cache_lock);
  auto it = table_objects_cache.find(table_object->table_oid);

  // forced evict if found
  if (forced == true && it != table_objects_cache.end()) {
    if (it->second == table_object) return false;  // no need to replace
    EvictTableObject(table_object->table_oid);     // evict old
    it = table_objects_cache.find(table_object->table_oid);
  }

  // insert into table object cache
  if (it == table_objects_cache.end()) {
    table_objects_cache.insert(
        std::make_pair(table_object->table_oid, table_object));
    // find and insert into database catalog object too
    auto database_object = GetDatabaseObject(table_object->database_oid);
    if (database_object) {
      database_object->InsertTableObject(table_object, forced);
    }
    return true;
  } else {
    return false;  // table oid already exists
  }
}

/*@brief   evict table catalog object from cache
* @param   table_oid
* @return  true if table_oid is found and evicted; false if not found
*/
bool CatalogCache::EvictTableObject(oid_t table_oid) {
  if (table_oid == INVALID_OID) return false;
  // std::lock_guard<std::mutex> lock(table_cache_lock);

  // find table oid from catalog cache
  auto it = table_objects_cache.find(table_oid);
  if (it != table_objects_cache.end()) {
    auto table_object = it->second;
    table_objects_cache.erase(it);
    if (table_object) {
      // find and evict table name from database object's cache
      auto database_object = GetDatabaseObject(table_object->database_oid);
      if (database_object) {
        database_object->EvictTableObject(
            table_object->table_name);  // evict old name
      }
    }
    return true;
  } else {
    return false;  // table oid not found in cache
  }
}

/*@brief   get table catalog object from cache
* @param   table_oid
* @return  table catalog object; if not found return object with invalid oid
*/
std::shared_ptr<TableCatalogObject> CatalogCache::GetTableObject(
    oid_t table_oid) {
  // std::lock_guard<std::mutex> lock(table_cache_lock);
  auto it = table_objects_cache.find(table_oid);
  if (it == table_objects_cache.end()) {
    // if (txn == nullptr) return TableCatalogObject();
    // bool success = InsertTableObject(
    //     TableCatalog::GetInstance()->GetTableObject(table_oid, txn));
    // PL_ASSERT(success == true);
    // it = table_objects_cache.find(table_oid)
    return nullptr;
  }
  return it->second;
}

}  // namespace catalog
}  // namespace peloton
