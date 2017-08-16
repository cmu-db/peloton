//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// catalog_cache.h
//
// Identification: src/include/catalog/catalog_cache.h
//
// Copyright (c) 2015-17, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <unordered_map>

#include "catalog/table_catalog.h"
#include "catalog/database_catalog.h"

namespace peloton {
namespace catalog {

class CatalogCache {
 public:
  CatalogCache() {}

  // database catalog cache interface
  bool InsertDatabaseObject(
      std::shared_ptr<DatabaseCatalogObject> &database_object,
      bool forced = false);
  bool EvictDatabaseObject(oid_t database_oid);

  std::shared_ptr<DatabaseCatalogObject> GetDatabaseObject(oid_t database_oid);
  std::shared_ptr<DatabaseCatalogObject> GetDatabaseObject(
      const std::string &name);

  // table catalog cache interface
  bool InsertTableObject(std::shared_ptr<TableCatalogObject> table_object,
                         bool forced = false);
  bool EvictTableObject(oid_t table_oid);

  std::shared_ptr<TableCatalogObject> GetTableObject(oid_t table_oid);

 private:
  // cache for database catalog object
  std::unordered_map<oid_t, std::shared_ptr<DatabaseCatalogObject>>
      database_objects_cache;
  std::unordered_map<std::string, oid_t> database_name_cache;
  std::mutex database_cache_lock;

  // cache for table catalog object
  std::unordered_map<oid_t, std::shared_ptr<TableCatalogObject>>
      table_objects_cache;
  std::mutex table_cache_lock;
};

}  // namespace catalog
}  // namespace peloton
