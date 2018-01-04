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

#include <mutex>
#include <unordered_map>

#include "common/internal_types.h"

namespace peloton {
namespace catalog {

class DatabaseCatalogObject;
class TableCatalogObject;
class IndexCatalogObject;

class CatalogCache {
  friend class Transaction;
  friend class DatabaseCatalog;
  friend class TableCatalog;
  friend class IndexCatalog;
  friend class DatabaseCatalogObject;
  friend class TableCatalogObject;
  friend class IndexCatalogObject;

 public:
  CatalogCache() {}
  CatalogCache(CatalogCache const &) = delete;
  CatalogCache &operator=(CatalogCache const &) = delete;

 private:
  std::shared_ptr<DatabaseCatalogObject> GetDatabaseObject(oid_t database_oid);
  std::shared_ptr<DatabaseCatalogObject> GetDatabaseObject(
      const std::string &name);

  std::shared_ptr<TableCatalogObject> GetCachedTableObject(oid_t table_oid);
  std::shared_ptr<IndexCatalogObject> GetCachedIndexObject(oid_t index_oid);
  std::shared_ptr<IndexCatalogObject> GetCachedIndexObject(
      const std::string &index_name);

  // database catalog cache interface
  bool InsertDatabaseObject(
      std::shared_ptr<DatabaseCatalogObject> database_object);
  bool EvictDatabaseObject(oid_t database_oid);
  bool EvictDatabaseObject(const std::string &database_name);

  // cache for database catalog object
  std::unordered_map<oid_t, std::shared_ptr<DatabaseCatalogObject>>
      database_objects_cache;
  std::unordered_map<std::string, std::shared_ptr<DatabaseCatalogObject>>
      database_name_cache;
};

}  // namespace catalog
}  // namespace peloton
