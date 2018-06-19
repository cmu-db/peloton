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

namespace concurrency {
class TransactionContext;
}

namespace planner {
class PlanUtil;
}  // namespace planner

namespace catalog {

class DatabaseCatalogObject;
class TableCatalogObject;
class IndexCatalogObject;

class CatalogCache {
  friend class concurrency::TransactionContext;
  friend class Catalog;
  friend class DatabaseCatalog;
  friend class TableCatalog;
  friend class IndexCatalog;
  friend class DatabaseCatalogObject;
  friend class TableCatalogObject;
  friend class IndexCatalogObject;
  friend class planner::PlanUtil;

 public:
  CatalogCache() : valid_database_objects(false) {}
  CatalogCache(CatalogCache const &) = delete;
  CatalogCache &operator=(CatalogCache const &) = delete;

 private:
  std::shared_ptr<DatabaseCatalogObject> GetDatabaseObject(oid_t database_oid);
  std::shared_ptr<DatabaseCatalogObject> GetDatabaseObject(
      const std::string &name);
  std::unordered_map<oid_t, std::shared_ptr<DatabaseCatalogObject>>
  GetDatabaseObjects(concurrency::TransactionContext *txn = nullptr);

  std::shared_ptr<TableCatalogObject> GetCachedTableObject(oid_t database_oid,
      oid_t table_oid);
  std::shared_ptr<IndexCatalogObject> GetCachedIndexObject(oid_t database_oid,
      oid_t index_oid);
  std::shared_ptr<IndexCatalogObject> GetCachedIndexObject(
      const std::string &database_name, const std::string &index_name,
      const std::string &schema_name);

  // database catalog cache interface
  bool InsertDatabaseObject(
      std::shared_ptr<DatabaseCatalogObject> database_object);
  bool EvictDatabaseObject(oid_t database_oid);
  bool EvictDatabaseObject(const std::string &database_name);

  void SetValidDatabaseObjects(bool valid = true) {
    valid_database_objects = valid;
  }
  bool IsValidDatabaseObjects() {
    // return true if this catalog cache contains all database
    // objects within the database
    return valid_database_objects;
  }

  // cache for database catalog object
  std::unordered_map<oid_t, std::shared_ptr<DatabaseCatalogObject>>
      database_objects_cache;
  std::unordered_map<std::string, std::shared_ptr<DatabaseCatalogObject>>
      database_name_cache;
  bool valid_database_objects;
};

}  // namespace catalog
}  // namespace peloton
