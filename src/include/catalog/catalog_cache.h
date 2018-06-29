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

namespace planner {
class PlanUtil;
}  // namespace planner

namespace catalog {

class DatabaseCatalogEntry;
class TableCatalogEntry;
class IndexCatalogEntry;

class CatalogCache {
  friend class Transaction;
  friend class DatabaseCatalog;
  friend class TableCatalog;
  friend class IndexCatalog;
  friend class DatabaseCatalogEntry;
  friend class TableCatalogEntry;
  friend class IndexCatalogEntry;
  friend class planner::PlanUtil;

 public:
  CatalogCache() {}
  DISALLOW_COPY(CatalogCache)

 private:
  std::shared_ptr<DatabaseCatalogEntry> GetDatabaseObject(oid_t database_oid);
  std::shared_ptr<DatabaseCatalogEntry> GetDatabaseObject(
      const std::string &name);

  std::shared_ptr<TableCatalogEntry> GetCachedTableObject(oid_t database_oid,
                                                           oid_t table_oid);
  std::shared_ptr<IndexCatalogEntry> GetCachedIndexObject(oid_t database_oid,
                                                           oid_t index_oid);
  std::shared_ptr<IndexCatalogEntry> GetCachedIndexObject(const std::string &database_name,
                                                           const std::string &schema_name,
                                                           const std::string &index_name);

  // database catalog cache interface
  bool InsertDatabaseObject(
      std::shared_ptr<DatabaseCatalogEntry> database_object);
  bool EvictDatabaseObject(oid_t database_oid);
  bool EvictDatabaseObject(const std::string &database_name);

  // cache for database catalog object
  std::unordered_map<oid_t, std::shared_ptr<DatabaseCatalogEntry>>
      database_objects_cache_;
  std::unordered_map<std::string, std::shared_ptr<DatabaseCatalogEntry>>
      database_name_cache_;
};

}  // namespace catalog
}  // namespace peloton
