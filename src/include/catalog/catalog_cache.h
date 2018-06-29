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

class DatabaseCatalogEntry;
class TableCatalogEntry;
class IndexCatalogEntry;

class CatalogCache {
  friend class concurrency::TransactionContext;
  friend class Catalog;
  friend class DatabaseCatalog;
  friend class TableCatalog;
  friend class IndexCatalog;
  friend class DatabaseCatalogEntry;
  friend class TableCatalogEntry;
  friend class IndexCatalogEntry;
  friend class planner::PlanUtil;

 public:
  CatalogCache() : valid_database_catalog_entry_(false) {}
  DISALLOW_COPY(CatalogCache)

 private:
  std::shared_ptr<DatabaseCatalogEntry> GetDatabaseCatalogEntry(oid_t database_oid);
  std::shared_ptr<DatabaseCatalogEntry> GetDatabaseCatalogEntry(
      const std::string &name);
  std::unordered_map<oid_t, std::shared_ptr<DatabaseCatalogEntry>>
  GetDatabaseCatalogEntries(concurrency::TransactionContext *txn = nullptr);

  std::shared_ptr<TableCatalogEntry> GetCachedTableCatalogEntry(oid_t database_oid,
                                                           oid_t table_oid);
  std::shared_ptr<IndexCatalogEntry> GetCachedIndexCatalogEntry(oid_t database_oid,
                                                           oid_t index_oid);
  std::shared_ptr<IndexCatalogEntry> GetCachedIndexCatalogEntry(const std::string &database_name,
                                                           const std::string &schema_name,
                                                           const std::string &index_name);

  // database catalog cache interface
  bool InsertDatabaseCatalogEntry(
      std::shared_ptr<DatabaseCatalogEntry> database_object);
  bool EvictDatabaseCatalogEntry(oid_t database_oid);
  bool EvictDatabaseCatalogEntry(const std::string &database_name);

  void SetValidDatabaseCatalogEntries(bool valid = true) {
    valid_database_catalog_entry_ = valid;
  }
  bool IsValidDatabaseCatalogEntries() {
    // return true if this catalog cache contains all database
    // objects within the database
    return valid_database_catalog_entry_;
  }

  // cache for database catalog object
  std::unordered_map<oid_t, std::shared_ptr<DatabaseCatalogEntry>>
      database_catalog_entries_cache_;
  std::unordered_map<std::string, std::shared_ptr<DatabaseCatalogEntry>>
      database_name_cache_;
  bool valid_database_catalog_entry_;
};

}  // namespace catalog
}  // namespace peloton
