//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// database_catalog.h
//
// Identification: src/include/catalog/database_catalog.h
//
// Copyright (c) 2015-17, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

//===----------------------------------------------------------------------===//
// pg_database
//
// Schema: (column offset: column_name)
// 0: database_oid (pkey)
// 1: database_name
//
// Indexes: (index offset: indexed columns)
// 0: database_oid (unique & primary key)
// 1: database_name (unique)
//
//===----------------------------------------------------------------------===//

#pragma once

#include <mutex>

#include "catalog/abstract_catalog.h"
#include "executor/logical_tile.h"

namespace peloton {
namespace catalog {

class TableCatalogEntry;
class IndexCatalogEntry;

class DatabaseCatalogEntry {
  friend class DatabaseCatalog;
  friend class TableCatalog;
  friend class CatalogCache;

 public:
  DatabaseCatalogEntry(concurrency::TransactionContext *txn,
                       executor::LogicalTile *tile);

  void EvictAllTableCatalogEntries();

  std::shared_ptr<TableCatalogEntry> GetTableCatalogEntry(oid_t table_oid,
                                                          bool cached_only = false);
  std::shared_ptr<TableCatalogEntry> GetTableCatalogEntry(
      const std::string &table_name, const std::string &schema_name,
      bool cached_only = false);

  bool IsValidTableCatalogEntries() {
    // return true if this database catalog entries contains all table
    // catalog entries within the database
    return valid_table_catalog_entries;
  }

  std::vector<std::shared_ptr<TableCatalogEntry>> GetTableCatalogEntries(
      const std::string &schema_name);

  std::unordered_map<oid_t, std::shared_ptr<TableCatalogEntry>>
  GetTableCatalogEntries(bool cached_only = false);

  inline oid_t GetDatabaseOid() { return database_oid_; }

  inline const std::string &GetDatabaseName() { return database_name_; }

 private:
  // member variables
  oid_t database_oid_;
  std::string database_name_;

  bool InsertTableCatalogEntry(std::shared_ptr<TableCatalogEntry> table_catalog_entry);

  bool EvictTableCatalogEntry(oid_t table_oid);

  bool EvictTableCatalogEntry(const std::string &table_name,
                              const std::string &schema_name);

  void SetValidTableCatalogEntries(bool valid = true) { valid_table_catalog_entries = valid; }

  std::shared_ptr<IndexCatalogEntry> GetCachedIndexCatalogEntry(oid_t index_oid);

  std::shared_ptr<IndexCatalogEntry> GetCachedIndexCatalogEntry(
      const std::string &index_name, const std::string &schema_name);

  // cache for table name to oid translation
  std::unordered_map<oid_t, std::shared_ptr<TableCatalogEntry>>
      table_catalog_entries_cache_;
  std::unordered_map<std::string, std::shared_ptr<TableCatalogEntry>>
      table_catalog_entries_cache_by_name;
  bool valid_table_catalog_entries;

  // Pointer to its corresponding transaction
  // This object is only visible during this transaction
  concurrency::TransactionContext *txn_;
};

class DatabaseCatalog : public AbstractCatalog {
  friend class DatabaseCatalogEntry;
  friend class TableCatalog;
  friend class CatalogCache;
  friend class Catalog;

 public:
  ~DatabaseCatalog();

  // Global Singleton, only the first call requires passing parameters.
  static DatabaseCatalog *GetInstance(concurrency::TransactionContext *txn = nullptr,
                                      storage::Database *pg_catalog = nullptr,
                                      type::AbstractPool *pool = nullptr);

  inline oid_t GetNextOid() { return oid_++ | DATABASE_OID_MASK; }

  void UpdateOid(oid_t add_value) { oid_ += add_value; }

  //===--------------------------------------------------------------------===//
  // write Related API
  //===--------------------------------------------------------------------===//
  bool InsertDatabase(concurrency::TransactionContext *txn,
                      oid_t database_oid,
                      const std::string &database_name,
                      type::AbstractPool *pool);

  bool DeleteDatabase(concurrency::TransactionContext *txn, oid_t database_oid);

 private:
  //===--------------------------------------------------------------------===//
  // Read Related API
  //===--------------------------------------------------------------------===//
  std::shared_ptr<DatabaseCatalogEntry> GetDatabaseCatalogEntry(concurrency::TransactionContext *txn,
                                                                oid_t database_oid);

  std::shared_ptr<DatabaseCatalogEntry> GetDatabaseCatalogEntry(concurrency::TransactionContext *txn,
                                                                const std::string &database_name);

  DatabaseCatalog(concurrency::TransactionContext *txn,
                  storage::Database *pg_catalog,
                  type::AbstractPool *pool);

  std::unique_ptr<catalog::Schema> InitializeSchema();

  enum ColumnId {
    DATABASE_OID = 0,
    DATABASE_NAME = 1,
    // Add new columns here in creation order
  };
  std::vector<oid_t> all_column_ids_ = {0, 1};

  enum IndexId {
    PRIMARY_KEY = 0,
    SKEY_DATABASE_NAME = 1,
    // Add new indexes here in creation order
  };
};

}  // namespace catalog
}  // namespace peloton
