//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// index_catalog.h
//
// Identification: src/include/catalog/index_catalog.h
//
// Copyright (c) 2015-17, Carnegie Mellon University Index Group
//
//===----------------------------------------------------------------------===//

//===----------------------------------------------------------------------===//
// pg_index
//
// Schema: (column: column_name)
// 0: index_oid (pkey)
// 1: index_name
// 2: table_oid (which table this index belongs to)
// 3: schema_name (which namespace this index belongs to)
// 4: index_type (default value is BWTREE)
// 5: index_constraint
// 6: unique_keys (is this index supports duplicate keys)
// 7: indexed_attributes (indicate which table columns this index indexes. For
// example a value of 0 2 would mean that the first and the third table columns
// make up the index.)
//
// Indexes: (index offset: indexed columns)
// 0: index_oid (unique & primary key)
// 1: index_name & schema_name (unique)
// 2: table_oid (non-unique)
//
//===----------------------------------------------------------------------===//

#pragma once

#include "catalog/abstract_catalog.h"
#include "executor/logical_tile.h"

namespace peloton {
namespace catalog {

class IndexCatalogEntry {
  friend class TableCatalogEntry;

 public:
  IndexCatalogEntry(executor::LogicalTile *tile, int tupleId = 0);

  inline oid_t GetIndexOid() { return index_oid_; }
  inline const std::string &GetIndexName() { return index_name_; }
  inline oid_t GetTableOid() { return table_oid_; }
  inline const std::string &GetSchemaName() { return schema_name_; }
  inline IndexType GetIndexType() { return index_type_; }
  inline IndexConstraintType GetIndexConstraint() { return index_constraint_; }
  inline bool HasUniqueKeys() { return unique_keys_; }
  inline const std::vector<oid_t> &GetKeyAttrs() { return key_attrs_; }

 private:
  // member variables
  oid_t index_oid_;
  std::string index_name_;
  oid_t table_oid_;
  std::string schema_name_;
  IndexType index_type_;
  IndexConstraintType index_constraint_;
  bool unique_keys_;
  std::vector<oid_t> key_attrs_;
};

class IndexCatalog : public AbstractCatalog {
  friend class IndexCatalogEntry;
  friend class TableCatalogEntry;
  friend class Catalog;

 public:
  IndexCatalog(concurrency::TransactionContext *txn,
               storage::Database *pg_catalog,
               type::AbstractPool *pool);

  ~IndexCatalog();

  inline oid_t GetNextOid() { return oid_++ | INDEX_OID_MASK; }

  void UpdateOid(oid_t add_value) { oid_ += add_value; }

  /** Write Related API */
  bool InsertIndex(concurrency::TransactionContext *txn,
                   const std::string &schema_name,
                   oid_t table_oid,
                   oid_t index_oid,
                   const std::string &index_name,
                   IndexType index_type,
                   IndexConstraintType index_constraint,
                   bool unique_keys,
                   std::vector<oid_t> index_keys,
                   type::AbstractPool *pool);

  bool DeleteIndex(concurrency::TransactionContext *txn,
                   oid_t database_oid,
                   oid_t index_oid);

  /** Read Related API */
  std::shared_ptr<IndexCatalogEntry> GetIndexCatalogEntry(concurrency::TransactionContext *txn,
                                                          const std::string &database_name,
                                                          const std::string &schema_name,
                                                          const std::string &index_name);

 private:
  std::shared_ptr<IndexCatalogEntry> GetIndexCatalogEntry(concurrency::TransactionContext *txn,
                                                          oid_t database_oid,
                                                          oid_t index_oid);

  const std::unordered_map<oid_t, std::shared_ptr<IndexCatalogEntry>>
  GetIndexCatalogEntries(
      concurrency::TransactionContext *txn,
      oid_t table_oid);

  std::unique_ptr<catalog::Schema> InitializeSchema();

  enum ColumnId {
    INDEX_OID = 0,
    INDEX_NAME = 1,
    TABLE_OID = 2,
    SCHEMA_NAME = 3,
    INDEX_TYPE = 4,
    INDEX_CONSTRAINT = 5,
    UNIQUE_KEYS = 6,
    INDEXED_ATTRIBUTES = 7,
    // Add new columns here in creation order
  };
  std::vector<oid_t> all_column_ids = {0, 1, 2, 3, 4, 5, 6, 7};

  enum IndexId {
    PRIMARY_KEY = 0,
    SKEY_INDEX_NAME = 1,
    SKEY_TABLE_OID = 2,
    // Add new indexes here in creation order
  };
};

}  // namespace catalog
}  // namespace peloton
