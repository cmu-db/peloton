//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// index_catalog.h
//
// Identification: src/include/catalog/index_catalog.h
//
// Copyright (c) 2015-2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <set>
#include "catalog/abstract_catalog.h"
#include "executor/logical_tile.h"

namespace peloton {
namespace catalog {

class IndexCatalogObject {
  friend class TableCatalogObject;

 public:
  IndexCatalogObject(executor::LogicalTile *tile, int tupleId = 0);

  // This constructor should only be used for what-if index API.
  IndexCatalogObject(oid_t index_oid, std::string index_name, oid_t table_oid,
                     IndexType index_type, IndexConstraintType index_constraint,
                     bool unique_keys, std::set<oid_t> key_attrs);

  inline oid_t GetIndexOid() { return index_oid; }
  inline const std::string &GetIndexName() { return index_name; }
  inline oid_t GetTableOid() { return table_oid; }
  inline IndexType GetIndexType() { return index_type; }
  inline IndexConstraintType GetIndexConstraint() { return index_constraint; }
  inline bool HasUniqueKeys() { return unique_keys; }
  inline const std::vector<oid_t> &GetKeyAttrs() { return key_attrs; }

 private:
  // member variables
  oid_t index_oid;
  std::string index_name;
  oid_t table_oid;
  IndexType index_type;
  IndexConstraintType index_constraint;
  bool unique_keys;
  std::vector<oid_t> key_attrs;
};

class IndexCatalog : public AbstractCatalog {
  friend class IndexCatalogObject;
  friend class TableCatalogObject;
  friend class Catalog;

 public:
  ~IndexCatalog();

  // Global Singleton, only the first call requires passing parameters.
  static IndexCatalog *GetInstance(
      storage::Database *pg_catalog = nullptr,
      type::AbstractPool *pool = nullptr,
      concurrency::TransactionContext *txn = nullptr);

  inline oid_t GetNextOid() { return oid_++ | INDEX_OID_MASK; }

  /** Write Related API */
  bool InsertIndex(oid_t index_oid, const std::string &index_name,
                   oid_t table_oid, IndexType index_type,
                   IndexConstraintType index_constraint, bool unique_keys,
                   std::vector<oid_t> indekeys, type::AbstractPool *pool,
                   concurrency::TransactionContext *txn);
  bool DeleteIndex(oid_t index_oid, concurrency::TransactionContext *txn);

  /** Read Related API */
  std::shared_ptr<IndexCatalogObject> GetIndexObject(
      const std::string &index_name, concurrency::TransactionContext *txn);

 private:
  std::shared_ptr<IndexCatalogObject> GetIndexObject(
      oid_t index_oid, concurrency::TransactionContext *txn);

  const std::unordered_map<oid_t, std::shared_ptr<IndexCatalogObject>>
  GetIndexObjects(oid_t table_oid, concurrency::TransactionContext *txn);

 private:
  IndexCatalog(storage::Database *pg_catalog, type::AbstractPool *pool,
               concurrency::TransactionContext *txn);

  std::unique_ptr<catalog::Schema> InitializeSchema();

  enum ColumnId {
    INDEX_OID = 0,
    INDEX_NAME = 1,
    TABLE_OID = 2,
    INDEX_TYPE = 3,
    INDEX_CONSTRAINT = 4,
    UNIQUE_KEYS = 5,
    INDEXED_ATTRIBUTES = 6,
    // Add new columns here in creation order
  };
  std::vector<oid_t> all_column_ids = {0, 1, 2, 3, 4, 5, 6};

  enum IndexId {
    PRIMARY_KEY = 0,
    SKEY_INDEX_NAME = 1,
    SKEY_TABLE_OID = 2,
    // Add new indexes here in creation order
  };
};

}  // namespace catalog
}  // namespace peloton
