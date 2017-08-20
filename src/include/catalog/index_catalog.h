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
// 3: index_type (default value is BWTREE)
// 4: index_constraint
// 5: unique_keys (is this index supports duplicate keys)
// 6: indexed_attributes (indicate which table columns this index indexes. For
// example a value of 0 2 would mean that the first and the third table columns
// make up the index.)
//
// Indexes: (index offset: indexed columns)
// 0: index_oid (unique & primary key)
// 1: index_name (unique)
// 2: table_oid (non-unique)
//
//===----------------------------------------------------------------------===//

#pragma once

#include "catalog/abstract_catalog.h"
#include "executor/logical_tile.h"

namespace peloton {
namespace catalog {

class IndexCatalogObject {
  friend class TableCatalogObject;

 public:
  IndexCatalogObject(oid_t index_oid = INVALID_OID)
      : index_oid(index_oid),
        index_name(),
        table_oid(INVALID_OID),
        index_type(IndexType::INVALID),
        index_constraint(IndexConstraintType::INVALID),
        unique_keys(false),
        key_attrs() {}
  IndexCatalogObject(executor::LogicalTile *tile, int tupleId = 0);

  const oid_t index_oid;
  const std::string index_name;
  const oid_t table_oid;
  const IndexType index_type;
  const IndexConstraintType index_constraint;
  const bool unique_keys;
  std::vector<oid_t> key_attrs;
};

class IndexCatalog : public AbstractCatalog {
  friend class IndexCatalogObject;
  friend class TableCatalogObject;
  friend class Catalog;

 public:
  ~IndexCatalog();

  // Global Singleton, only the first call requires passing parameters.
  static IndexCatalog *GetInstance(storage::Database *pg_catalog = nullptr,
                                   type::AbstractPool *pool = nullptr,
                                   concurrency::Transaction *txn = nullptr);

  inline oid_t GetNextOid() { return oid_++ | INDEX_OID_MASK; }

  //===--------------------------------------------------------------------===//
  // write Related API
  //===--------------------------------------------------------------------===//
  bool InsertIndex(oid_t index_oid, const std::string &index_name,
                   oid_t table_oid, IndexType index_type,
                   IndexConstraintType index_constraint, bool unique_keys,
                   std::vector<oid_t> indekeys, type::AbstractPool *pool,
                   concurrency::Transaction *txn);
  bool DeleteIndex(oid_t index_oid, concurrency::Transaction *txn);

 private:
  //===--------------------------------------------------------------------===//
  // Read-only Related API
  //===--------------------------------------------------------------------===//
  // std::shared_ptr<IndexCatalogObject> GetIndexObject(
  //     oid_t index_oid, concurrency::Transaction *txn);
  // std::shared_ptr<IndexCatalogObject> GetIndexObject(
  //     const std::string &index_name, concurrency::Transaction *txn);
  const std::unordered_map<oid_t, std::shared_ptr<IndexCatalogObject>>
  GetIndexObjects(oid_t table_oid, concurrency::Transaction *txn);

  // std::string GetIndexName(oid_t index_oid, concurrency::Transaction *txn);
  // oid_t GetTableOid(oid_t index_oid, concurrency::Transaction *txn);
  // IndexType GetIndexType(oid_t index_oid, concurrency::Transaction *txn);
  // IndexConstraintType GetIndexConstraint(oid_t index_oid,
  //                                        concurrency::Transaction *txn);
  // bool IsUniqueKeys(oid_t index_oid, concurrency::Transaction *txn);
  // std::vector<oid_t> GetIndexedAttributes(oid_t index_oid,
  //                                         concurrency::Transaction *txn);
  // oid_t GetIndexOid(const std::string &index_name,
  //                   concurrency::Transaction *txn);

  // std::vector<oid_t> GetIndexOids(oid_t table_oid,
  //                                 concurrency::Transaction *txn);

 private:
  IndexCatalog(storage::Database *pg_catalog, type::AbstractPool *pool,
               concurrency::Transaction *txn);

  std::unique_ptr<catalog::Schema> InitializeSchema();
};

}  // namespace catalog
}  // namespace peloton
