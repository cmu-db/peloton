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
// 5: indexed_attributes (indicate which table columns this index indexes. For
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

namespace peloton {
namespace catalog {

class IndexCatalog : public AbstractCatalog {
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

  //===--------------------------------------------------------------------===//
  // Read-only Related API
  //===--------------------------------------------------------------------===//
  std::string GetIndexName(oid_t index_oid, concurrency::Transaction *txn);
  oid_t GetTableOid(oid_t index_oid, concurrency::Transaction *txn);
  IndexType GetIndexType(oid_t index_oid, concurrency::Transaction *txn);
  IndexConstraintType GetIndexConstraint(oid_t index_oid,
                                         concurrency::Transaction *txn);
  bool IsUniqueKeys(oid_t index_oid, concurrency::Transaction *txn);
  std::vector<oid_t> GetIndexedAttributes(oid_t index_oid,
                                          concurrency::Transaction *txn);
  oid_t GetIndexOid(const std::string &index_name,
                    concurrency::Transaction *txn);

  std::vector<oid_t> GetIndexOids(oid_t table_oid,
                                  concurrency::Transaction *txn);

 private:
  IndexCatalog(storage::Database *pg_catalog, type::AbstractPool *pool,
               concurrency::Transaction *txn);

  std::unique_ptr<catalog::Schema> InitializeSchema();
};

}  // End catalog namespace
}  // End peloton namespace
