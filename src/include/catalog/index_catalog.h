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

#pragma once

#include "catalog/abstract_catalog.h"

namespace peloton {
namespace catalog {

class IndexCatalog : public AbstractCatalog {
 public:
  // Global Singleton
  static IndexCatalog *GetInstance(void);

  IndexCatalog();

  ~IndexCatalog();

  inline oid_t GetNextOid() { return oid_++ | static_cast<oid_t>(type::CatalogType::DATABASE); }

  std::unique_ptr<storage::Tuple> GetIndexCatalogTuple(
    oid_t index_oid, std::string index_name,
    oid_t table_oid, oid_t database_oid, bool unique_keys
    type::AbstractPool *pool)

  // Read-only API
  std::string GetNameByOid(oid_t id, concurrency::Transaction *txn);
  oid_t GetTableidByOid(oid_t id, concurrency::Transaction *txn);
  oid_t GetDatabaseidByOid(oid_t id, concurrency::Transaction *txn);
  bool GetUniquekeysByOid(oid_t id, concurrency::Transaction *txn);

  // Write related API
  void DeleteTuple(oid_t id, concurrency::Transaction *txn);

 private:
  std::unique_ptr<catalog::Schema> InitializeIndexCatalogSchema();
};

}  // End catalog namespace
}  // End peloton namespace
