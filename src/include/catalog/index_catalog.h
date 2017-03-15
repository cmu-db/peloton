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

  void DeleteTuple(oid_t id, concurrency::Transaction *txn);

  std::unique_ptr<storage::Tuple> GetIndexCatalogTuple(
    oid_t index_id, std::string index_name,
    oid_t table_id, oid_t database_id, bool unique_keys
    type::AbstractPool *pool)

 private:
  std::unique_ptr<catalog::Schema> InitializeIndexCatalogSchema();
};

}  // End catalog namespace
}  // End peloton namespace
