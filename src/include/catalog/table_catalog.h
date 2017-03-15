//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// table_catalog.h
//
// Identification: src/include/catalog/table_catalog.h
//
// Copyright (c) 2015-17, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include "catalog/abstract_catalog.h"

namespace peloton {
namespace catalog {

class TableCatalog : public AbstractCatalog {
 public:
  // Global Singleton
  static TableCatalog *GetInstance(void);

  TableCatalog();

  ~TableCatalog();

  inline oid_t GetNextOid() { return oid_++ | static_cast<oid_t>(type::CatalogType::TABLE); }

  void DeleteTuple(oid_t id, concurrency::Transaction *txn);

  std::unique_ptr<storage::Tuple> GetTableCatalogTuple(
    oid_t table_id, std::string table_name, oid_t database_id,
    std::string database_name, type::AbstractPool *pool);

 private:
  std::unique_ptr<catalog::Schema> InitializeTableCatalogSchema();
};

}  // End catalog namespace
}  // End peloton namespace
