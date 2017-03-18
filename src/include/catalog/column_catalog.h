//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// column_catalog.h
//
// Identification: src/include/catalog/column_catalog.h
//
// Copyright (c) 2015-17, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include "catalog/abstract_catalog.h"

namespace peloton {
namespace catalog {

class ColumnCatalog : public AbstractCatalog {
 public:
  // Global Singleton
  static ColumnCatalog *GetInstance(void);

  ColumnCatalog();

  ~ColumnCatalog();

  inline oid_t GetNextOid() {
    return oid_++ | static_cast<oid_t>(type::CatalogType::TABLE);
  }

  // Read-only API
  type::TypeId GetTypeByOid_Name(oid_t id, concurrency::Transaction *txn);
  type::TypeId GetTypeByOid_Offset(std::string &name,
                                   concurrency::Transaction *txn);
  std::string GetNameByOid_Offset(oid_t id, concurrency::Transaction *txn);
  oid_t GetOffsetByOid_Name(std::string &name, concurrency::Transaction *txn);

  // Write related API
  void DeleteTuple(oid_t id, std::string &column_name,
                   concurrency::Transaction *txn);

  std::unique_ptr<storage::Tuple> GetColumnCatalogTuple(
      oid_t table_id, std::string column_name, type::AbstractPool *pool);

 private:
  std::unique_ptr<catalog::Schema> InitializeColumnCatalogSchema();
};

}  // End catalog namespace
}  // End peloton namespace
