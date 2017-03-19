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
  type::TypeId GetTypeByOid_Name(oid_t table_id, std::string name,
                                 concurrency::Transaction *txn);
  type::TypeId GetTypeByOid_Offset(oid_t table_id, oid_t offset,
                                   concurrency::Transaction *txn);
  std::string GetNameByOid_Offset(oid_t table_id, oid_t offset,
                                  concurrency::Transaction *txn);
  oid_t GetOffsetByOid_Name(oid_t table_id, std::string &name,
                            concurrency::Transaction *txn);

  // Write related API
  void DeleteByOid_Name(oid_t table_id, std::string &name,
                        concurrency::Transaction *txn);

  bool Insert(oid_t table_id, std::string column_name, type::TypeId column_type,
              oid_t column_offset, bool column_isPrimary,
              bool column_hasConstrain, type::AbstractPool *pool,
              concurrency::Transaction *txn);

 private:
  std::unique_ptr<catalog::Schema> InitializeColumnCatalogSchema();
};

}  // End catalog namespace
}  // End peloton namespace
