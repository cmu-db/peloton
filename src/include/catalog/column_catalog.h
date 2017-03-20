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

  inline oid_t GetNextOid() {
    return oid_++ | static_cast<oid_t>(type::CatalogType::COLUMN);
  }

  // Write related API
  void DeleteByOid_Name(oid_t table_id, std::string &name,
                        concurrency::Transaction *txn);

  // TODO: api change
  bool Insert(oid_t table_id, oid_t column_offset, std::string column_name,
              type::TypeId column_type, bool is_inlined,
              std::vector<ConstraintType> constraints,
              concurrency::Transaction *txn);


  // Read-only API
  type::TypeId GetTypeByOidWithName(oid_t table_id, std::string name,
                                 concurrency::Transaction *txn);
  type::TypeId GetTypeByOidWithOffset(oid_t table_id, oid_t offset,
                                   concurrency::Transaction *txn);
  std::string GetNameByOidWithOffset(oid_t table_id, oid_t offset,
                                  concurrency::Transaction *txn);
  oid_t GetOffsetByOidWithName(oid_t table_id, std::string &name,
                            concurrency::Transaction *txn);

 private:
  ColumnCatalog();

  ~ColumnCatalog();

  std::unique_ptr<catalog::Schema> InitializeSchema();
};

}  // End catalog namespace
}  // End peloton namespace
