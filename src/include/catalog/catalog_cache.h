//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// catalog_cache.h
//
// Identification: src/include/catalog/catalog_cache.h
//
// Copyright (c) 2015-17, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <mutex>

#include <unordered_map>

#include "catalog/table_catalog.h"

namespace peloton {
namespace catalog {

class CatalogCache {
 public:
  CatalogCache() {}

  bool InsertObject(const TableCatalogObject table_object);

  const TableCatalogObject GetTableByOid(
      oid_t table_oid, concurrency::Transaction *txn = nullptr);

 private:
  std::unordered_map<oid_t, TableCatalogObject> table_objects_cache;
};

}  // namespace catalog
}  // namespace peloton
