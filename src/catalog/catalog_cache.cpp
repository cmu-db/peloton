//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// catalog_cache.cpp
//
// Identification: src/catalog/catalog_cache.cpp
//
// Copyright (c) 2015-17, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "catalog/catalog_cache.h"

namespace peloton {
namespace catalog {

bool CatalogCache::InsertObject(const TableCatalogObject table_object) {
  auto it = table_objects_cache.find(table_object.table_oid);
  if (it == table_objects_cache.end()) {
    table_objects_cache.insert(
        std::make_pair(table_object.table_oid, table_object));
    return true;
  } else {
    return false;
  }
}

const TableCatalogObject CatalogCache::GetTableByOid(
    oid_t table_oid, concurrency::Transaction *txn) {
  auto it = table_objects_cache.find(table_oid);
  if (it == table_objects_cache.end()) {
    if (txn == nullptr) return TableCatalogObject();
    auto table_object =
        TableCatalog::GetInstance()->GetTableByOid(table_oid, txn);
    table_objects_cache.insert(std::make_pair(table_oid, table_object));
  }
  return it->second;
}

}  // namespace catalog
}  // namespace peloton
