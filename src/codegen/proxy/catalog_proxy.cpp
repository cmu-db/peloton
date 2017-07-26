//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// catalog_proxy.cpp
//
// Identification: src/codegen/proxy/catalog_proxy.cpp
//
// Copyright (c) 2015-2017, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "codegen/proxy/catalog_proxy.h"

#include "codegen/proxy/data_table_proxy.h"

namespace peloton {
namespace codegen {

// Define the proxy type with the single opaque member field
DEFINE_TYPE(Catalog, "peloton::storage::StorageManager", MEMBER(opaque));

// Define a method that proxies catalog::Catalog::GetTableWithOid()
DEFINE_METHOD(Catalog, GetTableWithOid,
              &storage::StorageManager::GetTableWithOid,
              "_ZNK7peloton7storage14StorageManager15GetTableWithOidEjj");

}  // namespace codegen
}  // namespace peloton