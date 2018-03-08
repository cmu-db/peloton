//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// storage_manager_proxy.cpp
//
// Identification: src/codegen/proxy/storage_manager_proxy.cpp
//
// Copyright (c) 2015-2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "codegen/proxy/storage_manager_proxy.h"

#include "codegen/proxy/data_table_proxy.h"

namespace peloton {
namespace codegen {

// Define the proxy type with the single opaque member field
DEFINE_TYPE(StorageManager, "storage::StorageManager", opaque);

// Define a method that proxies storage::StorageManager::GetTableWithOid()
DEFINE_METHOD(peloton::storage, StorageManager, GetTableWithOid);

}  // namespace codegen
}  // namespace peloton
