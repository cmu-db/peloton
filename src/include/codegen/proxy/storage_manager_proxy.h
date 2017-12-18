//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// storage_manager_proxy.h
//
// Identification: src/include/codegen/proxy/storage_manager_proxy.h
//
// Copyright (c) 2015-2017, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include "codegen/proxy/proxy.h"
#include "codegen/proxy/type_builder.h"
#include "storage/storage_manager.h"

namespace peloton {
namespace codegen {

PROXY(StorageManager) {
  /// The data members of storage::StorageManager
  /// Note: For now, we don't need access to individual fields.  Instead, we
  /// use an opaque byte array whose size matches a catalog::Catalog object.
  DECLARE_MEMBER(0, char[sizeof(storage::StorageManager)], opaque);
  DECLARE_TYPE;

  /// Proxy peloton::storage::StorageManager::GetTableWithOid()
  DECLARE_METHOD(GetTableWithOid);
};

TYPE_BUILDER(StorageManager, storage::StorageManager);

}  // namespace codegen
}  // namespace peloton
