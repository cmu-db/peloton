//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// data_table_proxy.h
//
// Identification: src/include/codegen/proxy/data_table_proxy.h
//
// Copyright (c) 2015-2017, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include "codegen/codegen.h"
#include "codegen/proxy/proxy.h"
#include "storage/data_table.h"

namespace peloton {
namespace codegen {

PROXY(DataTable) {
  // For now, we don't need access to individual fields, so use an opaque byte
  // array
  PROXY_MEMBER_FIELD(0, char[sizeof(storage::DataTable)], opaque);

  PROXY_TYPE("peloton::storage::DataTable", char[sizeof(storage::DataTable)]);

  // Proxy DataTable::GetTileGroupCount()
  PROXY_METHOD(GetTileGroupCount, &storage::DataTable::GetTileGroupCount,
               "_ZNK7peloton7storage9DataTable17GetTileGroupCountEv");
};

namespace proxy {
template <>
struct TypeBuilder<::peloton::storage::DataTable> {
  using Type = llvm::Type *;
  static Type GetType(CodeGen &codegen) ALWAYS_INLINE {
    return DataTableProxy::GetType(codegen);
  }
};
}  // namespace proxy

}  // namespace codegen
}  // namespace peloton
