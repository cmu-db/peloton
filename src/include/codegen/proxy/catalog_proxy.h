//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// catalog_proxy.h
//
// Identification: src/include/codegen/proxy/catalog_proxy.h
//
// Copyright (c) 2015-2017, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include "catalog/catalog.h"
#include "codegen/codegen.h"
#include "codegen/proxy/proxy.h"
#include "codegen/proxy/data_table_proxy.h"

namespace peloton {
namespace codegen {

PROXY(Catalog) {
  // For now, we don't need access to individual fields, so use an opaque byte
  // array
  PROXY_MEMBER_FIELD(0, char[sizeof(catalog::Catalog)], opaque);

  // The type
  PROXY_TYPE("peloton::catalog::Catalog", char[sizeof(catalog::Catalog)]);

  // Proxy Catalog::GetTableWithOid()
  PROXY_METHOD(GetTableWithOid, &catalog::Catalog::GetTableWithOid,
               "_ZNK7peloton7catalog7Catalog15GetTableWithOidEjj");
};

namespace proxy {
template <>
struct TypeBuilder<catalog::Catalog> {
  static llvm::Type *GetType(CodeGen &codegen) ALWAYS_INLINE {
    return CatalogProxy::GetType(codegen);
  }
};
}  // namespace proxy

}  // namespace codegen
}  // namespace peloton