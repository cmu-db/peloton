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
#include "codegen/proxy/proxy.h"
#include "codegen/proxy/type_builder.h"

namespace peloton {
namespace codegen {

PROXY(Catalog) {
  /// The data members of catalog::Catalog
  /// Note: For now, we don't need access to individual fields.  Instead, we
  /// use an opaque byte array whose size matches a catalog::Catalog object.
  DECLARE_MEMBER(0, char[sizeof(catalog::Catalog)], opaque);
  DECLARE_TYPE;

  /// Proxy peloton::catalog::Catalog::GetTableWithOid()
  DECLARE_METHOD(GetTableWithOid);
};

TYPE_BUILDER(Catalog, catalog::Catalog);

}  // namespace codegen
}  // namespace peloton