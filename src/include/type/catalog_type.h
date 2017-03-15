//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// catalog_type.h
//
// Identification: src/include/type/catalog_type.h
//
// Copyright (c) 2015-17, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include "type/types.h"

namespace peloton {
namespace type {

#define CATALOG_TYPE_OFFSET 24

enum class CatalogType : uint32_t {
  INVALID = INVALID_TYPE_ID,
  DATABASE = 0, // Note: pg_catalog database oid is START_OID = 0
  TABLE = 1 << CATALOG_TYPE_OFFSET,
  INDEX = 2 << CATALOG_TYPE_OFFSET,
  COLUMN = 3 << CATALOG_TYPE_OFFSET,
  // To be added
};
}
}
