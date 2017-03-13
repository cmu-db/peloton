//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// catalog_table_type.h
//
// Identification: src/include/type/catalog_table_type.h
//
// Copyright (c) 2015-17, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include "type/types.h"

namespace peloton {
namespace type {

#define CATALOG_TABLE_TYPE_OFFSET 24

// xxx_oid = (GetNextOid() | static_cast<oid_t>(type::CatalogTableType::XXX));
enum class CatalogTableType : uint32_t {
  INVALID = INVALID_TYPE_ID,
  DATABASE = 0,
  TABLE = 1 << CATALOG_TABLE_TYPE_OFFSET,
  INDEX = 2 << CATALOG_TABLE_TYPE_OFFSET,
  COLUMN = 3 << CATALOG_TABLE_TYPE_OFFSET;
};
}
}
