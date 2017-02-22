//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// array_value.h
//
// Identification: src/backend/type/catalog_type.h
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include "type/types.h"
#include "type/value_factory.h"

namespace peloton {
namespace type {

#define CATALOG_OBJECT_TYPE_OFFSET 28

enum class CatalogObjectType : uint32_t {
  INVALID = INVALID_TYPE_ID,
  DATABASE = ((uint32_t) 1) << CATALOG_OBJECT_TYPE_OFFSET,
  TABLE = ((uint32_t) 2) << CATALOG_OBJECT_TYPE_OFFSET,
  INDEX = ((uint32_t) 3) << CATALOG_OBJECT_TYPE_OFFSET
};
}
}