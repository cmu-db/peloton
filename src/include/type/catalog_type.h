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

enum class CatalogObjectType {
	INVALID = INVALID_TYPE_ID,
	DATABASE = 1,
	TABLE = 2,
	INDEX = 3
};

}
}