//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// attribute_info.h
//
// Identification: src/include/planner/attribute_info.h
//
// Copyright (c) 2015-17, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//


#pragma once

#include "type/type.h"
#include "type/types.h"

#include <string>

namespace peloton {
namespace planner {

// Describes an attribute that is passed around in the query plan
struct AttributeInfo {
  // The actual type of this attribute (smallint, integer, varchar etc.)
  type::TypeId type;
  // Is this attribute nullable (i.e., can it take on a null value)
  bool nullable;
  // The ID of the attribute
  oid_t attribute_id;
  // The name of this attribute. This isn't always available, so no one should
  // rely on its existence.
  std::string name;
};

}  // namespace planner
}  // namespace peloton