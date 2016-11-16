//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// property.cpp
//
// Identification: src/optimizer/property.cpp
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "optimizer/property.h"

namespace peloton {
namespace optimizer {

Property::~Property() {}

hash_t Property::Hash() const {
  PropertyType t = Type();
  return util::Hash(&t);
}

bool Property::operator==(const Property &r) const {
  return Type() == r.Type();
}

} /* namespace optimizer */
} /* namespace peloton */
