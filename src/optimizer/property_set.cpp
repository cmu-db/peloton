//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// property_set.cpp
//
// Identification: src/optimizer/property_set.cpp
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "optimizer/property_set.h"

namespace peloton {
namespace optimizer {

PropertySet::PropertySet() {}

const std::vector<std::shared_ptr<Property>> &PropertySet::Properties() const {
  return properties_;
}

void PropertySet::AddProperty(Property *property) {
  properties_.push_back(std::shared_ptr<Property>(property));
}

const std::shared_ptr<Property> PropertySet::GetPropertyOfType(
    PropertyType type) const {
  for (auto &prop : properties_) {
    if (prop->Type() == type) {
      return prop;
    }
  }
  return nullptr;
}

bool PropertySet::IsSubset(const PropertySet &r) {
  (void)r;
  return true;
}

hash_t PropertySet::Hash() const {
  size_t prop_size = properties_.size();
  hash_t hash = util::Hash<size_t>(&prop_size);
  for (auto &prop : properties_) {
    hash = util::CombineHashes(hash, prop->Hash());
  }
  return hash;
}

bool PropertySet::operator==(const PropertySet &r) const {
  for (auto &left_prop : properties_) {
    bool match = false;
    for (auto &right_prop : r.properties_) {
      match = match || (*left_prop == *right_prop);
    }
    if (!match) return false;
  }
  return true;
}

} /* namespace optimizer */
} /* namespace peloton */
