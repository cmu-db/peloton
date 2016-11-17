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

bool PropertySet::operator>=(const PropertySet &r) const {
  for (auto property : r.properties_) {
    bool has_property = false;
    for (auto r_property : properties_) {
      if (*property >= *r_property) {
        has_property = true;
        break;
      }
    }
    if (has_property == false) return false;
  }
  return true;
}

bool PropertySet::operator==(const PropertySet &r) const {
  return *this >= r && r >= *this;
}

hash_t PropertySet::Hash() const {
  size_t prop_size = properties_.size();
  hash_t hash = util::Hash<size_t>(&prop_size);
  for (auto &prop : properties_) {
    hash = util::CombineHashes(hash, prop->Hash());
  }
  return hash;
}

} /* namespace optimizer */
} /* namespace peloton */
