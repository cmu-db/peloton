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

#include "util/hash_util.h"
#include "optimizer/property_set.h"
#include "common/logger.h"

namespace peloton {
namespace optimizer {

PropertySet::PropertySet() {}

const std::vector<std::shared_ptr<Property>> &PropertySet::Properties() const {
  return properties_;
}

void PropertySet::AddProperty(std::shared_ptr<Property> property) {
  LOG_TRACE("Add property with type %d", static_cast<int>(property->Type()));
  properties_.push_back(property);
}

const std::shared_ptr<Property> PropertySet::GetPropertyOfType(
    PropertyType type) const {
  for (auto &prop : properties_) {
    if (prop->Type() == type) {
      return prop;
    }
  }

  LOG_TRACE("Didn't find property with type %d", static_cast<int>(type));

  return nullptr;
}

bool PropertySet::HasProperty(const Property &r_property) const {
  for (auto property : properties_) {
    if (*property >= r_property) {
      return true;
    }
  }

  return false;
}

bool PropertySet::operator>=(const PropertySet &r) const {
  for (auto r_property : r.properties_) {
    if (HasProperty(*r_property) == false) return false;
  }
  return true;
}

bool PropertySet::operator==(const PropertySet &r) const {
  return *this >= r && r >= *this;
}

hash_t PropertySet::Hash() const {
  size_t prop_size = properties_.size();
  hash_t hash = HashUtil::Hash<size_t>(&prop_size);
  for (auto &prop : properties_) {
    hash = HashUtil::CombineHashes(hash, prop->Hash());
  }
  return hash;
}

} /* namespace optimizer */
} /* namespace peloton */
