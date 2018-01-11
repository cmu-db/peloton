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
#include "common/logger.h"
#include "util/hash_util.h"

namespace peloton {
namespace optimizer {

PropertySet::PropertySet() {}

PropertySet::PropertySet(std::vector<std::shared_ptr<Property>> properties)
    : properties_(std::move(properties)) {}

const std::vector<std::shared_ptr<Property>> &PropertySet::Properties() const {
  return properties_;
}

void PropertySet::AddProperty(std::shared_ptr<Property> property) {
  LOG_TRACE("Add property with type %d", static_cast<int>(property->Type()));
  auto iter = properties_.begin();
  for (; iter != properties_.end(); ++iter)
    if (property->Type() < (*iter)->Type()) break;

  properties_.insert(iter, property);
}

void PropertySet::RemoveProperty(PropertyType type) {
  auto iter = properties_.begin();
  for (; iter != properties_.end(); ++iter) {
    if ((*iter)->Type() == type) break;
  }
  if (iter != properties_.end()) properties_.erase(iter);
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

std::string PropertySet::ToString() const {
  std::string str = "PropertySet(";
  for (auto &property : properties_) {
    str += property->ToString() + ",";
  }
  str += ")\n";
  return str;
}

hash_t PropertySet::Hash() const {
  size_t prop_size = properties_.size();
  hash_t hash = HashUtil::Hash<size_t>(&prop_size);
  for (auto &prop : properties_) {
    hash = HashUtil::CombineHashes(hash, prop->Hash());
  }
  return hash;
}

}  // namespace optimizer
}  // namespace peloton
