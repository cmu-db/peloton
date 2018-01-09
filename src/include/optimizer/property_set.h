//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// property_set.h
//
// Identification: src/include/optimizer/property_set.h
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <memory>
#include <vector>

#include "optimizer/property.h"

namespace peloton {
namespace optimizer {

// A set of physical properties
class PropertySet {
 public:
  PropertySet();

  PropertySet(std::vector<std::shared_ptr<Property>> properties);

  const std::vector<std::shared_ptr<Property>> &Properties() const;

  void AddProperty(std::shared_ptr<Property> property);

  void RemoveProperty(PropertyType type);

  const std::shared_ptr<Property> GetPropertyOfType(PropertyType type) const;

  template <typename T>
  const T *GetPropertyOfTypeAs(PropertyType type) const {
    auto property = GetPropertyOfType(type);
    if (property) return property->As<T>();
    return nullptr;
  }

  hash_t Hash() const;

  // whether this property set contains a specific property
  bool HasProperty(const Property &r_property) const;

  bool operator>=(const PropertySet &r) const;

  bool operator==(const PropertySet &r) const;

  std::string ToString() const;

 private:
  std::vector<std::shared_ptr<Property>> properties_;
};

struct PropSetPtrHash {
  std::size_t operator()(std::shared_ptr<PropertySet> const& s) const {
    return s->Hash();
  }
};

struct PropSetPtrEq {
  bool operator()(std::shared_ptr<PropertySet> const& t1,
                  std::shared_ptr<PropertySet> const& t2) const {
    return *t1 == *t2;
  }
};

} // namespace optimizer 
} // namespace peloton 

namespace std {

template <>
struct hash<peloton::optimizer::PropertySet> {
  typedef peloton::optimizer::PropertySet argument_type;
  typedef std::size_t result_type;
  result_type operator()(argument_type const &s) const { return s.Hash(); }
};

}  // namespace std
