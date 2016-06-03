//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// property_set.h
//
// Identification: src/backend/optimizer/property_set.h
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include "backend/optimizer/property.h"
#include "backend/optimizer/util.h"

#include <vector>
#include <memory>

namespace peloton {
namespace optimizer {

class PropertySet {
 public:
  PropertySet();

  const std::vector<std::shared_ptr<Property>> &Properties() const;

  const std::shared_ptr<Property> GetPropertyOfType(PropertyType type) const;

  bool IsSubset(const PropertySet &r);

  hash_t Hash() const;

  bool operator==(const PropertySet &r);

 private:
  std::vector<std::shared_ptr<Property>> properties;
};

} /* namespace optimizer */
} /* namespace peloton */

namespace std {

template<> struct hash<peloton::optimizer::PropertySet> {
  typedef peloton::optimizer::PropertySet argument_type;
  typedef std::size_t result_type;
  result_type operator()(argument_type const& s) const {
    return s.Hash();
  }
};

}
