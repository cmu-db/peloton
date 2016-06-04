//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// property.h
//
// Identification: src/optimizer/property.h
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include "optimizer/util.h"

namespace peloton {
namespace optimizer {

enum class PropertyType {
  Sort,
  Columns,
};

class Property {
 public:
  virtual PropertyType Type() const = 0;

  virtual hash_t Hash() const = 0;

  virtual bool operator==(const Property &r) const = 0;

};

} /* namespace optimizer */
} /* namespace peloton */
