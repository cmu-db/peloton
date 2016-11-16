//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// property.h
//
// Identification: src/include/optimizer/property.h
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include "optimizer/util.h"

namespace peloton {
namespace optimizer {

enum class PropertyType {
  SORT,
  COLUMNS,
  PREDICATE,
};

class Property {
 public:
  virtual ~Property();

  virtual PropertyType Type() const = 0;

  virtual hash_t Hash() const;

  virtual bool operator==(const Property &r) const;
};

} /* namespace optimizer */
} /* namespace peloton */
