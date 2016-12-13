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

#include <typeinfo>

#include "optimizer/util.h"

namespace peloton {
namespace optimizer {

class PropertyVisitor;

enum class PropertyType {
  SORT,
  COLUMNS,
  PREDICATE,
  PROJECT,
};

/*
 * Physical properties are those fields that can be directly added to the plan,
 * and don't need to perform transformations on.
 *
 * Note: Sometimes there're different choices of physical properties. E.g., the
 * sorting order might be provided by either a sort executor or a underlying
 * sort merge join. But those different implementations are directly specified
 * when constructing the physical operator tree, other than using rule
 * transformation.
 */
class Property {
 public:
  virtual ~Property();

  virtual PropertyType Type() const = 0;

  virtual hash_t Hash() const;

  // Provide partial order
  virtual bool operator>=(const Property &r) const;

  virtual void Accept(PropertyVisitor *v) const = 0;

  template <typename T>
  T *As() {
    if (typeid(this) == typeid(T)) {
      return reinterpret_cast<T *>(this);
    }
    return nullptr;
  }
};

} /* namespace optimizer */
} /* namespace peloton */
