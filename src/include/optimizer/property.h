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

#include "common/internal_types.h"

namespace peloton {
namespace optimizer {

class PropertyVisitor;

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

  virtual std::string ToString() const;

  template <typename T>
  const T *As() const {
    if (typeid(*this) == typeid(T)) {
      return reinterpret_cast<const T *>(this);
    }
    return nullptr;
  }
};

} // namespace optimizer
} // namespace peloton
