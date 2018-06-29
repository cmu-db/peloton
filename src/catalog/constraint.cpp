//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// constraint.cpp
//
// Identification: src/catalog/constraint.cpp
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "catalog/constraint.h"

#include <sstream>

namespace peloton {
namespace catalog {

const std::string Constraint::GetInfo() const {
  std::ostringstream os;
  os << "Constraint[" << GetName() << ", "
     << ConstraintTypeToString(constraint_type_);

  if (GetType() == ConstraintType::CHECK) {
    os << ", " << exp_.first << " " << exp_.second.GetInfo();
  }
  os << "]";
  return os.str();
}

}  // namespace catalog
}  // namespace peloton
