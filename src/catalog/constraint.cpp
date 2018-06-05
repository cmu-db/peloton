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
  os << "Constraint[" << GetName()
  	 << "(" << std::to_string(GetConstraintOid()) << "), "
     << ConstraintTypeToString(constraint_type)
     << ", index_oid:" << std::to_string(index_oid);

  if (GetType() == ConstraintType::DEFAULT) {
  	os << "," << default_value->GetInfo();
  }

  if (GetType() == ConstraintType::CHECK) {
    os << ", " << exp.first << " " << exp.second.GetInfo();
  }
  os << "]";
  return os.str();
}

}  // namespace catalog
}  // namespace peloton
