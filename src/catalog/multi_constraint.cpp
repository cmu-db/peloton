//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// multi_constraint.cpp
//
// Identification: src/catalog/multi_constraint.cpp
//
// Copyright (c) 2015-2017, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "catalog/multi_constraint.h"
#include "common/internal_types.h"

#include <sstream>

namespace peloton {
namespace catalog {

const std::string MultiConstraint::GetInfo() const {
  std::ostringstream os;
  os << "Constraint[" << GetName() << ", "
     << ConstraintTypeToString(constraint_type) << " , related columns: (";
  bool first = true;
  for (auto id : column_ids) {
    if (first) {
      os << id;
      first = false;
    } else {
      os << " ," << id;
    }
  }
  os << ")]";
  return os.str();
}

}  // namespace catalog
}  // namespace peloton
