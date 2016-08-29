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
#include "common/types.h"

#include <sstream>

namespace peloton {
namespace catalog {

const std::string Constraint::GetInfo() const {
  std::ostringstream os;

  os << "\tCONSTRAINT ";

  os << GetName() << " ";
  os << ConstraintTypeToString(constraint_type);

  os << "\n\n";

  return os.str();
}

}  // End catalog namespace
}  // End peloton namespace
