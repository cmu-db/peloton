//===----------------------------------------------------------------------===//
//
//                         PelotonDB
//
// constraint.cpp
//
// Identification: src/backend/catalog/constraint.cpp
//
// Copyright (c) 2015, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "backend/catalog/constraint.h"
#include "backend/common/types.h"

#include <sstream>

namespace peloton {
namespace catalog {

const char *Constraint::GetInfo() const {
  std::ostringstream os;

  os << "\tCONSTRAINT ";

  os << GetName() << " ";
  os << ConstraintTypeToString(constraint_type);

  if (GetType() == CONSTRAINT_TYPE_DEFAULT) {
    os << " Default expression : " << nodeToString(expr);
  }

  os << "\n\n";

  return os.str().c_str();
}

}  // End catalog namespace
}  // End peloton namespace
