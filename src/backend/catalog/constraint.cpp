/*-------------------------------------------------------------------------
 *
 * constraint.cpp
 * file description
 *
 * Copyright(c) 2015, CMU
 *
 *-------------------------------------------------------------------------
 */

#include "backend/catalog/constraint.h"

namespace peloton {
namespace catalog {

std::ostream& operator<<(std::ostream& os, const Constraint& constraint) {

  os << "\tCONSTRAINT ";

  os << constraint.GetName() << " "
      << ConstraintTypeToString(constraint.constraint_type) << "\n";

  os << "\n";

  return os;
}


} // End catalog namespace
} // End peloton namespace


