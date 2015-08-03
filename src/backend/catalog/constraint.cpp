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
#include "backend/common/types.h"

#include <sstream>

namespace peloton {
namespace catalog {

std::ostream& operator<<(std::ostream& os, const Constraint& constraint) {
  os << "\tCONSTRAINT ";

  os << constraint.GetName() << " ";
  os << ConstraintTypeToString(constraint.constraint_type);
  
  if( constraint.GetType() == CONSTRAINT_TYPE_DEFAULT){
    os << " Default expression : " << nodeToString(constraint.raw_expr);
  }

  os << "\n\n";

  return os;
}

}  // End catalog namespace
}  // End peloton namespace
