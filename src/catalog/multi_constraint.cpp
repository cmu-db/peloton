#include "catalog/multi_constraint.h"
#include "type/types.h"

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

}  // End catalog namespace
}  // End peloton namespace
