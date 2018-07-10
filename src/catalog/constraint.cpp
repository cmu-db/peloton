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
     << "OID=" << constraint_oid_ << ", "
     << ConstraintTypeToString(constraint_type_) << ", ";

  os << "Column: (";
  bool first = true;
  for (auto col_id : column_ids_) {
    if(first) first = false;
    else os << ", ";
    os << std::to_string(col_id);
  }
  os << "), ";

  os << "index_oid:" << std::to_string(index_oid_);

  if (constraint_type_ == ConstraintType::FOREIGN) {
    os << ", Foreign key: (Sink table:"
       << std::to_string(fk_sink_table_oid_) << ", "
       << "Column:(";
    bool first = true;
    for (auto col_id : fk_sink_col_ids_) {
      if(first) first = false;
      else os << ", ";
      os << std::to_string(col_id);
    }
    os << "), " << FKConstrActionTypeToString(fk_update_action_) << ", "
       << FKConstrActionTypeToString(fk_delete_action_) << ")";
  }

  if (constraint_type_ == ConstraintType::CHECK) {
    os << ", Check: (" << check_exp_.first << " "
       << check_exp_.second.GetInfo() << ")";
  }
  os << "]";
  return os.str();
}

}  // namespace catalog
}  // namespace peloton
