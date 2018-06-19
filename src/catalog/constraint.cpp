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
     << "OID=" << constraint_oid << ", "
     << ConstraintTypeToString(constraint_type) << ", ";

  os << "Column: (";
  bool first = true;
  for (auto col_id : column_ids) {
    if(first) first = false;
    else os << ", ";
    os << std::to_string(col_id);
  }
  os << "), ";

  os << "index_oid:" << std::to_string(index_oid);

  if (constraint_type == ConstraintType::FOREIGN) {
    os << ", Foreign key: (Sink table:"
       << std::to_string(fk_sink_table_oid) << ", "
       << "Column:(";
    bool first = true;
    for (auto col_id : fk_sink_col_ids) {
      if(first) first = false;
      else os << ", ";
      os << std::to_string(col_id);
    }
    os << "), " << FKConstrActionTypeToString(fk_update_action) << ", "
       << FKConstrActionTypeToString(fk_delete_action) << ")";
  }

  if (constraint_type == ConstraintType::CHECK) {
    os << ", Check: (" << check_exp.first << " "
       << check_exp.second.GetInfo() << ")";
  }
  os << "]";
  return os.str();
}

}  // namespace catalog
}  // namespace peloton
