//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// column.cpp
//
// Identification: src/catalog/column.cpp
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "catalog/column.h"

#include <sstream>

#include "common/internal_types.h"

namespace peloton {
namespace catalog {

void Column::SetLength(size_t column_length) {
  // Set the column length based on whether it is inlined
  if (is_inlined) {
    fixed_length = column_length;
    variable_length = 0;
  } else {
    fixed_length = sizeof(uintptr_t);
    variable_length = column_length;
  }
}

void Column::SetInlined() {
  switch (column_type) {
    case type::TypeId::VARCHAR:
    case type::TypeId::VARBINARY:
      break;  // No change of inlined setting

    default:
      is_inlined = true;
      break;
  }
}

const std::string Column::GetInfo() const {
  std::ostringstream os;

  os << "Column[" << column_name << ", " << TypeIdToString(column_type) << ", "
     << "Offset:" << column_offset << ", ";

  if (is_inlined) {
    os << "FixedLength:" << fixed_length;
  } else {
    os << "VarLength:" << variable_length;
  }

  if (constraints.empty() == false) {
    os << ", {";
    bool first = true;
    for (auto constraint : constraints) {
      if (first) {
        first = false;
      } else {
        os << ", ";
      }
      os << constraint.GetInfo();
    }
    os << "}";
  }
  os << "]";

  return (os.str());
}

}  // namespace catalog
}  // namespace peloton
