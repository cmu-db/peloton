//===----------------------------------------------------------------------===//
//
//                         PelotonDB
//
// column.cpp
//
// Identification: src/backend/catalog/column.cpp
//
// Copyright (c) 2015, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <sstream>

#include "backend/catalog/column.h"
#include "backend/common/types.h"

namespace peloton {
namespace catalog {

void Column::SetLength(oid_t column_length) {
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
    case VALUE_TYPE_VARCHAR:
    case VALUE_TYPE_VARBINARY:
      is_inlined = false;
      break;

    default:
      is_inlined = true;
      break;
  }
}

// Get a string representation
std::ostream &operator<<(std::ostream &os, const Column &column_info) {
  os << " name = " << column_info.column_name << ","
     << " type = " << ValueTypeToString(column_info.column_type) << ","
     << " offset = " << column_info.column_offset << ","
     << " fixed length = " << column_info.fixed_length << ","
     << " variable length = " << column_info.variable_length << ","
     << " inlined = " << column_info.is_inlined << std::endl;

  for (auto constraint : column_info.constraints) {
    os << constraint;
  }

  return os;
}

}  // End catalog namespace
}  // End peloton namespace
