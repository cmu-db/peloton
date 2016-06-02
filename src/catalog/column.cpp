//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// column.cpp
//
// Identification: src/backend/catalog/column.cpp
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

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

const std::string Column::GetInfo() const {
  std::ostringstream os;

  os << " name = " << column_name << ","
     << " type = " << ValueTypeToString(column_type) << ","
     << " offset = " << column_offset << ","
     << " fixed length = " << fixed_length << ","
     << " variable length = " << variable_length << ","
     << " inlined = " << is_inlined;

  if(constraints.empty() == false) {
    os << "\n";
  }

  for (auto constraint : constraints) {
    os << constraint;
  }

  return os.str();
}

}  // End catalog namespace
}  // End peloton namespace
