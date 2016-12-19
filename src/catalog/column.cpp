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
#include "type/types.h"

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
    case type::Type::VARCHAR:
    case type::Type::VARBINARY:
      break;  // No change of inlined setting

    default:
      is_inlined = true;
      break;
  }
}

const std::string Column::GetInfo() const {
  std::ostringstream os;

  os << " name = " << column_name << ","
     << " type = " << column_type << ","
     << " offset = " << column_offset << ","
     << " fixed length = " << fixed_length << ","
     << " variable length = " << variable_length << ","
     << " inlined = " << is_inlined;

  if (constraints.empty() == false) {
    os << "\n";
  }

  for (auto constraint : constraints) {
    os << constraint;
  }

  return os.str();
}

}  // End catalog namespace
}  // End peloton namespace
