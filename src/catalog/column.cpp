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
  if (is_inlined_) {
    fixed_length_ = column_length;
    variable_length_ = 0;
  } else {
    fixed_length_ = sizeof(uintptr_t);
    variable_length_ = column_length;
  }
}

void Column::SetInlined() {
  switch (column_type_) {
    case type::TypeId::VARCHAR:
    case type::TypeId::VARBINARY:
      break;  // No change of inlined setting

    default:
      is_inlined_ = true;
      break;
  }
}

const std::string Column::GetInfo() const {
  std::ostringstream os;

  os << "Column[" << column_name_ << ", "
     << TypeIdToString(column_type_) << ", "

     << "Offset:" << column_offset_ << ", ";

  if (is_inlined_) {
    os << "FixedLength:" << fixed_length_;
  } else {
    os << "VarLength:" << variable_length_;
  }

  if (is_not_null_ && has_default_) {
    os << ", {NOT NULL, DEFAULT:"
       << default_value_->ToString() << "}";
  } else if (is_not_null_) {
    os << ", {NOT NULL}";
  } else if (has_default_) {
    os << ", {DEFAULT:"
       << default_value_->ToString() << "}";

  }

  os << "]";

  return (os.str());
}

}  // namespace catalog
}  // namespace peloton
