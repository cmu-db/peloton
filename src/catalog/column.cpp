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

// Serialize this column
void Column::SerializeTo(SerializeOutput &out) {
	// Column basic information
	out.WriteTextString(column_name);
	out.WriteInt((int)column_type);
	out.WriteInt(GetLength());
	out.WriteInt(column_offset);
	out.WriteBool(is_inlined);
	LOG_DEBUG("|- Column '%s %s': length %lu, offset %d, Inline %d",
			column_name.c_str(), TypeIdToString(column_type).c_str(),
			GetLength(), column_offset, is_inlined);

	// Column constraints
	out.WriteLong(constraints.size());
	for (auto constraint : constraints) {
		constraint.SerializeTo(out);
	}
}

// Deserialize this column
Column Column::DeserializeFrom(SerializeInput &in) {
	// read basic column information
	std::string column_name = in.ReadTextString();
	type::TypeId column_type = (type::TypeId)in.ReadInt();
	size_t column_length = in.ReadInt();
	oid_t column_offset = in.ReadInt();
	bool is_inlined = in.ReadBool();

	auto column = catalog::Column(column_type, column_length, column_name, is_inlined, column_offset);
	LOG_DEBUG("Create Column '%s %s' : %lu bytes, offset %d, inlined %d",
			column_name.c_str(), TypeIdToString(column_type).c_str(), column_length, column_offset, is_inlined);

	// recover column constraints
	size_t column_constraint_count = in.ReadLong();
	for (oid_t constraint_idx = 0; constraint_idx < column_constraint_count; constraint_idx++) {
		auto column_constraint = Constraint::DeserializeFrom(in, column_type);
		column.AddConstraint(column_constraint);
	}

	return column;
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
