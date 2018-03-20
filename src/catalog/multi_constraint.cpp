//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// multi_constraint.cpp
//
// Identification: src/catalog/multi_constraint.cpp
//
// Copyright (c) 2015-2017, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "catalog/multi_constraint.h"
#include "common/internal_types.h"

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

// Serialize this multi-column constraint
void MultiConstraint::SerializeTo(SerializeOutput &out) {
	// multi-column constraint basic information
	out.WriteTextString(constraint_name);
	out.WriteInt((int)constraint_type);
	out.WriteLong(column_ids.size());
	LOG_DEBUG("Multi-column constraint '%s': type %s, %lu column",
			constraint_name.c_str(), ConstraintTypeToString(constraint_type).c_str(),	column_ids.size());

	// multi-column constraint columns
	for (auto column_id : column_ids) {
		out.WriteInt(column_id);
		LOG_DEBUG(" |- Column %d", column_id);
	}
}

// Deserialize this multi-column constraint
MultiConstraint MultiConstraint::DeserializeFrom(SerializeInput &in){
	// multi-column constraint basic information
	std::string constraint_name = in.ReadTextString();
	ConstraintType constraint_type = (ConstraintType)in.ReadInt();
	size_t constraint_column_count = in.ReadLong();

	LOG_DEBUG("Multi-column constraint '%s': type %s, %lu columns",
			constraint_name.c_str(), ConstraintTypeToString(constraint_type).c_str(), constraint_column_count);

	// multi-column constraint columns
	std::vector<oid_t> constraint_columns;
	for (oid_t constraint_column_idx; constraint_column_idx < constraint_column_count; constraint_column_idx++) {
		oid_t column_oid = in.ReadInt();
		LOG_DEBUG(" |- Column %d", column_oid);

		constraint_columns.push_back(column_oid);
	}

	return MultiConstraint(constraint_type, constraint_name, constraint_columns);

}

}  // namespace catalog
}  // namespace peloton
