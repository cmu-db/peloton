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

// Serialize this constraint
void Constraint::SerializeTo(SerializeOutput &out) {
	out.WriteTextString(constraint_name);
	out.WriteInt((int)constraint_type);
	out.WriteInt(fk_list_offset);
	out.WriteInt(unique_index_list_offset);
	LOG_DEBUG("|  |- Column constraint '%s %s': FK offset %d, Unique offset %d",
			constraint_name.c_str(), ConstraintTypeToString(constraint_type).c_str(),
			fk_list_offset, unique_index_list_offset);

	if (constraint_type == ConstraintType::DEFAULT) {
		default_value->SerializeTo(out);
		LOG_DEBUG("|  |     Default value %s", default_value->ToString().c_str());
	}

	if (constraint_type == ConstraintType::CHECK) {
		out.WriteInt((int)exp.first);
		exp.second.SerializeTo(out);
		LOG_DEBUG("|  |     Check expression %s %s",
				ExpressionTypeToString(exp.first).c_str(), exp.second.ToString().c_str());
	}
}

// Deserialize this constraint
Constraint Constraint::DeserializeFrom(SerializeInput &in, const type::TypeId column_type) {
	std::string constraint_name = in.ReadTextString();
	ConstraintType constraint_type = (ConstraintType)in.ReadInt();
	oid_t foreign_key_list_offset = in.ReadInt();
	oid_t unique_index_offset = in.ReadInt();

	auto column_constraint = Constraint(constraint_type, constraint_name);
	column_constraint.SetForeignKeyListOffset(foreign_key_list_offset);
	column_constraint.SetUniqueIndexOffset(unique_index_offset);
	LOG_DEBUG("|- Column constraint '%s %s': Foreign key list offset %d, Unique index offset %d",
			constraint_name.c_str(), ConstraintTypeToString(constraint_type).c_str(),
			foreign_key_list_offset, unique_index_offset);

	if (constraint_type == ConstraintType::DEFAULT) {
		type::Value default_value = type::Value::DeserializeFrom(in, column_type);
		column_constraint.addDefaultValue(default_value);
		LOG_DEBUG("|     Default value %s", default_value.ToString().c_str());
	}

	if (constraint_type == ConstraintType::CHECK) {
		auto exp = column_constraint.GetCheckExpression();
		ExpressionType exp_type = (ExpressionType)in.ReadInt();
		type::Value exp_value = type::Value::DeserializeFrom(in, column_type);
		column_constraint.AddCheck(exp_type, exp_value);
		LOG_DEBUG("|     Check expression %s %s",
				ExpressionTypeToString(exp_type).c_str(), exp_value.ToString().c_str());
	}
	return column_constraint;
}


const std::string Constraint::GetInfo() const {
  std::ostringstream os;
  os << "Constraint[" << GetName() << ", "
     << ConstraintTypeToString(constraint_type);

  if (GetType() == ConstraintType::CHECK) {
    os << ", " << exp.first << " " << exp.second.GetInfo();
  }
  os << "]";
  return os.str();
}

}  // namespace catalog
}  // namespace peloton
