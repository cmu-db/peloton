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
void Constraint::SerializeTo(SerializeOutput &out) const {
  out.WriteTextString(constraint_name_);
  out.WriteInt((int)constraint_type_);
  out.WriteInt(fk_list_offset_);
  out.WriteInt(unique_index_list_offset_);

  if (constraint_type_ == ConstraintType::DEFAULT) {
    default_value_->SerializeTo(out);
  }

  if (constraint_type_ == ConstraintType::CHECK) {
    out.WriteInt((int)exp_.first);
    exp_.second.SerializeTo(out);
  }
}

// Deserialize this constraint
Constraint Constraint::DeserializeFrom(SerializeInput &in,
                                       const type::TypeId column_type) {
  std::string constraint_name = in.ReadTextString();
  ConstraintType constraint_type = (ConstraintType)in.ReadInt();
  oid_t foreign_key_list_offset = in.ReadInt();
  oid_t unique_index_offset = in.ReadInt();

  auto column_constraint = Constraint(constraint_type, constraint_name);
  column_constraint.SetForeignKeyListOffset(foreign_key_list_offset);
  column_constraint.SetUniqueIndexOffset(unique_index_offset);

  if (constraint_type == ConstraintType::DEFAULT) {
    type::Value default_value = type::Value::DeserializeFrom(in, column_type);
    column_constraint.addDefaultValue(default_value);
  }

  if (constraint_type == ConstraintType::CHECK) {
    auto exp = column_constraint.GetCheckExpression();
    ExpressionType exp_type = (ExpressionType)in.ReadInt();
    type::Value exp_value = type::Value::DeserializeFrom(in, column_type);
    column_constraint.AddCheck(exp_type, exp_value);
  }
  return column_constraint;
}

const std::string Constraint::GetInfo() const {
  std::ostringstream os;
  os << "Constraint[" << GetName() << ", "
     << ConstraintTypeToString(constraint_type_);

  if (GetType() == ConstraintType::CHECK) {
    os << ", " << exp_.first << " " << exp_.second.GetInfo();
  }
  if (GetType() == ConstraintType::DEFAULT) {
    os << ", " << default_value_->GetInfo();
  }
  os << "]";
  return os.str();
}

}  // namespace catalog
}  // namespace peloton
