//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// group_expression.cpp
//
// Identification: src/optimizer/group_expression.cpp
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "optimizer/group_expression.h"
#include "optimizer/group.h"

namespace peloton {
namespace optimizer {

//===--------------------------------------------------------------------===//
// Group Expression
//===--------------------------------------------------------------------===//
GroupExpression::GroupExpression(Operator op, std::vector<GroupID> child_groups)
    : group_id(UNDEFINED_GROUP), op(op), child_groups(child_groups) {}

GroupID GroupExpression::GetGroupID() const { return group_id; }

void GroupExpression::SetGroupID(GroupID id) { group_id = id; }

const std::vector<GroupID> &GroupExpression::GetChildGroupIDs() const {
  return child_groups;
}

const GroupID GroupExpression::GetChildGroupId(int child_idx) const {
  return child_groups[child_idx];
}

Operator GroupExpression::Op() const { return op; }

std::shared_ptr<Stats> GroupExpression::GetStats(
    PropertySet requirements) const {
  return std::get<1>(lowest_cost_table_.find(requirements)->second);
}

double GroupExpression::GetCost(std::shared_ptr<PropertySet>& requirements) const {
  return std::get<0>(lowest_cost_table_.find(requirements)->second);
}

std::vector<std::shared_ptr<PropertySet>> GroupExpression::GetInputProperties(
    std::shared_ptr<PropertySet> requirements) const {
  return std::get<1>(lowest_cost_table_.find(requirements)->second);
}

void GroupExpression::SetLocalHashTable(
    const std::shared_ptr<PropertySet> &output_properties,
    const std::vector<std::shared_ptr<PropertySet>> &input_properties_list, double cost) {
  auto it = lowest_cost_table_.find(output_properties);
  if (it == lowest_cost_table_.end()) {
    // No other cost to compare against
    lowest_cost_table_.insert(std::make_pair(
        output_properties, std::make_tuple(cost, input_properties_list)));
  } else {
    // Only insert if the cost is lower than the existing cost
    if (std::get<0>(it->second) > cost) {
      lowest_cost_table_[output_properties] = std::make_tuple(
          cost, input_properties_list);
    }
  }
}

hash_t GroupExpression::Hash() const {
  size_t hash = op.Hash();

  for (size_t i = 0; i < child_groups.size(); ++i) {
    hash = HashUtil::CombineHashes(hash,
                                   HashUtil::Hash<GroupID>(&(child_groups[i])));
  }

  return hash;
}

bool GroupExpression::operator==(const GroupExpression &r) {
  bool eq = (op == r.Op());

  for (size_t i = 0; i < child_groups.size(); ++i) {
    eq = eq && (child_groups[i] == r.child_groups[i]);
  }

  return eq;
}

}  // namespace optimizer
}  // namespace peloton
