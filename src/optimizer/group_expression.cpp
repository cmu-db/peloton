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

#include "common/internal_types.h"
#include "optimizer/group_expression.h"
#include "optimizer/group.h"
#include "optimizer/rule.h"

namespace peloton {
namespace optimizer {

//===--------------------------------------------------------------------===//
// Group Expression
//===--------------------------------------------------------------------===//
template <class Node, class OperatorType, class OperatorExpr>
GroupExpression<Node,OperatorType,OperatorExpr>::GroupExpression(Node op, std::vector<GroupID> child_groups)
    : group_id(UNDEFINED_GROUP),
      op(op),
      child_groups(child_groups),
      stats_derived_(false) {}

template <class Node, class OperatorType, class OperatorExpr>
GroupID GroupExpression<Node,OperatorType,OperatorExpr>::GetGroupID() const { return group_id; }

template <class Node, class OperatorType, class OperatorExpr>
void GroupExpression<Node,OperatorType,OperatorExpr>::SetGroupID(GroupID id) { group_id = id; }

template <class Node, class OperatorType, class OperatorExpr>
void GroupExpression<Node,OperatorType,OperatorExpr>::SetChildGroupID(int child_group_idx, GroupID group_id) {
  child_groups[child_group_idx] = group_id;
}

template <class Node, class OperatorType, class OperatorExpr>
const std::vector<GroupID> &GroupExpression<Node,OperatorType,OperatorExpr>::GetChildGroupIDs() const {
  return child_groups;
}

template <class Node, class OperatorType, class OperatorExpr>
GroupID GroupExpression<Node,OperatorType,OperatorExpr>::GetChildGroupId(int child_idx) const {
  return child_groups[child_idx];
}

template <class Node, class OperatorType, class OperatorExpr>
Node GroupExpression<Node,OperatorType,OperatorExpr>::Op() const { return op; }

template <class Node, class OperatorType, class OperatorExpr>
double GroupExpression<Node,OperatorType,OperatorExpr>::GetCost(
    std::shared_ptr<PropertySet> &requirements) const {
  return std::get<0>(lowest_cost_table_.find(requirements)->second);
}

template <class Node, class OperatorType, class OperatorExpr>
std::vector<std::shared_ptr<PropertySet>> GroupExpression<Node,OperatorType,OperatorExpr>::GetInputProperties(
    std::shared_ptr<PropertySet> requirements) const {
  return std::get<1>(lowest_cost_table_.find(requirements)->second);
}

template <class Node, class OperatorType, class OperatorExpr>
void GroupExpression<Node,OperatorType,OperatorExpr>::SetLocalHashTable(
    const std::shared_ptr<PropertySet> &output_properties,
    const std::vector<std::shared_ptr<PropertySet>> &input_properties_list,
    double cost) {
  auto it = lowest_cost_table_.find(output_properties);
  if (it == lowest_cost_table_.end()) {
    // No other cost to compare against
    lowest_cost_table_.insert(std::make_pair(
        output_properties, std::make_tuple(cost, input_properties_list)));
  } else {
    // Only insert if the cost is lower than the existing cost
    if (std::get<0>(it->second) > cost) {
      lowest_cost_table_[output_properties] =
          std::make_tuple(cost, input_properties_list);
    }
  }
}

template <class Node, class OperatorType, class OperatorExpr>
hash_t GroupExpression<Node,OperatorType,OperatorExpr>::Hash() const {
  size_t hash = op.Hash();

  for (size_t i = 0; i < child_groups.size(); ++i) {
    hash = HashUtil::CombineHashes(hash,
                                   HashUtil::Hash<GroupID>(&(child_groups[i])));
  }

  return hash;
}

template <class Node, class OperatorType, class OperatorExpr>
bool GroupExpression<Node,OperatorType,OperatorExpr>::operator==(const GroupExpression<Node,OperatorType,OperatorExpr> &r) {
  return (op == r.Op()) && (child_groups == r.child_groups);
}

template <class Node, class OperatorType, class OperatorExpr>
void GroupExpression<Node,OperatorType,OperatorExpr>::SetRuleExplored(Rule<Node,OperatorType,OperatorExpr> *rule) {
  rule_mask_.set(rule->GetRuleIdx(), true);
}

template <class Node, class OperatorType, class OperatorExpr>
bool GroupExpression<Node,OperatorType,OperatorExpr>::HasRuleExplored(Rule<Node,OperatorType,OperatorExpr> *rule) {
  return rule_mask_.test(rule->GetRuleIdx());
}

// Explicitly instantiate to prevent linker errors
template class GroupExpression<Operator,OpType,OperatorExpression>;

}  // namespace optimizer
}  // namespace peloton
