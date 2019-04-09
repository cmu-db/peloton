//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// group_expression.h
//
// Identification: src/include/optimizer/group_expression.h
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include "optimizer/operator_node.h"
#include "optimizer/stats/stats.h"
#include "optimizer/util.h"
#include "optimizer/property_set.h"
#include "common/internal_types.h"

#include <map>
#include <tuple>
#include <vector>

namespace peloton {
namespace optimizer {

template <class Node, class OperatorType, class OperatorExpr>
class Rule;

using GroupID = int32_t;

//===--------------------------------------------------------------------===//
// Group Expression
//===--------------------------------------------------------------------===//
template <class Node, class OperatorType, class OperatorExpr>
class GroupExpression {
 public:
  GroupExpression(Node op, std::vector<GroupID> child_groups);

  GroupID GetGroupID() const;

  void SetGroupID(GroupID group_id);

  void SetChildGroupID(int child_group_idx, GroupID group_id);

  const std::vector<GroupID> &GetChildGroupIDs() const;

  GroupID GetChildGroupId(int child_idx) const;

  Node Op() const;

  double GetCost(std::shared_ptr<PropertySet>& requirements) const;

  std::vector<std::shared_ptr<PropertySet>> GetInputProperties(std::shared_ptr<PropertySet> requirements) const;

  void SetLocalHashTable(const std::shared_ptr<PropertySet> &output_properties,
                         const std::vector<std::shared_ptr<PropertySet>> &input_properties_list,
                         double cost);

  void SetStats(const PropertySet &output_properties,
                std::shared_ptr<Stats> stats);

  hash_t Hash() const;

  bool operator==(const GroupExpression<Node, OperatorType, OperatorExpr> &r);

  void SetRuleExplored(Rule<Node,OperatorType,OperatorExpr> *rule);

  bool HasRuleExplored(Rule<Node,OperatorType,OperatorExpr> *rule);

  void SetDerivedStats() { stats_derived_ = true; }

  bool HasDerivedStats() { return stats_derived_;}

  inline size_t GetChildrenGroupsSize() const { return child_groups.size(); }

 private:
  GroupID group_id;
  Node op;
  std::vector<GroupID> child_groups;
  std::bitset<static_cast<uint32_t>(RuleType::NUM_RULES)> rule_mask_;
  bool stats_derived_;

  // Mapping from output properties to the corresponding best cost, statistics,
  // and child properties
  std::unordered_map<std::shared_ptr<PropertySet>,
                     std::tuple<double, std::vector<std::shared_ptr<PropertySet>>>,
                     PropSetPtrHash, PropSetPtrEq> lowest_cost_table_;
};

}  // namespace optimizer
}  // namespace peloton

namespace std {

template <class Node, class OperatorType, class OperatorExpr>
struct hash<peloton::optimizer::GroupExpression<Node,OperatorType,OperatorExpr>> {
  typedef peloton::optimizer::GroupExpression<Node,OperatorType,OperatorExpr> argument_type;
  typedef std::size_t result_type;
  result_type operator()(argument_type const &s) const { return s.Hash(); }
};

}  // namespace std
