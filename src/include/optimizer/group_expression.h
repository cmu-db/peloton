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
#include "optimizer/stats.h"
#include "optimizer/util.h"
#include "optimizer/property_set.h"
#include "common/internal_types.h"

#include <map>
#include <tuple>
#include <vector>

namespace peloton {
namespace optimizer {

class Rule;

using GroupID = int32_t;

//===--------------------------------------------------------------------===//
// Group Expression
//===--------------------------------------------------------------------===//
class GroupExpression {
 public:
  GroupExpression(Operator op, std::vector<GroupID> child_groups);

  GroupID GetGroupID() const;

  void SetGroupID(GroupID group_id);

  void SetChildGroupID(int child_group_idx, GroupID group_id);

  const std::vector<GroupID> &GetChildGroupIDs() const;

  GroupID GetChildGroupId(int child_idx) const;

  Operator Op() const;

  double GetCost(std::shared_ptr<PropertySet>& requirements) const;

  std::vector<std::shared_ptr<PropertySet>> GetInputProperties(std::shared_ptr<PropertySet> requirements) const;

  void SetLocalHashTable(const std::shared_ptr<PropertySet> &output_properties,
                         const std::vector<std::shared_ptr<PropertySet>> &input_properties_list,
                         double cost);

  void SetStats(const PropertySet &output_properties,
                std::shared_ptr<Stats> stats);

  hash_t Hash() const;

  bool operator==(const GroupExpression &r);

  void SetRuleExplored(Rule *rule);

  bool HasRuleExplored(Rule *rule);

  inline size_t GetChildrenGroupsSize() const { return child_groups.size(); }

 private:
  GroupID group_id;
  Operator op;
  std::vector<GroupID> child_groups;
  std::bitset<static_cast<uint32_t>(RuleType::NUM_RULES)> rule_mask_;

  // Mapping from output properties to the corresponding best cost, statistics,
  // and child properties
  std::unordered_map<std::shared_ptr<PropertySet>,
                     std::tuple<double, std::vector<std::shared_ptr<PropertySet>>>,
                     PropSetPtrHash, PropSetPtrEq> lowest_cost_table_;
};

}  // namespace optimizer
}  // namespace peloton

namespace std {

template <>
struct hash<peloton::optimizer::GroupExpression> {
  typedef peloton::optimizer::GroupExpression argument_type;
  typedef std::size_t result_type;
  result_type operator()(argument_type const &s) const { return s.Hash(); }
};

}  // namespace std
