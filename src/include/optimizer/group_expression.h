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
#include "optimizer/rule.h"

#include <map>
#include <tuple>
#include <vector>

namespace peloton {
namespace optimizer {

using GroupID = int32_t;

//===--------------------------------------------------------------------===//
// Group Expression
//===--------------------------------------------------------------------===//
class GroupExpression {
 public:
  GroupExpression(Operator op, std::vector<GroupID> child_groups);

  GroupID GetGroupID() const;

  void SetGroupID(GroupID group_id);

  const std::vector<GroupID> &GetChildGroupIDs() const;

  Operator Op() const;

  std::shared_ptr<Stats> GetStats(PropertySet requirements) const;

  double GetCost(PropertySet requirements) const;

  std::vector<PropertySet> GetInputProperties(PropertySet requirements) const;

  void SetLocalHashTable(const PropertySet &output_properties,
                         const std::vector<PropertySet> &input_properties_list,
                         double cost, std::shared_ptr<Stats> stats);

  void SetStats(const PropertySet &output_properties,
                std::shared_ptr<Stats> stats);

  hash_t Hash() const;

  bool operator==(const GroupExpression &r);

  void ResetRuleMask(size_t num_rules) {
    rule_mask_.reset(new std::vector<bool>(num_rules, false));
  }

  void SetRuleExplored(Rule *rule) {
    (*rule_mask_.get())[rule->GetRuleIdx()] = true;
  }

  bool HasRuleExplored(Rule *rule) {
    return rule_mask_->at(rule->GetRuleIdx());
  }

  inline size_t GetChildrenGroupsSize() const { return child_groups.size(); }

 private:
  GroupID group_id;
  Operator op;
  std::vector<GroupID> child_groups;
  std::unique_ptr<std::vector<bool>> rule_mask_;

  // Mapping from output properties to the corresponding best cost, statistics,
  // and child properties
  std::unordered_map<PropertySet, std::tuple<double, std::shared_ptr<Stats>,
                                             std::vector<PropertySet>>>
      lowest_cost_table_;
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
