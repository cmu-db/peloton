//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// group_expression.h
//
// Identification: src/optimizer/group_expression.h
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include "optimizer/operator_node.h"
#include "optimizer/stats.h"
#include "optimizer/util.h"

#include <tuple>
#include <vector>
#include <map>

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

  std::shared_ptr<Stats> GetStats() const;

  double GetCost() const;

  void DeriveStatsAndCost(std::vector<std::shared_ptr<Stats>> child_stats,
                          std::vector<double> child_costs);

  hash_t Hash() const;

  bool operator==(const GroupExpression &r);

 private:
  GroupID group_id;
  Operator op;
  std::vector<GroupID> child_groups;

  std::shared_ptr<Stats> stats;
  double cost;
};

} /* namespace optimizer */
} /* namespace peloton */

namespace std {

template<> struct hash<peloton::optimizer::GroupExpression> {
  typedef peloton::optimizer::GroupExpression argument_type;
  typedef std::size_t result_type;
  result_type operator()(argument_type const& s) const {
    return s.Hash();
  }
};

} /* namespace std */
