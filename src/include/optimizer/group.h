//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// group.h
//
// Identification: src/include/optimizer/group.h
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//


#pragma once

#include "optimizer/operator_node.h"
#include "optimizer/property.h"
#include "optimizer/group_expression.h"

#include <vector>
#include <unordered_map>

namespace peloton {
namespace optimizer {

using GroupID = int32_t;

const GroupID UNDEFINED_GROUP = -1;

//===--------------------------------------------------------------------===//
// Group
//===--------------------------------------------------------------------===//
class Group {
 public:
  Group(GroupID id);
  void add_item(Operator op);

  void AddExpression(std::shared_ptr<GroupExpression> expr);

  void SetExpressionCost(std::shared_ptr<GroupExpression> expr, double cost,
                         PropertySet properties);

  std::shared_ptr<GroupExpression> GetBestExpression(PropertySet properties);

  const std::vector<std::shared_ptr<GroupExpression>> &GetExpressions() const;

 private:
  GroupID id;
  std::vector<Operator> items;
  std::vector<std::shared_ptr<GroupExpression>> expressions;
  std::unordered_map<PropertySet,
                     std::tuple<double, std::shared_ptr<GroupExpression>>>
      lowest_cost_expressions;
};

} /* namespace optimizer */
} /* namespace peloton */
