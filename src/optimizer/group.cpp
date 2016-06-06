//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// group.cpp
//
// Identification: src/optimizer/group.cpp
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "optimizer/group.h"

namespace peloton {
namespace optimizer {

//===--------------------------------------------------------------------===//
// Group
//===--------------------------------------------------------------------===//
Group::Group(GroupID id) : id(id) {}

void Group::AddExpression(std::shared_ptr<GroupExpression> expr) {
  // Do duplicate detection
  expr->SetGroupID(id);
  expressions.push_back(expr);
}

void Group::SetExpressionCost(std::shared_ptr<GroupExpression> expr,
                              double cost, PropertySet properties) {
  auto it = lowest_cost_expressions.find(properties);
  if (it == lowest_cost_expressions.end()) {
    // No other cost to compare against
    lowest_cost_expressions.insert(
        std::make_pair(properties, std::make_tuple(cost, expr)));
  } else {
    // Only insert if the cost is lower than the existing cost
    if (std::get<0>(it->second) > cost) {
      lowest_cost_expressions[properties] = std::make_tuple(cost, expr);
    }
  }
}

std::shared_ptr<GroupExpression> Group::GetBestExpression(
    PropertySet properties) {
  auto it = lowest_cost_expressions.find(properties);
  if (it != lowest_cost_expressions.end()) {
    return std::get<1>(it->second);
  }
  return nullptr;
}

const std::vector<std::shared_ptr<GroupExpression>> &Group::GetExpressions()
    const {
  return expressions;
}

} /* namespace optimizer */
} /* namespace peloton */
