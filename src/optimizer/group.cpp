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
Group::Group(GroupID id, std::unordered_set<std::string> table_aliases)
    : id_(id), table_aliases_(std::move(table_aliases)) {
  has_explored_ = false;
  has_implemented_ = false;
}
void Group::add_item(Operator op) {
  // TODO(abpoms): do duplicate checking
  items_.push_back(op);
}
void Group::AddExpression(std::shared_ptr<GroupExpression> expr,
                          bool enforced) {
  // Do duplicate detection
  expr->SetGroupID(id_);
  if (enforced)
    enforced_exprs_.push_back(expr);
  else
    expressions_.push_back(expr);
}

void Group::SetExpressionCost(std::shared_ptr<GroupExpression> expr,
                              double cost, PropertySet properties) {
  LOG_TRACE("Adding expression cost on group %d with op %s", expr->GetGroupID(),
            expr->Op().name().c_str());
  auto it = lowest_cost_expressions_.find(properties);
  if (it == lowest_cost_expressions_.end()) {
    // No other cost to compare against
    lowest_cost_expressions_.insert(
        std::make_pair(properties, std::make_tuple(cost, expr)));
  } else {
    // Only insert if the cost is lower than the existing cost
    if (std::get<0>(it->second) > cost) {
      lowest_cost_expressions_[properties] = std::make_tuple(cost, expr);
    }
  }
}

std::shared_ptr<GroupExpression> Group::GetBestExpression(
    PropertySet properties) {
  auto it = lowest_cost_expressions_.find(properties);
  if (it != lowest_cost_expressions_.end()) {
    return std::get<1>(it->second);
  }
  LOG_TRACE("Didn't get best expression with required properties!");
  return nullptr;
}

const std::vector<std::shared_ptr<GroupExpression>> &Group::GetExpressions()
    const {
  return expressions_;
}

} /* namespace optimizer */
} /* namespace peloton */
