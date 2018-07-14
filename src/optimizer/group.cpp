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

#include "common/logger.h"

namespace peloton {
namespace optimizer {

//===--------------------------------------------------------------------===//
// Group
//===--------------------------------------------------------------------===//
Group::Group(GroupID id, std::unordered_set<std::string> table_aliases)
    : id_(id), table_aliases_(std::move(table_aliases)) {
  has_explored_ = false;
}

void Group::AddExpression(std::shared_ptr<GroupExpression> expr,
                          bool enforced) {
  // Do duplicate detection
  expr->SetGroupID(id_);
  if (enforced)
    enforced_exprs_.push_back(expr);
  else if (expr->Op().IsPhysical())
    physical_expressions_.push_back(expr);
  else
    logical_expressions_.push_back(expr);
}

bool Group::SetExpressionCost(GroupExpression *expr, double cost,
                              std::shared_ptr<PropertySet> &properties) {
  LOG_TRACE("Adding expression cost on group %d with op %s, req %s",
            expr->GetGroupID(), expr->Op().GetName().c_str(),
            properties->ToString().c_str());
  auto it = lowest_cost_expressions_.find(properties);
  if (it == lowest_cost_expressions_.end() || std::get<0>(it->second) > cost) {
    // No other cost to compare against or the cost is lower than the existing
    // cost
    lowest_cost_expressions_[properties] = std::make_tuple(cost, expr);
    return true;
  }
  return false;
}
GroupExpression *Group::GetBestExpression(
    std::shared_ptr<PropertySet> &properties) {
  auto it = lowest_cost_expressions_.find(properties);
  if (it != lowest_cost_expressions_.end()) {
    return std::get<1>(it->second);
  }
  LOG_TRACE("Didn't get best expression with properties %s",
            properties->ToString().c_str());
  return nullptr;
}

bool Group::HasExpressions(
    const std::shared_ptr<PropertySet> &properties) const {
  const auto &it = lowest_cost_expressions_.find(properties);
  return (it != lowest_cost_expressions_.end());
}

std::shared_ptr<ColumnStats> Group::GetStats(std::string column_name) {
  if (!stats_.count(column_name)) {
    return nullptr;
  }
  return stats_[column_name];
}

const std::string Group::GetInfo(int num_indent) const {
    std::ostringstream os;
    os << StringUtil::Indent(num_indent)
       << "GroupID: " << GetID() << std::endl;

    if (logical_expressions_.size() > 0)
        os << StringUtil::Indent(num_indent + 2)
           << "logical_expressions_: \n";
    
    for (auto expr : logical_expressions_) {
        os << StringUtil::Indent(num_indent + 4)
           << expr->Op().GetName() << std::endl;
        const std::vector<GroupID> ChildGroupIDs = expr->GetChildGroupIDs();
        if (ChildGroupIDs.size() > 0) {        
            os << StringUtil::Indent(num_indent + 6)
               << "ChildGroupIDs: ";
            for (auto childGroupID : ChildGroupIDs)
                os << childGroupID << " ";
            os << std::endl;
        }
    }

    if (physical_expressions_.size() > 0)
        os << StringUtil::Indent(num_indent + 2)
           << "physical_expressions_: \n";
    for (auto expr : physical_expressions_) {
        os << StringUtil::Indent(num_indent + 4)
           << expr->Op().GetName() << std::endl;
        const std::vector<GroupID> ChildGroupIDs = expr->GetChildGroupIDs();
        if (ChildGroupIDs.size() > 0) {
            os << StringUtil::Indent(num_indent + 6)
               << "ChildGroupIDs: ";
            for (auto childGroupID : ChildGroupIDs) 
                os << childGroupID << " ";
            os << std::endl;
        }

    }

    if (enforced_exprs_.size() > 0)
        os << StringUtil::Indent(num_indent + 2)
           << "enforced_exprs_: \n";
    for (auto expr : enforced_exprs_) {
        os << StringUtil::Indent(num_indent + 4)
           << expr->Op().GetName() << std::endl;
        const std::vector<GroupID> ChildGroupIDs = expr->GetChildGroupIDs();
        if (ChildGroupIDs.size() > 0) {
            os << StringUtil::Indent(num_indent + 6)
               << "ChildGroupIDs: \n";
            for (auto childGroupID : ChildGroupIDs) {
                os << childGroupID << " ";
            }
            os << std::endl;
        }
    }

    return os.str();
}

const std::string Group::GetInfo() const {
    std::ostringstream os;
    os << GetInfo(0);
    return os.str();
}


void Group::AddStats(std::string column_name,
                     std::shared_ptr<ColumnStats> stats) {
  PELOTON_ASSERT((size_t)GetNumRows() == stats->num_rows);
  stats_[column_name] = stats;
}

bool Group::HasColumnStats(std::string column_name) {
  return stats_.count(column_name);
}

}  // namespace optimizer
}  // namespace peloton
