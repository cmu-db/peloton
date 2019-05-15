//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// memo.cpp
//
// Identification: src/optimizer/memo.cpp
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "optimizer/group_expression.h"
#include "optimizer/memo.h"
#include "optimizer/operators.h"
#include "optimizer/stats/stats_calculator.h"
#include "optimizer/absexpr_expression.h"
#include "expression/group_marker_expression.h"

namespace peloton {
namespace optimizer {

//===--------------------------------------------------------------------===//
// Memo
//===--------------------------------------------------------------------===//
Memo::Memo() {}

//===--------------------------------------------------------------------===//
// Memo remaining interface functions
//===--------------------------------------------------------------------===//
GroupExpression *Memo::InsertExpression(std::shared_ptr<GroupExpression> gexpr,
                                        GroupID target_group, bool enforced) {

  // If leaf, then just return
  if (gexpr->Node()->GetOpType() == OpType::Leaf &&
      gexpr->Node()->GetExpType() == ExpressionType::INVALID) {
    const LeafOperator *leaf = gexpr->Node()->As<LeafOperator>();
    PELOTON_ASSERT(target_group == UNDEFINED_GROUP ||
           target_group == leaf->origin_group);
    gexpr->SetGroupID(leaf->origin_group);
    return nullptr;
  }

  if (gexpr->Node()->GetOpType() == OpType::Undefined &&
      gexpr->Node()->GetExpType() == ExpressionType::GROUP_MARKER) {

    auto abs_node = std::dynamic_pointer_cast<AbsExprNode>(gexpr->Node());
    PELOTON_ASSERT(abs_node != nullptr);

    auto gm_expr = std::dynamic_pointer_cast<expression::GroupMarkerExpression>(abs_node->GetExpr());
    PELOTON_ASSERT(gm_expr != nullptr);
    PELOTON_ASSERT(target_group == UNDEFINED_GROUP || target_group == gm_expr->GetGroupID());
    gexpr->SetGroupID(gm_expr->GetGroupID());
    return nullptr;
  }

  auto it = group_expressions_.find(gexpr.get());
  if (it != group_expressions_.end()) {
    gexpr->SetGroupID((*it)->GetGroupID());
    return *it;
  } else {
    group_expressions_.insert(gexpr.get());
    // New expression, so try to insert into an existing group or
    // create a new group if none specified
    GroupID group_id;
    if (target_group == UNDEFINED_GROUP) {
      group_id = AddNewGroup(gexpr);
    } else {
      group_id = target_group;
    }

    Group *group = GetGroupByID(group_id);
    group->AddExpression(gexpr, enforced);
    return gexpr.get();
  }
}

GroupExpression *Memo::InsertExpression(std::shared_ptr<GroupExpression> gexpr,
                                        bool enforced) {

  return InsertExpression(gexpr, UNDEFINED_GROUP, enforced);
}

std::vector<std::unique_ptr<Group>> &Memo::Groups() {
  return groups_;
}

Group *Memo::GetGroupByID(GroupID id) { return groups_[id].get(); }

const std::string Memo::GetInfo(int num_indent) const {
    std::ostringstream os;
    os << StringUtil::Indent(num_indent) << "Memo::\n";
    os << StringUtil::Indent(num_indent + 1) 
       << "rule_set_size_: " << rule_set_size_ << std::endl;
    
    for (auto &group : groups_) {
        auto groupInfo = group->GetInfo(num_indent + 2);
        os << groupInfo;
    }
    return os.str();
}

const std::string Memo::GetInfo() const {
    std::ostringstream os;
    os << GetInfo(0);
    return os.str();
}

//===--------------------------------------------------------------------===//
// Memo::AddNewGroup
//===--------------------------------------------------------------------===//
GroupID Memo::AddNewGroup(std::shared_ptr<GroupExpression> gexpr) {
  GroupID new_group_id = groups_.size();
  // Find out the table alias that this group represents
  std::unordered_set<std::string> table_aliases;
  auto op_type = gexpr->Node()->GetOpType();

  // TODO(ncx): specialize (if not OpType, then just add new group)
  if (op_type == OpType::Get) {
    // For base group, the table alias can get directly from logical get
    const LogicalGet *logical_get = gexpr->Node()->As<LogicalGet>();
    table_aliases.insert(logical_get->table_alias);
  } else if (op_type == OpType::LogicalQueryDerivedGet) {
    const LogicalQueryDerivedGet *query_get =
        gexpr->Node()->As<LogicalQueryDerivedGet>();
    table_aliases.insert(query_get->table_alias);
  } else {
    // For other groups, need to aggregate the table alias from children
    for (auto child_group_id : gexpr->GetChildGroupIDs()) {
      Group *child_group = GetGroupByID(child_group_id);
      for (auto &table_alias : child_group->GetTableAliases()) {
        table_aliases.insert(table_alias);
      }
    }
  }

  groups_.emplace_back(new Group(new_group_id, std::move(table_aliases)));
  return new_group_id;
}

}  // namespace optimizer
}  // namespace peloton
