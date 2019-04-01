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

namespace peloton {
namespace optimizer {

//===--------------------------------------------------------------------===//
// Memo
//===--------------------------------------------------------------------===//
template <class Node, class OperatorType, class OperatorExpr>
Memo<Node,OperatorType,OperatorExpr>::Memo() {}

//===--------------------------------------------------------------------===//
// Memo::AddNewGroup (declare here to prevent specialization error)
//===--------------------------------------------------------------------===//
template <class Node, class OperatorType, class OperatorExpr>
GroupID Memo<Node,OperatorType,OperatorExpr>::AddNewGroup(std::shared_ptr<GroupExpression<Node,OperatorType,OperatorExpr>> gexpr) {
  (void)gexpr;

  GroupID new_group_id = groups_.size();
  // Find out the table alias that this group represents
  std::unordered_set<std::string> table_aliases;

  groups_.emplace_back(
      new Group<Node,OperatorType,OperatorExpr>(new_group_id, std::move(table_aliases)));
  return new_group_id;
}

template <>
GroupID Memo<Operator,OpType,OperatorExpression>::AddNewGroup(std::shared_ptr<GroupExpression<Operator,OpType,OperatorExpression>> gexpr) {
  GroupID new_group_id = groups_.size();
  // Find out the table alias that this group represents
  std::unordered_set<std::string> table_aliases;
  auto op_type = gexpr->Op().GetType();
  if (op_type == OpType::Get) {
    // For base group, the table alias can get directly from logical get
    const LogicalGet *logical_get = gexpr->Op().As<LogicalGet>();
    table_aliases.insert(logical_get->table_alias);
  } else if (op_type == OpType::LogicalQueryDerivedGet) {
    const LogicalQueryDerivedGet *query_get =
        gexpr->Op().As<LogicalQueryDerivedGet>();
    table_aliases.insert(query_get->table_alias);
  } else {
    // For other groups, need to aggregate the table alias from children
    for (auto child_group_id : gexpr->GetChildGroupIDs()) {
      Group<Operator,OpType,OperatorExpression> *child_group = GetGroupByID(child_group_id);
      for (auto &table_alias : child_group->GetTableAliases()) {
        table_aliases.insert(table_alias);
      }
    }
  }

  groups_.emplace_back(
      new Group<Operator,OpType,OperatorExpression>(new_group_id, std::move(table_aliases)));
  return new_group_id;
}

//===--------------------------------------------------------------------===//
// Memo remaining interface functions
//===--------------------------------------------------------------------===//
template <class Node, class OperatorType, class OperatorExpr>
GroupExpression<Node,OperatorType,OperatorExpr> *Memo<Node,OperatorType,OperatorExpr>::InsertExpression(
  std::shared_ptr<GroupExpression<Node,OperatorType,OperatorExpr>> gexpr,
  bool enforced) {

  return InsertExpression(gexpr, UNDEFINED_GROUP, enforced);
}

template <class Node, class OperatorType, class OperatorExpr>
GroupExpression<Node,OperatorType,OperatorExpr> *Memo<Node,OperatorType,OperatorExpr>::InsertExpression(
  std::shared_ptr<GroupExpression<Node,OperatorType,OperatorExpr>> gexpr,
  GroupID target_group,
  bool enforced) {

  //(TODO): refactor this with the specialization
  auto it = group_expressions_.find(gexpr.get());
  std::cout << "group_expressions_.size(): " << group_expressions_.size() << "\n";
  std::cout << "InsertExpression (" << gexpr << "," << target_group << ")\n";

  if (it != group_expressions_.end()) {
    gexpr->SetGroupID((*it)->GetGroupID());
    std::cout << "Same Group discovered..\n";
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

    Group<Node,OperatorType,OperatorExpr> *group = GetGroupByID(group_id);
    group->AddExpression(gexpr, enforced);

    std::cout << "Inserted into new group...size(): " << group_expressions_.size() << "\n";
    return gexpr.get();
  }
}

// Specialization for Memo::InsertExpression due to OpType
template <>
GroupExpression<Operator,OpType,OperatorExpression> *Memo<Operator,OpType,OperatorExpression>::InsertExpression(
  std::shared_ptr<GroupExpression<Operator,OpType,OperatorExpression>> gexpr,
  GroupID target_group,
  bool enforced) {
  // If leaf, then just return
  if (gexpr->Op().GetType() == OpType::Leaf) {
    const LeafOperator *leaf = gexpr->Op().As<LeafOperator>();
    PELOTON_ASSERT(target_group == UNDEFINED_GROUP ||
           target_group == leaf->origin_group);
    gexpr->SetGroupID(leaf->origin_group);
    return nullptr;
  }

  // Lookup in hash table
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
    Group<Operator,OpType,OperatorExpression> *group = GetGroupByID(group_id);
    group->AddExpression(gexpr, enforced);
    return gexpr.get();
  }
}

template <class Node, class OperatorType, class OperatorExpr>
std::vector<std::unique_ptr<Group<Node,OperatorType,OperatorExpr>>> &Memo<Node,OperatorType,OperatorExpr>::Groups() {
  return groups_;
}

template <class Node, class OperatorType, class OperatorExpr>
Group<Node,OperatorType,OperatorExpr> *Memo<Node,OperatorType,OperatorExpr>::GetGroupByID(GroupID id) { return groups_[id].get(); }

template <class Node, class OperatorType, class OperatorExpr>
const std::string Memo<Node,OperatorType,OperatorExpr>::GetInfo(int num_indent) const {
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

template <class Node, class OperatorType, class OperatorExpr>
const std::string Memo<Node,OperatorType,OperatorExpr>::GetInfo() const {
    std::ostringstream os;
    os << GetInfo(0);
    return os.str();
}

// Explicitly instantiate template
template class Memo<Operator,OpType,OperatorExpression>;
template class Memo<AbsExpr_Container,ExpressionType,AbsExpr_Expression>;

}  // namespace optimizer
}  // namespace peloton
